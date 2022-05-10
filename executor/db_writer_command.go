package executor

import (
	"context"
	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
	"go.mongodb.org/mongo-driver/bson"

	LOG "github.com/vinllen/log4go"
)

// use run_command to execute command
type CommandWriter struct {
	// mongo connection
	conn *utils.MongoCommunityConn
	// init sync finish timestamp
	fullFinishTs int64
}

func (cw *CommandWriter) doInsert(database, collection string, metadata bson.M, oplogs []*OplogRecord,
	dupUpdate bool) error {

	var inserts []bson.D
	for _, log := range oplogs {
		newObject := log.original.partialLog.Object
		inserts = append(inserts, newObject)
		LOG.Debug("command_writer:: insert %v", log.original.partialLog)
	}
	dbHandle := cw.conn.Client.Database(database)

	var err error
	if err = dbHandle.RunCommand(context.Background(), bson.D{{"insert", collection},
		{"bypassDocumentValidation", false},
		{"documents", inserts},
		{"ordered", ExecuteOrdered}}).Err(); err == nil {
		return nil
	}

	LOG.Warn("doInsert failed: %v", err)

	// error can be ignored
	if IgnoreError(err, "i", parseLastTimestamp(oplogs) <= cw.fullFinishTs) {
		LOG.Warn("error[%v] can be ignored", err)
		return nil
	}

	if utils.DuplicateKey(err) {
		RecordDuplicatedOplog(cw.conn, collection, oplogs)
		// update on duplicated key occur
		if dupUpdate {
			LOG.Info("Duplicated document found. reinsert or update to [%s] [%s]", database, collection)
			return cw.doUpdateOnInsert(database, collection, metadata, oplogs, conf.Options.IncrSyncExecutorUpsert)
		}
		return nil
	}
	return err
}

func (cw *CommandWriter) doUpdateOnInsert(database, collection string, metadata bson.M,
	oplogs []*OplogRecord, upsert bool) error {

	var updates []bson.D
	for _, log := range oplogs {
		// insert must have _id
		if id := oplog.GetKey(log.original.partialLog.Object, ""); id != nil {
			updates = append(updates, bson.D{
				{"q", bson.M{"_id": id}},
				{"u", log.original.partialLog.Object},
				{"upsert", upsert},
				{"multi", false},
			})
		} else {
			LOG.Warn("Insert on duplicated update _id look up failed. %v", log)
		}
		LOG.Debug("command_writer:: updateOnInsert %v", log.original.partialLog)
	}

	var err error
	if err = cw.conn.Client.Database(database).RunCommand(context.Background(),
		bson.D{{"update", collection},
			{"bypassDocumentValidation", false},
			{"updates", updates},
			{"ordered", ExecuteOrdered}}).Err(); err == nil {
		return nil
	}

	LOG.Warn("doUpdateOnInsert failed: %v", err)

	// error can be ignored
	if IgnoreError(err, "u", parseLastTimestamp(oplogs) <= cw.fullFinishTs) {
		return nil
	}

	// ignore duplicated again
	if utils.DuplicateKey(err) {
		LOG.Info("Duplicated document found on doUpdateOnInsert [%s] [%s]", database, collection)
		return nil
	}
	return err
}

func (cw *CommandWriter) doUpdate(database, collection string, metadata bson.M,
	oplogs []*OplogRecord, upsert bool) error {

	var updates []bson.D
	for _, log := range oplogs {
		log.original.partialLog.Object = oplog.RemoveFiled(log.original.partialLog.Object, versionMark)
		updates = append(updates, bson.D{
			{"q", log.original.partialLog.Query},
			{"u", log.original.partialLog.Object},
			{"upsert", upsert},
			{"multi", false}})
		LOG.Debug("command_writer:: update %v", log.original.partialLog)
	}

	var err error
	if err = cw.conn.Client.Database(database).RunCommand(context.Background(),
		bson.D{{"update", collection},
			{"bypassDocumentValidation", false},
			{"updates", updates},
			{"ordered", ExecuteOrdered}}).Err(); err == nil {
		return nil
	}

	LOG.Warn("doUpdate failed: %v", err)

	// error can be ignored
	if IgnoreError(err, "u", parseLastTimestamp(oplogs) <= cw.fullFinishTs) {
		return nil
	}

	// ignore dup error
	if utils.DuplicateKey(err) {
		RecordDuplicatedOplog(cw.conn, collection, oplogs)
		LOG.Info("Duplicated document found on doUpdateOnInsert [%s] [%s]", database, collection)
		return nil
	}
	return err
}

func (cw *CommandWriter) doDelete(database, collection string, metadata bson.M,
	oplogs []*OplogRecord) error {

	var deleted []bson.D
	var err error
	for _, log := range oplogs {
		deleted = append(deleted, bson.D{{"q", log.original.partialLog.Object}, {"limit", 0}})
		LOG.Debug("command_writer:: delete %v", log.original.partialLog)
	}

	if err = cw.conn.Client.Database(database).RunCommand(context.Background(),
		bson.D{{"delete", collection},
			{"deletes", deleted},
			{"ordered", ExecuteOrdered}}).Err(); err == nil {

		return nil
	}

	LOG.Warn("doDelete failed: %v", err)

	// error can be ignored
	if IgnoreError(err, "d", parseLastTimestamp(oplogs) <= cw.fullFinishTs) {
		return nil
	}

	return err
}

func (cw *CommandWriter) doCommand(database string, metadata bson.M, oplogs []*OplogRecord) error {
	var err error
	for _, log := range oplogs {
		operation, found := oplog.ExtraCommandName(log.original.partialLog.Object)
		if conf.Options.FilterDDLEnable || (found && oplog.IsSyncDataCommand(operation)) {
			// execute one by one with sequence order
			if err = RunCommand(database, operation, log.original.partialLog, cw.conn.Client); err == nil {
				LOG.Info("Execute command (op==c) oplog , operation [%s]", conf.Options.FilterDDLEnable,
					operation)
			} else if IgnoreError(err, "c", parseLastTimestamp(oplogs) <= cw.fullFinishTs) {
				LOG.Debug("Ignore error[%v] [%s] [%v]", err, database, log.original.partialLog)
				return nil
			} else {
				return err
			}
		} else {
			// exec.batchExecutor.ReplMetric.AddFilter(1)
		}
		LOG.Debug("command_writer:: command %v", log.original.partialLog)
	}
	return nil
}
