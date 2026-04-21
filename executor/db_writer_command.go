package executor

import (
	"context"
	"strings"

	"go.mongodb.org/mongo-driver/bson"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
	l "github.com/alibaba/MongoShake/v2/pkg/log"
)

// CommandWriter use run_command to execute command
type CommandWriter struct {
	// mongo connection
	conn *utils.MongoCommunityConn
	// init sync finish timestamp
	fullFinishTs int64
}

func (cw *CommandWriter) doInsert(database, collection string, metadata bson.E, oplogs []*OplogRecord,
	dupUpdate bool) error {

	var inserts []bson.D
	for _, log := range oplogs {
		newObject := log.original.partialLog.Object
		inserts = append(inserts, newObject)
		l.Logger.Debugf("command_writer:: insert %v", log.original.partialLog)
	}
	dbHandle := cw.conn.Client.Database(database)

	var err error
	insertCmd := bson.D{
		{"insert", collection},
		{"documents", inserts},
		{"ordered", ExecuteOrdered},
	}
	if conf.Options.IncrSyncBypassDocumentValidation {
		insertCmd = append(insertCmd, bson.E{Key: "bypassDocumentValidation", Value: true})
	} else {
		insertCmd = append(insertCmd, bson.E{Key: "bypassDocumentValidation", Value: false})
	}
	if metadata.Key == "g" {
		insertCmd = append(insertCmd, metadata)
	}
	if err = dbHandle.RunCommand(context.Background(), insertCmd).Err(); err == nil {
		return nil
	}

	l.Logger.Warnf("doInsert failed: %v", err)

	// error can be ignored
	if IgnoreError(err, "i", parseLastTimestamp(oplogs) <= cw.fullFinishTs) {
		l.Logger.Warnf("error[%v] can be ignored", err)
		return nil
	}

	if utils.DuplicateKey(err) {
		RecordDuplicatedOplog(cw.conn, collection, oplogs)
		// update on duplicated key occur
		if dupUpdate {
			l.Logger.Infof("Duplicated document found. reinsert or update to [%s.%s]", database, collection)
			return cw.doUpdateOnInsert(database, collection, metadata, oplogs, conf.Options.IncrSyncExecutorUpsert)
		}
		return nil
	}
	return err
}

func (cw *CommandWriter) doUpdateOnInsert(database, collection string, metadata bson.E, oplogs []*OplogRecord,
	upsert bool) error {

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
			l.Logger.Warnf("Insert on duplicated update _id look up failed. %v", log)
		}
		l.Logger.Debugf("command_writer:: updateOnInsert %v", log.original.partialLog)
	}

	var err error
	updateCmd := bson.D{
		{"update", collection},
		{"updates", updates},
		{"ordered", ExecuteOrdered},
	}
	if conf.Options.IncrSyncBypassDocumentValidation {
		updateCmd = append(updateCmd, bson.E{Key: "bypassDocumentValidation", Value: true})
	} else {
		updateCmd = append(updateCmd, bson.E{Key: "bypassDocumentValidation", Value: false})
	}
	if metadata.Key == "g" {
		updateCmd = append(updateCmd, metadata)
	}
	if err = cw.conn.Client.Database(database).RunCommand(context.Background(), updateCmd).Err(); err == nil {
		return nil
	}

	l.Logger.Warnf("doUpdateOnInsert failed: %v", err)

	// error can be ignored
	if IgnoreError(err, "u", parseLastTimestamp(oplogs) <= cw.fullFinishTs) {
		return nil
	}

	// ignore duplicated again
	if utils.DuplicateKey(err) {
		l.Logger.Infof("Duplicated document found on doUpdateOnInsert [%s] [%s]", database, collection)
		return nil
	}
	return err
}

func (cw *CommandWriter) doUpdate(database, collection string, metadata bson.E, oplogs []*OplogRecord,
	upsert bool) error {

	var updates []bson.D
	for _, log := range oplogs {
		var newObject interface{}
		var transErr error
		oplogVer, ok := oplog.GetKey(log.original.partialLog.Object, versionMark).(int32)
		// handle oplog {o.$v:2} with 'diff' field
		if ok && oplogVer == 2 {
			if newObject, transErr = oplog.DiffUpdateOplogToNormal(log.original.partialLog.Object); transErr != nil {
				// Time-series bucket update with column-store binary diff (e.g., sdata.b)
				// cannot be converted to normal $set/$unset. Fall back to applyOps replay.
				if strings.HasPrefix(collection, utils.VarSystemBucketsPrefix) {
					l.Logger.Infof("command_writer: fall back to applyOps for time-series bucket update on %s.%s: %v",
						database, collection, transErr)
					if applyErr := replayUpdateViaApplyOps(cw.conn.Client, log.original.partialLog); applyErr != nil {
						return applyErr
					}
					continue
				}
				l.Logger.Errorf("doUpdate run failed err[%v] org_doc[%v]", transErr, log.original.partialLog)
				return transErr
			}
		} else {
			log.original.partialLog.Object = oplog.RemoveFiled(log.original.partialLog.Object, versionMark)
			newObject = log.original.partialLog.Object
		}

		updates = append(updates, bson.D{
			{"q", log.original.partialLog.Query},
			{"u", newObject},
			{"upsert", upsert},
			{"multi", false}})
		l.Logger.Debugf("command_writer:: update %v", log.original.partialLog)
	}

	if len(updates) == 0 {
		return nil
	}

	var err error
	updateCmd := bson.D{
		{"update", collection},
		{"updates", updates},
		{"ordered", ExecuteOrdered},
	}
	if conf.Options.IncrSyncBypassDocumentValidation {
		updateCmd = append(updateCmd, bson.E{Key: "bypassDocumentValidation", Value: true})
	} else {
		updateCmd = append(updateCmd, bson.E{Key: "bypassDocumentValidation", Value: false})
	}
	if metadata.Key == "g" {
		updateCmd = append(updateCmd, metadata)
	}
	if err = cw.conn.Client.Database(database).RunCommand(context.Background(), updateCmd).Err(); err == nil {
		return nil
	}

	l.Logger.Warnf("doUpdate failed: %v", err)

	// error can be ignored
	if IgnoreError(err, "u", parseLastTimestamp(oplogs) <= cw.fullFinishTs) {
		return nil
	}

	// ignore dup error
	if utils.DuplicateKey(err) {
		RecordDuplicatedOplog(cw.conn, collection, oplogs)
		l.Logger.Infof("Duplicated document found on doUpdateOnInsert [%s] [%s]", database, collection)
		return nil
	}
	return err
}

func (cw *CommandWriter) doDelete(database, collection string, metadata bson.E, oplogs []*OplogRecord) error {

	var deleted []bson.D
	var err error
	for _, log := range oplogs {
		deleted = append(deleted, bson.D{{"q", log.original.partialLog.Object}, {"limit", 0}})
		l.Logger.Debugf("command_writer:: delete %v", log.original.partialLog)
	}

	deleteCmd := bson.D{
		{"delete", collection},
		{"deletes", deleted},
		{"ordered", ExecuteOrdered},
	}
	if metadata.Key == "g" {
		deleteCmd = append(deleteCmd, metadata)
	}
	if err = cw.conn.Client.Database(database).RunCommand(context.Background(), deleteCmd).Err(); err == nil {

		return nil
	}

	l.Logger.Warnf("doDelete failed: %v", err)

	// error can be ignored
	if IgnoreError(err, "d", parseLastTimestamp(oplogs) <= cw.fullFinishTs) {
		return nil
	}

	return err
}

func (cw *CommandWriter) doCommand(database string, metadata bson.E, oplogs []*OplogRecord) error {
	var err error
	for _, log := range oplogs {
		operation, found := oplog.ExtraCommandName(log.original.partialLog.Object)
		if conf.Options.FilterDDLEnable || (found && oplog.IsSyncDataCommand(operation)) {
			// execute one by one with sequence order
			if err = RunCommand(database, operation, log.original.partialLog, cw.conn.Client); err == nil {
				l.Logger.Infof("Execute command(op=c) oplog, operation[%s]", conf.Options.FilterDDLEnable,
					operation)
			} else if IgnoreError(err, "c", parseLastTimestamp(oplogs) <= cw.fullFinishTs) {
				l.Logger.Debugf("Ignore error[%v] db[%s] oplog[%v]", err, database, log.original.partialLog)
				return nil
			} else {
				return err
			}
		} else {
			// exec.batchExecutor.ReplMetric.AddFilter(1)
		}
		l.Logger.Debugf("command_writer:: command %v", log.original.partialLog)
	}
	return nil
}
