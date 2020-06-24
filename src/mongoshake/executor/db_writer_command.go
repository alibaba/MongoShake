package executor

import (
	"mongoshake/collector/configure"
	"mongoshake/oplog"

	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
)

// use run_command to execute command
type CommandWriter struct {
	// mongo connection
	session *mgo.Session
	// init sync finish timestamp
	fullFinishTs int64
}

func (cw *CommandWriter) doInsert(database, collection string, metadata bson.M, oplogs []*OplogRecord,
	dupUpdate bool) error {
	var inserts []bson.D
	for _, log := range oplogs {
		// newObject := utils.AdjustDBRef(log.original.partialLog.Object, conf.Options.DBRef)
		newObject := log.original.partialLog.Object
		inserts = append(inserts, newObject)
		LOG.Debug("writer: insert %v", log.original.partialLog)
	}
	dbHandle := cw.session.DB(database)

	var err error
	if err = dbHandle.RunCommand(
		"insert",
		bson.D{{"insert", collection},
			{"bypassDocumentValidation", false},
			{"documents", inserts},
			{"ordered", ExecuteOrdered}},
		metadata,
		bson.M{}, nil); err == nil {
		return nil
	}

	LOG.Warn("doInsert failed: %v", err)

	// error can be ignored
	if IgnoreError(err, "i", parseLastTimestamp(oplogs) <= cw.fullFinishTs) {
		LOG.Warn("error[%v] can be ignored", err)
		return nil
	}

	if mgo.IsDup(err) {
		HandleDuplicated(dbHandle.C(collection), oplogs, OpInsert)
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
	var updates []bson.M
	for _, log := range oplogs {
		// insert must have _id
		if id := oplog.GetKey(log.original.partialLog.Object, ""); id != nil {
			// newObject := utils.AdjustDBRef(log.original.partialLog.Object, conf.Options.DBRef)
			updates = append(updates, bson.M{
				"q":      bson.M{"_id": id},
				"u":      log.original.partialLog.Object,
				"upsert": upsert,
				"multi":  false,
			})
		} else {
			LOG.Warn("Insert on duplicated update _id look up failed. %v", log)
		}
		LOG.Debug("writer: updateOnInsert %v", log.original.partialLog)
	}

	var err error
	if err = cw.session.DB(database).RunCommand(
		"update",
		bson.D{{"update", collection},
			{"bypassDocumentValidation", false},
			{"updates", updates},
			{"ordered", ExecuteOrdered}},
		metadata,
		bson.M{}, nil); err == nil {
		return nil
	}

	LOG.Warn("doUpdateOnInsert failed: %v", err)

	// error can be ignored
	if IgnoreError(err, "u", parseLastTimestamp(oplogs) <= cw.fullFinishTs) {
		return nil
	}

	// ignore duplicated again
	if mgo.IsDup(err) {
		return nil
	}
	return err
}

func (cw *CommandWriter) doUpdate(database, collection string, metadata bson.M,
	oplogs []*OplogRecord, upsert bool) error {
	var updates []bson.M
	for _, log := range oplogs {
		// newObject := utils.AdjustDBRef(log.original.partialLog.Object, conf.Options.DBRef)
		// we should handle the special case: "o" field may include "$v" in mongo-3.6 which is not support in mgo.v2 library
		//if _, ok := newObject[versionMark]; ok {
		//	delete(newObject, versionMark)
		//}
		log.original.partialLog.Object = oplog.RemoveFiled(log.original.partialLog.Object, versionMark)
		updates = append(updates, bson.M{
			"q":      log.original.partialLog.Query,
			"u":      log.original.partialLog.Object,
			"upsert": upsert,
			"multi":  false})
		LOG.Debug("writer: update %v", log.original.partialLog)
	}

	var err error
	dbHandle := cw.session.DB(database)
	if err = dbHandle.RunCommand(
		"update",
		bson.D{{"update", collection},
			{"bypassDocumentValidation", false},
			{"updates", updates},
			{"ordered", ExecuteOrdered}},
		metadata,
		bson.M{}, nil); err == nil {

		return nil
	}

	LOG.Warn("doUpdate failed: %v", err)

	// error can be ignored
	if IgnoreError(err, "u", parseLastTimestamp(oplogs) <= cw.fullFinishTs) {
		return nil
	}

	// ignore dup error
	if mgo.IsDup(err) {
		HandleDuplicated(dbHandle.C(collection), oplogs, OpUpdate)
		return nil
	}
	return err
}

func (cw *CommandWriter) doDelete(database, collection string, metadata bson.M,
	oplogs []*OplogRecord) error {
	var deleted []bson.M
	var err error
	for _, log := range oplogs {
		deleted = append(deleted, bson.M{"q": log.original.partialLog.Object, "limit": 0})
		LOG.Debug("writer: delete %v", log.original.partialLog)
	}

	if err = cw.session.DB(database).RunCommand(
		"delete",
		bson.D{{"delete", collection},
			{"deletes", deleted},
			{"ordered", ExecuteOrdered}},
		metadata,
		bson.M{}, nil); err == nil {

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
		// newObject := utils.AdjustDBRef(log.original.partialLog.Object, conf.Options.DBRef)
		operation, found := oplog.ExtraCommandName(log.original.partialLog.Object)
		if conf.Options.FilterDDLEnable || (found && oplog.IsSyncDataCommand(operation)) {
			// execute one by one with sequence order
			if err = cw.applyOps(database, metadata, []*oplog.PartialLog{log.original.
				partialLog}); err == nil {
				LOG.Info("Execute command (op==c) oplog , operation [%s]", conf.Options.FilterDDLEnable,
					operation)
			} else if IgnoreError(err, "c", parseLastTimestamp(oplogs) <= cw.fullFinishTs) {
				return nil
			} else {
				return err
			}
		} else {
			// exec.batchExecutor.ReplMetric.AddFilter(1)
		}
		LOG.Debug("writer: command %v", log.original.partialLog)
	}
	return nil
}

func (cw *CommandWriter) applyOps(database string, metadata bson.M, oplogs []*oplog.PartialLog) error {
	type Result struct {
		OK int `bson:"ok"`
		N  int `bson:"n"`
	}

	result := &Result{}
	succeed := 0
	for succeed < len(oplogs) {
		if err := cw.session.DB(database).RunCommand(
			"applyOps",
			// only one field. therefore use bson.M simply
			bson.M{"applyOps": oplogs[succeed:]},
			metadata,
			bson.M{}, result); err != nil {

			if result.N == 0 {
				// the first oplog is failed
				LOG.Warn("ApplyOps failed. only skip error[%v], %v", err, oplogs[succeed])
				succeed += 1
			} else {
				succeed += result.N
			}
		} else {
			// all successfully
			break
		}
	}

	return nil
}