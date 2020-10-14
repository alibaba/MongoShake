package executor

import (
	"mongoshake/collector/configure"
	"mongoshake/oplog"
	"mongoshake/common"

	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
)

// use general bulk interface such like Insert/Update/Delete to execute command
type BulkWriter struct {
	// mongo connection
	session *mgo.Session
	// init sync finish timestamp
	fullFinishTs int64
}

func (bw *BulkWriter) doInsert(database, collection string, metadata bson.M, oplogs []*OplogRecord,
	dupUpdate bool) error {
	var inserts []interface{}
	for _, log := range oplogs {
		// newObject := utils.AdjustDBRef(log.original.partialLog.Object, conf.Options.DBRef)
		newObject := log.original.partialLog.Object
		inserts = append(inserts, newObject)

		LOG.Debug("writer: insert %v", log.original.partialLog)
	}
	// collectionHandle := bw.session.DB(database).C(collection)
	bulk := bw.session.DB(database).C(collection).Bulk()
	bulk.Unordered()
	bulk.Insert(inserts...)

	if _, err := bulk.Run(); err != nil {
		// error can be ignored
		if IgnoreError(err, "i", parseLastTimestamp(oplogs) <= bw.fullFinishTs) {
			LOG.Warn("ignore error[%v] when run operation[%v], initialSync[%v]", err, "i", parseLastTimestamp(oplogs) <= bw.fullFinishTs)
			return nil
		}

		if mgo.IsDup(err) {
			HandleDuplicated(bw.session.DB(database).C(collection), oplogs, OpInsert)
			// update on duplicated key occur
			if dupUpdate {
				LOG.Info("Duplicated document found. reinsert or update to [%s] [%s]", database, collection)
				return bw.doUpdateOnInsert(database, collection, metadata, oplogs, conf.Options.IncrSyncExecutorUpsert)
			}
			return nil
		}
		return err
	}
	return nil
}

func (bw *BulkWriter) doUpdateOnInsert(database, collection string, metadata bson.M,
	oplogs []*OplogRecord, upsert bool) error {
	var update []interface{}
	for _, log := range oplogs {
		// insert must have _id
		// if id, exist := log.original.partialLog.Object["_id"]; exist {
		if id := oplog.GetKey(log.original.partialLog.Object, ""); id != nil {
			// newObject := utils.AdjustDBRef(log.original.partialLog.Object, conf.Options.DBRef)
			newObject := log.original.partialLog.Object
			update = append(update, bson.M{"_id": id}, newObject)
		} else {
			LOG.Warn("Insert on duplicated update _id look up failed. %v", log)
		}

		LOG.Debug("writer: updateOnInsert %v", log.original.partialLog)
	}

	bulk := bw.session.DB(database).C(collection).Bulk()
	if upsert {
		bulk.Upsert(update...)
	} else {
		bulk.Update(update...)
	}

	if _, err := bulk.Run(); err != nil {
		// parse error
		index, errMsg, dup := utils.FindFirstErrorIndexAndMessage(err.Error())
		LOG.Error("detail error info with index[%v] msg[%v] dup[%v]", index, errMsg, dup)

		// error can be ignored
		if IgnoreError(err, "u", parseLastTimestamp(oplogs) <= bw.fullFinishTs) {
			var oplogRecord *OplogRecord
			if index != -1 {
				oplogRecord = oplogs[index]
			}
			LOG.Warn("ignore error[%v] when run operation[%v], initialSync[%v], oplog[%v]",
				err, "u", parseLastTimestamp(oplogs) <= bw.fullFinishTs, oplogRecord)
			return nil
		}

		if mgo.IsDup(err) {
			// create single writer to write one by one
			sw := NewDbWriter(bw.session, bson.M{}, false, bw.fullFinishTs)
			return sw.doUpdateOnInsert(database, collection, metadata, oplogs[index:], upsert)
		}
		LOG.Error("doUpdateOnInsert run upsert/update[%v] failed[%v]", upsert, err)
		return err
	}
	return nil
}

func (bw *BulkWriter) doUpdate(database, collection string, metadata bson.M,
	oplogs []*OplogRecord, upsert bool) error {
	var update []interface{}
	for _, log := range oplogs {
		// newObject := utils.AdjustDBRef(log.original.partialLog.Object, conf.Options.DBRef)
		// we should handle the special case: "o" field may include "$v" in mongo-3.6 which is not support in mgo.v2 library
		//if _, ok := newObject[versionMark]; ok {
		//	delete(newObject, versionMark)
		//}
		log.original.partialLog.Object = oplog.RemoveFiled(log.original.partialLog.Object, versionMark)
		update = append(update, log.original.partialLog.Query, log.original.partialLog.Object)

		LOG.Debug("writer: update %v", log.original.partialLog.Object)
	}

	LOG.Debug("writer: update %v", update)

	bulk := bw.session.DB(database).C(collection).Bulk()
	if upsert {
		bulk.Upsert(update...)
	} else {
		bulk.Update(update...)
	}

	if _, err := bulk.Run(); err != nil {
		// parse error
		index, errMsg, dup := utils.FindFirstErrorIndexAndMessage(err.Error())
		var oplogRecord *OplogRecord
		if index != -1 {
			oplogRecord = oplogs[index]
		}
		LOG.Warn("detail error info with index[%v] msg[%v] dup[%v], isFullSyncStage[%v], oplog[%v]",
			index, errMsg, dup, parseLastTimestamp(oplogs) <= bw.fullFinishTs, oplogRecord)

		// error can be ignored
		if IgnoreError(err, "u", parseLastTimestamp(oplogs) <= bw.fullFinishTs) {
			LOG.Warn("ignore error[%v] when run operation[%v], initialSync[%v]", err, "u", parseLastTimestamp(oplogs) <= bw.fullFinishTs)
			// re-run (index, len(oplogs) - 1]
			sw := NewDbWriter(bw.session, bson.M{}, false, bw.fullFinishTs)
			return sw.doUpdate(database, collection, metadata, oplogs[index + 1:], upsert)
		}

		if mgo.IsDup(err) {
			HandleDuplicated(bw.session.DB(database).C(collection), oplogs, OpUpdate)
			// create single writer to write one by one
			sw := NewDbWriter(bw.session, bson.M{}, false, bw.fullFinishTs)
			return sw.doUpdate(database, collection, metadata, oplogs[index:], upsert)
		}
		LOG.Error("doUpdate run upsert/update[%v] failed[%v]", upsert, err)
		return err
	}
	return nil
}

func (bw *BulkWriter) doDelete(database, collection string, metadata bson.M,
	oplogs []*OplogRecord) error {
	var delete []interface{}
	for _, log := range oplogs {
		delete = append(delete, log.original.partialLog.Object)

		LOG.Debug("writer: delete %v", log.original.partialLog)
	}

	bulk := bw.session.DB(database).C(collection).Bulk()
	bulk.Unordered()
	bulk.Remove(delete...)
	if _, err := bulk.Run(); err != nil {
		// error can be ignored
		if IgnoreError(err, "d", parseLastTimestamp(oplogs) <= bw.fullFinishTs) {
			LOG.Warn("ignore error[%v] when run operation[%v], initialSync[%v]", err, "d", parseLastTimestamp(oplogs) <= bw.fullFinishTs)
			return nil
		}

		LOG.Error("doDelete run delete[%v] failed[%v]", delete, err)
		return err
	}
	return nil
}

func (bw *BulkWriter) doCommand(database string, metadata bson.M, oplogs []*OplogRecord) error {
	var err error
	for _, log := range oplogs {
		// newObject := utils.AdjustDBRef(log.original.partialLog.Object, conf.Options.DBRef)
		newObject := log.original.partialLog.Object
		operation, found := oplog.ExtraCommandName(newObject)
		if conf.Options.FilterDDLEnable || (found && oplog.IsSyncDataCommand(operation)) {
			// execute one by one with sequence order
			if err = RunCommand(database, operation, log.original.partialLog, bw.session); err == nil {
				LOG.Info("Execute command (op==c) oplog, operation [%s]", operation)
			} else if err.Error() == "ns not found" {
				LOG.Info("Execute command (op==c) oplog, operation [%s], ignore error[ns not found]", operation)
			} else if IgnoreError(err, "c", parseLastTimestamp(oplogs) <= bw.fullFinishTs) {
				LOG.Warn("ignore error[%v] when run operation[%v], initialSync[%v]", err, "c", parseLastTimestamp(oplogs) <= bw.fullFinishTs)
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