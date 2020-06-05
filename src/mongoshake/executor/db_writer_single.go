package executor

import (
	"fmt"

	"mongoshake/collector/configure"
	"mongoshake/oplog"

	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
	"mongoshake/common"
)

// use general single writer interface to execute command
type SingleWriter struct {
	// mongo connection
	session *mgo.Session
	// init sync finish timestamp
	fullFinishTs int64
}

func (sw *SingleWriter) doInsert(database, collection string, metadata bson.M, oplogs []*OplogRecord,
	dupUpdate bool) error {
	collectionHandle := sw.session.DB(database).C(collection)
	var upserts []*OplogRecord
	for _, log := range oplogs {
		// newObject := utils.AdjustDBRef(log.original.partialLog.Object, conf.Options.DBRef)
		newObject := log.original.partialLog.Object
		if err := collectionHandle.Insert(newObject); err != nil {
			// error can be ignored
			if IgnoreError(err, "i", utils.TimestampToInt64(log.original.partialLog.Timestamp) <= sw.fullFinishTs) {
				continue
			}

			if mgo.IsDup(err) {
				upserts = append(upserts, log)
			} else {
				LOG.Error("insert data[%v] failed[%v]", newObject, err)
				return err
			}
		}

		LOG.Debug("writer: insert %v", log.original.partialLog)
	}

	if len(upserts) != 0 {
		HandleDuplicated(collectionHandle, upserts, OpInsert)
		// update on duplicated key occur
		if dupUpdate {
			LOG.Info("Duplicated document found. reinsert or update to [%s] [%s]", database, collection)
			return sw.doUpdateOnInsert(database, collection, metadata, upserts, conf.Options.IncrSyncExecutorUpsert)
		}
		return nil
	}
	return nil
}

func (sw *SingleWriter) doUpdateOnInsert(database, collection string, metadata bson.M,
	oplogs []*OplogRecord, upsert bool) error {
	type pair struct {
		id   interface{}
		data interface{}
	}
	var updates []*pair
	for _, log := range oplogs {
		// insert must have _id
		// if id, exist := log.original.partialLog.Object["_id"]; exist {
		if id := oplog.GetKey(log.original.partialLog.Object, ""); id != nil {
			// newObject := utils.AdjustDBRef(log.original.partialLog.Object, conf.Options.DBRef)
			newObject := log.original.partialLog.Object
			updates = append(updates, &pair{id: id, data: newObject})
		} else {
			return fmt.Errorf("insert on duplicated update _id look up failed. %v", log.original.partialLog)
		}

		LOG.Debug("writer: updateOnInsert %v", log.original.partialLog)
	}

	collectionHandle := sw.session.DB(database).C(collection)
	if upsert {
		for i, update := range updates {
			if _, err := collectionHandle.UpsertId(update.id, update.data); err != nil {
				// error can be ignored
				if IgnoreError(err, "u", utils.TimestampToInt64(oplogs[i].original.partialLog.Timestamp) <= sw.fullFinishTs) {
					continue
				}

				LOG.Error("upsert _id[%v] with data[%v] failed[%v]", update.id, update.data, err)
				return err
			}
		}
	} else {
		for i, update := range updates {
			if err := collectionHandle.UpdateId(update.id, update.data); err != nil && mgo.IsDup(err) == false {
				// error can be ignored
				if IgnoreError(err, "u", utils.TimestampToInt64(oplogs[i].original.partialLog.Timestamp) <= sw.fullFinishTs) {
					continue
				}

				LOG.Error("update _id[%v] with data[%v] failed[%v]", update.id, update.data, err.Error())
				return err
			}
		}
	}

	return nil
}

func (sw *SingleWriter) doUpdate(database, collection string, metadata bson.M,
	oplogs []*OplogRecord, upsert bool) error {
	collectionHandle := sw.session.DB(database).C(collection)
	if upsert {
		for _, log := range oplogs {
			//newObject := utils.AdjustDBRef(log.original.partialLog.Object, conf.Options.DBRef)
			//// we should handle the special case: "o" filed may include "$v" in mongo-3.6 which is not support in mgo.v2 library
			//if _, ok := newObject[versionMark]; ok {
			//	delete(newObject, versionMark)
			//}
			log.original.partialLog.Object = oplog.RemoveFiled(log.original.partialLog.Object, versionMark)
			_, err := collectionHandle.Upsert(log.original.partialLog.Query, log.original.partialLog.Object)
			if err != nil {
				// error can be ignored
				if IgnoreError(err, "u", utils.TimestampToInt64(log.original.partialLog.Timestamp) <= sw.fullFinishTs) {
					continue
				}

				if mgo.IsDup(err) {
					HandleDuplicated(collectionHandle, oplogs, OpUpdate)
					continue
				}
				LOG.Error("doUpdate[upsert] old-data[%v] with new-data[%v] failed[%v]",
					log.original.partialLog.Query, log.original.partialLog.Object, err)
				return err
			}

			LOG.Debug("writer: upsert %v", log.original.partialLog)
		}
	} else {
		for _, log := range oplogs {
			//newObject := utils.AdjustDBRef(log.original.partialLog.Object, conf.Options.DBRef)
			//// we should handle the special case: "o" filed may include "$v" in mongo-3.6 which is not support in mgo.v2 library
			//if _, ok := newObject[versionMark]; ok {
			//	delete(newObject, versionMark)
			//}
			log.original.partialLog.Object = oplog.RemoveFiled(log.original.partialLog.Object, versionMark)
			err := collectionHandle.Update(log.original.partialLog.Query, log.original.partialLog.Object)
			if err != nil {
				// error can be ignored
				if IgnoreError(err, "u", utils.TimestampToInt64(log.original.partialLog.Timestamp) <= sw.fullFinishTs) {
					continue
				}

				if utils.IsNotFound(err) {
					return fmt.Errorf("doUpdate[update] data[%v] not found", log.original.partialLog.Query)
				} else if mgo.IsDup(err) {
					HandleDuplicated(collectionHandle, oplogs, OpUpdate)
				} else {
					LOG.Error("doUpdate[update] old-data[%v] with new-data[%v] failed[%v]",
						log.original.partialLog.Query, log.original.partialLog.Object, err)
					return err
				}
			}

			LOG.Debug("writer: update %v", log.original.partialLog)
		}
	}

	return nil

}

func (sw *SingleWriter) doDelete(database, collection string, metadata bson.M,
	oplogs []*OplogRecord) error {
	collectionHandle := sw.session.DB(database).C(collection)
	for _, log := range oplogs {
		// ignore ErrNotFound
		id := oplog.GetKey(log.original.partialLog.Object, "")
		if err := collectionHandle.RemoveId(id); err != nil {
			// error can be ignored
			if IgnoreError(err, "d", utils.TimestampToInt64(log.original.partialLog.Timestamp) <= sw.fullFinishTs) {
				continue
			}

			if utils.IsNotFound(err) {
				return fmt.Errorf("doDelete data[%v] not found", log.original.partialLog.Query)
			} else {
				LOG.Error("delete data[%v] failed[%v]", log.original.partialLog.Query, err)
				return err
			}
		}

		LOG.Debug("writer: delete %v", log.original.partialLog)
	}

	return nil
}

func (sw *SingleWriter) doCommand(database string, metadata bson.M, oplogs []*OplogRecord) error {
	var err error
	for _, log := range oplogs {
		// newObject := utils.AdjustDBRef(log.original.partialLog.Object, conf.Options.DBRef)
		newObject := log.original.partialLog.Object
		operation, found := oplog.ExtraCommandName(newObject)
		if conf.Options.FilterDDLEnable || (found && oplog.IsSyncDataCommand(operation)) {
			// execute one by one with sequence order
			if err = RunCommand(database, operation, log.original.partialLog, sw.session); err == nil {
				LOG.Info("Execute command (op==c) oplog, operation [%s]", operation)
			} else if err.Error() == "ns not found" {
				LOG.Info("Execute command (op==c) oplog, operation [%s], ignore error[ns not found]", operation)
			} else if IgnoreError(err, "c", utils.TimestampToInt64(log.original.partialLog.Timestamp) <= sw.fullFinishTs) {
				continue
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