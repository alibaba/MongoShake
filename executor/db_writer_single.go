package executor

import (
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	"github.com/alibaba/MongoShake/v2/oplog"

	utils "github.com/alibaba/MongoShake/v2/common"
	LOG "github.com/vinllen/log4go"
)

// use general single writer interface to execute command
type SingleWriter struct {
	// mongo connection
	conn *utils.MongoCommunityConn

	// init sync finish timestamp
	fullFinishTs int64
}

func (sw *SingleWriter) doInsert(database, collection string, metadata bson.M, oplogs []*OplogRecord,
	dupUpdate bool) error {

	collectionHandle := sw.conn.Client.Database(database).Collection(collection)
	var upserts []*OplogRecord

	for _, log := range oplogs {
		if _, err := collectionHandle.InsertOne(nil, log.original.partialLog.Object); err != nil {

			if utils.DuplicateKey(err) {
				upserts = append(upserts, log)
				continue
			} else {
				LOG.Error("insert data[%v] failed[%v]", log.original.partialLog.Object, err)
				return err
			}
		}

		LOG.Debug("single_writer: insert %v", log.original.partialLog)
	}

	if len(upserts) != 0 {
		RecordDuplicatedOplog(sw.conn, collection, upserts)

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
		id    interface{}
		data  bson.D
		index int
	}
	var updates []*pair
	for i, log := range oplogs {
		newObject := log.original.partialLog.Object
		if upsert && len(log.original.partialLog.DocumentKey) > 0 {
			updates = append(updates, &pair{id: log.original.partialLog.DocumentKey, data: newObject, index: i})
		} else {
			if upsert {
				LOG.Warn("doUpdateOnInsert runs upsert but lack documentKey: %v", log.original.partialLog)
			}
			// insert must have _id
			if id := oplog.GetKeyN(log.original.partialLog.Object, ""); id != nil {
				updates = append(updates, &pair{id: id, data: newObject, index: i})
			} else {
				return fmt.Errorf("insert on duplicated update _id look up failed. %v", log.original.partialLog)
			}
		}

		LOG.Debug("single_writer: updateOnInsert %v", log.original.partialLog)
	}

	collectionHandle := sw.conn.Client.Database(database).Collection(collection)
	if upsert {
		for _, update := range updates {

			opts := options.Update().SetUpsert(true)
			res, err := collectionHandle.UpdateOne(nil, bson.D{{"_id", update.id}},
				bson.D{{"$set", update.data}}, opts)
			if err != nil {
				LOG.Warn("upsert _id[%v] with data[%v] meets err[%v] res[%v], try to solve",
					update.id, update.data, err, res)

				// error can be ignored(insert fail & oplog is before full end)
				//if IgnoreError(err, "ui", utils.DatetimeToInt64(oplogs[i].original.partialLog.Timestamp) <= sw.fullFinishTs) {
				if utils.DuplicateKey(err) &&
					utils.DatetimeToInt64(oplogs[update.index].original.partialLog.Timestamp) <= sw.fullFinishTs {
					continue
				}

				LOG.Error("upsert _id[%v] with data[%v] failed[%v]", update.id, update.data, err)
				return err
			}
		}
	} else {
		for i, update := range updates {

			res, err := collectionHandle.UpdateOne(nil, bson.D{{"_id", update.id}},
				bson.D{{"$set", update.data}}, nil)
			if err != nil && utils.DuplicateKey(err) == false {
				LOG.Warn("update _id[%v] with data[%v] meets err[%v] res[%v], try to solve",
					update.id, update.data, err, res)

				// error can be ignored
				if utils.DatetimeToInt64(oplogs[i].original.partialLog.Timestamp) <= sw.fullFinishTs {
					er, ok := err.(mongo.ServerError)
					if ok && (er.HasErrorCode(28) || er.HasErrorCode(211)) {
						continue
					}
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
	collectionHandle := sw.conn.Client.Database(database).Collection(collection)
	if upsert {
		for _, log := range oplogs {
			//newObject := utils.AdjustDBRef(log.original.partialLog.Object, conf.Options.DBRef)
			//// we should handle the special case: "o" filed may include "$v" in mongo-3.6 which is not support in mgo.v2 library
			//if _, ok := newObject[versionMark]; ok {
			//	delete(newObject, versionMark)
			//}
			log.original.partialLog.Object = oplog.RemoveFiled(log.original.partialLog.Object, versionMark)
			newObject, _ := oplog.ConvertBsonD2M(log.original.partialLog.Object)
			var err error
			opts := options.Update().SetUpsert(true)
			if upsert && len(log.original.partialLog.DocumentKey) > 0 {
				_, err = collectionHandle.UpdateOne(nil, log.original.partialLog.DocumentKey,
					newObject, opts)
			} else {
				if upsert {
					LOG.Warn("doUpdate runs upsert but lack documentKey: %v", log.original.partialLog)
				}

				_, err = collectionHandle.UpdateOne(nil, log.original.partialLog.Query,
					newObject, opts)
			}
			if err != nil {
				// error can be ignored
				if IgnoreError(err, "u", utils.DatetimeToInt64(log.original.partialLog.Timestamp) <= sw.fullFinishTs) {
					continue
				}

				if utils.DuplicateKey(err) {
					RecordDuplicatedOplog(sw.conn, collection, oplogs)
					continue
				}
				LOG.Error("doUpdate[upsert] old-data[%v] with new-data[%v] failed[%v]",
					log.original.partialLog.Query, newObject, err)
				return err
			}

			LOG.Debug("single_writer: upsert %v", log.original.partialLog)
		}
	} else {
		for _, log := range oplogs {
			//newObject := utils.AdjustDBRef(log.original.partialLog.Object, conf.Options.DBRef)
			//// we should handle the special case: "o" filed may include "$v" in mongo-3.6 which is not support in mgo.v2 library
			//if _, ok := newObject[versionMark]; ok {
			//	delete(newObject, versionMark)
			//}
			log.original.partialLog.Object = oplog.RemoveFiled(log.original.partialLog.Object, versionMark)
			newObject, _ := oplog.ConvertBsonD2M(log.original.partialLog.Object)
			_, err := collectionHandle.UpdateOne(nil, log.original.partialLog.Query,
				newObject, nil)
			if err != nil {
				// error can be ignored
				if IgnoreError(err, "u", utils.DatetimeToInt64(log.original.partialLog.Timestamp) <= sw.fullFinishTs) {
					continue
				}

				// err.Error() == "not found" ??
				if utils.IsNotFound(err) {
					return fmt.Errorf("doUpdate[update] data[%v] not found", log.original.partialLog.Query)
				} else if utils.DuplicateKey(err) {
					RecordDuplicatedOplog(sw.conn, collection, oplogs)
				} else {
					LOG.Error("doUpdate[update] old-data[%v] with new-data[%v] failed[%v]",
						log.original.partialLog.Query, newObject, err)
					return err
				}
			}

			LOG.Debug("single_writer: update %v", log.original.partialLog)
		}
	}

	return nil

}

func (sw *SingleWriter) doDelete(database, collection string, metadata bson.M,
	oplogs []*OplogRecord) error {
	collectionHandle := sw.conn.Client.Database(database).Collection(collection)
	for _, log := range oplogs {
		// ignore ErrNotFound
		id := oplog.GetKeyN(log.original.partialLog.Object, "")

		if _, err := collectionHandle.DeleteOne(nil, bson.D{{"_id", id}}); err != nil {
			// error can be ignored
			if IgnoreError(err, "d", utils.DatetimeToInt64(log.original.partialLog.Timestamp) <= sw.fullFinishTs) {
				continue
			}

			if IgnoreError(err, "d", parseLastTimestamp(oplogs) <= sw.fullFinishTs) {
				LOG.Warn("ignore error[%v] when run operation[%v], initialSync[%v]",
					err, "d", parseLastTimestamp(oplogs) <= sw.fullFinishTs)
				return nil
			} else {
				LOG.Error("delete data[%v] failed[%v]", log.original.partialLog.Query, err)
				return err
			}
		}

		LOG.Debug("single_writer: delete %v", log.original.partialLog)
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
			if err = RunCommand(database, operation, log.original.partialLog, sw.conn); err == nil {
				LOG.Info("Execute command (op==c) oplog, operation [%s]", operation)
			} else if err.Error() == "ns not found" {
				LOG.Info("Execute command (op==c) oplog, operation [%s], ignore error[ns not found]", operation)
			} else if IgnoreError(err, "c", utils.DatetimeToInt64(log.original.partialLog.Timestamp) <= sw.fullFinishTs) {
				continue
			} else {
				return err
			}
		} else {
			// exec.batchExecutor.ReplMetric.AddFilter(1)
		}

		LOG.Debug("single_writer: command %v", log.original.partialLog)
	}
	return nil
}
