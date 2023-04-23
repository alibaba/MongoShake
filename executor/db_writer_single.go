package executor

import (
	"context"
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

// { "op" : "i", "ns" : "test.c", "ui" : UUID("4654d08e-db1f-4e94-9778-90aeee4feff0"), "o" : { "_id" : ObjectId("627a1f83b95fae5fca006bac"), "a" : 1, "b" : 1, "c" : 1 }, "ts" : Timestamp(1652170627, 2), "t" : NumberLong(1), "wall" : ISODate("2022-05-10T08:17:07.558Z"), "v" : NumberLong(2) }
func (sw *SingleWriter) doInsert(database, collection string, metadata bson.M, oplogs []*OplogRecord,
	dupUpdate bool) error {

	collectionHandle := sw.conn.Client.Database(database).Collection(collection)
	var upserts []*OplogRecord

	for _, log := range oplogs {
		if _, err := collectionHandle.InsertOne(context.Background(), log.original.partialLog.Object); err != nil {

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
			if id := oplog.GetKey(log.original.partialLog.Object, ""); id != nil {
				updates = append(updates, &pair{id: bson.D{{"_id", id}}, data: newObject, index: i})
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
			res, err := collectionHandle.UpdateOne(context.Background(), update.id,
				bson.D{{"$set", update.data}}, opts)
			if err != nil {
				LOG.Warn("upsert _id[%v] with data[%v] meets err[%v] res[%v], try to solve",
					update.id, update.data, err, res)

				// error can be ignored(insert fail & oplog is before full end)
				if utils.DuplicateKey(err) &&
					utils.TimeStampToInt64(oplogs[update.index].original.partialLog.Timestamp) <= sw.fullFinishTs {
					continue
				}

				LOG.Error("upsert _id[%v] with data[%v] failed[%v]", update.id, update.data, err)
				return err
			}
			if res != nil {
				if res.MatchedCount != 1 && res.UpsertedCount != 1 {
					return fmt.Errorf("Update fail(MatchedCount:%d ModifiedCount:%d UpsertedCount:%d) upsert _id[%v] with data[%v]",
						res.MatchedCount, res.ModifiedCount, res.UpsertedCount, update.id, update.data)
				}
			}
		}
	} else {
		for i, update := range updates {

			res, err := collectionHandle.UpdateOne(context.Background(), update.id,
				bson.D{{"$set", update.data}}, nil)
			if err != nil && utils.DuplicateKey(err) == false {
				LOG.Warn("update _id[%v] with data[%v] meets err[%v] res[%v], try to solve",
					update.id, update.data, err, res)

				// error can be ignored
				if IgnoreError(err, "u",
					utils.TimeStampToInt64(oplogs[i].original.partialLog.Timestamp) <= sw.fullFinishTs) {
					continue
				}

				LOG.Error("update _id[%v] with data[%v] failed[%v]", update.id, update.data, err.Error())
				return err
			}
			if res != nil {
				if res.MatchedCount != 1 {
					return fmt.Errorf("Update fail(MatchedCount:%d, ModifiedCount:%d) old-data[%v] with new-data[%v]",
						res.MatchedCount, res.ModifiedCount, update.id, update.data)
				}
			}
		}
	}

	return nil
}

/*
	replace(update all):
		db.c.insert({"a":1,"b":1,"c":1}) + db.c.update({"a":1}, {"b":2})
		{ "op" : "u", "ns" : "test.c", "ui" : UUID("4654d08e-db1f-4e94-9778-90aeee4feff0"), "o" : { "_id" : ObjectId("627a2492b95fae5fca006bad"), "b" : 2 }, "o2" : { "_id" : ObjectId("627a2492b95fae5fca006bad") }, "ts" : Timestamp(1652171939, 1), "t" : NumberLong(1), "wall" : ISODate("2022-05-10T08:38:59.701Z"), "v" : NumberLong(2) }
	updateOne:
		db.c.insert({"a":1,"b":1,"c":1}) + db.c.updateOne({"a":1}, {"$set":{"b":2}})
		{ "op" : "u", "ns" : "test.c", "ui" : UUID("4654d08e-db1f-4e94-9778-90aeee4feff0"), "o" : { "$v" : 1, "$set" : { "b" : 3 }, "$unset" : { "c" : true } }, "o2" : { "_id" : ObjectId("627a1f83b95fae5fca006bac") }, "ts" : Timestamp(1652170892, 1), "t" : NumberLong(1), "wall" : ISODate("2022-05-10T08:21:32.695Z"), "v" : NumberLong(2) }
*/
func (sw *SingleWriter) doUpdate(database, collection string, metadata bson.M,
	oplogs []*OplogRecord, upsert bool) error {
	collectionHandle := sw.conn.Client.Database(database).Collection(collection)

	for _, log := range oplogs {
		var update interface{}
		var err error
		var res *mongo.UpdateResult

		updateCmd := "update"
		LOG.Debug("single_writer: org_doc %v", log.original.partialLog)
		if oplog.FindFiledPrefix(log.original.partialLog.Object, "$") {
			var oplogErr error

			oplogVer, ok := oplog.GetKey(log.original.partialLog.Object, versionMark).(int32)
			LOG.Debug("single_writer doUpdate: have $, org_object:%v "+
				"object_ver:%v\n", log.original.partialLog.Object, oplogVer)

			if ok && oplogVer == 2 {
				if update, oplogErr = oplog.DiffUpdateOplogToNormal(log.original.partialLog.Object); oplogErr != nil {
					LOG.Error("doUpdate run Faild err[%v] org_doc[%v]", oplogErr, log.original.partialLog)
					return oplogErr
				}
			} else {
				log.original.partialLog.Object = oplog.RemoveFiled(log.original.partialLog.Object, versionMark)
				update = log.original.partialLog.Object
			}

			opts := options.Update()
			if upsert {
				opts.SetUpsert(true)
			}
			if upsert && len(log.original.partialLog.DocumentKey) > 0 {
				res, err = collectionHandle.UpdateOne(context.Background(), log.original.partialLog.DocumentKey,
					update, opts)
			} else {
				if upsert {
					LOG.Warn("doUpdate runs upsert but lack documentKey: %v", log.original.partialLog)
				}

				res, err = collectionHandle.UpdateOne(context.Background(), log.original.partialLog.Query,
					update, opts)
			}
		} else {
			update = log.original.partialLog.Object

			opts := options.Replace()
			if upsert {
				opts.SetUpsert(true)
			}
			if upsert && len(log.original.partialLog.DocumentKey) > 0 {
				res, err = collectionHandle.ReplaceOne(context.Background(), log.original.partialLog.DocumentKey,
					update, opts)
			} else {
				res, err = collectionHandle.ReplaceOne(context.Background(), log.original.partialLog.Query,
					update, opts)
			}

			updateCmd = "replace"
		}
		LOG.Debug("single_writer: %s %v aftermodify_doc:%v", updateCmd, update, log.original.partialLog)

		if err != nil {
			// error can be ignored
			if IgnoreError(err, "u",
				utils.TimeStampToInt64(log.original.partialLog.Timestamp) <= sw.fullFinishTs) {
				continue
			}

			if utils.DuplicateKey(err) {
				RecordDuplicatedOplog(sw.conn, collection, oplogs)
				continue
			}

			LOG.Error("doUpdate[upsert] old-data[%v] with new-data[%v] failed[%v]",
				log.original.partialLog.Query, log.original.partialLog.Object, err)
			return err
		}
		if res != nil {
			if upsert {
				if res.MatchedCount != 1 && res.UpsertedCount != 1 {
					return fmt.Errorf("Update fail(MatchedCount:%d ModifiedCount:%d UpsertedCount:%d) old-data[%v] with new-data[%v]",
						res.MatchedCount, res.ModifiedCount, res.UpsertedCount,
						log.original.partialLog.Query, log.original.partialLog.Object)
				}
			} else {
				if res.MatchedCount != 1 {
					return fmt.Errorf("Update fail(MatchedCount:%d ModifiedCount:%d MatchedCount:%d) old-data[%v] with new-data[%v]",
						res.MatchedCount, res.ModifiedCount, res.MatchedCount,
						log.original.partialLog.Query, log.original.partialLog.Object)
				}
			}
		}

		LOG.Debug("single_writer: aftermodify_doc %v %s[%v]", log.original.partialLog, updateCmd, update)
	}

	return nil

}

// { "op" : "d", "ns" : "test.c", "ui" : UUID("4654d08e-db1f-4e94-9778-90aeee4feff0"), "o" : { "_id" : ObjectId("627a1f83b95fae5fca006bac") }, "ts" : Timestamp(1652171085, 1), "t" : NumberLong(1), "wall" : ISODate("2022-05-10T08:24:45.828Z"), "v" : NumberLong(2) }
func (sw *SingleWriter) doDelete(database, collection string, metadata bson.M,
	oplogs []*OplogRecord) error {
	collectionHandle := sw.conn.Client.Database(database).Collection(collection)
	for _, log := range oplogs {
		// ignore ErrNotFound

		_, err := collectionHandle.DeleteOne(context.Background(), log.original.partialLog.Object)
		if err != nil {
			LOG.Error("delete data[%v] failed[%v]", log.original.partialLog.Query, err)
			return err
		}

		LOG.Debug("single_writer: delete %v", log.original.partialLog)
	}

	return nil
}

func (sw *SingleWriter) doCommand(database string, metadata bson.M, oplogs []*OplogRecord) error {
	var err error
	for _, log := range oplogs {
		newObject := log.original.partialLog.Object
		operation, found := oplog.ExtraCommandName(newObject)
		if conf.Options.FilterDDLEnable || (found && oplog.IsSyncDataCommand(operation)) {
			// execute one by one with sequence order
			if err = RunCommand(database, operation, log.original.partialLog, sw.conn.Client); err == nil {
				LOG.Info("Execute command (op==c) oplog, operation [%s]", operation)
			} else if err.Error() == "ns not found" {
				LOG.Info("Execute command (op==c) oplog, operation [%s], ignore error[ns not found]", operation)
			} else if IgnoreError(err, "c", utils.TimeStampToInt64(log.original.partialLog.Timestamp) <= sw.fullFinishTs) {
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
