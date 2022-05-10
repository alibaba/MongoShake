package executor

import (
	"context"
	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	LOG "github.com/vinllen/log4go"
)

// use general bulk interface such like Insert/Update/Delete to execute command
type BulkWriter struct {
	// mongo connection
	conn *utils.MongoCommunityConn
	// init sync finish timestamp
	fullFinishTs int64
}

func (bw *BulkWriter) doInsert(database, collection string, metadata bson.M, oplogs []*OplogRecord,
	dupUpdate bool) error {

	var models []mongo.WriteModel
	for _, log := range oplogs {
		models = append(models, mongo.NewInsertOneModel().SetDocument(log.original.partialLog.Object))

		LOG.Debug("bulk_writer: insert org_oplog:%v insert_doc:%v",
			log.original.partialLog, log.original.partialLog.Object)
	}

	opts := options.BulkWrite().SetOrdered(false)
	res, err := bw.conn.Client.Database(database).Collection(collection).BulkWrite(nil, models, opts)

	if err != nil {
		LOG.Warn("insert docs with length[%v] into ns[%v] of dest mongo failed[%v] res[%v]",
			len(models), database+"."+collection, (err.(mongo.BulkWriteException)).WriteErrors[0], res)

		if utils.DuplicateKey(err) {
			RecordDuplicatedOplog(bw.conn, collection, oplogs)
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
	var models []mongo.WriteModel

	for _, log := range oplogs {
		newObject := log.original.partialLog.Object
		if upsert && len(log.original.partialLog.DocumentKey) > 0 {

			models = append(models, mongo.NewUpdateOneModel().
				SetFilter(log.original.partialLog.DocumentKey).
				SetUpdate(bson.D{{"$set", newObject}}).SetUpsert(true))
		} else {
			if upsert {
				LOG.Warn("doUpdateOnInsert runs upsert but lack documentKey: %v", log.original.partialLog)
			}
			// insert must have _id
			if id := oplog.GetKey(log.original.partialLog.Object, ""); id != nil {

				model := mongo.NewUpdateOneModel().
					SetFilter(bson.D{{"_id", id}}).
					SetUpdate(bson.D{{"$set", newObject}})
				if upsert {
					model.SetUpsert(true)
				}
				models = append(models, model)
			} else {
				LOG.Warn("Insert on duplicated update _id look up failed. %v", log)
			}
		}

		LOG.Debug("bulk_writer: updateOnInsert %v", log.original.partialLog)
	}

	res, err := bw.conn.Client.Database(database).Collection(collection).BulkWrite(nil, models, nil)

	if err != nil {
		// parse error
		index, errMsg, dup := utils.FindFirstErrorIndexAndMessageN(err)
		LOG.Error("detail error info with index[%v] msg[%v] dup[%v] res[%v]", index, errMsg, dup, res)

		if utils.DuplicateKey(err) {
			// create single writer to write one by one
			sw := NewDbWriter(bw.conn, bson.M{}, false, bw.fullFinishTs)
			return sw.doUpdateOnInsert(database, collection, metadata, oplogs[index:], upsert)
		}

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

		LOG.Error("doUpdateOnInsert run upsert/update[%v] failed[%v]", upsert, err)
		return err
	}
	return nil
}

func (bw *BulkWriter) doUpdate(database, collection string, metadata bson.M,
	oplogs []*OplogRecord, upsert bool) error {

	var models []mongo.WriteModel
	for _, log := range oplogs {
		log.original.partialLog.Object = oplog.RemoveFiled(log.original.partialLog.Object, versionMark)
		newObject := log.original.partialLog.Object

		if upsert && len(log.original.partialLog.DocumentKey) > 0 {
			models = append(models, mongo.NewUpdateOneModel().
				SetFilter(log.original.partialLog.DocumentKey).
				SetUpdate(bson.D{{"$set", newObject}}).SetUpsert(true))
		} else {
			if upsert {
				LOG.Warn("doUpdate runs upsert but lack documentKey: %v", log.original.partialLog)
			}

			model := mongo.NewUpdateOneModel().
				SetFilter(log.original.partialLog.Query).
				SetUpdate(bson.D{{"$set", newObject}})
			if upsert {
				model.SetUpsert(true)
			}
			models = append(models, model)
		}

		LOG.Debug("bulk_writer: update %v", log.original.partialLog.Object)
	}

	LOG.Debug("bulk_writer: update %v", models)

	res, err := bw.conn.Client.Database(database).Collection(collection).BulkWrite(
		context.Background(), models, nil)

	if err != nil {
		// parse error
		index, errMsg, dup := utils.FindFirstErrorIndexAndMessageN(err)
		var oplogRecord *OplogRecord
		if index != -1 {
			oplogRecord = oplogs[index]
		}
		LOG.Warn("detail error info with index[%v] msg[%v] dup[%v], isFullSyncStage[%v], oplog[%v] res[%v]",
			index, errMsg, dup, parseLastTimestamp(oplogs) <= bw.fullFinishTs,
			*oplogRecord.original.partialLog, res)

		if utils.DuplicateKey(err) {
			RecordDuplicatedOplog(bw.conn, collection, oplogs)
			// create single writer to write one by one
			sw := NewDbWriter(bw.conn, bson.M{}, false, bw.fullFinishTs)
			return sw.doUpdate(database, collection, metadata, oplogs[index:], upsert)
		}

		// error can be ignored
		if IgnoreError(err, "u", parseLastTimestamp(oplogs) <= bw.fullFinishTs) {
			LOG.Warn("ignore error[%v] when run operation[%v], initialSync[%v]", err, "u",
				parseLastTimestamp(oplogs) <= bw.fullFinishTs)

			// re-run (index, len(oplogs) - 1]
			sw := NewDbWriter(bw.conn, bson.M{}, false, bw.fullFinishTs)
			return sw.doUpdate(database, collection, metadata, oplogs[index+1:], upsert)
		}

		LOG.Error("doUpdate run upsert/update[%v] failed[%v]", upsert, err)
		return err
	}
	// TODO(jianyou) deprecate
	//if res != nil {
	//	oplogsLen := int64(len(oplogs))
	//	if res.MatchedCount != oplogsLen && res.InsertedCount+res.UpsertedCount+res.ModifiedCount != oplogsLen {
	//		return fmt.Errorf("update fail(MatchedCount:%d ModifiedCount:%d UpsertedCount:%d InsertedCount:%d oplogsLen:%d) modules:%v",
	//			res.MatchedCount, res.ModifiedCount, res.UpsertedCount, res.InsertedCount,
	//			int64(len(oplogs)), models[0])
	//	}
	//}
	return nil
}

func (bw *BulkWriter) doDelete(database, collection string, metadata bson.M,
	oplogs []*OplogRecord) error {
	var models []mongo.WriteModel
	for _, log := range oplogs {
		models = append(models, mongo.NewDeleteOneModel().SetFilter(log.original.partialLog.Object))

		LOG.Debug("bulk_writer: delete %v", log.original.partialLog)
	}

	opts := options.BulkWrite().SetOrdered(false)
	res, err := bw.conn.Client.Database(database).Collection(collection).BulkWrite(context.Background(), models, opts)
	if err != nil {
		// error can be ignored
		if IgnoreError(err, "d", parseLastTimestamp(oplogs) <= bw.fullFinishTs) {
			LOG.Warn("ignore error[%v] when run operation[%v], initialSync[%v]",
				err, "d", parseLastTimestamp(oplogs) <= bw.fullFinishTs)
			return nil
		}

		LOG.Error("doDelete run delete[%v] failed[%v] res[%v]", models, err, res)
		return err
	}
	return nil
}

func (bw *BulkWriter) doCommand(database string, metadata bson.M, oplogs []*OplogRecord) error {
	var err error
	for _, log := range oplogs {
		newObject := log.original.partialLog.Object
		operation, found := oplog.ExtraCommandName(newObject)
		if conf.Options.FilterDDLEnable || (found && oplog.IsSyncDataCommand(operation)) {
			// execute one by one with sequence order
			if err = RunCommand(database, operation, log.original.partialLog, bw.conn); err == nil {
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

		LOG.Debug("bulk_writer: command %v", log.original.partialLog)
	}
	return nil
}
