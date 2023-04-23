package executor

import (
	"context"
	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
	LOG "github.com/vinllen/log4go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"strings"
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

/*
 replacement oplog:
	{"ts":{"T":1664192510,"I":1},"t":1,"h":null,"v":2,"op":"u","ns":"test.car","o":[{"Key":"_id","Value":"63318f67024749a30fc12af6"},{"Key":"b","Value":3}],"o2":[{"Key":"_id","Value":"63318f67024749a30fc12af6"}],"PrevOpTime":null,"ui":{"Subtype":4,"Data":"3p7boGbmTvqYSWp42PaZnw=="}}
*/
func (bw *BulkWriter) doUpdate(database, collection string, metadata bson.M,
	oplogs []*OplogRecord, upsert bool) error {

	var models []mongo.WriteModel
	for _, log := range oplogs {
		var newObject interface{}

		updateCmd := "update"
		LOG.Debug("bulk_writer doUpdate: org_doc:%v", log.original.partialLog)
		if oplog.FindFiledPrefix(log.original.partialLog.Object, "$") {
			var oplogErr error

			oplogVer, ok := oplog.GetKey(log.original.partialLog.Object, versionMark).(int32)
			LOG.Debug("bulk_writer doUpdate: have $, org_object:%v "+
				"object_ver:%v\n", log.original.partialLog.Object, oplogVer)

			if ok && oplogVer == 2 {
				if newObject, oplogErr = oplog.DiffUpdateOplogToNormal(log.original.partialLog.Object); oplogErr != nil {
					LOG.Error("doUpdate run Faild err[%v] org_doc[%v]", oplogErr, log.original.partialLog)
					return oplogErr
				}
			} else {
				log.original.partialLog.Object = oplog.RemoveFiled(log.original.partialLog.Object, versionMark)
				newObject = log.original.partialLog.Object
			}

			if upsert && len(log.original.partialLog.DocumentKey) > 0 {
				models = append(models, mongo.NewUpdateOneModel().
					SetFilter(log.original.partialLog.DocumentKey).
					SetUpdate(newObject).SetUpsert(true))
			} else {
				if upsert {
					LOG.Warn("doUpdate runs upsert but lack documentKey: %v", log.original.partialLog)
				}

				model := mongo.NewUpdateOneModel().
					SetFilter(log.original.partialLog.Query).
					SetUpdate(newObject)
				if upsert {
					model.SetUpsert(true)
				}
				models = append(models, model)
			}
		} else {
			newObject = log.original.partialLog.Object

			if upsert && len(log.original.partialLog.DocumentKey) > 0 {
				models = append(models, mongo.NewReplaceOneModel().
					SetFilter(log.original.partialLog.DocumentKey).
					SetReplacement(log.original.partialLog.Object).
					SetUpsert(true))
			} else {
				model := mongo.NewReplaceOneModel().
					SetFilter(log.original.partialLog.Query).
					SetReplacement(log.original.partialLog.Object)
				if upsert {
					model.SetUpsert(true)
				}
				models = append(models, model)
			}
			updateCmd = "replace"
		}

		LOG.Debug("bulk_writer: %s %v aftermodify_doc:%v", updateCmd, newObject, log.original.partialLog)
	}

	LOG.Debug("bulk_writer: update models len %v", len(models))

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
		if strings.Contains(err.Error(), shardKeyupdateErr) {
			LOG.Error("multiUpdateShardKey err_string:%s, index:%d, redo update shardkey singly",
				err.Error(), index)

			sw := NewDbWriter(bw.conn, bson.M{}, false, bw.fullFinishTs)
			return sw.doUpdate(database, collection, metadata, oplogs[index:], upsert)
		}

		LOG.Error("doUpdate run upsert/update[%v] failed[%v]", upsert, err)
		return err
	}
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
			if err = RunCommand(database, operation, log.original.partialLog, bw.conn.Client); err == nil {
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
