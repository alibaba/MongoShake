package executor

import (
	"fmt"
	"strings"

	"mongoshake/collector/configure"
	"mongoshake/oplog"

	"github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
	"mongoshake/common"
)

const (
	versionMark = "$v"
	uuidMark    = "ui"
)

type BasicWriter interface {
	// insert operation
	doInsert(database, collection string, metadata bson.M, oplogs []*OplogRecord,
		dupUpdate bool) error

	// update when insert duplicated
	doUpdateOnInsert(database, collection string, metadata bson.M,
		oplogs []*OplogRecord, upsert bool) error

	// update operation
	doUpdate(database, collection string, metadata bson.M,
		oplogs []*OplogRecord, upsert bool) error

	// delete operation
	doDelete(database, collection string, metadata bson.M,
		oplogs []*OplogRecord) error

	/*
	 * command operation
	 * Generally speaking, we should use `applyOps` command in mongodb to insert these data,
	 * but this way will make the oplog in the target bigger than the source.
	 * In the following two cases, this will raise error:
	 *    1. mongoshake cascade: the oplog will be bigger every time go through mongoshake
	 *    2. the oplog is near 16MB(the oplog max threshold), use `applyOps` command will
	 *       make the oplog bigger than 16MB so that rejected by the target mongodb.
	 */
	doCommand(database string, metadata bson.M, oplogs []*OplogRecord) error
}

func NewDbWriter(session *mgo.Session, metadata bson.M, bulkInsert bool) BasicWriter {
	if !bulkInsert { // bulk insertion disable
		// LOG.Info("db writer create: SingleWriter")
		return &SingleWriter{session: session}
	} else if _, ok := metadata["g"]; ok { // has gid
		// LOG.Info("db writer create: CommandWriter")
		return &CommandWriter{session: session}
	}
	// LOG.Info("db writer create: BulkWriter")
	return &BulkWriter{session: session} // bulk insertion enable
}

// use run_command to execute command
type CommandWriter struct {
	// mongo connection
	session *mgo.Session
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
		newObject := oplog.RemoveFiled(log.original.partialLog.Object, versionMark)
		updates = append(updates, bson.M{
			"q":      log.original.partialLog.Query,
			"u":      newObject,
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
				LOG.Info("Execute command (op==c) oplog ddl_enable mode [%t], operation [%s]", conf.Options.FilterDDLEnable, operation)
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

// use general bulk interface such like Insert/Update/Delete to execute command
type BulkWriter struct {
	// mongo connection
	session *mgo.Session
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
		if mgo.IsDup(err) {
			// create single writer to write one by one
			sw := NewDbWriter(bw.session, bson.M{}, false)
			return sw.doUpdateOnInsert(database, collection, metadata, oplogs[index:], upsert)
		}
		return fmt.Errorf("doUpdateOnInsert run upsert/update[%v] failed[%v]", upsert, err)
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
		newObject := oplog.RemoveFiled(log.original.partialLog.Object, versionMark)
		update = append(update, log.original.partialLog.Query, newObject)

		LOG.Debug("writer: update %v", log.original.partialLog)
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
		if mgo.IsDup(err) {
			HandleDuplicated(bw.session.DB(database).C(collection), oplogs, OpUpdate)
			// create single writer to write one by one
			sw := NewDbWriter(bw.session, bson.M{}, false)
			return sw.doUpdate(database, collection, metadata, oplogs[index:], upsert)
		}
		return fmt.Errorf("doUpdate run upsert/update[%v] failed[%v]", upsert, err)
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
		return fmt.Errorf("doDelete run delete[%v] failed[%v]", delete, err)
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
				LOG.Info("Execute command (op==c) oplog ddl_enable mode [%t], operation [%s]",
					conf.Options.FilterDDLEnable, operation)
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

// use general single writer interface to execute command
type SingleWriter struct {
	// mongo connection
	session *mgo.Session
}

func (sw *SingleWriter) doInsert(database, collection string, metadata bson.M, oplogs []*OplogRecord,
	dupUpdate bool) error {
	collectionHandle := sw.session.DB(database).C(collection)
	var upserts []*OplogRecord
	var errMsgs []string
	for _, log := range oplogs {
		// newObject := utils.AdjustDBRef(log.original.partialLog.Object, conf.Options.DBRef)
		newObject := log.original.partialLog.Object
		if err := collectionHandle.Insert(newObject); err != nil {
			if mgo.IsDup(err) {
				upserts = append(upserts, log)
			} else {
				errMsg := fmt.Sprintf("insert data[%v] failed[%v]", newObject, err)
				errMsgs = append(errMsgs, errMsg)
			}
		}

		LOG.Debug("writer: insert %v", log.original.partialLog)
	}

	if len(errMsgs) != 0 {
		return fmt.Errorf(strings.Join(errMsgs, ";"))
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
	var errMsgs []string
	if upsert {
		for _, update := range updates {
			if _, err := collectionHandle.UpsertId(update.id, update.data); err != nil {
				errMsg := fmt.Sprintf("upsert _id[%v] with data[%v] failed[%v]", update.id, update.data,
					err)
				errMsgs = append(errMsgs, errMsg)
			}
		}
	} else {
		for _, update := range updates {
			if err := collectionHandle.UpdateId(update.id, update.data); err != nil && mgo.IsDup(err) == false {
				errMsg := fmt.Sprintf("update _id[%v] with data[%v] failed[%v]", update.id, update.data,
					err.Error())
				errMsgs = append(errMsgs, errMsg)
			}
		}
	}

	if len(errMsgs) != 0 {
		return fmt.Errorf(strings.Join(errMsgs, "; "))
	}
	return nil
}

func (sw *SingleWriter) doUpdate(database, collection string, metadata bson.M,
	oplogs []*OplogRecord, upsert bool) error {
	collectionHandle := sw.session.DB(database).C(collection)
	var errMsgs []string
	if upsert {
		for _, log := range oplogs {
			//newObject := utils.AdjustDBRef(log.original.partialLog.Object, conf.Options.DBRef)
			//// we should handle the special case: "o" filed may include "$v" in mongo-3.6 which is not support in mgo.v2 library
			//if _, ok := newObject[versionMark]; ok {
			//	delete(newObject, versionMark)
			//}
			newObject := oplog.RemoveFiled(log.original.partialLog.Object, versionMark)
			_, err := collectionHandle.Upsert(log.original.partialLog.Query, newObject)
			if err != nil {
				if mgo.IsDup(err) {
					HandleDuplicated(collectionHandle, oplogs, OpUpdate)
					continue
				}
				errMsg := fmt.Sprintf("doUpdate[upsert] old-data[%v] with new-data[%v] failed[%v]",
					log.original.partialLog.Query, newObject, err)
				errMsgs = append(errMsgs, errMsg)
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
			newObject := oplog.RemoveFiled(log.original.partialLog.Object, versionMark)
			err := collectionHandle.Update(log.original.partialLog.Query, newObject)
			if err != nil {
				if utils.IsNotFound(err) {
					return fmt.Errorf("doUpdate[update] data[%v] not found", log.original.partialLog.Query)
				} else if mgo.IsDup(err) {
					HandleDuplicated(collectionHandle, oplogs, OpUpdate)
				} else {
					errMsg := fmt.Sprintf("doUpdate[update] old-data[%v] with new-data[%v] failed[%v]",
						log.original.partialLog.Query, newObject, err)
					errMsgs = append(errMsgs, errMsg)
				}
			}

			LOG.Debug("writer: update %v", log.original.partialLog)
		}
	}

	if len(errMsgs) != 0 {
		return fmt.Errorf(strings.Join(errMsgs, "; "))
	}
	return nil

}

func (sw *SingleWriter) doDelete(database, collection string, metadata bson.M,
	oplogs []*OplogRecord) error {
	collectionHandle := sw.session.DB(database).C(collection)
	var errMsgs []string
	for _, log := range oplogs {
		// ignore ErrNotFound
		id := oplog.GetKey(log.original.partialLog.Object, "")
		if err := collectionHandle.RemoveId(id); err != nil {
			if utils.IsNotFound(err) {
				return fmt.Errorf("doDelete data[%v] not found", log.original.partialLog.Query)
			} else {
				errMsg := fmt.Sprintf("delete data[%v] failed[%v]", log.original.partialLog.Query,
					err)
				errMsgs = append(errMsgs, errMsg)
			}
		}

		LOG.Debug("writer: delete %v", log.original.partialLog)
	}

	if len(errMsgs) != 0 {
		return fmt.Errorf(strings.Join(errMsgs, "; "))
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
				LOG.Info("Execute command (op==c) oplog ddl_enable mode [%t], operation [%s]",
					conf.Options.FilterDDLEnable, operation)
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

func RunCommand(database, operation string, log *oplog.PartialLog, session *mgo.Session) error {
	defer LOG.Debug("RunCommand run DDL: %v", log.Dump(nil, true))
	dbHandler := session.DB(database)
	LOG.Info("RunCommand run DDL with type[%s]", operation)
	var err error
	switch operation {
	case "createIndexes":
		/*
		 * after v3.6, the given oplog should have uuid when run applyOps with createIndexes.
		 * so we modify oplog base this ref:
		 * https://docs.mongodb.com/manual/reference/command/createIndexes/#dbcmd.createIndexes
		 */
		var innerBsonD, indexes bson.D
		for i, ele := range log.Object {
			if i == 0 {
				nimo.AssertTrue(ele.Name == "createIndexes", "should panic when ele.Name != 'createIndexes'")
			} else {
				innerBsonD = append(innerBsonD, ele)
			}
		}
		indexes = append(indexes, log.Object[0]) // createIndexes
		indexes = append(indexes, bson.DocElem{
			Name: "indexes",
			Value: []bson.D{ // only has 1 bson.D
				innerBsonD,
			},
		})
		err = dbHandler.Run(indexes, nil)
	case "applyOps":
		/*
		 * Strictly speaking, we should handle applysOps nested case, but it is
		 * complicate to fulfill, so we just use "applyOps" to run the command directly.
		 */
		var store bson.D
		for _, ele := range log.Object {
			if utils.ApplyOpsFilter(ele.Name) {
				continue
			}
			if ele.Name == "applyOps" {
				arr := ele.Value.([]interface{})
				for i, ele := range arr {
					doc := ele.(bson.D)
					arr[i] = oplog.RemoveFiled(doc, uuidMark)
				}
			}
			store = append(store, ele)
		}
		err = dbHandler.Run(store, nil)
	case "dropDatabase":
		err = dbHandler.DropDatabase()
	case "create":
		fallthrough
	case "collMod":
		fallthrough
	case "drop":
		fallthrough
	case "deleteIndex":
		fallthrough
	case "deleteIndexes":
		fallthrough
	case "dropIndex":
		fallthrough
	case "dropIndexes":
		fallthrough
	case "convertToCapped":
		fallthrough
	case "renameCollection":
		fallthrough
	case "emptycapped":
		if !oplog.IsRunOnAdminCommand(operation) {
			err = dbHandler.Run(log.Object, nil)
		} else {
			err = session.DB("admin").Run(log.Object, nil)
		}
	default:
		LOG.Info("type[%s] not found, use applyOps", operation)

		// filter log.Object
		var rec bson.D
		for _, ele := range log.Object {
			if utils.ApplyOpsFilter(ele.Name) {
				continue
			}

			rec = append(rec, ele)
		}
		log.Object = rec // reset log.Object

		var store bson.D
		store = append(store, bson.DocElem{
			Name: "applyOps",
			Value: []bson.D{
				log.Dump(nil, true),
			},
		})
		err = dbHandler.Run(store, nil)
	}

	return err
}
