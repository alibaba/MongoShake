package executor

import(
	"fmt"
	"strings"

	"mongoshake/common"
	"mongoshake/collector/configure"
	"mongoshake/oplog"

	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
	LOG "github.com/vinllen/log4go"
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

	// command operation
	doCommand(database string, metadata bson.M, oplogs []*OplogRecord) error
}

func NewDbWriter(session *mgo.Session, metadata bson.M) BasicWriter {
	//if _, ok := metadata["g"]; ok {
	//	return &CommandWriter{session: session}
	//}
	//return &GeneralWriter{session: session}
	return &CommandWriter{session: session}
}

// use run_command to execute command
type CommandWriter struct {
	// mongo connection
	session *mgo.Session
}

func (cw *CommandWriter) doInsert(database, collection string, metadata bson.M, oplogs []*OplogRecord,
		dupUpdate bool) error {
	var inserts []bson.M
	for _, log := range oplogs {
		inserts = append(inserts, log.original.partialLog.Object)
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
			return cw.doUpdateOnInsert(database, collection, metadata, oplogs, conf.Options.ReplayerExecutorUpsert)
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
		if id, exist := log.original.partialLog.Object["_id"]; exist {
			updates = append(updates, bson.M{
				"q":      bson.M{"_id": id},
				"u":      log.original.partialLog.Object,
				"upsert": upsert,
				"multi":  false,
			})
		} else {
			LOG.Warn("Insert on duplicated update _id look up failed. %v", log)
		}
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
		updates = append(updates, bson.M{
			"q":      log.original.partialLog.Query,
			"u":      log.original.partialLog.Object,
			"upsert": upsert,
			"multi":  false})
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
		operation, found := extraCommandName(log.original.partialLog.Object)
		if !conf.Options.ReplayerDMLOnly || (found && isSyncDataCommand(operation)) {
			// execute one by one with sequence order
			if err = cw.applyOps(database, metadata, []*oplog.PartialLog{log.original.
				partialLog}); err == nil {
				LOG.Info("Execute command (op==c) oplog dml_only mode [%t], operation [%s]", conf.Options.ReplayerDMLOnly, operation)
			} else {
				return err
			}
		} else {
			// exec.batchExecutor.ReplMetric.AddFilter(1)
		}
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

// use general interface such like Insert/Update/Delete to execute command
type GeneralWriter struct {
	// mongo connection
	session *mgo.Session
}

func (gw *GeneralWriter) doInsert(database, collection string, metadata bson.M, oplogs []*OplogRecord,
		dupUpdate bool) error {
	var inserts []interface{}
	for _, log := range oplogs {
		inserts = append(inserts, log.original.partialLog.Object)
	}
	collectionHandle := gw.session.DB(database).C(collection)

	var err error
	if err = collectionHandle.Insert(inserts...); err != nil {
		return nil
	}

	if mgo.IsDup(err) {
		HandleDuplicated(collectionHandle, oplogs, OpInsert)
		// update on duplicated key occur
		if dupUpdate {
			LOG.Info("Duplicated document found. reinsert or update to [%s] [%s]", database, collection)
			return gw.doUpdateOnInsert(database, collection, metadata, oplogs, conf.Options.ReplayerExecutorUpsert)
		}
		return nil
	}
	return err
}

func (gw *GeneralWriter) doUpdateOnInsert(database, collection string, metadata bson.M,
	oplogs []*OplogRecord, upsert bool) error {
	type pair struct {
		id   interface{}
		data interface{}
	}
	var updates []*pair
	for _, log := range oplogs {
		// insert must have _id
		if id, exist := log.original.partialLog.Object["_id"]; exist {
			updates = append(updates, &pair{id: id, data: log.original.partialLog.Object})
		} else {
			LOG.Warn("Insert on duplicated update _id look up failed. %v", log)
		}
	}

	collectionHandle := gw.session.DB(database).C(collection)
	var errMsgs []string
	if upsert {
		for _, update := range updates {
			if _, err := collectionHandle.UpsertId(update.id, update.data); err != nil {
				errMsg := fmt.Sprintf("upsert _id[%v] with data[%v] failed[%v]", update.id, update.data,
					err.Error())
				// LOG.Warn(errMsg)
				errMsgs = append(errMsgs, errMsg)
			}
		}
	} else {
		for _, update := range updates {
			if err := collectionHandle.UpdateId(update.id, update.data); err != nil && mgo.IsDup(err) == false {
				errMsg := fmt.Sprintf("update _id[%v] with data[%v] failed[%v]", update.id, update.data,
					err.Error())
				// LOG.Warn(errMsg)
				errMsgs = append(errMsgs, errMsg)
			}
		}
	}

	if len(errMsgs) != 0 {
		return fmt.Errorf(strings.Join(errMsgs, "; "))
	}
	return nil
}

func (gw *GeneralWriter) doUpdate(database, collection string, metadata bson.M,
		oplogs []*OplogRecord, upsert bool) error {
	collectionHandle := gw.session.DB(database).C(collection)
	var errMsgs []string
	var err error
	for _, log := range oplogs {
		if upsert {
			_, err = collectionHandle.Upsert(log.original.partialLog.Query, log.original.partialLog.Object)
		} else {
			err = collectionHandle.Update(log.original.partialLog.Query, log.original.partialLog.Object)
		}
		if err != nil && mgo.IsDup(err) == false && !isNotFound(err) {
			errMsg := fmt.Sprintf("update/upsert old-data[%v] with new-data[%v] failed[%v]",
				log.original.partialLog.Query, log.original.partialLog.Object, err.Error())
			errMsgs = append(errMsgs, errMsg)
		}
	}

	if len(errMsgs) != 0 {
		return fmt.Errorf(strings.Join(errMsgs, "; "))
	}
	return nil
}

func (gw *GeneralWriter) doDelete(database, collection string, metadata bson.M,
		oplogs []*OplogRecord) error {
	collectionHandle := gw.session.DB(database).C(collection)
	var errMsgs []string
	for _, log := range oplogs {
		// ignore ErrNotFound
		if err := collectionHandle.Remove(log.original.partialLog.Query); err != nil && !isNotFound(err) {
			errMsg := fmt.Sprintf("delete data[%v] failed[%v]", log.original.partialLog.Query,
				err.Error())
			errMsgs = append(errMsgs, errMsg)
		}
	}

	if len(errMsgs) != 0 {
		return fmt.Errorf(strings.Join(errMsgs, "; "))
	}
	return nil
}

func (gw *GeneralWriter) doCommand(database string, metadata bson.M, oplogs []*OplogRecord) error {
	var err error
	for _, log := range oplogs {
		operation, found := extraCommandName(log.original.partialLog.Object)
		if !conf.Options.ReplayerDMLOnly || (found && isSyncDataCommand(operation)) {
			// execute one by one with sequence order
			if err = gw.applyOps(database, operation, log.original.partialLog); err == nil {
				LOG.Info("Execute command (op==c) oplog dml_only mode [%t], operation [%s]", conf.Options.ReplayerDMLOnly, operation)
			} else {
				return err
			}
		} else {
			// exec.batchExecutor.ReplMetric.AddFilter(1)
		}
	}
	return nil
}

func (gw *GeneralWriter) applyOps(database, operation string, log *oplog.PartialLog) error {
	dbHandle := gw.session.DB(database)

	var err error
	switch operation {
	case "dropDatabase":
		err = dbHandle.DropDatabase()
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
	// case "renameCollection":
	// 	fallthrough
	case "convertToCapped":
		fallthrough
	case "emptycapped":
		// convert bson.M to bson.D
		var store bson.D
		for key, value := range log.Object {
			store = append(store, bson.DocElem{Name: key, Value: value})
		}
		// call Run()
		err = dbHandle.Run(store, nil)
	default:
		LOG.Info("applyOps meets type[%s] which is not implemented", operation)
	}

	return err
}

func HandleDuplicated(collection *mgo.Collection, records []*OplogRecord, op int8) {
	for _, record := range records {
		log := record.original.partialLog
		switch conf.Options.ReplayerConflictWriteTo {
		case DumpConflictToDB:
			// general process : write record to specific database
			session := collection.Database.Session
			// discard conflict again
			session.DB(utils.APPConflictDatabase).C(collection.Name).Insert(log.Object)
		case DumpConflictToSDK, NoDumpConflict:
		}

		if utils.SentinelOptions.DuplicatedDump {
			SnapshotDiffer{op: op, log: log}.dump(collection)
		}
	}
}

type SnapshotDiffer struct {
	op        int8
	log       *oplog.PartialLog
	foundInDB bson.M
}

func (s SnapshotDiffer) write2Log() {
	LOG.Info("Found in DB ==> %v", s.foundInDB)
	LOG.Info("Oplog ==> %v", s.log.Object)
}

func (s SnapshotDiffer) dump(coll *mgo.Collection) {
	if s.op == OpUpdate {
		coll.Find(s.log.Query).One(s.foundInDB)
	} else {
		coll.Find(bson.M{"_id": s.log.Object["_id"]}).One(s.foundInDB)
	}
	s.write2Log()
}

func isNotFound(err error) bool {
	return err.Error() == mgo.ErrNotFound.Error()
}