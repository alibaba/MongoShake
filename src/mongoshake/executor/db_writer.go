package executor

import (
	"mongoshake/oplog"
	"mongoshake/common"

	"github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
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

func NewDbWriter(session *mgo.Session, metadata bson.M, bulkInsert bool, fullFinishTs int64) BasicWriter {
	if !bulkInsert { // bulk insertion disable
		// LOG.Info("db writer create: SingleWriter")
		return &SingleWriter{session: session, fullFinishTs: fullFinishTs}
	} else if _, ok := metadata["g"]; ok { // has gid
		// LOG.Info("db writer create: CommandWriter")
		return &CommandWriter{session: session, fullFinishTs: fullFinishTs}
	}
	// LOG.Info("db writer create: BulkWriter")
	return &BulkWriter{session: session, fullFinishTs: fullFinishTs} // bulk insertion enable
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
				switch v := ele.Value.(type) {
				case []interface{}:
					for i, ele := range v {
						doc := ele.(bson.D)
						v[i] = oplog.RemoveFiled(doc, uuidMark)
					}
				case bson.D:
					ret := make(bson.D, 0, len(v))
					for _, ele := range v {
						if ele.Name == uuidMark {
							continue
						}
						ret = append(ret, ele)
					}
					ele.Value = ret
				case []bson.M:
					for _, ele := range v {
						if _, ok := ele[uuidMark]; ok {
							delete(ele, uuidMark)
						}
					}
				}

			}
			store = append(store, ele)
		}
		err = dbHandler.Run(store, nil)
	case "dropDatabase":
		err = dbHandler.DropDatabase()
	case "create":
		if oplog.GetKey(log.Object, "autoIndexId") != nil &&
			oplog.GetKey(log.Object, "idIndex") != nil {
			// exits "autoIndexId" and "idIndex", remove "autoIndexId"
			log.Object = oplog.RemoveFiled(log.Object, "autoIndexId")
		}
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

// true means error can be ignored
// https://github.com/mongodb/mongo/blob/master/src/mongo/base/error_codes.yml
func IgnoreError(err error, op string, isFullSyncStage bool) bool {
	if err == nil {
		return true
	}

	errorCode := mgo.ErrorCodeList(err)
	if err != nil && len(errorCode) == 0 {
		return false
	}

	for _, err := range errorCode {
		switch op {
		case "i":
		case "u":
			if isFullSyncStage {
				if err == 28 { // PathNotViable
					continue
				}
			}
		case "d":
			if err == 26 { // NamespaceNotFound
				continue
			}
		case "c":
			if err == 26 { // NamespaceNotFound
				continue
			}
		default:
			return false
		}
		return false
	}

	return true
}

func parseLastTimestamp(oplogs []*OplogRecord) int64 {
	if len(oplogs) == 0 {
		return 0
	}

	return utils.TimestampToInt64(oplogs[len(oplogs) - 1].original.partialLog.Timestamp)
}