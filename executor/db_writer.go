package executor

import (
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
	nimo "github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
	bson "go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

const (
	versionMark       = "$v"
	uuidMark          = "ui"
	shardKeyupdateErr = "Document shard key value updates that cause the doc to move shards must be sent with write batch of size 1"
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

// oplog writer
func NewDbWriter(conn *utils.MongoCommunityConn, metadata bson.M, bulkInsert bool, fullFinishTs int64) BasicWriter {
	if !bulkInsert { // bulk insertion disable
		// LOG.Info("db writer create: SingleWriter")
		return &SingleWriter{conn: conn, fullFinishTs: fullFinishTs}
	} else if _, ok := metadata["g"]; ok { // has gid
		// LOG.Info("db writer create: CommandWriter")
		return &CommandWriter{conn: conn, fullFinishTs: fullFinishTs}
	}
	// LOG.Info("db writer create: BulkWriter")
	return &BulkWriter{conn: conn, fullFinishTs: fullFinishTs} // bulk insertion enable
}

func RunCommand(database, operation string, log *oplog.PartialLog, client *mongo.Client) error {
	defer LOG.Debug("RunCommand run DDL: %v", log.Dump(nil, true))
	dbHandler := client.Database(database)
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
				nimo.AssertTrue(ele.Key == "createIndexes", "should panic when ele.Name != 'createIndexes'")
			} else {
				innerBsonD = append(innerBsonD, ele)
			}
		}
		indexes = append(indexes, log.Object[0]) // createIndexes
		indexes = append(indexes, primitive.E{
			Key: "indexes",
			Value: []bson.D{ // only has 1 bson.D
				innerBsonD,
			},
		})
		err = dbHandler.RunCommand(nil, indexes).Err()
	case "commitIndexBuild":
		/*
			If multiple indexes are created, commitIndexBuild only generate one oplog, CreateIndexes multiple oplogs
			{ "op" : "c", "ns" : "test.$cmd", "ui" : UUID("617ffe90-6dac-4e71-b570-1825422c1896"),
			  "o" : { "commitIndexBuild" : "car", "indexBuildUUID" : UUID("4e9b7457-b612-42bb-bbad-bd6e9a2d63a7"),
			          "indexes" : [
			                      { "v" : 2, "key" : { "count" : 1 }, "name" : "count_1" },
			                      { "v" : 2, "key" : { "type" : 1 }, "name" : "type_1" }
			                      ]},
			  "ts" : Timestamp(1653620229, 6), "t" : NumberLong(1), "v" : NumberLong(2), "wall" : ISODate("2022-05-27T02:57:09.187Z") }

			CreateIndexes Command: db.car.createIndexes([{"count":1},{"type":1}])
			{ "ts" : Timestamp(1653620582, 3), "t" : NumberLong(2), "h" : NumberLong(0), "v" : 2, "op" : "c", "ns" : "test.$cmd", "ui" : UUID("51d35827-e8b5-4891-8818-41326718505d"), "wall" : ISODate("2022-05-27T03:03:02.282Z"), "o" : { "createIndexes" : "car", "v" : 2, "key" : { "type" : 1 }, "name" : "type_1" } }
			{ "ts" : Timestamp(1653620582, 2), "t" : NumberLong(2), "h" : NumberLong(0), "v" : 2, "op" : "c", "ns" : "test.$cmd", "ui" : UUID("51d35827-e8b5-4891-8818-41326718505d"), "wall" : ISODate("2022-05-27T03:03:02.281Z"), "o" : { "createIndexes" : "car", "v" : 2, "key" : { "count" : 1 }, "name" : "count_1" } }
		*/
		var indexes bson.D
		for i, ele := range log.Object {
			if i == 0 {
				indexes = append(indexes, primitive.E{
					Key:   "createIndexes",
					Value: ele.Value.(string),
				})
				nimo.AssertTrue(ele.Key == "commitIndexBuild", "should panic when ele.Name != 'commitIndexBuild'")
			} else {
				if ele.Key == "indexes" {
					indexes = append(indexes, primitive.E{
						Key:   "indexes",
						Value: ele.Value,
					})
				}
			}
		}

		nimo.AssertTrue(len(indexes) >= 2, "indexes must at least have two elements")
		LOG.Debug("RunCommand commitIndexBuild oplog after conversion[%v]", indexes)
		err = dbHandler.RunCommand(nil, indexes).Err()
	case "applyOps":
		/*
		 * Strictly speaking, we should handle applysOps nested case, but it is
		 * complicate to fulfill, so we just use "applyOps" to run the command directly.
		 */
		var store bson.D
		for _, ele := range log.Object {
			if utils.ApplyOpsFilter(ele.Key) {
				continue
			}
			if ele.Key == "applyOps" {
				switch v := ele.Value.(type) {
				case []interface{}:
					for i, ele := range v {
						doc := ele.(bson.D)
						v[i] = oplog.RemoveFiled(doc, uuidMark)
					}
				case bson.D:
					ret := make(bson.D, 0, len(v))
					for _, ele := range v {
						if ele.Key == uuidMark {
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
		err = dbHandler.RunCommand(nil, store).Err()
	case "dropDatabase":
		err = dbHandler.Drop(nil)
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
			err = dbHandler.RunCommand(nil, log.Object).Err()
		} else {
			err = client.Database("admin").RunCommand(nil, log.Object).Err()
		}
	default:
		LOG.Info("type[%s] not found, use applyOps", operation)

		// filter log.Object
		var rec bson.D
		for _, ele := range log.Object {
			if utils.ApplyOpsFilter(ele.Key) {
				continue
			}

			rec = append(rec, ele)
		}
		log.Object = rec // reset log.Object

		var store bson.D
		store = append(store, primitive.E{
			Key: "applyOps",
			Value: []bson.D{
				log.Dump(nil, true),
			},
		})
		err = dbHandler.RunCommand(nil, store).Err()
	}

	return err
}

// true means error can be ignored
// https://github.com/mongodb/mongo/blob/master/src/mongo/base/error_codes.yml
func IgnoreError(err error, op string, isFullSyncStage bool) bool {
	if err == nil {
		return true
	}

	er, ok := err.(mongo.ServerError)
	if !ok {
		return false
	}

	switch op {
	case "i":
		/*if isFullSyncStage {
			if err == 11000 { // duplicate key
				continue
			}
		}*/
	case "u":
		if isFullSyncStage {
			if er.HasErrorCode(28) || er.HasErrorCode(211) { // PathNotViable
				return true
			}
		}
	case "ui":
		if isFullSyncStage {
			if er.HasErrorCode(11000) { // duplicate key
				return true
			}
		}
	case "d":
		if er.HasErrorCode(26) { // NamespaceNotFound
			return true
		}
	case "c":
		if er.HasErrorCode(26) { // NamespaceNotFound
			return true
		}
	default:
		return false
	}

	return false
}

func parseLastTimestamp(oplogs []*OplogRecord) int64 {
	if len(oplogs) == 0 {
		return 0
	}

	return utils.TimeStampToInt64(oplogs[len(oplogs)-1].original.partialLog.Timestamp)
}
