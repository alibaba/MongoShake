package executor

import (
	"context"
	"fmt"

	nimo "github.com/gugemichael/nimo4go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
	l "github.com/alibaba/MongoShake/v2/pkg/log"
)

const (
	versionMark       = "$v"
	uuidMark          = "ui"
	shardKeyUpdateErr = "Document shard key value updates that cause the doc to move shards must be sent with write batch of size 1"
)

type BasicWriter interface {
	// insert operation
	doInsert(database, collection string, metadata bson.E, oplogs []*OplogRecord, dupUpdate bool) error

	// update when insert duplicated
	doUpdateOnInsert(database, collection string, metadata bson.E, oplogs []*OplogRecord, upsert bool) error

	// update operation
	doUpdate(database, collection string, metadata bson.E, oplogs []*OplogRecord, upsert bool) error

	// delete operation
	doDelete(database, collection string, metadata bson.E, oplogs []*OplogRecord) error

	/*
	 * command operation
	 * Generally speaking, we should use `applyOps` command in mongodb to insert these data,
	 * but this way will make the oplog in the target bigger than the source.
	 * In the following two cases, this will raise error:
	 *    1. mongoshake cascade: the oplog will be bigger every time go through mongoshake
	 *    2. the oplog is near 16MB(the oplog max threshold), use `applyOps` command will
	 *       make the oplog bigger than 16MB so that rejected by the target mongodb.
	 */
	doCommand(database string, metadata bson.E, oplogs []*OplogRecord) error
}

// NewDbWriter return a new writer, could be:
// 1) SingleWriter;
// 2) BulkWriter; (by default for MongoDB3.2+)
// 3) CommandWriter; (for gid enabled)
func NewDbWriter(conn *utils.MongoCommunityConn, metadata bson.E, bulkInsert bool, fullFinishTs int64) BasicWriter {
	if !bulkInsert { // bulk insertion disable
		// l.Logger.Infof("db writer create: SingleWriter")
		return &SingleWriter{conn: conn, fullFinishTs: fullFinishTs}
	} else if metadata.Key == "g" { // has gid
		// l.Logger.Infof("db writer create: CommandWriter")
		return &CommandWriter{conn: conn, fullFinishTs: fullFinishTs}
	}
	// l.Logger.Infof("db writer create: BulkWriter")
	return &BulkWriter{conn: conn, fullFinishTs: fullFinishTs} // bulk insertion enable
}

func RunCommand(database, operation string, log *oplog.PartialLog, client *mongo.Client) error {
	defer l.Logger.Debugf("RunCommand run DDL: %v", log.Dump(nil, true))
	dbHandler := client.Database(database)
	l.Logger.Infof("RunCommand run DDL with type[%s]", operation)
	var err error
	switch operation {
	case "createIndexes":
		/*
		 * after v3.6, the given oplog should have uuid when run applyOps with createIndexes.
		 * so we modify oplog base this ref:
		 * https://docs.mongodb.com/manual/reference/command/createIndexes/#dbcmd.createIndexes
		 * Note: Here we have to handle original createIndex oplog (only 1 index) or
		 * createIndexes oplog (maybe multi indexes) converted from change events.
		 */
		var innerBsonD, command bson.D
		var indexes bson.A
		var ok bool
		for i, ele := range log.Object {
			if i == 0 {
				nimo.AssertTrue(ele.Key == "createIndexes", "should panic when ele.Name != 'createIndexes'")
			} else if ele.Key == "indexes" {
				indexes, ok = ele.Value.(bson.A)
				if !ok {
					err = fmt.Errorf("unexpected createIndexes oplog:%v", log)
				}
			} else {
				// "o" : { "createIndexes" : "test", "v" : 2, "key" : { "a" : "hashed" }, "name" : "a_hashed" }
				innerBsonD = append(innerBsonD, ele)
			}
		}
		command = append(command, log.Object[0]) // createIndexes
		if len(indexes) != 0 {
			command = append(command, primitive.E{Key: "indexes", Value: indexes})
		} else {
			command = append(command, primitive.E{
				Key: "indexes",
				Value: []bson.D{ // only has 1 bson.D
					innerBsonD,
				},
			})
		}
		err = dbHandler.RunCommand(nil, command).Err()
	case "commitIndexBuild":
		/*
			If multiple indexes are created, 'commitIndexBuild' only generate one oplog,
			however 'createIndexes' will generate multi oplogs.
				{
					"op" : "c",
					"ns" : "test.$cmd",
					"ui" : UUID("617ffe90-6dac-4e71-b570-1825422c1896"),
					"o" : {
						"commitIndexBuild" : "car",
						"indexBuildUUID" : UUID("4e9b7457-b612-42bb-bbad-bd6e9a2d63a7"),
						"indexes" : [
							{ "v" : 2, "key" : { "count" : 1 }, "name" : "count_1" },
							{ "v" : 2, "key" : { "type" : 1 }, "name" : "type_1" }
						]},
					"ts" : Timestamp(1653620229, 6),
					"t" : NumberLong(1),
					"v" : NumberLong(2),
					"wall" : ISODate("2022-05-27T02:57:09.187Z")
				}
				Equivalent 'createIndexes' command:
				> db.car.createIndexes([{"count":1},{"type":1}])
				{
					"ts": Timestamp(1653620582, 3),
					"t": NumberLong(2),
					"h": NumberLong(0),
					"v": 2,
					"op": "c",
					"ns": "test.$cmd",
					"ui": UUID("51d35827-e8b5-4891-8818-41326718505d"),
					"wall": ISODate("2022-05-27T03:03:02.282Z"),
					"o": {
						"createIndexes": "car",
						"v": 2,
						"key": {"type": 1},
						"name": "type_1"
					}
				}
				{
					"ts": Timestamp(1653620582, 2),
					"t": NumberLong(2),
					"h": NumberLong(0),
					"v": 2,
					"op": "c",
					"ns": "test.$cmd",
					"ui": UUID("51d35827-e8b5-4891-8818-41326718505d"),
					"wall": ISODate("2022-05-27T03:03:02.281Z"),
					"o": {
						"createIndexes": "car",
						"v": 2,
						"key": {"count": 1},
						"name": "count_1"
					}
				}
		*/
		var command bson.D
		for i, ele := range log.Object {
			if i == 0 {
				command = append(command, primitive.E{
					Key:   "createIndexes",
					Value: ele.Value.(string),
				})
				nimo.AssertTrue(ele.Key == "commitIndexBuild", "should panic when ele.Name != 'commitIndexBuild'")
			} else {
				if ele.Key == "indexes" {
					command = append(command, primitive.E{
						Key:   "indexes",
						Value: ele.Value,
					})
				}
			}
		}

		nimo.AssertTrue(len(command) >= 2, "createIndexes command must at least have two elements")
		l.Logger.Debugf("RunCommand commitIndexBuild oplog after conversion[%v]", command)
		err = dbHandler.RunCommand(nil, command).Err()
	case "applyOps":
		/*
		 * Strictly speaking, we should handle applyOps nested case, but it is
		 * complicate to fulfill, so we just use "applyOps" to run the command directly.
		 */
		ops, err := oplog.NormalizeApplyOps(log.Object)
		if err != nil {
			return err
		}

		var store bson.D
		for _, ele := range log.Object {
			if utils.ApplyOpsFilter(ele.Key) {
				continue
			}
			if ele.Key == "applyOps" {
				for i, doc := range ops {
					ops[i] = oplog.RemoveFiled(doc, uuidMark)
				}
				ele.Value = ops
			}
			store = append(store, ele)
		}
		err = dbHandler.RunCommand(nil, store).Err()
	case "dropDatabase":
		err = dbHandler.Drop(nil)
	case "renameCollection":
		// handle '"dropTarget": UUID("52c1c147-2408-4d96-9d0f-889a759ab079")' in object,
		// change to {dropTarget: true} as described in:
		//https://www.mongodb.com/docs/v5.0/reference/method/db.collection.renameCollection/
		tmpCmd := log.Object
		if log.Object != nil && oplog.GetKey(log.Object, "dropTarget") != nil {
			oplog.SetFiled(tmpCmd, "dropTarget", true)
		}
		err = client.Database("admin").RunCommand(nil, tmpCmd).Err()
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
		// using applyOps directly, refer to mongo-tools:HandleNonTxnOp
		// ensure every shard collection's uuid is same
		doc := bson.M{}
		doc["ts"] = log.Timestamp
		if log.Version > 0 {
			doc["v"] = log.Version
		}
		doc["op"] = log.Operation
		if log.Gid != "" {
			doc["g"] = log.Gid
		}
		doc["ns"] = log.Namespace
		if log.Object != nil && len(log.Object) > 0 {
			doc["o"] = log.Object
		}
		if log.Query != nil && len(log.Query) > 0 {
			doc["o2"] = log.Query
		}
		// uuid BinDataType == 3 or 4, see "BinDataType" in src/mongo/bson/bsontypes.h
		if log.UI != nil && (log.UI.Subtype == 3 || log.UI.Subtype == 4) {
			doc["ui"] = log.UI
		}
		l.Logger.Infof("run applyOps for %s: %v", operation, doc)
		err = client.Database("admin").RunCommand(context.Background(),
			bson.D{{"applyOps", []interface{}{doc}}}, nil).Err()
		if err != nil {
			l.Logger.Errorf("run applyOps[%v] for %s failed: %v", doc, operation, err)
			return err
		}
	case "convertToCapped":
		fallthrough
	case "emptycapped":
		fallthrough

		// NOTE: below commands are carefully-constructed oplogs transformed from change events which could call
		//'runCommand()' directly, this does not mean that these exist in the native MongoDB oplog.
	case "shardCollection":
		fallthrough
	case "reshardCollection":
		fallthrough
	case "refineCollectionShardKey":
		if !oplog.IsRunOnAdminCommand(operation) {
			err = dbHandler.RunCommand(nil, log.Object).Err()
		} else {
			err = client.Database("admin").RunCommand(nil, log.Object).Err()
		}
	default:
		l.Logger.Infof("type[%s] not found, use applyOps", operation)

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

// IgnoreError return true if error can be ignored
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
			if err == 11000 { // DuplicateKey
				continue
			}
		}*/
	case "u":
		if isFullSyncStage {
			if er.HasErrorCode(28) || er.HasErrorCode(211) { // PathNotViable or KeyNotFound
				return true
			}
		}
	case "ui":
		if isFullSyncStage {
			if er.HasErrorCode(11000) { // DuplicateKey
				return true
			}
		}
	case "d":
		if er.HasErrorCode(26) { // NamespaceNotFound
			return true
		}
	case "c":
		if er.HasErrorCode(26) || er.HasErrorCode(48) { // NamespaceNotFound or NamespaceExists
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
