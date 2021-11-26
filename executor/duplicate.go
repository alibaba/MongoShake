package executor

import (
	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"

	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
)

func HandleDuplicated(conn *utils.MongoCommunityConn, coll string, records []*OplogRecord, op int8) {
	for _, record := range records {
		log := record.original.partialLog
		switch conf.Options.IncrSyncConflictWriteTo {
		case DumpConflictToDB:
			// discard conflict again
			conn.Client.Database(utils.APPConflictDatabase).Collection(coll).InsertOne(nil, log.Object)
		case DumpConflictToSDK, NoDumpConflict:
		}

		if utils.IncrSentinelOptions.DuplicatedDump {
			// TODO(zhangst)
			//SnapshotDiffer{op: op, log: log}.dump(collection)
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
		coll.Find(bson.M{"_id": oplog.GetKey(s.log.Object, "")}).One(s.foundInDB)
	}
	s.write2Log()
}
