package executor

import (
	"mongoshake/collector/configure"
	"mongoshake/common"
	"mongoshake/oplog"

	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
)

func HandleDuplicated(collection *mgo.Collection, records []*OplogRecord, op int8) {
	for _, record := range records {
		log := record.original.partialLog
		switch conf.Options.IncrSyncConflictWriteTo {
		case DumpConflictToDB:
			// general process : write record to specific database
			session := collection.Database.Session
			// discard conflict again
			session.DB(utils.APPConflictDatabase).C(collection.Name).Insert(log.Object)
		case DumpConflictToSDK, NoDumpConflict:
		}

		if utils.IncrSentinelOptions.DuplicatedDump {
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
		coll.Find(bson.M{"_id": oplog.GetKey(s.log.Object, "")}).One(s.foundInDB)
	}
	s.write2Log()
}
