package docsyncer

import (
	"fmt"
	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
	"mongoshake/collector/configure"
	utils "mongoshake/common"
)

const (
	CheckpointName   = "name"
	CheckpointAckTs  = "ackTs"
	CheckpointSyncTs = "syncTs"
)

func LoadCheckpoint() (map[string]bson.MongoTimestamp, error) {
	url := conf.Options.ContextStorageUrl
	db := utils.AppDatabase()
	table := conf.Options.ContextStorageCollection
	conn, err := utils.NewMongoConn(url, utils.ConnectModePrimary, true)
	if err != nil {
		return nil, fmt.Errorf("LoadCheckpoint connect to %v failed. %v", url, err)
	}

	ckptMap := make(map[string]bson.MongoTimestamp)
	iter := conn.Session.DB(db).C(table).Find(bson.M{}).Iter()
	ckptDoc := make(map[string]interface{})
	for iter.Next(ckptDoc) {
		replset, ok1 := ckptDoc[CheckpointName].(string)
		ackTs, ok2 := ckptDoc[CheckpointAckTs].(bson.MongoTimestamp)
		if !ok1 || !ok2 {
			return nil, fmt.Errorf("LoadCheckpoint load illegal record %v. ok1[%v] ok2[%v]",
				ckptDoc, ok1, ok2)
		} else {
			ckptMap[replset] = ackTs
			LOG.Info("LoadCheckpoint load replset[%v] ackTs[%v]", replset, utils.TimestampToLog(ackTs))
		}
	}
	if err := iter.Close(); err != nil {
		LOG.Critical("CheckpointManager close iterator failed. %v", err)
	}
	conn.Close()
	return ckptMap, nil
}

func FlushCheckpoint(ckptMap map[string]bson.MongoTimestamp) error {
	url := conf.Options.ContextStorageUrl
	db := utils.AppDatabase()
	table := conf.Options.ContextStorageCollection
	conn, err := utils.NewMongoConn(url, utils.ConnectModePrimary, true)
	if err != nil {
		return fmt.Errorf("FlushCheckpoint connect to %v failed. %v", url, err)
	}
	conn.Session.EnsureSafe(&mgo.Safe{WMode: utils.MajorityWriteConcern})

	for replset, ackTs := range ckptMap {
		ckptDoc := map[string]interface{}{
			CheckpointName:   replset,
			CheckpointAckTs:  ackTs,
			CheckpointSyncTs: ackTs,
		}
		if _, err := conn.Session.DB(db).C(table).
			Upsert(bson.M{CheckpointName: replset}, ckptDoc); err != nil {
			conn.Close()
			return fmt.Errorf("CheckpointManager upsert %v error. %v", ckptDoc, err)
		}
	}
	conn.Close()
	return nil
}
