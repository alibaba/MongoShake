package docsyncer

import (
	"fmt"
	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
	"mongoshake/collector/configure"
	utils "mongoshake/common"
)

func LoadCheckpoint() (map[string]bson.MongoTimestamp, error) {
	url := conf.Options.ContextStorageUrl
	db := utils.AppDatabase()
	table := conf.Options.ContextStorageCollection
	conn, err := utils.NewMongoConn(url, utils.ConnectModePrimary, true)
	if err != nil {
		return nil, fmt.Errorf("LoadCheckpoint connect to %v failed. %v", url, err)
	}
	defer conn.Close()
	ckptMap := make(map[string]bson.MongoTimestamp)

	var versionDoc map[string]interface{}
	if err := conn.Session.DB(db).C(table).
		Find(bson.M{}).One(&versionDoc); err != nil && err != mgo.ErrNotFound {
		return nil, LOG.Critical("LoadCheckpoint versionDoc error. %v", err)
	}
	oplogTable := table + "_oplog"
	stage, ok := versionDoc[utils.CheckpointStage]
	if ok {
		switch stage {
		case utils.StageFlushed:
			oplogTable = "tmp_" + oplogTable
		case utils.StageRename:
			// rename tmp table to original table
			colNames, err := conn.Session.DB(db).CollectionNames()
			if err != nil {
				return nil, LOG.Critical("LoadCheckpoint obtain collection names error. %v", err)
			}
			for _, col := range colNames {
				if col == "tmp_"+oplogTable {
					oplogTable = "tmp_" + oplogTable
				}
			}
		}
	}

	iter := conn.Session.DB(db).C(oplogTable).Find(bson.M{}).Iter()
	ckptDoc := make(map[string]interface{})
	for iter.Next(ckptDoc) {
		replset, ok1 := ckptDoc[utils.CheckpointName].(string)
		ackTs, ok2 := ckptDoc[utils.CheckpointAckTs].(bson.MongoTimestamp)
		if !ok1 || !ok2 {
			return nil, fmt.Errorf("LoadCheckpoint load illegal record %v. ok1[%v] ok2[%v]",
				ckptDoc, ok1, ok2)
		} else {
			ckptMap[replset] = ackTs
			LOG.Info("LoadCheckpoint load replset[%v] ackTs[%v]", replset, utils.TimestampToLog(ackTs))
		}
	}
	if err := iter.Close(); err != nil {
		LOG.Critical("LoadCheckpoint close iterator failed. %v", err)
	}
	return ckptMap, nil
}

func FlushCheckpoint(ckptMap map[string]bson.MongoTimestamp) error {
	url := conf.Options.ContextStorageUrl
	db := utils.AppDatabase()
	table := conf.Options.ContextStorageCollection
	oplogTable := table + "_oplog"
	conn, err := utils.NewMongoConn(url, utils.ConnectModePrimary, true)
	if err != nil {
		return fmt.Errorf("FlushCheckpoint connect to %v failed. %v", url, err)
	}
	conn.Session.EnsureSafe(&mgo.Safe{WMode: utils.MajorityWriteConcern})
	defer conn.Close()

	if _, err := conn.Session.DB(db).C(table).
		Upsert(bson.M{}, bson.M{utils.CheckpointStage: utils.StageOriginal}); err != nil {
		return LOG.Critical("FlushCheckpoint upsert versionDoc error. %v", err)
	}
	for replset, ackTs := range ckptMap {
		ckptDoc := map[string]interface{}{
			utils.CheckpointName:   replset,
			utils.CheckpointAckTs:  ackTs,
			utils.CheckpointSyncTs: ackTs,
		}
		if _, err := conn.Session.DB(db).C(oplogTable).
			Upsert(bson.M{utils.CheckpointName: replset}, ckptDoc); err != nil {
			return fmt.Errorf("FlushCheckpoint upsert %v error. %v", ckptDoc, err)
		}
	}
	return nil
}
