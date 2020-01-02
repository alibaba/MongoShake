package collector

import (
	"fmt"
	nimo "github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
	"mongoshake/collector/configure"
	utils "mongoshake/common"
	"sync"
	"time"
)

const (
	StorageTypeAPI = "api"
	StorageTypeDB  = "database"

	NonDirectCheckpointInterval = 180 // s
)

type Persist interface {
	Load(tablePrefix string) error
	Flush(tablePrefix string) error
	GetTableList(tablePrefix string) []string
}

type CheckpointManager struct {
	syncMap   map[string]*OplogSyncer
	mutex     sync.RWMutex
	FlushChan chan bool

	url   string
	db    string
	table string
	conn  *utils.MongoConn

	beginTsMap  map[string]bson.MongoTimestamp
	persistList []Persist
}

func NewCheckpointManager(beginTsMap map[string]bson.MongoTimestamp) *CheckpointManager {
	manager := &CheckpointManager{
		syncMap:    make(map[string]*OplogSyncer),
		FlushChan:  make(chan bool),
		url:        conf.Options.ContextStorageUrl,
		db:         utils.AppDatabase(),
		table:      conf.Options.ContextStorageCollection,
		beginTsMap: beginTsMap,
	}
	manager.persistList = append(manager.persistList, manager)
	return manager
}

func (manager *CheckpointManager) addOplogSyncer(syncer *OplogSyncer) {
	manager.syncMap[syncer.replset] = syncer
}

func (manager *CheckpointManager) registerPersis(persist Persist) {
	manager.persistList = append(manager.persistList, persist)
}

func (manager *CheckpointManager) start() {
	startTime := time.Now()
	checkTime := time.Now()

	nimo.GoRoutineInLoop(func() {
		now := time.Now()
		select {
		case <-manager.FlushChan:
			LOG.Info("CheckpointManager flush immediately begin")
			if err := manager.flushAll(); err != nil {
				LOG.Warn("CheckpointManager flush immediately failed. %v", err)
			} else {
				LOG.Info("CheckpointManager flush immediately successful")
			}
		case <-time.After(time.Second):
			// we delayed a few minutes to tolerate the receiver's flush buffer
			// in AckRequired() tunnel. such as "rpc". While collector is restarted,
			// we can't get the correct worker ack offset since collector have lost
			// the unack offset...
			if conf.Options.Tunnel != "direct" && now.Before(startTime.Add(NonDirectCheckpointInterval*time.Second)) {
				return
			}
			if now.Before(checkTime.Add(time.Duration(conf.Options.CheckpointInterval) * time.Second)) {
				return
			}
			LOG.Info("CheckpointManager flush periodically begin")
			if err := manager.flushAll(); err != nil {
				LOG.Warn("CheckpointManager flush periodically failed. %v", err)
			} else {
				LOG.Info("CheckpointManager flush periodically successful")
			}
			checkTime = time.Now()
		}
	})
}

// firstly load checkpoit info to CheckpointManager without concurrent access
func (manager *CheckpointManager) loadAll() error {
	if !manager.ensureNetwork() {
		return fmt.Errorf("CheckpointManager connect to %v failed", manager.url)
	}
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	conn := manager.conn
	db := manager.db
	// obtain checkpoint stage
	var versionDoc map[string]interface{}
	if err := manager.conn.Session.DB(manager.db).C(manager.table).
		Find(bson.M{}).One(&versionDoc); err != nil && err != mgo.ErrNotFound {
		manager.conn.Close()
		manager.conn = nil
		return LOG.Critical("CheckpointManager loadAll versionDoc error. %v", err)
	}
	stage, ok := versionDoc[utils.CheckpointStage]
	if ok {
		switch stage {
		case utils.StageOriginal:
			// drop tmp table
			for _, persist := range manager.persistList {
				tablePrefix := "tmp_" + manager.table
				for _, tmpTable := range persist.GetTableList(tablePrefix) {
					if err := conn.Session.DB(db).C(tmpTable).
						DropCollection(); err != nil && err.Error() != "ns not found" {
						manager.conn.Close()
						manager.conn = nil
						return LOG.Critical("CheckpointManager loadAll drop collection %v failed. %v",
							tmpTable, err)
					}
				}
			}
		case utils.StageFlushed:
			// drop original table
			for _, persist := range manager.persistList {
				for _, origTable := range persist.GetTableList(manager.table) {
					if err := conn.Session.DB(db).C(origTable).
						DropCollection(); err != nil && err.Error() != "ns not found" {
						manager.conn.Close()
						manager.conn = nil
						return LOG.Critical("CheckpointManager loadAll drop collection %v failed. %v",
							origTable, err)
					}
				}
			}
			fallthrough
		case utils.StageRename:
			// rename tmp table to original table
			for _, persist := range manager.persistList {
				for _, origTable := range persist.GetTableList(manager.table) {
					origNs := fmt.Sprintf("%v.%v", manager.db, origTable)
					tmpNs := fmt.Sprintf("%v.tmp_%v", manager.db, origTable)
					if err := conn.Session.DB("admin").
						Run(bson.D{{"renameCollection", tmpNs}, {"to", origNs}},
							nil); err != nil && err.Error() != "source namespace does not exist" {
						manager.conn.Close()
						manager.conn = nil
						return LOG.Critical("CheckpointManager loadAll rename collection %v to %v failed. %v",
							tmpNs, origNs, err)
					}
				}
			}
		default:
			return LOG.Critical("CheckpointManager loadAll no checkpoint")
		}
	}
	for _, persist := range manager.persistList {
		if err := persist.Load(manager.table); err != nil {
			manager.conn.Close()
			manager.conn = nil
			return LOG.Critical("CheckpointManager loadAll persist load error %v", err)
		}
	}
	if ok {
		if _, err := manager.conn.Session.DB(manager.db).C(manager.table).
			Upsert(bson.M{}, bson.M{utils.CheckpointStage: utils.StageOriginal}); err != nil {
			manager.conn.Close()
			manager.conn = nil
			return LOG.Critical("CheckpointManager loadAll upsert versionDoc error. %v", err)
		}
	}
	return nil
}

func (manager *CheckpointManager) Load(tablePrefix string) error {
	loadTime := time.Now()
	loadSet := make(map[string]bool)

	iter := manager.conn.Session.DB(manager.db).C(tablePrefix + "_oplog").Find(bson.M{}).Iter()
	ckptDoc := make(map[string]interface{})
	for iter.Next(ckptDoc) {
		replset, ok := ckptDoc[utils.CheckpointName].(string)
		if !ok {
			return fmt.Errorf("CheckpointManager load checkpoint illegal record %v. ok[%v]", ckptDoc, ok)
		}
		syncer, ok := manager.syncMap[replset]
		if !ok {
			LOG.Error("CheckpointManager load checkpoint unknown replset %v, maybe remove shard", ckptDoc)
			continue
		}
		loadSet[replset] = true
		if err := syncer.LoadByDoc(ckptDoc, loadTime); err != nil {
			return err
		}
	}
	if err := iter.Close(); err != nil {
		LOG.Critical("CheckpointManager close iterator failed. %v", err)
	}
	for replset, syncer := range manager.syncMap {
		// there is no checkpoint before or this is a new node
		if _, ok := loadSet[replset]; !ok {
			ckptDoc = make(map[string]interface{})
			beginTs := manager.beginTsMap[replset]
			ckptDoc[utils.CheckpointAckTs] = beginTs
			ckptDoc[utils.CheckpointSyncTs] = beginTs
			LOG.Info("CheckpointManager load checkpoint initialize replset[%v]", replset)
			if err := syncer.LoadByDoc(ckptDoc, loadTime); err != nil {
				return err
			}
		}
	}
	return nil
}

func (manager *CheckpointManager) flushAll() error {
	if !manager.ensureNetwork() {
		return fmt.Errorf("CheckpointManager connect to %v failed", manager.url)
	}
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	conn := manager.conn
	db := manager.db
	// Original Stage: drop tmp table and do checkpoint
	if _, err := manager.conn.Session.DB(manager.db).C(manager.table).
		Upsert(bson.M{}, bson.M{utils.CheckpointStage: utils.StageOriginal}); err != nil {
		manager.conn.Close()
		manager.conn = nil
		return LOG.Critical("CheckpointManager flushAll upsert versionDoc error. %v", err)
	}
	for _, persist := range manager.persistList {
		tablePrefix := "tmp_" + manager.table
		for _, tmpTable := range persist.GetTableList(tablePrefix) {
			if err := conn.Session.DB(db).C(tmpTable).DropCollection(); err != nil && err.Error() != "ns not found" {
				manager.conn.Close()
				manager.conn = nil
				return LOG.Critical("CheckpointManager flushAll drop collection %v failed. %v", tmpTable, err)
			}
		}
		if err := persist.Flush(tablePrefix); err != nil {
			manager.conn.Close()
			manager.conn = nil
			return LOG.Critical("CheckpointManager flushAll persist flush error %v", err)
		}
	}
	// Flushed Stage: drop original table
	if _, err := manager.conn.Session.DB(manager.db).C(manager.table).
		Upsert(bson.M{}, bson.M{utils.CheckpointStage: utils.StageFlushed}); err != nil {
		manager.conn.Close()
		manager.conn = nil
		return LOG.Critical("CheckpointManager flushAll upsert versionDoc error. %v", err)
	}
	for _, persist := range manager.persistList {
		for _, origTable := range persist.GetTableList(manager.table) {
			if err := conn.Session.DB(db).C(origTable).DropCollection(); err != nil && err.Error() != "ns not found" {
				manager.conn.Close()
				manager.conn = nil
				return LOG.Critical("CheckpointManager flushAll drop collection %v failed. %v", origTable, err)
			}
		}
	}
	// Rename Stage: rename tmp table to original table
	if _, err := manager.conn.Session.DB(manager.db).C(manager.table).
		Upsert(bson.M{}, bson.M{utils.CheckpointStage: utils.StageRename}); err != nil {
		manager.conn.Close()
		manager.conn = nil
		return LOG.Critical("CheckpointManager flushAll upsert versionDoc error. %v", err)
	}
	for _, persist := range manager.persistList {
		for _, origTable := range persist.GetTableList(manager.table) {
			origNs := fmt.Sprintf("%v.%v", manager.db, origTable)
			tmpNs := fmt.Sprintf("%v.tmp_%v", manager.db, origTable)
			if err := conn.Session.DB("admin").
				Run(bson.D{{"renameCollection", tmpNs}, {"to", origNs}},
					nil); err != nil && err.Error() != "source namespace does not exist" {
				manager.conn.Close()
				manager.conn = nil
				return LOG.Critical("CheckpointManager flushAll rename collection %v to %v failed. %v",
					tmpNs, origNs, err)
			}
		}
	}
	if _, err := manager.conn.Session.DB(manager.db).C(manager.table).
		Upsert(bson.M{}, bson.M{utils.CheckpointStage: utils.StageOriginal}); err != nil {
		manager.conn.Close()
		manager.conn = nil
		return LOG.Critical("CheckpointManager flushAll upsert versionDoc error. %v", err)
	}
	return nil
}

func (manager *CheckpointManager) Flush(tablePrefix string) error {
	for replset, syncer := range manager.syncMap {
		ckptDoc := syncer.FlushByDoc()
		if _, err := manager.conn.Session.DB(manager.db).C(tablePrefix+"_oplog").
			Upsert(bson.M{utils.CheckpointName: replset}, ckptDoc); err != nil {
			return fmt.Errorf("CheckpointManager upsert %v error. %v", ckptDoc, err)
		}
	}
	return nil
}

func (manager *CheckpointManager) GetTableList(tablePrefix string) []string {
	return []string{tablePrefix + "_oplog"}
}

func (manager *CheckpointManager) ensureNetwork() bool {
	// make connection if we don't already established
	if manager.conn == nil {
		if conn, err := utils.NewMongoConn(manager.url, utils.ConnectModePrimary, true); err == nil {
			manager.conn = conn
		} else {
			LOG.Warn("CheckpointManager connect to %v failed. %v", manager.url, err)
			manager.conn = nil
			return false
		}
	}
	// set WriteMajority while checkpoint is writing to ConfigServer
	if conf.Options.IsShardCluster() {
		manager.conn.Session.EnsureSafe(&mgo.Safe{WMode: utils.MajorityWriteConcern})
	}
	return true
}
