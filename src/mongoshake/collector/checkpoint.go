package collector

import (
	"errors"
	"fmt"
	nimo "github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
	"mongoshake/collector/configure"
	utils "mongoshake/common"
	"sort"
	"sync"
	"sync/atomic"
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
			if err := manager.FlushAll(); err != nil {
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
			if err := manager.FlushAll(); err != nil {
				LOG.Warn("CheckpointManager flush periodically failed. %v", err)
			} else {
				LOG.Info("CheckpointManager flush periodically successful")
			}
			checkTime = time.Now()
		}
	})
}

func (manager *CheckpointManager) Get(replset string) bson.MongoTimestamp {
	ackTs, err := calculateSyncerAckTs(manager.syncMap[replset])
	if err != nil {
		LOG.Warn("CheckpointManager get ackTs for sycner %v failed. %v", replset, err)
	}
	return ackTs
}

// firstly load checkpoit info to CheckpointManager without concurrent access
func (manager *CheckpointManager) LoadAll() error {
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
		return LOG.Critical("CheckpointManager LoadAll versionDoc error. %v", err)
	}
	stage, ok := versionDoc[utils.CheckpointStage]
	if ok {
		switch stage {
		case utils.StageOriginal:
			// drop tmp table
			for _, persist := range manager.persistList {
				tablePrefix := "tmp_" + manager.table
				for _, tmpTable := range persist.GetTableList(tablePrefix) {
					if err := conn.Session.DB(db).C(tmpTable).DropCollection(); err != nil && err.Error() != "ns not found" {
						manager.conn.Close()
						manager.conn = nil
						return LOG.Critical("CheckpointManager LoadAll drop collection %v failed. %v", tmpTable, err)
					}
				}
			}
		case utils.StageFlushed:
			// drop original table
			for _, persist := range manager.persistList {
				for _, origTable := range persist.GetTableList(manager.table) {
					if err := conn.Session.DB(db).C(origTable).DropCollection(); err != nil && err.Error() != "ns not found" {
						manager.conn.Close()
						manager.conn = nil
						return LOG.Critical("CheckpointManager LoadAll drop collection %v failed. %v", origTable, err)
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
						Run(bson.D{{"renameCollection", tmpNs}, {"to", origNs}}, nil); err != nil && err.Error() != "source namespace does not exist" {
						manager.conn.Close()
						manager.conn = nil
						return LOG.Critical("CheckpointManager LoadAll rename collection %v to %v failed. %v", tmpNs, origNs, err)
					}
				}
			}
		default:
			return LOG.Critical("CheckpointManager LoadAll no checkpoint")
		}
	}
	for _, persist := range manager.persistList {
		if err := persist.Load(manager.table); err != nil {
			manager.conn.Close()
			manager.conn = nil
			return LOG.Critical("CheckpointManager LoadAll persist load error %v", err)
		}
	}
	if _, err := manager.conn.Session.DB(manager.db).C(manager.table).
		Upsert(bson.M{}, bson.M{utils.CheckpointStage: utils.StageOriginal}); err != nil {
		manager.conn.Close()
		manager.conn = nil
		return LOG.Critical("CheckpointManager LoadAll upsert versionDoc error. %v", err)
	}
	return nil
}

func (manager *CheckpointManager) Load(tablePrefix string) error {
	iter := manager.conn.Session.DB(manager.db).C(tablePrefix + "_oplog").Find(bson.M{}).Iter()
	ckptDoc := make(map[string]interface{})
	for iter.Next(ckptDoc) {
		replset, ok1 := ckptDoc[utils.CheckpointName].(string)
		ackTs, ok2 := ckptDoc[utils.CheckpointAckTs].(bson.MongoTimestamp)
		syncTs, ok3 := ckptDoc[utils.CheckpointSyncTs].(bson.MongoTimestamp)
		if !ok1 || !ok2 || !ok3 {
			return fmt.Errorf("CheckpointManager load checkpoint illegal record %v. ok1[%v] ok2[%v] ok3[%v]",
				ckptDoc, ok1, ok2, ok3)
		} else if syncer, ok := manager.syncMap[replset]; !ok {
			LOG.Error("CheckpointManager load checkpoint unknown replset %v", ckptDoc)
		} else {
			syncer.batcher.syncTs = syncTs
			syncer.batcher.unsyncTs = syncTs
			for _, worker := range syncer.batcher.workerGroup {
				worker.unack = int64(ackTs)
				worker.ack = int64(ackTs)
			}
			LOG.Info("CheckpointManager load checkpoint set replset[%v] checkpoint to exist ackTs[%v] syncTs[%v]",
				replset, utils.TimestampToLog(ackTs), utils.TimestampToLog(syncTs))
		}
	}
	if err := iter.Close(); err != nil {
		LOG.Critical("CheckpointManager close iterator failed. %v", err)
	}
	for replset, syncer := range manager.syncMap {
		// there is no checkpoint before or this is a new node
		if syncer.batcher.syncTs == 0 {
			beginTs := manager.beginTsMap[replset]
			syncer.batcher.syncTs = bson.MongoTimestamp(beginTs)
			syncer.batcher.unsyncTs = bson.MongoTimestamp(beginTs)
			for _, worker := range syncer.batcher.workerGroup {
				worker.unack = int64(beginTs)
				worker.ack = int64(beginTs)
			}
			LOG.Info("CheckpointManager load checkpoint set replset[%v] checkpoint to start position %v",
				replset, utils.TimestampToLog(beginTs))
		}
	}
	return nil
}

func (manager *CheckpointManager) FlushAll() error {
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
		return LOG.Critical("CheckpointManager FlushAll upsert versionDoc error. %v", err)
	}
	for _, persist := range manager.persistList {
		tablePrefix := "tmp_" + manager.table
		for _, tmpTable := range persist.GetTableList(tablePrefix) {
			if err := conn.Session.DB(db).C(tmpTable).DropCollection(); err != nil && err.Error() != "ns not found" {
				manager.conn.Close()
				manager.conn = nil
				return LOG.Critical("CheckpointManager FlushAll drop collection %v failed. %v", tmpTable, err)
			}
		}
		if err := persist.Flush(tablePrefix); err != nil {
			manager.conn.Close()
			manager.conn = nil
			return LOG.Critical("CheckpointManager FlushAll persist flush error %v", err)
		}
	}
	// Flushed Stage: drop original table
	if _, err := manager.conn.Session.DB(manager.db).C(manager.table).
		Upsert(bson.M{}, bson.M{utils.CheckpointStage: utils.StageFlushed}); err != nil {
		manager.conn.Close()
		manager.conn = nil
		return LOG.Critical("CheckpointManager FlushAll upsert versionDoc error. %v", err)
	}
	for _, persist := range manager.persistList {
		for _, origTable := range persist.GetTableList(manager.table) {
			if err := conn.Session.DB(db).C(origTable).DropCollection(); err != nil && err.Error() != "ns not found" {
				manager.conn.Close()
				manager.conn = nil
				return LOG.Critical("CheckpointManager FlushAll drop collection %v failed. %v", origTable, err)
			}
		}
	}
	// Rename Stage: rename tmp table to original table
	if _, err := manager.conn.Session.DB(manager.db).C(manager.table).
		Upsert(bson.M{}, bson.M{utils.CheckpointStage: utils.StageRename}); err != nil {
		manager.conn.Close()
		manager.conn = nil
		return LOG.Critical("CheckpointManager FlushAll upsert versionDoc error. %v", err)
	}
	for _, persist := range manager.persistList {
		for _, origTable := range persist.GetTableList(manager.table) {
			origNs := fmt.Sprintf("%v.%v", manager.db, origTable)
			tmpNs := fmt.Sprintf("%v.tmp_%v", manager.db, origTable)
			if err := conn.Session.DB("admin").
				Run(bson.D{{"renameCollection", tmpNs}, {"to", origNs}}, nil); err != nil && err.Error() != "source namespace does not exist" {
				manager.conn.Close()
				manager.conn = nil
				return LOG.Critical("CheckpointManager FlushAll rename collection %v to %v failed. %v", tmpNs, origNs, err)
			}
		}
	}
	if _, err := manager.conn.Session.DB(manager.db).C(manager.table).
		Upsert(bson.M{}, bson.M{utils.CheckpointStage: utils.StageOriginal}); err != nil {
		manager.conn.Close()
		manager.conn = nil
		return LOG.Critical("CheckpointManager FlushAll upsert versionDoc error. %v", err)
	}
	return nil
}

func (manager *CheckpointManager) Flush(tablePrefix string) error {
	for replset, syncer := range manager.syncMap {
		ackTs := manager.Get(replset)
		syncTs := syncer.batcher.syncTs
		unsyncTs := syncer.batcher.unsyncTs
		nimo.AssertTrue(syncTs == unsyncTs, "should panic when syncTs ¡= unsyncTs")
		for _, worker := range syncer.batcher.workerGroup {
			ack := bson.MongoTimestamp(atomic.LoadInt64(&worker.ack))
			unack := bson.MongoTimestamp(atomic.LoadInt64(&worker.unack))
			LOG.Info("syncer %v worker ack[%v] unack[%v] syncTs[%v]", replset,
				utils.TimestampToLog(ack), utils.TimestampToLog(unack), utils.TimestampToLog(syncTs))
		}
		syncer.replMetric.AddCheckpoint(1)
		syncer.replMetric.SetLSNCheckpoint(int64(ackTs))
		ckptDoc := map[string]interface{}{
			utils.CheckpointName:   replset,
			utils.CheckpointAckTs:  ackTs,
			utils.CheckpointSyncTs: syncTs,
		}
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

func calculateSyncerAckTs(sync *OplogSyncer) (v bson.MongoTimestamp, err error) {
	// no need to lock and eventually consistence is acceptable
	allAcked := true
	candidates := make([]int64, 0, len(sync.batcher.workerGroup))
	allAckValues := make([]int64, 0, len(sync.batcher.workerGroup))
	for _, worker := range sync.batcher.workerGroup {
		// read ack value first because of we don't wanna
		// a result of ack > unack. There wouldn't be cpu
		// reorder under atomic !
		ack := atomic.LoadInt64(&worker.ack)
		unack := atomic.LoadInt64(&worker.unack)
		if ack == 0 && unack == 0 {
			// have no oplogs synced in this worker. skip
		} else if ack == unack || worker.IsAllAcked() {
			// all oplogs have been acked for right now or previous status
			worker.AllAcked(true)
			allAckValues = append(allAckValues, ack)
		} else if unack > ack {
			// most likely. partial oplogs acked (0 is possible)
			candidates = append(candidates, ack)
			allAcked = false
		} else if unack < ack && unack == 0 {
			// collector restarts. receiver unack value if from buffer
			// this is rarely happened. However we have delayed for
			// a bit log time. so we could use it
			allAcked = false
		} else if unack < ack && unack != 0 {
			// we should wait the bigger unack follows up the ack
			// they (unack and ack) will be equivalent soon !
			return 0, fmt.Errorf("candidates should follow up unack[%d] ack[%d]", unack, ack)
		}
	}
	if allAcked && len(allAckValues) != 0 {
		// free to choose the maximum value. ascend order
		// the last one is the biggest
		sort.Sort(utils.Int64Slice(allAckValues))
		return bson.MongoTimestamp(allAckValues[len(allAckValues)-1]), nil
	}

	if len(candidates) == 0 {
		return 0, errors.New("no candidates ack values found")
	}
	// ascend order. first is the smallest
	sort.Sort(utils.Int64Slice(candidates))

	if candidates[0] == 0 {
		return 0, errors.New("smallest candidates is zero")
	}
	LOG.Info("worker offset %v use lowest %d", candidates, candidates[0])
	return bson.MongoTimestamp(candidates[0]), nil
}
