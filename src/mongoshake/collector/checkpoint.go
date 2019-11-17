package collector

import (
	"errors"
	"fmt"
	nimo "github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
	"math"
	"mongoshake/collector/configure"
	utils "mongoshake/common"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	StorageTypeAPI   = "api"
	StorageTypeDB    = "database"
	CheckpointName   = "name"
	CheckpointAckTs  = "ackTs"
	CheckpointSyncTs = "syncTs"

	MajorityWriteConcern          = "majority"
	CheckpointMoveChunkIntervalMS = 5000
)

type Persist interface {
	Load() error
	Flush() error
}

type CheckpointManager struct {
	syncMap map[string]*OplogSyncer
	mutex   sync.RWMutex
	FlushChan chan bool

	url           string
	db            string
	table         string
	startPosition int64
	conn          *utils.MongoConn

	persistList []Persist
}

func NewCheckpointManager(startPosition int64) *CheckpointManager {
	// we can't insert Timestamp(0, 0) that will be treat as Now() inserted
	// into mongo. so we use Timestamp(0, 1)
	startPosition = int64(math.Max(float64(startPosition), 1))
	manager := &CheckpointManager{
		syncMap:       make(map[string]*OplogSyncer),
		FlushChan:     make(chan bool),
		url:           conf.Options.ContextStorageUrl,
		db:            utils.AppDatabase(),
		table:         conf.Options.ContextStorageCollection,
		startPosition: startPosition,
	}
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
	var intervalMs int64
	if conf.Options.MoveChunkEnable && conf.Options.CheckpointInterval < CheckpointMoveChunkIntervalMS {
		intervalMs = CheckpointMoveChunkIntervalMS
	} else {
		intervalMs = conf.Options.CheckpointInterval
	}

	nimo.GoRoutineInLoop(func() {
		now := time.Now()
		select {
		case <-manager.FlushChan:
			LOG.Info("CheckpointManager flush immediately begin")
			manager.mutex.Lock()
			if err := manager.Flush(); err != nil {
				LOG.Warn("CheckpointManager flush immediately failed. %v", err)
			} else {
				LOG.Info("CheckpointManager flush immediately successful")
			}
			manager.mutex.Unlock()
		case <-time.After(time.Second):
			// update checkpoint ackts each second

			// we delayed a few minutes to tolerate the receiver's flush buffer
			// in AckRequired() tunnel. such as "rpc". While collector is restarted,
			// we can't get the correct worker ack offset since collector have lost
			// the unack offset...
			if conf.Options.Tunnel != "direct" && now.Before(startTime.Add(3*time.Minute)) {
				return
			}
			if now.Before(checkTime.Add(time.Duration(intervalMs) * time.Millisecond)) {
				return
			}
			LOG.Info("CheckpointManager flush periodically begin")
			manager.mutex.Lock()
			if err := manager.Flush(); err != nil {
				LOG.Warn("CheckpointManager flush periodically failed. %v", err)
			} else {
				LOG.Info("CheckpointManager flush periodically successful")
			}
			manager.mutex.Unlock()
			checkTime = time.Now()
		}
	})
}

func (manager *CheckpointManager) Get(replset string) bson.MongoTimestamp {
	ackTs, err := calculateSyncerAckTs(manager.syncMap[replset])
	if err != nil {
		LOG.Info("CheckpointManager get ackTs for sycner %v failed. %v", replset, err)
	}
	return ackTs
}

// firstly load checkpoit info to CheckpointManager without concurrent access
func (manager *CheckpointManager) Load() error {
	if !manager.ensureNetwork() {
		return fmt.Errorf("CheckpointManager connect to %v failed", manager.url)
	}
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	iter := manager.conn.Session.DB(manager.db).C(manager.table).Find(bson.M{}).Iter()
	ckptDoc := make(map[string]interface{})
	for iter.Next(ckptDoc) {
		replset, ok1 := ckptDoc[CheckpointName].(string)
		ackTs, ok2 := ckptDoc[CheckpointAckTs].(bson.MongoTimestamp)
		syncTs, ok3 := ckptDoc[CheckpointSyncTs].(bson.MongoTimestamp)
		if !ok1 || !ok2 || !ok3 {
			return fmt.Errorf("CheckpointManager load checkpoint illegal record %v. ok1[%v] ok2[%v] ok3[%v]",
				ckptDoc, ok1, ok2, ok3)
		} else if syncer, ok := manager.syncMap[replset]; !ok {
			fmt.Errorf("CheckpointManager load checkpoint unknown replset %v", ckptDoc)
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
			startTs := manager.startPosition << 32
			syncer.batcher.syncTs = bson.MongoTimestamp(startTs)
			syncer.batcher.unsyncTs = bson.MongoTimestamp(startTs)
			for _, worker := range syncer.batcher.workerGroup {
				worker.unack = startTs
				worker.ack = startTs
			}
			LOG.Info("CheckpointManager load checkpoint set replset[%v] checkpoint to start position %v",
				replset, utils.TimestampToLog(startTs))
		}
	}
	for _, persist := range manager.persistList {
		if err := persist.Load(); err != nil {
			LOG.Critical("CheckpointManager flush persist object error %v", err)
			manager.conn.Close()
			manager.conn = nil
			return err
		}
	}
	return nil
}

func (manager *CheckpointManager) Flush() error {
	if !manager.ensureNetwork() {
		return fmt.Errorf("CheckpointManager connect to %v failed", manager.url)
	}
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
			CheckpointName:   replset,
			CheckpointAckTs:  ackTs,
			CheckpointSyncTs: syncTs,
		}
		if _, err := manager.conn.Session.DB(manager.db).C(manager.table).
			Upsert(bson.M{CheckpointName: replset}, ckptDoc); err != nil {
			manager.conn.Close()
			manager.conn = nil
			return fmt.Errorf("CheckpointManager upsert %v error. %v", ckptDoc, err)
		}
	}
	for _, persist := range manager.persistList {
		if err := persist.Flush(); err != nil {
			manager.conn.Close()
			manager.conn = nil
			return err
		}
	}
	return nil
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
		manager.conn.Session.EnsureSafe(&mgo.Safe{WMode: MajorityWriteConcern})
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