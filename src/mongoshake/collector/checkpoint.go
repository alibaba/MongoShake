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
	StorageTypeAPI            = "api"
	StorageTypeDB             = "database"
	CheckpointDefaultDatabase = utils.AppDatabase
	CheckpointAdminDatabase   = "admin"
	CheckpointName            = "name"
	CheckpointTimestamp       = "ckpt"

	MajorityWriteConcern = "majority"
)

type PersistObject interface {
	Persist(conn *utils.MongoConn, db string, tablePrefix string) error
}

type SyncerCheckpoint struct {
	syncer *OplogSyncer
	ackTs  bson.MongoTimestamp
}

type CheckpointManager struct {
	ckptMap map[string]*SyncerCheckpoint
	mutex   sync.Mutex

	url           string
	db            string
	table         string
	startPosition int64
	conn          *utils.MongoConn

	persistList []PersistObject

	//startTime   time.Time
	//ckptTime    time.Time
}

func NewCheckpointManager(startPosition int64) *CheckpointManager {
	db := CheckpointDefaultDatabase
	if conf.Options.IsShardCluster() {
		db = CheckpointAdminDatabase
	}
	// we can't insert Timestamp(0, 0) that will be treat as Now() inserted
	// into mongo. so we use Timestamp(0, 1)
	startPosition = int64(math.Max(float64(startPosition), 1))
	manager := &CheckpointManager{
		ckptMap:       make(map[string]*SyncerCheckpoint),
		url:           conf.Options.ContextStorageUrl,
		db:            db,
		table:         conf.Options.ContextAddress,
		startPosition: startPosition,
	}
	return manager
}

func (manager *CheckpointManager) addOplogSyncer(syncer *OplogSyncer) {
	manager.ckptMap[syncer.replset] = &SyncerCheckpoint{syncer: syncer, ackTs: 0}
}

func (manager *CheckpointManager) registerPersis(persistObj PersistObject) {
	manager.persistList = append(manager.persistList, persistObj)
}

func (manager *CheckpointManager) start() {
	startTime := time.Now()
	ckptTime := time.Now()

	nimo.GoRoutineInLoop(func() {
		now := time.Now()

		// do checkpoint every once in a while
		if ckptTime.Add(time.Duration(conf.Options.CheckpointInterval) * time.Millisecond).After(now) {
			return
		}

		// we force update the ckpt time even failed
		ckptTime = now

		// we delayed a few minutes to tolerate the receiver's flush buffer
		// in AckRequired() tunnel. such as "rpc". While collector is restarted,
		// we can't get the correct worker ack offset since collector have lost
		// the unack offset...
		if conf.Options.Tunnel != "direct" && now.Before(startTime.Add(3*time.Minute)) {
			return
		}

		manager.OplogCheckpoint()

		time.Sleep(1 * time.Second)
	})
}

func (manager *CheckpointManager) Load() error {
	if !manager.ensureNetwork() {
		return fmt.Errorf("CheckpointManager connect to %v failed", manager.url)
	}
	manager.mutex.Lock()
	iter := manager.conn.Session.DB(manager.db).C(manager.table).Find(bson.M{}).Iter()
	var ckptDoc map[string]interface{}
	for iter.Next(ckptDoc) {
		if replset, ok := ckptDoc[CheckpointName].(string); ok {
			if info, ok := manager.ckptMap[replset]; ok {
				info.ackTs = ckptDoc[CheckpointTimestamp].(bson.MongoTimestamp)
			} else {
				LOG.Warn("CheckpointManager load checkpoint map with illegal record %v", ckptDoc)
			}
		} else {
			LOG.Warn("CheckpointManager load checkpoint map with illegal record %v", ckptDoc)
		}
	}
	for replset, info := range manager.ckptMap {
		if info.ackTs == 0 {
			info.ackTs = bson.MongoTimestamp(manager.startPosition << 32)
			LOG.Info("CheckpointManager load checkpoint map set replset[%v] checkpoint to %v",
				manager.ckptMap[replset])
		}
	}
	manager.mutex.Unlock()

	LOG.Info("CheckpointManager load %v", manager.ckptMap)
	return nil
}

func (manager *CheckpointManager) Get(replset string) bson.MongoTimestamp {
	if info, ok := manager.ckptMap[replset]; ok {
		return info.ackTs
	} else {
		return 0
	}
}

func (manager *CheckpointManager) Update(replset string, ts bson.MongoTimestamp) {
	manager.mutex.Lock()
	manager.ckptMap[replset].ackTs = ts
	manager.mutex.Unlock()
}

func (manager *CheckpointManager) Flush() error {
	if !manager.ensureNetwork() {
		return fmt.Errorf("CheckpointManager connect to %v failed", manager.url)
	}
	manager.mutex.Lock()
	for replset, info := range manager.ckptMap {
		ckptDoc := map[string]interface{}{
			CheckpointName:      replset,
			CheckpointTimestamp: info.ackTs,
		}
		if _, err := manager.conn.Session.DB(manager.db).C(manager.table).
			Upsert(bson.M{CheckpointName: replset}, ckptDoc); err != nil {
			LOG.Critical("CheckpointManager flush checkpoint %v upsert error %v", ckptDoc, err)
			manager.conn.Close()
			return err
		}
	}
	for _, persistObj := range manager.persistList {
		if err := persistObj.Persist(manager.conn, manager.db, manager.table); err != nil {
			LOG.Critical("CheckpointManager flush persist object error %v", err)
			manager.conn.Close()
			return err
		}
	}
	manager.mutex.Unlock()
	return nil
}

func (manager *CheckpointManager) OplogCheckpoint() {
	for replset, info := range manager.ckptMap {
		var lowestTs int64
		var err error
		lowestTs, err = calculateWorkerLowestCheckpoint(info.syncer)

		if lowestTs > 0 && err == nil {
			switch {
			case bson.MongoTimestamp(lowestTs) > info.ackTs:
				manager.Update(replset, bson.MongoTimestamp(lowestTs))
				LOG.Info("checkpoint replset %v updated from %v to %v", replset,
					utils.ExtractTimestampForLog(info.ackTs), utils.ExtractTimestampForLog(lowestTs))
				info.syncer.replMetric.AddCheckpoint(1)
				info.syncer.replMetric.SetLSNCheckpoint(lowestTs)
			case bson.MongoTimestamp(lowestTs) < info.ackTs:
				LOG.Info("checkpoint replset %v calculated is smaller than value in memory. lowest %v current %v",
					replset, utils.ExtractTimestampForLog(lowestTs), utils.ExtractTimestampForLog(info.ackTs))
			}
		} else {
			// this log will be print if no ack calculated
			LOG.Warn("checkpoint replset %v updated is not suitable. lowest[%d] current[%d] reason: %v",
				replset, lowestTs, utils.TimestampToInt64(info.ackTs), err)
		}
	}
	if err := manager.Flush(); err != nil {
		LOG.Warn("checkpoint flush failed. %v", err)
	}
}

func (manager *CheckpointManager) ensureNetwork() bool {
	// make connection if we don't already established
	if manager.conn == nil {
		if conn, err := utils.NewMongoConn(manager.url, utils.ConnectModePrimary, true); err == nil {
			manager.conn = conn
		} else {
			LOG.Warn("CheckpointManager connect to %v failed. %v", manager.url, err)
			return false
		}
	}
	// set WriteMajority while checkpoint is writing to ConfigServer
	if conf.Options.IsShardCluster() {
		manager.conn.Session.EnsureSafe(&mgo.Safe{WMode: MajorityWriteConcern})
	}
	return true
}

func calculateWorkerLowestCheckpoint(sync *OplogSyncer) (v int64, err error) {
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
		return allAckValues[len(allAckValues)-1], nil
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
	return candidates[0], nil
}
