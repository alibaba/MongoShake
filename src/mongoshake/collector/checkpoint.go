package collector

import (
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"mongoshake/collector/ckpt"
	"mongoshake/collector/configure"
	"mongoshake/common"

	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo/bson"
)

func (sync *OplogSyncer) newCheckpointManager(name string, startPosition int64) {
	LOG.Info("Oplog sync[%v] create checkpoint manager with url[%s] table[%s.%s] start-position[%v]",
		name, conf.Options.CheckpointStorageUrl, conf.Options.CheckpointStorageDb,
		conf.Options.CheckpointStorageCollection, utils.ExtractTimestampForLog(startPosition))
	sync.ckptManager = ckpt.NewCheckpointManager(name, startPosition)
}

/*
 * load checkpoint and do some checks
 */
func (sync *OplogSyncer) loadCheckpoint() error {
	checkpoint, exists, err := sync.ckptManager.Get()
	if err != nil {
		return fmt.Errorf("load checkpoint[%v] failed[%v]", sync.Replset, err)
	}
	LOG.Info("load checkpoint value: %s", checkpoint)

	// set fetch method if not exists or empty
	if !exists || checkpoint.FetchMethod == "" {
		sync.ckptManager.SetFetchMethod(conf.Options.IncrSyncMongoFetchMethod)
	}

	// not enable oplog persist?
	if !conf.Options.FullSyncReaderOplogStoreDisk {
		sync.persister.SetFetchStage(utils.FetchStageStoreMemoryApply)
		return nil
	}

	ts := time.Now()

	// if no checkpoint exists
	if !exists {
		sync.persister.SetFetchStage(utils.FetchStageStoreDiskNoApply)
		dqName := fmt.Sprintf("diskqueue-%v-%v", sync.Replset, ts.Format("20060102-150405"))
		sync.persister.InitDiskQueue(dqName)
		sync.ckptManager.SetOplogDiskQueueName(dqName)
		sync.ckptManager.SetOplogDiskFinishTs(ckpt.InitCheckpoint) // set as init
		return nil
	}

	// check if checkpoint real ts >= checkpoint disk last ts
	if checkpoint.OplogDiskQueueFinishTs > 0 && checkpoint.Timestamp >= checkpoint.OplogDiskQueueFinishTs {
		// no need to init disk queue again
		sync.persister.SetFetchStage(utils.FetchStageStoreMemoryApply)
		return nil
	}

	// TODO, there is a bug if MongoShake restarts

	// need to init
	sync.persister.SetFetchStage(utils.FetchStageStoreDiskNoApply)
	sync.persister.InitDiskQueue(checkpoint.OplogDiskQueue)
	return nil
}

/*
 * calculate and update current checkpoint value. `flush` means whether force calculate & update checkpoint.
 * if inputTs is given(> 0), use this value to update checkpoint, otherwise, calculate from workers.
 */
func (sync *OplogSyncer) checkpoint(flush bool, inputTs bson.MongoTimestamp) {
	now := time.Now()

	// do checkpoint every once in a while
	if !flush && sync.ckptTime.Add(time.Duration(conf.Options.CheckpointInterval)*time.Millisecond).After(now) {
		return
	}
	// we force update the ckpt time even failed
	sync.ckptTime = now

	// we delayed a few minutes to tolerate the receiver's flush buffer
	// in AckRequired() tunnel. such as "rpc". While collector is restarted,
	// we can't get the correct worker ack offset since collector have lost
	// the unack offset...
	if !flush && conf.Options.Tunnel != utils.VarTunnelDirect &&
		now.Before(sync.startTime.Add(1 * time.Minute)) {
		// LOG.Info("CheckpointOperation requires three minutes at least to flush receiver's buffer")
		return
	}

	// read all workerGroup self ckpt. get minimum of all updated checkpoint
	inMemoryTs := sync.ckptManager.GetInMemory().Timestamp
	var lowest int64 = 0
	var err error
	if inputTs > 0 {
		// use inputTs if inputTs is > 0
		lowest = utils.TimestampToInt64(inputTs)
	} else {
		lowest, err = sync.calculateWorkerLowestCheckpoint()
	}

	lowestInt64 := bson.MongoTimestamp(lowest)
	// if all oplogs from disk has been replayed successfully, store the newest oplog timestamp
	if conf.Options.FullSyncReaderOplogStoreDisk && sync.persister.diskQueueLastTs > 0 {
		if lowestInt64 >= sync.persister.diskQueueLastTs {
			sync.ckptManager.SetOplogDiskFinishTs(sync.persister.diskQueueLastTs)
			sync.persister.diskQueueLastTs = -2 // mark -1 so next time won't call
		}
	}

	if lowest > 0 && err == nil {
		switch {
		case lowestInt64 > inMemoryTs:
			if err = sync.ckptManager.Update(lowestInt64); err == nil {
				LOG.Info("CheckpointOperation write success. updated from %v to %v",
					utils.ExtractTimestampForLog(inMemoryTs), utils.ExtractTimestampForLog(lowest))
				sync.replMetric.AddCheckpoint(1)
				sync.replMetric.SetLSNCheckpoint(lowest)
				return
			}
		case lowestInt64 < inMemoryTs:
			LOG.Info("CheckpointOperation calculated[%v] is smaller than value in memory[%v]",
				utils.ExtractTimestampForLog(lowest), utils.ExtractTimestampForLog(inMemoryTs))
			return
		case lowestInt64 == inMemoryTs:
			return
		}
	}

	// this log will be print if no ack calculated
	LOG.Warn("CheckpointOperation updated is not suitable. lowest [%d]. current [%v]. inputTs [%v]. reason : %v",
		lowest, utils.ExtractTimestampForLog(inMemoryTs), inputTs, err)
}

func (sync *OplogSyncer) calculateWorkerLowestCheckpoint() (v int64, err error) {
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
	LOG.Info("worker offset %v use lowest %v", candidates, utils.ExtractTimestampForLog(candidates[0]))
	return candidates[0], nil
}

