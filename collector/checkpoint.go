package collector

import (
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/alibaba/MongoShake/v2/collector/ckpt"
	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	"github.com/alibaba/MongoShake/v2/collector/spool"
	utils "github.com/alibaba/MongoShake/v2/common"
	l "github.com/alibaba/MongoShake/v2/pkg/log"
)

func (sync *OplogSyncer) newCheckpointManager(name string, startPosition interface{}) {
	l.Logger.Infof("Oplog sync[%v] create checkpoint manager with url[%s] table[%s.%s] start-position[%v]",
		name, utils.BlockMongoUrlPassword(conf.Options.CheckpointStorageUrl, "***"),
		conf.Options.CheckpointStorageDb,
		conf.Options.CheckpointStorageCollection, utils.ExtractTimestampForLog(startPosition))
	if val, ok := startPosition.(int64); ok {
		sync.ckptManager = ckpt.NewCheckpointManager(name, val)
	} else {
		sync.ckptManager = ckpt.NewCheckpointManager(name, 0)
	}
}

/*
 * load checkpoint and do some checks
 */
func (sync *OplogSyncer) loadCheckpoint() error {
	checkpoint, exists, err := sync.ckptManager.Get()
	if err != nil {
		return fmt.Errorf("load checkpoint[%v] failed[%v]", sync.Replset, err)
	}
	l.Logger.Infof("load checkpoint value: %s", checkpoint)

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
		dqName := fmt.Sprintf("oplog-spool-%v-%v", sync.Replset, ts.Format("20060102-150405"))
		if err := sync.persister.InitDiskQueue(dqName, true); err != nil {
			return fmt.Errorf("init pebble oplog spool[%v] failed[%v]", dqName, err)
		}
		sync.ckptManager.SetOplogDiskQueueName(dqName)
		sync.ckptManager.SetOplogDiskFinishTs(ckpt.InitCheckpoint) // set as init
		if err := sync.ckptManager.Update(checkpoint.Timestamp); err != nil {
			cleanupErr := sync.persister.DiskQueue.Delete()
			sync.persister.DiskQueue = nil
			if cleanupErr != nil {
				return fmt.Errorf("persist new pebble oplog spool[%v] metadata failed[%v]; cleanup failed[%v]",
					dqName, err, cleanupErr)
			}
			return fmt.Errorf("persist new pebble oplog spool[%v] metadata failed[%v]", dqName, err)
		}
		return nil
	}

	cleanupPending := checkpoint.OplogDiskQueue != "" &&
		checkpoint.OplogDiskQueueFinishTs > ckpt.InitCheckpoint &&
		checkpoint.Timestamp >= checkpoint.OplogDiskQueueFinishTs
	if cleanupPending {
		if err := spool.DeletePath(conf.Options.LogDirectory, checkpoint.OplogDiskQueue); err != nil {
			return fmt.Errorf("delete completed pebble oplog spool[%v] failed[%v]",
				checkpoint.OplogDiskQueue, err)
		}
		sync.ckptManager.SetOplogDiskQueueName("")
		sync.ckptManager.SetOplogDiskFinishTs(checkpoint.OplogDiskQueueFinishTs)
		if err := sync.ckptManager.Update(checkpoint.Timestamp); err != nil {
			return fmt.Errorf("clear completed pebble oplog spool[%v] checkpoint metadata failed[%v]",
				checkpoint.OplogDiskQueue, err)
		}
		sync.persister.SetFetchStage(utils.FetchStageStoreMemoryApply)
		return nil
	}

	// InitCheckpoint is the initial unfinished marker and is only complete
	// when the queue name has already been cleared.
	diskQueueFinished := checkpoint.OplogDiskQueue == "" &&
		(checkpoint.OplogDiskQueueFinishTs <= ckpt.InitCheckpoint ||
			(checkpoint.OplogDiskQueueFinishTs > ckpt.InitCheckpoint &&
				checkpoint.Timestamp >= checkpoint.OplogDiskQueueFinishTs))
	if diskQueueFinished {
		// no need to init disk queue again
		sync.persister.SetFetchStage(utils.FetchStageStoreMemoryApply)
		return nil
	}

	if checkpoint.OplogDiskQueue == "" {
		return fmt.Errorf("checkpoint[%v] has unfinished disk spool finish ts[%v] but empty queue name",
			sync.Replset, utils.ExtractTimestampForLog(checkpoint.OplogDiskQueueFinishTs))
	}

	// need to init
	sync.persister.SetFetchStage(utils.FetchStageStoreDiskNoApply)
	if err := sync.persister.InitDiskQueue(checkpoint.OplogDiskQueue, false); err != nil {
		return fmt.Errorf("open existing pebble oplog spool[%v] for checkpoint[%v] failed[%v]",
			checkpoint.OplogDiskQueue, sync.Replset, err)
	}
	durableLastTs, err := sync.persister.DiskQueue.LastWriteTimestamp()
	if err != nil {
		return fmt.Errorf("read existing pebble oplog spool[%v] last timestamp failed[%v]",
			checkpoint.OplogDiskQueue, err)
	}
	if durableLastTs > checkpoint.Timestamp && sync.reader != nil {
		sync.reader.UpdateQueryTimestamp(durableLastTs)
		l.Logger.Infof("resume source reader for replset[%v] from durable pebble spool timestamp[%v] "+
			"instead of remote checkpoint[%v]", sync.Replset,
			utils.ExtractTimestampForLog(durableLastTs), utils.ExtractTimestampForLog(checkpoint.Timestamp))
	}
	return nil
}

// checkpoint calculate and update current checkpoint value. `flush` means whether force calculate & update checkpoint.
// if inputTs is given(> 0), use this value to update checkpoint, otherwise, calculate from workers.
// Return true if we do update checkpoint, else return false.
func (sync *OplogSyncer) checkpoint(flush bool, inputTs int64) bool {
	now := time.Now()

	// do checkpoint every once in a while
	if !flush && sync.ckptTime.Add(time.Duration(conf.Options.CheckpointInterval)*time.Millisecond).After(now) {
		l.Logger.Debugf("do not repeat update checkpoint in %v milliseconds", conf.Options.CheckpointInterval)
		return false
	}
	// we force update the ckpt time even failed
	sync.ckptTime = now
	l.Logger.Infof("checkpoint update sync.ckptTime to:%v", sync.ckptTime)

	// we delayed a few minutes to tolerate the receiver's flush buffer
	// in AckRequired() tunnel. such as "rpc". While collector is restarted,
	// we can't get the correct worker ack offset since collector have lost
	// the unack offset...
	if !flush && conf.Options.Tunnel != utils.VarTunnelDirect &&
		now.Before(sync.startTime.Add(1*time.Minute)) {
		l.Logger.Infof("CheckpointOperation requires three minutes at least to flush receiver's buffer")
		return false
	}

	// read all workerGroup self ckpt. get minimum of all updated checkpoint
	inMemoryTs := sync.ckptManager.GetInMemory().Timestamp
	var lowest int64 = 0
	var err error
	if inputTs > 0 {
		// use inputTs if inputTs is > 0
		lowest = inputTs
	} else {
		lowest, err = sync.calculateWorkerLowestCheckpoint()
	}
	l.Logger.Infof("checkpoint func lowest:%v inMemoryTs:%v flush:%v inputTs:%v",
		utils.ExtractTimestampForLog(lowest), utils.ExtractTimestampForLog(inMemoryTs), flush, inputTs)

	lowestInt64 := lowest
	if conf.Options.FullSyncReaderOplogStoreDisk && sync.persister.diskQueueLastTs > 0 {
		checkpointTs := inMemoryTs
		if err == nil && lowestInt64 > checkpointTs {
			checkpointTs = lowestInt64
		}
		lastTs := sync.persister.diskQueueLastTs
		if lastTs == ckpt.InitCheckpoint {
			return sync.finishEmptyDiskSpool(checkpointTs)
		}
		context := sync.ckptManager.GetInMemory()
		cleanupReady := context != nil && context.OplogDiskQueue != "" &&
			context.OplogDiskQueueFinishTs == lastTs && context.Timestamp >= lastTs
		if cleanupReady || (err == nil && lowestInt64 >= lastTs) {
			return sync.finishNonEmptyDiskSpool(checkpointTs, lastTs)
		}
	}

	if lowest > 0 && err == nil {
		switch {
		case lowestInt64 > inMemoryTs:
			if err = sync.ckptManager.Update(lowestInt64); err == nil {
				l.Logger.Infof("CheckpointOperation write success. updated from %v to %v",
					utils.ExtractTimestampForLog(inMemoryTs), utils.ExtractTimestampForLog(lowest))
				sync.replMetric.AddCheckpoint(1)
				sync.replMetric.SetLSNCheckpoint(lowest)
				return true
			}
		case lowestInt64 < inMemoryTs:
			l.Logger.Infof("CheckpointOperation calculated[%v] is smaller than value in memory[%v]",
				utils.ExtractTimestampForLog(lowest), utils.ExtractTimestampForLog(inMemoryTs))
			return false
		case lowestInt64 == inMemoryTs:
			return false
		}
	}

	// this log will be print if no ack calculated
	l.Logger.Warnf("CheckpointOperation updated is not suitable. lowest [%d]. current [%v]. inputTs [%v]. reason : %v",
		lowest, utils.ExtractTimestampForLog(inMemoryTs), inputTs, err)
	return false
}

func (sync *OplogSyncer) finishNonEmptyDiskSpool(checkpointTs, finishTs int64) bool {
	context := sync.ckptManager.GetInMemory()
	if context == nil || context.OplogDiskQueue == "" {
		l.Logger.Errorf("CheckpointOperation cannot finish non-empty disk spool without queue name")
		return false
	}
	queueName := context.OplogDiskQueue

	if context.OplogDiskQueueFinishTs != finishTs || context.Timestamp < finishTs {
		sync.ckptManager.SetOplogDiskFinishTs(finishTs)
		sync.ckptManager.SetOplogDiskQueueName(queueName)
		if err := sync.ckptManager.Update(checkpointTs); err != nil {
			l.Logger.Warnf("CheckpointOperation persist disk spool finish marker failed: %v", err)
			return false
		}
	}

	if sync.persister.DiskQueue != nil {
		if err := sync.persister.DiskQueue.Delete(); err != nil {
			l.Logger.Warnf("CheckpointOperation delete finished pebble spool[%v] failed: %v", queueName, err)
			return false
		}
		sync.persister.DiskQueue = nil
	}

	sync.ckptManager.SetOplogDiskFinishTs(finishTs)
	sync.ckptManager.SetOplogDiskQueueName("")
	if err := sync.ckptManager.Update(checkpointTs); err != nil {
		l.Logger.Warnf("CheckpointOperation clear finished disk spool metadata failed: %v", err)
		return false
	}
	sync.persister.diskQueueLastTs = -2
	sync.recordDiskSpoolCheckpoint(checkpointTs)
	return true
}

func (sync *OplogSyncer) finishEmptyDiskSpool(checkpointTs int64) bool {
	if checkpointTs <= ckpt.InitCheckpoint {
		checkpointTs = ckpt.InitCheckpoint
	}
	context := sync.ckptManager.GetInMemory()
	if context == nil {
		l.Logger.Errorf("CheckpointOperation cannot finish empty disk spool without checkpoint context")
		return false
	}
	queueName := context.OplogDiskQueue

	sync.ckptManager.SetOplogDiskFinishTs(checkpointTs)
	sync.ckptManager.SetOplogDiskQueueName("")
	if err := sync.ckptManager.Update(checkpointTs); err != nil {
		l.Logger.Warnf("CheckpointOperation clear empty disk spool metadata failed: %v", err)
		return false
	}

	if sync.persister.DiskQueue != nil {
		if err := sync.persister.DiskQueue.Delete(); err != nil {
			l.Logger.Warnf("CheckpointOperation delete empty pebble spool[%v] failed: %v", queueName, err)
			return false
		}
		sync.persister.DiskQueue = nil
	} else if queueName != "" {
		if err := spool.DeletePath(conf.Options.LogDirectory, queueName); err != nil {
			l.Logger.Warnf("CheckpointOperation delete empty pebble spool path[%v] failed: %v", queueName, err)
			return false
		}
	}

	sync.persister.diskQueueLastTs = -2
	sync.recordDiskSpoolCheckpoint(checkpointTs)
	return true
}

func (sync *OplogSyncer) recordDiskSpoolCheckpoint(ts int64) {
	l.Logger.Infof("CheckpointOperation write success. completed disk spool cleanup at %v",
		utils.ExtractTimestampForLog(ts))
	if sync.replMetric != nil {
		sync.replMetric.AddCheckpoint(1)
		sync.replMetric.SetLSNCheckpoint(ts)
	}
}

func (sync *OplogSyncer) calculateWorkerLowestCheckpoint() (v int64, err error) {
	// no need to lock and eventually consistence is acceptable
	allAcked := true
	candidates := make([]int64, 0, len(sync.batcher.workerGroup))
	allAckValues := make([]int64, 0, len(sync.batcher.workerGroup))
	for _, worker := range sync.batcher.workerGroup {
		// read ack value first because  we don't want
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
			// this is rarely happened. However, we have delayed for
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
	l.Logger.Infof("worker offset %v use lowest %v", candidates, utils.ExtractTimestampForLog(candidates[0]))
	return candidates[0], nil
}
