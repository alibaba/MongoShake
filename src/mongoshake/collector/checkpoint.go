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

	"github.com/vinllen/mgo/bson"
	LOG "github.com/vinllen/log4go"
)

func (sync *OplogSyncer) newCheckpointManager(name string) {
	LOG.Info("Oplog sync create checkpoint manager with [%s] [%s]",
		conf.Options.ContextStorage, conf.Options.ContextAddress)
	sync.ckptManager = ckpt.NewCheckpointManager(name)
}

func (sync *OplogSyncer) checkpoint() {
	now := time.Now()

	// do checkpoint every once in a while
	if sync.ckptTime.Add(time.Duration(conf.Options.CheckpointInterval) * time.Millisecond).After(now) {
		return
	}
	// we force update the ckpt time even failed
	sync.ckptTime = now

	// TODO: we delayed a few minutes to tolerate the receiver's flush buffer
	// in AckRequired() tunnel. such as "rpc". While collector is restarted,
	// we can't get the correct worker ack offset since collector have lost
	// the unack offset...
	if now.Before(sync.startTime.Add(3 * time.Minute)) {
		//LOG.Info("CheckpointOperation requires three minutes at least to flush receiver's buffer")
		return
	}

	// read all workerGroup self ckpt. get minimum of all updated checkpoint
	inMemoryTs := sync.ckptManager.GetInMemory().Timestamp
	var lowest int64 = 0
	var err error
	if lowest, err = sync.calculateWorkerLowestCheckpoint(); lowest > 0 && err == nil {
		switch {
		case bson.MongoTimestamp(lowest) > inMemoryTs:
			if err = sync.ckptManager.Update(bson.MongoTimestamp(lowest)); err == nil {
				LOG.Info("CheckpointOperation write success. updated from %d to %d", inMemoryTs, lowest)
				sync.replMetric.AddCheckpoint(1)
				sync.replMetric.SetLSNCheckpoint(lowest)
				return
			}
		case bson.MongoTimestamp(lowest) < inMemoryTs:
			LOG.Info("CheckpointOperation calculated is smaller than value in memory. lowest %d current %d",
				lowest, inMemoryTs)
			return
		case bson.MongoTimestamp(lowest) == inMemoryTs:
			return
		}
	}
	LOG.Warn("CheckpointOperation updated is not suitable. lowest [%d]. current [%d]. reason : %v",
		lowest, utils.TimestampToInt64(inMemoryTs), err)
}

func (sync *OplogSyncer) calculateWorkerLowestCheckpoint() (v int64, err error) {
	// don't need to lock and eventually consistence is acceptable
	allAcked := true
	candidates := make([]int64, 0, 128)
	allAckValues := make([]int64, 0, 128)
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
			return 0, fmt.Errorf("cadidates should follow up unack[%d] ack[%d]", unack, ack)
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
