package collector

import (
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
	"github.com/alibaba/MongoShake/v2/tunnel"

	nimo "github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
)

const MaxUnAckListLength = 128 * 256

type Worker struct {
	// parent syncer
	syncer *OplogSyncer

	// worker sequence id
	id uint32

	// job queue from OplogTailer
	queue chan []*oplog.GenericOplog
	// worker tunnel controller (include tunnel and modules)
	writeController *WriteController
	// buffered oplogs . include unack buffer
	// and send buffer
	listUnACK []*oplog.GenericOplog

	// ack offset (used for checkpoint)
	ack, unack int64
	count      uint64
	// if all oplogs are acked for righ now
	allAcked bool
	// retransmit on tunnel controller tunnel required
	retransmit bool

	// event listener
	eventListener *TransferEventListener
}

type TransferEventListener struct {
	whenTransferBatchSuccess func(worker *Worker, buffer []*oplog.GenericOplog)
	whenTransferRetry        func(worker *Worker, buffer []*oplog.GenericOplog)
}

func NewWorker(syncer *OplogSyncer, id uint32) *Worker {
	return &Worker{
		syncer: syncer,
		id:     id,
		queue:  make(chan []*oplog.GenericOplog, conf.Options.IncrSyncWorkerBatchQueueSize),
	}
}

func (worker *Worker) String() string {
	return fmt.Sprintf("Collector-worker-%d", worker.id)
}

func (worker *Worker) Init() bool {
	worker.RestAPI()
	worker.writeController = NewWriteController(worker)
	if worker.writeController == nil {
		return false
	}

	return true
}

func (worker *Worker) SetInitSyncFinishTs(fullSyncFinishPosition int64) {
	worker.writeController.SetInitSyncFinishTs(fullSyncFinishPosition)
}

func (worker *Worker) IsAllAcked() bool {
	return worker.allAcked
}

func (worker *Worker) AllAcked(allAcked bool) {
	worker.allAcked = allAcked
}

func (worker *Worker) Offer(batch []*oplog.GenericOplog) {
	if batch != nil {
		atomic.StoreInt64(&worker.unack, utils.TimestampToInt64(batch[len(batch)-1].Parsed.Timestamp))
	}
	worker.queue <- batch
}

func (worker *Worker) shouldDelay() bool {
	// unack buffer is too big. There should be a mass of accumulated oplogs
	// have already sent but not be ack yet. No more oplogs pushed !
	if len(worker.listUnACK) > MaxUnAckListLength {
		// try to transfer remained oplogs to free some space
		return true
	}
	return false
}

func (worker *Worker) shouldStall() bool {
	// suspend while system pause is set. This is always operated by outside
	// manual system. It needs stopping here util turned off
	return utils.IncrSentinelOptions.Pause
}

func (worker *Worker) findFirstAvailableBatch() []*oplog.GenericOplog {
	var batch []*oplog.GenericOplog
	for {
		select {
		case batch = <-worker.queue:
		case <-time.After(DDLCheckpointInterval * time.Millisecond): // timeout, add probe message here
			return nil
		}

		// if we got empty (nil) batch. check the queue still has more
		// oplogs. That will merge continuous nil batch to single one
		if batch != nil || len(worker.queue) == 0 {
			return batch
		}
	}
}

func (worker *Worker) StartWorker() {
	LOG.Info("%s start working with jobs batch queue. buffer capacity %d",
		worker, cap(worker.queue))

	var batch []*oplog.GenericOplog
	for {
		switch {
		case worker.shouldDelay():
			// we guess there were lots of oplogs have been pended in jobs queue.
			// we need wait for a few moment
			worker.probe()
			worker.purgeACK()
			fallthrough
		case worker.shouldStall():
			utils.DelayFor(10)
		default:
			if batch = worker.findFirstAvailableBatch(); batch == nil {
				worker.probe()
			} else {
				worker.transfer(batch)
				worker.syncer.replMetric.AddConsume(uint64(len(batch)))
			}
			utils.DEBUG_LOG("%s poll queued batch oplogs. total[%d]", worker, len(batch))
		}
	}
}

/*
 *
 *  [ Before transfer ]
 *
 *	batch 			|9,10,11|
 *	listSent			|1,2,3,4,5,6,7,8|
 *
 *  [ After transfer ]
 *
 *	batch			| (empty) |
 *	listSent			|1,2,3,4,5,6,7,8,9,10,11|
 *
 *  [ Purge listWaitACK (ack == 7) ]
 *
 *	listUnACK		|8,9,10,11|
 *
 */
func (worker *Worker) transfer(batch []*oplog.GenericOplog) {
	nimo.AssertTrue(batch != nil, "batch oplogs should not empty")
	var logs []*oplog.GenericOplog
	var tag uint32
	done := false

	// transfer util current batch is sent(done == true)
	for !done {
		if worker.retransmit {
			// TODO: send all unack logs ? it's possible very big
			logs = worker.listUnACK
			tag = tunnel.MsgRetransmission
			worker.syncer.replMetric.AddRetransmission(1)
		} else {
			logs = batch
			tag = tunnel.MsgNormal
		}
		replyAndAcked := worker.writeController.Send(logs, tag)

		LOG.Info("%s transfer retransmit:%t send [%d] logs. reply_acked [%v], list_unack [%d] ",
			worker, worker.retransmit, len(logs), utils.ExtractTimestampForLog(replyAndAcked), len(worker.listUnACK))

		switch {
		case replyAndAcked >= 0:
			if !worker.retransmit {
				worker.count += uint64(len(logs))
				worker.syncer.replMetric.SetLSNACK(replyAndAcked)
				worker.syncer.replMetric.AddApply(uint64(len(logs)))
				worker.retain(batch)
				// update ack
				atomic.StoreInt64(&worker.ack, replyAndAcked)
				done = true
			}
			// remove all unack values which are smaller than worker.ack
			worker.purgeACK()
			// reset
			worker.retransmit = false
			// notify success listener
			worker.syncer.replMetric.ReplStatus.Clear(utils.TunnelSendBad)

		case replyAndAcked == tunnel.ReplyRetransmission:
			LOG.Info("%s received ReplyRetransmission reply, now status %t", worker, worker.retransmit)
			// next step. keep trying with retransmission util we received
			// a non-retransmission message
			worker.retransmit = true

		default:
			LOG.Warn("%s transfer oplogs failed with reply value %d", worker, replyAndAcked)
			// we treat batched logs fail as just one time failed. and
			// notify failed retry listener
			worker.syncer.replMetric.AddFailed(1)
			//worker.retransmit = true
			worker.syncer.replMetric.ReplStatus.Update(utils.TunnelSendBad)
		}
	}
}

func (worker *Worker) probe() {
	if replyAcked := worker.writeController.Send([]*oplog.GenericOplog{}, tunnel.MsgProbe); replyAcked > 0 {
		// only change ack offset on reply is OK
		worker.syncer.replMetric.SetLSNACK(replyAcked)
		atomic.StoreInt64(&worker.ack, replyAcked)
	}
}

func (worker *Worker) retain(batch []*oplog.GenericOplog) {
	worker.listUnACK = append(worker.listUnACK, batch...)
	LOG.Debug("%s copy batch oplogs [%d] to listUnACK count. UnACK remained [%d]", worker, len(batch), len(worker.listUnACK))
}

func (worker *Worker) purgeACK() {
	bigger := sort.Search(len(worker.listUnACK), func(i int) bool {
		return utils.TimestampToInt64(worker.listUnACK[i].Parsed.Timestamp) > worker.ack
	})

	if bigger != 0 {
		LOG.Debug("%s purge unacked [lsn_ack:%d]. keep slice position from %d util %d",
			worker, worker.ack, bigger, len(worker.listUnACK))
		worker.listUnACK = worker.listUnACK[bigger:]
		worker.syncer.replMetric.AddSuccess(uint64(bigger))
	}
}

func (worker *Worker) RestAPI() {
	type WorkerInfo struct {
		Id              uint32 `json:"worker_id"`
		JobsQueued      int    `json:"jobs_in_queue"`
		JobsUnACKBuffer int    `json:"jobs_unack_buffer"`
		LastUnACK       string `json:"last_unack"`
		LastACK         string `json:"last_ack"`
		COUNT           uint64 `json:"count"`
	}

	utils.IncrSyncHttpApi.RegisterAPI("/worker", nimo.HttpGet, func([]byte) interface{} {
		return &WorkerInfo{
			Id:              worker.id,
			JobsQueued:      len(worker.queue),
			JobsUnACKBuffer: len(worker.listUnACK),
			LastUnACK:       utils.Int64ToString(worker.unack),
			LastACK:         utils.Int64ToString(worker.ack),
			COUNT:           worker.count,
		}
	})
}
