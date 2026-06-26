package collector

// persist oplog on disk

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	nimo "github.com/gugemichael/nimo4go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/alibaba/MongoShake/v2/collector/ckpt"
	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	"github.com/alibaba/MongoShake/v2/collector/spool"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
	l "github.com/alibaba/MongoShake/v2/pkg/log"
)

const (
	FullSyncReaderOplogStoreDiskReadBatch = 10000
)

var (
	persisterRetrieveWaitInterval = 3 * time.Second
	persisterRetrieveReadInterval = time.Second
)

type Persister struct {
	replset string       // name
	sync    *OplogSyncer // not owned, inner call

	// batch data([]byte) together and send to downstream
	Buffer            [][]byte
	bufferSize        uint64
	nextQueuePosition uint64

	// enable disk persist
	enableDiskPersist bool

	// stage of fetch and store oplog
	fetchStage int32
	// disk queue name is kept for checkpoint compatibility, but the implementation is a Pebble spool.
	DiskQueue       spool.OplogSpool
	diskQueueMutex  sync.Mutex // disk queue mutex
	diskQueueLastTs int64      // the last oplog timestamp in disk queue(full timestamp, have T + I)

	// metric info, used in print
	diskWriteCount uint64
	diskReadCount  uint64
}

func NewPersister(replset string, sync *OplogSyncer) *Persister {
	p := &Persister{
		replset:           replset,
		sync:              sync,
		Buffer:            make([][]byte, 0, conf.Options.IncrSyncFetcherBufferCapacity),
		bufferSize:        0,
		nextQueuePosition: 0,
		enableDiskPersist: conf.Options.SyncMode == utils.VarSyncModeAll &&
			conf.Options.FullSyncReaderOplogStoreDisk,
		fetchStage:      utils.FetchStageStoreUnknown,
		diskQueueLastTs: -1, // initial set 1
	}
	p.updateBufferUsedMetric()

	return p
}

func (p *Persister) Start() {
	if p.enableDiskPersist {
		go p.retrieve()
	}
}

func (p *Persister) SetFetchStage(fetchStage int32) {
	l.Logger.Infof("persister replset[%v] update fetch status to: %v", p.replset, utils.LogFetchStage(fetchStage))
	atomic.StoreInt32(&p.fetchStage, fetchStage)
}

func (p *Persister) GetFetchStage() int32 {
	return atomic.LoadInt32(&p.fetchStage)
}

func (p *Persister) InitDiskQueue(dqName string, createIfMissing bool) error {
	fetchStage := p.GetFetchStage()
	// fetchStage shouldn't change between here
	if fetchStage != utils.FetchStageStoreDiskNoApply && fetchStage != utils.FetchStageStoreDiskApply {
		return fmt.Errorf("persister replset[%v] init disk queue in illegal fetchStage %v",
			p.replset, utils.LogFetchStage(fetchStage))
	}
	if p.DiskQueue != nil {
		return fmt.Errorf("init disk queue failed: already exist")
	}

	opened, err := spool.Open(spool.OpenOptions{
		Name:            dqName,
		LogDir:          conf.Options.LogDirectory,
		CreateIfMissing: createIfMissing,
		MetricName:      p.replset,
		MetricStage:     utils.TypeIncr,
		MaxBytesMB:      conf.Options.FullSyncReaderOplogStoreDiskMaxSize,
	})
	if err != nil {
		return err
	}
	p.DiskQueue = opened
	l.Logger.Infof("persister replset[%v] open pebble oplog spool name[%v] path[%v] create[%v]",
		p.replset, dqName, opened.Stats().Path, createIfMissing)
	return nil
}

func (p *Persister) GetQueryTsFromDiskQueue() primitive.Timestamp {
	if p.DiskQueue == nil {
		l.Logger.Panicf("persister replset[%v] get query timestamp from nil disk queue", p.replset)
	}

	logData, err := p.DiskQueue.LastWriteData()
	if err != nil {
		l.Logger.Panicf("persister replset[%v] get last write data from disk queue failed[%v]",
			p.replset, err)
	}
	if len(logData) == 0 {
		return primitive.Timestamp{}
	}

	if conf.Options.IncrSyncMongoFetchMethod == utils.VarIncrSyncMongoFetchMethodOplog {
		ts, err := oplog.ExtractRawTimestamp(logData, "ts")
		if err != nil {
			l.Logger.Panicf("parse raw oplog timestamp failed[%v]", err)
		}
		return ts
	} else {
		// change_stream
		log := new(oplog.Event)
		if err := bson.Unmarshal(logData, log); err != nil {
			l.Logger.Panicf("unmarshal oplog[%v] failed[%v]", logData, err)
		}

		// assert
		if log.OperationType == "" {
			l.Logger.Panicf("unmarshal data to change stream event failed: %v", log)
		}
		return log.ClusterTime
	}
}

// Inject inject data
func (p *Persister) Inject(input []byte) {
	// only used to test the reader, discard anything
	switch conf.Options.IncrSyncReaderDebug {
	case utils.VarIncrSyncReaderDebugNone:
		break
	case utils.VarIncrSyncReaderDebugDiscard:
		return
	case utils.VarIncrSyncReaderDebugPrint:
		var test interface{}
		err := bson.Unmarshal(input, &test)
		if err != nil {
			l.Logger.Errorf("unmarshal failed: %v", err)
			return
		}
		l.Logger.Infof("print debug: %v", test)
	default:
		break
	}

	if p.enableDiskPersist {
		// current fetch stage
		fetchStage := p.GetFetchStage()
		if fetchStage == utils.FetchStageStoreMemoryApply {
			p.PushToPendingQueue(input)
		} else if p.DiskQueue != nil {
			if input == nil {
				// no need to store
				return
			}

			// store local
			p.diskQueueMutex.Lock()
			defer p.diskQueueMutex.Unlock()
			if p.DiskQueue != nil { // double check
				// should send to diskQueue
				if err := p.DiskQueue.Put(input); err != nil {
					l.Logger.Panicf("persister inject replset[%v] put oplog to disk queue failed[%v]",
						p.replset, err)
				}
				atomic.AddUint64(&p.diskWriteCount, 1)
			} else {
				// should send to pending queue
				p.PushToPendingQueue(input)
			}
		} else {
			l.Logger.Panicf("persister inject replset[%v] has no diskQueue with fetch stage[%v]",
				p.replset, utils.LogFetchStage(fetchStage))
		}
	} else {
		p.PushToPendingQueue(input)
	}
}

func (p *Persister) bufferInput(input []byte) {
	p.Buffer = append(p.Buffer, input)
	p.bufferSize += uint64(len(input))
	p.updateBufferUsedMetric()
}
func (p *Persister) shouldDispatchBuffer(flush bool) bool {
	if len(p.Buffer) >= conf.Options.IncrSyncFetcherBufferCapacity {
		return true
	}
	if conf.Options.IncrSyncFetcherBufferSizeThresholdInKB > 0 &&
		p.bufferSize >= uint64(conf.Options.IncrSyncFetcherBufferSizeThresholdInKB*1024) {
		return true
	}
	return flush && len(p.Buffer) != 0
}
func (p *Persister) dispatchBuffer() {
	// we could simply ++syncer.resolverIndex. The max uint64 is 9223372036854774807
	// and discard the skip situation. we assume nextQueueCursor couldn't be overflow
	selected := int(p.nextQueuePosition % uint64(len(p.sync.PendingQueue)))
	p.sync.PendingQueue[selected] <- p.Buffer
	p.sync.updatePendingQueueMetric(selected)
	// clear old Buffer, we shouldn't use "p.Buffer = p.Buffer[:0]" because these address won't
	// be changed in the channel.
	// p.Buffer = p.Buffer[:0]
	p.Buffer = make([][]byte, 0, conf.Options.IncrSyncFetcherBufferCapacity)
	p.bufferSize = 0
	p.updateBufferUsedMetric()
	// queue position = (queue position + 1) % n
	p.nextQueuePosition++
}
func (p *Persister) PushToPendingQueue(input []byte) {
	flush := false
	if input != nil {
		p.bufferInput(input)
	} else {
		flush = true
	}

	if p.shouldDispatchBuffer(flush) {
		p.dispatchBuffer()
	}
}

func (p *Persister) updateBufferUsedMetric() {
	if p == nil {
		return
	}

	used := len(p.Buffer)
	capacity := cap(p.Buffer)
	utils.PersisterBufferUsedProm.WithLabelValues(p.replset, utils.TypeIncr).Set(float64(used))
	utils.PersisterBufferCapacityProm.WithLabelValues(p.replset, utils.TypeIncr).Set(float64(capacity))
	utils.PersisterBufferUsedRatioProm.WithLabelValues(p.replset, utils.TypeIncr).Set(utils.QueueUsedRatio(used, capacity))
}

func (p *Persister) retrieve() {
	waitTicker := time.NewTicker(persisterRetrieveWaitInterval)
	defer waitTicker.Stop()
	waitRounds := 0
Wait:
	for range waitTicker.C {
		waitRounds++
		stage := atomic.LoadInt32(&p.fetchStage)
		switch stage {
		case utils.FetchStageStoreDiskApply:
			l.Logger.Infof("persister retrieve for replset[%v] entered disk-apply stage after %d wait rounds",
				p.replset, waitRounds)
			break Wait
		case utils.FetchStageStoreMemoryApply:
			l.Logger.Infof("persister retrieve for replset[%v] skip disk replay after %d wait rounds: "+
				"fetchStage is MemoryApply (restart with checkpoint caught up to OplogDiskQueueFinishTs)",
				p.replset, waitRounds)
			return
		case utils.FetchStageStoreUnknown:
			// do nothing
		case utils.FetchStageStoreDiskNoApply:
			// do nothing
		default:
			l.Logger.Panicf("invalid fetch stage[%v]", utils.LogFetchStage(stage))
		}
	}

	if p.DiskQueue == nil {
		l.Logger.Panicf("persister retrieve for replset[%v] entered disk-apply stage with nil spool", p.replset)
	}
	depth, err := p.DiskQueue.Depth()
	if err != nil {
		l.Logger.Panicf("persister retrieve for replset[%v] get pebble spool depth failed[%v]", p.replset, err)
	}
	l.Logger.Infof("persister retrieve for replset[%v] begin to read from pebble spool with depth[%v]",
		p.replset, depth)

	readTicker := time.NewTicker(persisterRetrieveReadInterval)
	defer readTicker.Stop()
	replayedAny := false
	for {
		readData, err := p.DiskQueue.ReadBatch(FullSyncReaderOplogStoreDiskReadBatch)
		if err != nil {
			l.Logger.Panicf("persister replset[%v] retrieve read pebble spool failed[%v]", p.replset, err)
		}
		if len(readData) > 0 {
			atomic.AddUint64(&p.diskReadCount, uint64(len(readData)))
			for _, data := range readData {
				p.PushToPendingQueue(data)
			}
			replayedAny = true

			// move to next read
			if err := p.DiskQueue.Advance(len(readData)); err != nil {
				l.Logger.Panicf("persister replset[%v] retrieve advance pebble spool failed[%v]", p.replset, err)
			}
			if len(readData) < FullSyncReaderOplogStoreDiskReadBatch {
				break
			}
			continue
		}

		<-readTicker.C
		depth, err = p.DiskQueue.Depth()
		if err != nil {
			l.Logger.Panicf("persister retrieve for replset[%v] get pebble spool depth failed[%v]", p.replset, err)
		}
		if depth < FullSyncReaderOplogStoreDiskReadBatch {
			break
		}
	}

	stats := p.DiskQueue.Stats()
	l.Logger.Infof("persister retrieve for replset[%v] block fetch with pebble spool read_seq[%v] write_seq[%v] depth[%v]",
		p.replset, stats.ReadSeq, stats.WriteSeq, stats.Depth)

	// wait to finish retrieve and continue fetch to store to memory
	p.diskQueueMutex.Lock()
	defer p.diskQueueMutex.Unlock() // lock till the end
	readData, err := p.DiskQueue.ReadAll()
	if err != nil {
		l.Logger.Panicf("persister replset[%v] retrieve drain pebble spool failed[%v]", p.replset, err)
	}
	if len(readData) > 0 {
		atomic.AddUint64(&p.diskReadCount, uint64(len(readData)))
		for _, data := range readData {
			// or.oplogChan <- &retOplog{&bson.Raw{Kind: 3, Data: data}, nil}
			p.PushToPendingQueue(data)
		}
		replayedAny = true

		if err := p.DiskQueue.Advance(len(readData)); err != nil {
			l.Logger.Panicf("persister replset[%v] retrieve advance drained pebble spool failed[%v]", p.replset, err)
		}
	}
	if replayedAny {
		p.PushToPendingQueue(nil)

		// parse the last oplog timestamp
		lastTs := utils.TimeStampToInt64(p.GetQueryTsFromDiskQueue())
		if lastTs > 0 {
			p.diskQueueLastTs = lastTs
		}
	} else {
		p.diskQueueLastTs = ckpt.InitCheckpoint
	}
	depth, err = p.DiskQueue.Depth()
	if err != nil {
		l.Logger.Panicf("persister retrieve for replset[%v] get final pebble spool depth failed[%v]", p.replset, err)
	}
	if depth != 0 {
		l.Logger.Panicf("persister retrieve for replset[%v] finish, but pebble spool depth[%v] is not empty",
			p.replset, depth)
	}
	p.SetFetchStage(utils.FetchStageStoreMemoryApply)

	if err := p.DiskQueue.Delete(); err != nil {
		l.Logger.Criticalf("persister retrieve for replset[%v] delete pebble spool error. %v", p.replset, err)
	} else {
		l.Logger.Infof("persister retrieve for replset[%v] delete pebble spool success", p.replset)
	}
	p.DiskQueue = nil
	l.Logger.Infof("persister retriever for replset[%v] exits", p.replset)
}

func (p *Persister) spoolStats() spool.Stats {
	if p == nil || p.DiskQueue == nil {
		return spool.Stats{}
	}
	return p.DiskQueue.Stats()
}

func (p *Persister) RestAPI() {
	type PersistNode struct {
		BufferUsed              int    `json:"buffer_used"`
		BufferSize              int    `json:"buffer_size"`
		BufferSizeThresholdInKB int    `json:"buffer_size_threshold_in_kb"`
		EnableDiskPersist       bool   `json:"enable_disk_persist"`
		FetchStage              string `json:"fetch_stage"`
		DiskWriteCount          uint64 `json:"disk_write_count"`
		DiskReadCount           uint64 `json:"disk_read_count"`
		SpoolDepth              uint64 `json:"spool_depth"`
		SpoolReadSeq            uint64 `json:"spool_read_seq"`
		SpoolWriteSeq           uint64 `json:"spool_write_seq"`
	}

	utils.IncrSyncHttpApi.RegisterAPI("/persist", nimo.HttpGet, func([]byte) interface{} {
		stats := p.spoolStats()
		return &PersistNode{
			BufferSize:              conf.Options.IncrSyncFetcherBufferCapacity,
			BufferSizeThresholdInKB: conf.Options.IncrSyncFetcherBufferSizeThresholdInKB,
			BufferUsed:              len(p.Buffer),
			EnableDiskPersist:       p.enableDiskPersist,
			FetchStage:              utils.LogFetchStage(p.GetFetchStage()),
			DiskWriteCount:          atomic.LoadUint64(&p.diskWriteCount),
			DiskReadCount:           atomic.LoadUint64(&p.diskReadCount),
			SpoolDepth:              stats.Depth,
			SpoolReadSeq:            stats.ReadSeq,
			SpoolWriteSeq:           stats.WriteSeq,
		}
	})
}
