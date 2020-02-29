package collector

// persist oplog on disk

import (
	"mongoshake/collector/diskQueue"
	"mongoshake/collector/configure"
	"sync"
	"mongoshake/common"
	"sync/atomic"
	"time"

	"github.com/vinllen/mgo/bson"
	LOG "github.com/vinllen/log4go"
	"mongoshake/oplog"
)

const (
	ReplayerOplogStoreDiskReadBatch = 10000
)

type Persister struct {
	replset string // name
	sync *OplogSyncer // not owned, inner call

	// batch data([]byte) together and send to downstream
	buffer            [][]byte
	nextQueuePosition uint64

	// enable disk persist
	enableDiskPersist bool

	// stage of fetch and store oplog
	fetchStage int32
	// disk queue used to store oplog temporarily
	DiskQueue     *diskQueue.DiskQueue
	diskQueueMutex sync.Mutex // disk queue mutex
	diskQueueLastTs bson.MongoTimestamp // the last oplog timestamp in disk queue
}

func NewPersister(replset string, sync *OplogSyncer) *Persister {
	p := &Persister{
		replset: replset,
		sync: sync,
		buffer: make([][]byte, 0, conf.Options.FetcherBufferCapacity),
		nextQueuePosition: 0,
		enableDiskPersist: conf.Options.FullSyncOplogStoreDisk,
		fetchStage: utils.FetchStageStoreUnknown,
		diskQueueLastTs: -1,
	}

	if p.enableDiskPersist {
		go p.retrieve()
	}
	return p
}

func (p *Persister) SetFetchStage(fetchStage int32) {
	LOG.Info("persister replset[%v] update fetch status to: %v", p.replset, utils.LogFetchStage(fetchStage))
	atomic.StoreInt32(&p.fetchStage, fetchStage)
}

func (p *Persister) GetFetchStage() int32 {
	return atomic.LoadInt32(&p.fetchStage)
}

func (p *Persister) InitDiskQueue(dqName string) {
	fetchStage := p.GetFetchStage()
	// fetchStage shouldn't change between here
	if fetchStage != utils.FetchStageStoreDiskNoApply && fetchStage != utils.FetchStageStoreDiskApply {
		LOG.Crashf("persister replset[%v] init disk queue in illegal fetchStage %v",
			p.replset, utils.LogFetchStage(fetchStage))
	}
	if p.DiskQueue != nil {
		LOG.Crashf("init disk queue failed: already exist")
	}

	p.DiskQueue = diskQueue.NewDiskQueue(dqName, conf.Options.LogDirectory,
		conf.Options.FullSyncOplogStoreDiskMaxSize, ReplayerOplogStoreDiskReadBatch,
		1 << 30, 0, 1 << 26,
		1000, 2 * time.Second)
}

func (p *Persister) GetQueryTsFromDiskQueue() bson.MongoTimestamp {
	if p.DiskQueue == nil {
		LOG.Crashf("persister replset[%v] get query timestamp from nil disk queue", p.replset)
	}

	logData := p.DiskQueue.GetLastWriteData()
	if len(logData) == 0 {
		return 0
	}

	// TODO, oplog or change stream event?
	log := new(oplog.PartialLog)
	if err := bson.Unmarshal(logData, log); err != nil {
		LOG.Crashf("unmarshal oplog[%v] failed[%v]", logData, err)
	}
	return log.Timestamp
}

// inject data
func (p *Persister) Inject(input []byte) {
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
			if p.DiskQueue != nil { // double check
				// should send to disQueue
				if err := p.DiskQueue.Put(input); err != nil {
					LOG.Crashf("persister inject replset[%v] put oplog to disk queue failed[%v]",
						p.replset, err)
				}
			} else {
				// should send to pending queue
				p.PushToPendingQueue(input)
			}
		} else {
			LOG.Crashf("persister inject replset[%v] has no diskQueue with fetch stage[%v]",
				p.replset, utils.LogFetchStage(fetchStage))
		}
	} else {
		p.PushToPendingQueue(input)
	}
}

func (p *Persister) PushToPendingQueue(input []byte) {
	flush := false
	if input != nil {
		p.buffer = append(p.buffer, input)
	} else {
		flush = true
	}

	if len(p.buffer) >= conf.Options.FetcherBufferCapacity || (flush && len(p.buffer) != 0) {
		// we could simply ++syncer.resolverIndex. The max uint64 is 9223372036854774807
		// and discard the skip situation. we assume nextQueueCursor couldn't be overflow
		selected := int(p.nextQueuePosition % uint64(len(p.sync.PendingQueue)))
		p.sync.PendingQueue[selected] <- p.buffer

		// clear old buffer
		p.buffer = p.buffer[:0]

		// queue position = (queue position + 1) % n
		p.nextQueuePosition++
	}
}

func (p *Persister) retrieve() {
	for {
		stage := atomic.LoadInt32(&p.fetchStage)
		switch stage {
		case utils.FetchStageStoreDiskApply:
			break
		case utils.FetchStageStoreUnknown:
			// do nothing
		case utils.FetchStageStoreDiskNoApply:
			// do nothing
		default:
			LOG.Crashf("invalid fetch stage[%v]", utils.LogFetchStage(stage))
		}
		time.Sleep(3 * time.Second)
	}

	LOG.Info("persister retrieve for replset[%v] begin to read from disk queue with depth[%v]",
		p.replset, p.DiskQueue.Depth())
	ticker := time.NewTicker(time.Second)
Loop:
	for {
		select {
		case readData := <-p.DiskQueue.ReadChan():
			if len(readData) > 0 {
				for _, data := range readData {
					// or.oplogChan <- &retOplog{&bson.Raw{Kind: 3, Data: data}, nil}
					p.PushToPendingQueue(data)
				}

				if err := p.DiskQueue.Next(); err != nil {
					LOG.Crashf("persister replset[%v] retrieve get next failed[%v]", p.replset, err)
				}
			}
		case <-ticker.C:
			if p.DiskQueue.Depth() < p.DiskQueue.BatchCount() {
				break Loop
			}
		}
	}

	LOG.Info("persister retrieve for replset[%v] block fetch with disk queue depth[%v]",
		p.replset, p.DiskQueue.Depth())

	// wait to finish retrieve and continue fetch to store to memory
	p.diskQueueMutex.Lock()
	defer p.diskQueueMutex.Unlock() // lock till the end
	readData := p.DiskQueue.ReadAll()
	if len(readData) > 0 {
		for _, data := range readData {
			// or.oplogChan <- &retOplog{&bson.Raw{Kind: 3, Data: data}, nil}
			p.PushToPendingQueue(data)
		}

		// parse the last oplog timestamp
		// TODO, oplog or change stream event
		p.diskQueueLastTs = oplog.ParseTimestampFromBson(readData[len(readData)-1])

		if err := p.DiskQueue.Next(); err != nil {
			LOG.Crash(err)
		}
	}
	if p.DiskQueue.Depth() != 0 {
		LOG.Crashf("persister retrieve for replset[%v] finish, but disk queue depth[%v] is not empty",
			p.replset, p.DiskQueue.Depth())
	}
	p.SetFetchStage(utils.FetchStageStoreMemoryApply)

	if err := p.DiskQueue.Delete(); err != nil {
		LOG.Critical("persister retrieve for replset[%v] close disk queue error. %v", p.replset, err)
	}
	LOG.Info("persister retriever for replset[%v] exits", p.replset)
}
