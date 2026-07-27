package ckpt

import (
	"errors"
	"fmt"
	"sync"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
)

const (
	CheckpointName = "name"
)

type CheckpointManager struct {
	Type string

	ctx        *CheckpointContext
	ctxRecLock sync.Mutex
	ctxRec     *CheckpointContext // only used to store temporary value that will be lazy load
	finishSet  bool
	queueSet   bool
	fetchSet   bool
	delegate   CheckpointOperation
}

func NewCheckpointManager(name string, startPosition int64) *CheckpointManager {
	newManager := &CheckpointManager{}

	switch conf.Options.CheckpointStorage {
	case utils.VarCheckpointStorageApi:
		newManager.delegate = &HttpApiCheckpoint{
			CheckpointContext: CheckpointContext{
				Name:                   name,
				Timestamp:              startPosition,
				Version:                utils.FcvCheckpoint.CurrentVersion,
				OplogDiskQueue:         "",
				OplogDiskQueueFinishTs: InitCheckpoint,
			},
			URL: conf.Options.CheckpointStorageCollection,
		}
	case utils.VarCheckpointStorageDatabase:
		newManager.delegate = &MongoCheckpoint{
			CheckpointContext: CheckpointContext{
				Name:                   name,
				Timestamp:              startPosition,
				Version:                utils.FcvCheckpoint.CurrentVersion,
				OplogDiskQueue:         "",
				OplogDiskQueueFinishTs: InitCheckpoint,
			},
			DB:    conf.Options.CheckpointStorageDb,
			URL:   conf.Options.CheckpointStorageUrl,
			Table: conf.Options.CheckpointStorageCollection,
		}
	default:
		return nil
	}
	return newManager
}

// Get return persistent checkpoint
func (manager *CheckpointManager) Get() (*CheckpointContext, bool, error) {
	var exist bool
	manager.ctx, exist = manager.delegate.Get()
	if manager.ctx == nil {
		return nil, exist, fmt.Errorf("get by checkpoint info from db or api failed, please see err log")
	}

	// check fcv
	if exist && utils.FcvCheckpoint.IsCompatible(manager.ctx.Version) == false {
		return nil, exist, fmt.Errorf("current required checkpoint version[%v] > input[%v],"+
			" please upgrade MongoShake to version >= %v",
			utils.FcvCheckpoint.CurrentVersion, manager.ctx.Version,
			utils.LowestCheckpointVersion[utils.FcvCheckpoint.CurrentVersion])
	}

	return manager.ctx, exist, nil
}

// GetInMemory return in-memory checkpoint
func (manager *CheckpointManager) GetInMemory() *CheckpointContext {
	return manager.ctx
}

// Update checkpoint update memory & persistence(db or file)
func (manager *CheckpointManager) Update(ts int64) error {
	if manager.ctx == nil || len(manager.ctx.Name) == 0 {
		// must run Get() first
		return errors.New("current ckpt context is empty")
	}

	candidate := *manager.ctx
	candidate.Timestamp = ts
	candidate.Version = utils.FcvCheckpoint.CurrentVersion

	// update OplogDiskQueueFinishTs if set
	manager.ctxRecLock.Lock()
	defer manager.ctxRecLock.Unlock()
	if manager.ctxRec != nil {
		if manager.finishSet {
			candidate.OplogDiskQueueFinishTs = manager.ctxRec.OplogDiskQueueFinishTs
		}
		if manager.queueSet {
			candidate.OplogDiskQueue = manager.ctxRec.OplogDiskQueue
		}
		if manager.fetchSet {
			candidate.FetchMethod = manager.ctxRec.FetchMethod
		}
	}

	if err := manager.delegate.Insert(&candidate); err != nil {
		return err
	}
	manager.ctx = &candidate
	manager.ctxRec = nil
	manager.finishSet = false
	manager.queueSet = false
	manager.fetchSet = false
	return nil
}

// SetOplogDiskFinishTs
// OplogDiskQueueFinishTs and OplogDiskQueue won't take effect immediately, will be inserted in the next Update call.
func (manager *CheckpointManager) SetOplogDiskFinishTs(ts int64) {
	manager.ctxRecLock.Lock()
	defer manager.ctxRecLock.Unlock()
	if manager.ctxRec == nil {
		manager.ctxRec = new(CheckpointContext)
	}
	manager.ctxRec.OplogDiskQueueFinishTs = ts
	manager.finishSet = true
}

func (manager *CheckpointManager) SetOplogDiskQueueName(name string) {
	manager.ctxRecLock.Lock()
	defer manager.ctxRecLock.Unlock()
	if manager.ctxRec == nil {
		manager.ctxRec = new(CheckpointContext)
	}
	manager.ctxRec.OplogDiskQueue = name
	manager.queueSet = true
}

func (manager *CheckpointManager) SetFetchMethod(method string) {
	manager.ctxRecLock.Lock()
	defer manager.ctxRecLock.Unlock()
	if manager.ctxRec == nil {
		manager.ctxRec = new(CheckpointContext)
	}
	manager.ctxRec.FetchMethod = method
	manager.fetchSet = true
}
