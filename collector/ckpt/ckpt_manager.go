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
	fullFinSet bool
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
// The lock covers the whole read-modify-write so a concurrent caller (e.g. the
// parallel-mode full-finish backfill, #1047) cannot interleave with the
// checkpoint routine.
func (manager *CheckpointManager) Update(ts int64) error {
	manager.ctxRecLock.Lock()
	defer manager.ctxRecLock.Unlock()

	if manager.ctx == nil || len(manager.ctx.Name) == 0 {
		// must run Get() first
		return errors.New("current ckpt context is empty")
	}

	candidate := *manager.ctx
	candidate.Timestamp = ts
	candidate.Version = utils.FcvCheckpoint.CurrentVersion

	// update OplogDiskQueueFinishTs if set
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
		if manager.fullFinSet {
			candidate.FullFinishTs = manager.ctxRec.FullFinishTs
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
	manager.fullFinSet = false
	return nil
}

// FlushStaged persists staged fields at the current timestamp without advancing
// it. Needed when no oplog traffic moves the checkpoint forward (e.g. full sync
// just finished on an idle source) but the staged value must survive a restart.
func (manager *CheckpointManager) FlushStaged() error {
	manager.ctxRecLock.Lock()
	if manager.ctx == nil {
		manager.ctxRecLock.Unlock()
		return errors.New("current ckpt context is empty")
	}
	ts := manager.ctx.Timestamp
	manager.ctxRecLock.Unlock()
	return manager.Update(ts)
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

// SetFullFinishTs stages the full-sync finish timestamp (#1047); it is written
// on the next Update call, like the other staged fields.
func (manager *CheckpointManager) SetFullFinishTs(ts int64) {
	manager.ctxRecLock.Lock()
	defer manager.ctxRecLock.Unlock()
	if manager.ctxRec == nil {
		manager.ctxRec = new(CheckpointContext)
	}
	manager.ctxRec.FullFinishTs = ts
	manager.fullFinSet = true
}
