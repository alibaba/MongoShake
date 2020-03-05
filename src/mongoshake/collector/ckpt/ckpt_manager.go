package ckpt

import (
	"errors"
	"fmt"

	"mongoshake/collector/configure"
	"mongoshake/common"

	"github.com/vinllen/mgo/bson"
	"sync"
)

const (
	CheckpointName            = "name"
)

type CheckpointManager struct {
	Type string

	ctx        *CheckpointContext
	ctxRecLock sync.Mutex
	ctxRec     *CheckpointContext // only used to store temporary value that will be lazy load
	delegate   CheckpointOperation
}

func NewCheckpointManager(name string, startPosition int32) *CheckpointManager {
	newManager := &CheckpointManager{}

	switch conf.Options.CheckpointStorage {
	case utils.VarCheckpointStorageApi:
		newManager.delegate = &HttpApiCheckpoint{
			CheckpointContext: CheckpointContext{
				Name:                   name,
				Timestamp:              bson.MongoTimestamp(int64(startPosition) << 32),
				Version:                utils.FcvCheckpoint.CurrentVersion,
				OplogDiskQueue:         "",
				OplogDiskQueueFinishTs: InitCheckpoint,
			},
			URL: conf.Options.CheckpointStorageCollection,
		}
	case utils.VarCheckpointStorageDatabase:
		db := utils.AppDatabase
		if conf.Options.IsShardCluster() {
			db = utils.VarCheckpointStorageDbShardingDefault
		}
		newManager.delegate = &MongoCheckpoint{
			CheckpointContext: CheckpointContext{
				Name:                   name,
				Timestamp:              bson.MongoTimestamp(int64(startPosition) << 32),
				Version:                utils.FcvCheckpoint.CurrentVersion,
				OplogDiskQueue:         "",
				OplogDiskQueueFinishTs: InitCheckpoint,
			},
			DB:    db,
			URL:   conf.Options.CheckpointStorageUrl,
			Table: conf.Options.CheckpointStorageCollection,
		}
	default:
		return nil
	}
	return newManager
}

// get persist checkpoint
func (manager *CheckpointManager) Get() (*CheckpointContext, bool, error) {
	var exist bool
	manager.ctx, exist = manager.delegate.Get()
	if manager.ctx == nil {
		return nil, exist, fmt.Errorf("get by checkpoint manager[%v] failed", manager.Type)
	}

	// check fcv
	if exist && utils.FcvCheckpoint.IsCompatible(manager.ctx.Version) == false {
		return nil, exist, fmt.Errorf("current checkpoint version is %v, input[%v] isn't compatible",
			utils.FcvCheckpoint.CurrentVersion, manager.ctx.Version)
	}

	return manager.ctx, exist, nil
}

// get in memory checkpoint
func (manager *CheckpointManager) GetInMemory() *CheckpointContext {
	return manager.ctx
}

func (manager *CheckpointManager) Update(ts bson.MongoTimestamp) error {
	if manager.ctx == nil || len(manager.ctx.Name) == 0 {
		return errors.New("current ckpt context is empty")
	}

	manager.ctx.Timestamp = ts
	manager.ctx.Version = utils.FcvCheckpoint.CurrentVersion

	// update OplogDiskQueueFinishTs if set
	if manager.ctxRec != nil {
		if manager.ctx.OplogDiskQueueFinishTs != manager.ctxRec.OplogDiskQueueFinishTs {
			manager.ctx.OplogDiskQueueFinishTs = manager.ctxRec.OplogDiskQueueFinishTs
		}
		if manager.ctx.OplogDiskQueue != manager.ctxRec.OplogDiskQueue {
			manager.ctx.OplogDiskQueue = manager.ctxRec.OplogDiskQueue
		}
	}

	return manager.delegate.Insert(manager.ctx)
}

// OplogDiskQueueFinishTs and OplogDiskQueue won't immediate effect, will be inserted in the next Update call.
func (manager *CheckpointManager) SetOplogDiskFinishTs(ts bson.MongoTimestamp) {
	if manager.ctxRec == nil {
		manager.ctxRecLock.Lock()
		if manager.ctxRec == nil { // double check
			manager.ctxRec = new(CheckpointContext)
		}
		manager.ctxRecLock.Unlock()
	}
	manager.ctxRec.OplogDiskQueueFinishTs = ts
}

func (manager *CheckpointManager) SetOplogDiskQueueName(name string) {
	if manager.ctxRec == nil {
		manager.ctxRecLock.Lock()
		if manager.ctxRec == nil { // double check
			manager.ctxRec = new(CheckpointContext)
		}
		manager.ctxRecLock.Unlock()
	}
	manager.ctxRec.OplogDiskQueue = name
}
