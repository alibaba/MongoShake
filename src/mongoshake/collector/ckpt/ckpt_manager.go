package ckpt

import (
	"errors"

	"mongoshake/collector/configure"
	"mongoshake/common"

	"github.com/vinllen/mgo/bson"
	"fmt"
)

const (
	StorageTypeAPI            = "api"
	StorageTypeDB             = "database"
	CheckpointDefaultDatabase = utils.AppDatabase
	CheckpointAdminDatabase   = "admin"
	CheckpointName            = "name"

	/*
	 * version: 0(or set not), MongoShake < 2.4, fcv == 0
	 * version: 1, MongoShake >= 2.4, 0 <= fcv <= 1
	 */
	CurrentVersion           = 1
	FeatureCompatibleVersion = 0
)

type CheckpointContext struct {
	Name                   string              `bson:"name" json:"name"`
	Timestamp              bson.MongoTimestamp `bson:"ckpt" json:"ckpt"`
	Version                int                 `bson:"version" json:"version"`
	OplogDiskQueue         string              `bson:"oplog_disk_queue" json:"oplog_disk_queue"`
	OplogDiskQueueFinishTs bson.MongoTimestamp `bson:"oplog_disk_queue_apply_finish_ts" json:"oplog_disk_queue_apply_finish_ts"`
}

type CheckpointManager struct {
	Type string

	ctx      *CheckpointContext
	ctxRec   *CheckpointContext // only used to store temporary value that will be lazy load
	delegate CheckpointOperation
}

func NewCheckpointManager(name string, startPosition int32) *CheckpointManager {
	newManager := &CheckpointManager{}

	switch conf.Options.ContextStorage {
	case StorageTypeAPI:
		newManager.delegate = &HttpApiCheckpoint{
			CheckpointContext: CheckpointContext{
				Name:                   name,
				Timestamp:              bson.MongoTimestamp(int64(startPosition) << 32),
				Version:                CurrentVersion,
				OplogDiskQueue:         "",
				OplogDiskQueueFinishTs: InitCheckpoint,
			},
			URL: conf.Options.ContextAddress,
		}
	case StorageTypeDB:
		db := CheckpointDefaultDatabase
		if conf.Options.IsShardCluster() {
			db = CheckpointAdminDatabase
		}
		newManager.delegate = &MongoCheckpoint{
			CheckpointContext: CheckpointContext{
				Name:                   name,
				Timestamp:              bson.MongoTimestamp(int64(startPosition) << 32),
				Version:                CurrentVersion,
				OplogDiskQueue:         "",
				OplogDiskQueueFinishTs: InitCheckpoint,
			},
			DB:    db,
			URL:   conf.Options.ContextStorageUrl,
			Table: conf.Options.ContextAddress,
		}
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
	if manager.ctx.Version < FeatureCompatibleVersion || manager.ctx.Version > CurrentVersion {
		return nil, exist, fmt.Errorf("current checkpoint version is %v, should >= %d and <= %d", manager.ctx.Version,
			FeatureCompatibleVersion, CurrentVersion)
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
	manager.ctx.Version = CurrentVersion
	// update OplogDiskQueueFinishTs if set
	if manager.ctxRec != nil && manager.ctx.OplogDiskQueueFinishTs != manager.ctxRec.OplogDiskQueueFinishTs {
		manager.ctx.OplogDiskQueueFinishTs = manager.ctxRec.OplogDiskQueueFinishTs
	}
	// update OplogDiskQueueFinishTs if set
	if manager.ctxRec != nil && manager.ctx.OplogDiskQueue != manager.ctxRec.OplogDiskQueue {
		manager.ctx.OplogDiskQueue = manager.ctxRec.OplogDiskQueue
	}
	return manager.delegate.Insert(manager.ctx)
}

// OplogDiskQueueFinishTs and OplogDiskQueue won't immediate effect, will be inserted in next Update call.
func (manager *CheckpointManager) SetOplogDiskFinishTs(ts bson.MongoTimestamp) {
	if manager.ctxRec == nil {
		manager.ctxRec = new(CheckpointContext)
	}
	manager.ctxRec.OplogDiskQueueFinishTs = ts
}

func (manager *CheckpointManager) SetOplogDiskQueueName(name string) {
	if manager.ctxRec == nil {
		manager.ctxRec = new(CheckpointContext)
	}
	manager.ctxRec.OplogDiskQueue = name
}
