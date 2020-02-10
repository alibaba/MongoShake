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
	OplogDiskQueueFinishTs bson.MongoTimestamp `bson:"oplog_disk_queue_finish_ts" json:"oplog_disk_queue_finish_ts"`
}

type Checkpoint struct {
	Name                   string
	StartPosition          int32 // 32 bits timestamp
	Version                int
	OplogDiskQueue         string
	OplogDiskQueueFinishTs int32
}

type CheckpointManager struct {
	Type string

	ctx      *CheckpointContext
	delegate CheckpointOperation
}

func NewCheckpointManager(name string, startPosition int32) *CheckpointManager {
	newManager := &CheckpointManager{}

	switch conf.Options.ContextStorage {
	case StorageTypeAPI:
		newManager.delegate = &HttpApiCheckpoint{
			Checkpoint: Checkpoint{
				Name:          name,
				StartPosition: startPosition,
			},
			URL: conf.Options.ContextAddress,
		}
	case StorageTypeDB:
		db := CheckpointDefaultDatabase
		if conf.Options.IsShardCluster() {
			db = CheckpointAdminDatabase
		}
		newManager.delegate = &MongoCheckpoint{
			Checkpoint: Checkpoint{
				Name:          name,
				StartPosition: startPosition,
			},
			DB:    db,
			URL:   conf.Options.ContextStorageUrl,
			Table: conf.Options.ContextAddress,
		}
	}
	return newManager
}

// get persist checkpoint
func (manager *CheckpointManager) Get() (*CheckpointContext, error) {
	manager.ctx = manager.delegate.Get()
	if manager.ctx == nil {
		return nil, fmt.Errorf("get by checkpoint manager[%v] failed", manager.Type)
	}

	// check fcv
	if manager.ctx.Version < FeatureCompatibleVersion || manager.ctx.Version > CurrentVersion {
		return nil, fmt.Errorf("current checkpoint version is %v, should >= %d and <= %d", manager.ctx.Version,
			FeatureCompatibleVersion, CurrentVersion)
	}

	return manager.ctx, nil
}

// get in memory checkpoint
func (manager *CheckpointManager) GetInMemory() *CheckpointContext {
	// TODO, vinllen, 2020_02_06 judge version
	return manager.ctx
}

func (manager *CheckpointManager) Update(ts bson.MongoTimestamp) error {
	if manager.ctx == nil || len(manager.ctx.Name) == 0 {
		return errors.New("current ckpt context is empty")
	}

	manager.ctx.Timestamp = ts
	manager.ctx.Version = CurrentVersion
	return manager.delegate.Insert(manager.ctx)
}

func (manager *CheckpointManager) SetOplogDiskFinishTs(ts bson.MongoTimestamp) {
	manager.ctx.OplogDiskQueueFinishTs = ts
}

func (manager *CheckpointManager) SetOplogDiskQueueName(name string) {
	manager.ctx.OplogDiskQueue = name
}