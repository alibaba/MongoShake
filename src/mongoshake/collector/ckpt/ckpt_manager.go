package ckpt

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"math"
	"net/http"

	"mongoshake/collector/configure"
	"mongoshake/common"
	"mongoshake/dbpool"

	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
)

const (
	StorageTypeAPI            = "api"
	StorageTypeDB             = "database"
	CheckpointDefaultDatabase = utils.AppDatabase
	CheckpointName            = "name"

	MajorityWriteConcern = "majority"
)

type CheckpointContext struct {
	Name      string              `bson:"name" json:"name"`
	Timestamp bson.MongoTimestamp `bson:"ckpt" json:"ckpt"`
}

type Checkpoint struct {
	Name          string
	StartPosition int64
}

type CheckpointManager struct {
	Type string

	ctx      *CheckpointContext
	delegate CheckpointOperation
}

func NewCheckpointManager(name string) *CheckpointManager {
	newManager := &CheckpointManager{}

	switch conf.Options.ContextStorage {
	case StorageTypeAPI:
		newManager.delegate = &HttpApiCheckpoint{
			Checkpoint: Checkpoint{
				Name:          name,
				StartPosition: conf.Options.ContextStartPosition,
			},
			URL: conf.Options.ContextAddress,
		}
	case StorageTypeDB:
		db := CheckpointDefaultDatabase
		newManager.delegate = &MongoCheckpoint{
			Checkpoint: Checkpoint{
				Name:          name,
				StartPosition: conf.Options.ContextStartPosition,
			},
			DB:    db,
			URL:   conf.Options.ContextStorageUrl,
			Table: conf.Options.ContextAddress,
		}
	}
	return newManager
}

func (manager *CheckpointManager) Get() *CheckpointContext {
	manager.ctx = manager.delegate.Get()
	return manager.ctx
}

func (manager *CheckpointManager) GetInMemory() *CheckpointContext {
	return manager.ctx
}

func (manager *CheckpointManager) Update(ts bson.MongoTimestamp) error {
	if manager.ctx == nil || len(manager.ctx.Name) == 0 {
		return errors.New("current ckpt context is empty")
	}

	manager.ctx.Timestamp = ts
	return manager.delegate.Insert(manager.ctx)
}

type CheckpointOperation interface {
	// read checkpoint from remote storage. and encapsulation
	// with CheckpointContext struct
	Get() *CheckpointContext

	// save checkpoint
	Insert(ckpt *CheckpointContext) error
}

type MongoCheckpoint struct {
	Checkpoint

	Conn        *dbpool.MongoConn
	QueryHandle *mgo.Collection

	// connection info
	URL       string
	DB, Table string
}

func (ckpt *MongoCheckpoint) ensureNetwork() bool {
	// make connection if we don't already established
	if ckpt.Conn == nil {
		if conn, err := dbpool.NewMongoConn(ckpt.URL, true); err == nil {
			ckpt.Conn = conn
			ckpt.QueryHandle = conn.Session.DB(ckpt.DB).C(ckpt.Table)
		} else {
			LOG.Warn("CheckpointOperation manager connect mongo cluster failed. %v", err)
			return false
		}
	}

	// set WriteMajority while checkpoint is writing to ConfigServer
	if conf.Options.IsShardCluster() {
		ckpt.Conn.Session.EnsureSafe(&mgo.Safe{WMode: MajorityWriteConcern})
	}
	return true
}

func (ckpt *MongoCheckpoint) close() {
	ckpt.Conn.Close()
	ckpt.Conn = nil
}

func (ckpt *MongoCheckpoint) Get() *CheckpointContext {
	if !ckpt.ensureNetwork() {
		LOG.Warn("Reload ckpt ensure network failed. %v", ckpt.Conn)
		return nil
	}

	var err error
	value := new(CheckpointContext)
	if err = ckpt.QueryHandle.Find(bson.M{CheckpointName: ckpt.Name}).One(value); err == nil {
		LOG.Info("Load exist checkpoint. content %v", value)
		return value
	} else if err == mgo.ErrNotFound {
		// we can't insert Timestamp(0, 0) that will be treat as Now() inserted
		// into mongo. so we use Timestamp(0, 1)
		ckpt.StartPosition = int64(math.Max(float64(ckpt.StartPosition), 1))
		value.Name = ckpt.Name
		value.Timestamp = bson.MongoTimestamp(ckpt.StartPosition << 32)
		LOG.Info("Regenerate checkpoint but won't insert. content %v", value)
		// insert current ckpt snapshot in memory
		// ckpt.QueryHandle.Insert(value)
		return value
	}

	ckpt.close()
	LOG.Warn("Reload ckpt find context fail. %v", err)
	return nil
}

func (ckpt *MongoCheckpoint) Insert(updates *CheckpointContext) error {
	if !ckpt.ensureNetwork() {
		LOG.Warn("Record ckpt ensure network failed. %v", ckpt.Conn)
		return errors.New("record ckpt network failed")
	}

	if _, err := ckpt.QueryHandle.Upsert(bson.M{CheckpointName: ckpt.Name}, updates); err != nil {
		LOG.Warn("Record checkpoint %v upsert error %v", updates, err)
		ckpt.close()
		return err
	}

	LOG.Info("Record new checkpoint success [%d]", int64(utils.ExtractMongoTimestamp(updates.Timestamp)))
	return nil
}

type HttpApiCheckpoint struct {
	Checkpoint

	URL string
}

func (ckpt *HttpApiCheckpoint) Get() *CheckpointContext {
	var err error
	var resp *http.Response
	var stream []byte
	value := new(CheckpointContext)
	if resp, err = http.Get(ckpt.URL); err != nil {
		LOG.Warn("Http api ckpt request failed, %v", err)
		return nil
	}

	if stream, err = ioutil.ReadAll(resp.Body); err != nil {
		return nil
	}
	if err = json.Unmarshal(stream, value); err != nil {
		return nil
	}
	if value.Timestamp == 0 {
		// use default start position
		value.Timestamp = bson.MongoTimestamp(ckpt.StartPosition)
	}
	if len(value.Name) == 0 {
		// default name
		value.Name = ckpt.Name
	}
	return value
}

func (ckpt *HttpApiCheckpoint) Insert(insert *CheckpointContext) error {
	body, _ := json.Marshal(insert)
	if resp, err := http.Post(ckpt.URL, "application/json", bytes.NewReader(body)); err != nil || resp.StatusCode != http.StatusOK {
		LOG.Warn("Context api manager write request failed, %v", err)
		return err
	}

	LOG.Info("Record new checkpoint success [%d]", int64(utils.ExtractMongoTimestamp(insert.Timestamp)))
	return nil
}
