package ckpt

import (
	"net/http"
	"io/ioutil"
	"encoding/json"
	"bytes"
	"errors"

	"mongoshake/collector/configure"
	"mongoshake/common"

	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
)

const (
	// we can't insert Timestamp(0, 0) that will be treat as Now() inserted
	// into mongo. so we use Timestamp(0, 1)
	InitCheckpoint  = bson.MongoTimestamp(1)
	EmptyCheckpoint = bson.MongoTimestamp(0)
)

type CheckpointContext struct {
	Name                   string              `bson:"name" json:"name"`
	Timestamp              bson.MongoTimestamp `bson:"ckpt" json:"ckpt"`
	Version                int                 `bson:"version" json:"version"`
	OplogDiskQueue         string              `bson:"oplog_disk_queue" json:"oplog_disk_queue"`
	OplogDiskQueueFinishTs bson.MongoTimestamp `bson:"oplog_disk_queue_apply_finish_ts" json:"oplog_disk_queue_apply_finish_ts"`
}

func (cc *CheckpointContext) String() string {
	if ret, err := json.Marshal(cc); err != nil {
		return err.Error()
	} else {
		return string(ret)
	}
}

type CheckpointOperation interface {
	// read checkpoint from remote storage. and encapsulation
	// with CheckpointContext struct
	// bool means whether exists on remote
	Get() (*CheckpointContext, bool)

	// save checkpoint
	Insert(ckpt *CheckpointContext) error
}

// mongo
type MongoCheckpoint struct {
	CheckpointContext

	Conn        *utils.MongoConn
	QueryHandle *mgo.Collection

	// connection info
	URL       string
	DB, Table string
}

func (ckpt *MongoCheckpoint) ensureNetwork() bool {
	// make connection if we haven't already established one
	if ckpt.Conn == nil {
		if conn, err := utils.NewMongoConn(ckpt.URL, utils.VarMongoConnectModePrimary, true); err == nil {
			ckpt.Conn = conn
			ckpt.QueryHandle = conn.Session.DB(ckpt.DB).C(ckpt.Table)
		} else {
			LOG.Warn("CheckpointOperation manager connect mongo cluster failed. %v", err)
			return false
		}
	}

	// set WriteMajority while checkpoint is writing to ConfigServer
	if conf.Options.IsShardCluster() {
		ckpt.Conn.Session.EnsureSafe(&mgo.Safe{WMode: utils.MajorityWriteConcern})
	}
	return true
}

func (ckpt *MongoCheckpoint) close() {
	ckpt.Conn.Close()
	ckpt.Conn = nil
}

func (ckpt *MongoCheckpoint) Get() (*CheckpointContext, bool) {
	if !ckpt.ensureNetwork() {
		LOG.Warn("Reload ckpt ensure network failed. %v", ckpt.Conn)
		return nil, false
	}

	var err error
	value := new(CheckpointContext)
	if err = ckpt.QueryHandle.Find(bson.M{CheckpointName: ckpt.Name}).One(value); err == nil {
		LOG.Info("Load exist checkpoint. content %v", value)
		return value, true
	} else if err == mgo.ErrNotFound {
		if InitCheckpoint > ckpt.Timestamp {
			ckpt.Timestamp = InitCheckpoint
		}
		value.Name = ckpt.Name
		value.Timestamp = ckpt.Timestamp
		value.Version = ckpt.Version
		value.OplogDiskQueue = ckpt.OplogDiskQueue
		value.OplogDiskQueueFinishTs = ckpt.OplogDiskQueueFinishTs
		LOG.Info("Regenerate checkpoint but won't persist. content: %s", value)
		// insert current ckpt snapshot in memory
		// ckpt.QueryHandle.Insert(value)
		return value, false
	}

	ckpt.close()
	LOG.Warn("Reload ckpt find context fail. %v", err)
	return nil, false
}

func (ckpt *MongoCheckpoint) Insert(updates *CheckpointContext) error {
	if !ckpt.ensureNetwork() {
		LOG.Warn("Record ckpt ensure network failed. %v", ckpt.Conn)
		return errors.New("record ckpt network failed")
	}

	if _, err := ckpt.QueryHandle.Upsert(bson.M{CheckpointName: ckpt.Name}, bson.M{"$set": updates}); err != nil {
		LOG.Warn("Record checkpoint %v upsert error %v", updates, err)
		ckpt.close()
		return err
	}

	LOG.Info("Record new checkpoint success [%d]", int64(utils.ExtractMongoTimestamp(updates.Timestamp)))
	return nil
}

// http
type HttpApiCheckpoint struct {
	CheckpointContext

	URL string
}

func (ckpt *HttpApiCheckpoint) Get() (*CheckpointContext, bool) {
	var err error
	var resp *http.Response
	var stream []byte
	value := new(CheckpointContext)
	if resp, err = http.Get(ckpt.URL); err != nil {
		LOG.Warn("Http api ckpt request failed, %v", err)
		return nil, false
	}

	if stream, err = ioutil.ReadAll(resp.Body); err != nil {
		return nil, false
	}
	if err = json.Unmarshal(stream, value); err != nil {
		return nil, false
	}
	// TODO, may have problem
	if value.Timestamp == 0 {
		// use default start position
		value.Timestamp = ckpt.Timestamp
		value.Name = ckpt.Name
		value.OplogDiskQueueFinishTs = ckpt.OplogDiskQueueFinishTs
		value.OplogDiskQueue = ckpt.OplogDiskQueue
		return value, false
	}
	return value, true
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
