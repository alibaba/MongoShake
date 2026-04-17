package ckpt

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/Masterminds/semver/v3"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	LOG "github.com/alibaba/MongoShake/v2/third_party/log4go"
)

const (
	// InitCheckpoint
	// Note: we can't insert Timestamp(0, 0) that will be treat as Now() inserted
	// into mongo. so we use Timestamp(0, 1)
	InitCheckpoint  = int64(1)
	EmptyCheckpoint = int64(0)
)

type CheckpointContext struct {
	Name                   string `bson:"name" json:"name"`
	Timestamp              int64  `bson:"ckpt" json:"ckpt"`
	Version                int    `bson:"version" json:"version"`
	FetchMethod            string `bson:"fetch_method" json:"fetch_method"`
	OplogDiskQueue         string `bson:"oplog_disk_queue" json:"oplog_disk_queue"`
	OplogDiskQueueFinishTs int64  `bson:"oplog_disk_queue_apply_finish_ts" json:"oplog_disk_queue_apply_finish_ts"`
}

func (cc *CheckpointContext) String() string {
	if ret, err := json.Marshal(cc); err != nil {
		return err.Error()
	} else {
		return string(ret)
	}
}

type CheckpointOperation interface {
	// Get read checkpoint from remote storage. and encapsulation
	// with CheckpointContext struct
	// bool means whether exists on remote
	Get() (*CheckpointContext, bool)

	// Insert save checkpoint
	Insert(ckpt *CheckpointContext) error

	// String return log info
	String() string
}

// MongoCheckpoint for mongo
type MongoCheckpoint struct {
	CheckpointContext

	client *utils.MongoCommunityConn

	// connection info
	URL       string
	DB, Table string
}

func (ckpt *MongoCheckpoint) ensureNetwork() bool {
	if ckpt.client == nil {
		// mongodb3.6- doesn't support {readConcern:majority}, here we use 'local' first to get dbVersion,
		// then change to 'majority' if possible.
		tmpClient, err := utils.NewMongoCommunityConn(ckpt.URL, utils.VarMongoConnectModePrimary, true,
			"local", "", conf.Options.CheckpointStorageUrlMongoSslRootCaFile)
		if err != nil {
			LOG.Error("%s MongoCheckpoint create client failed:%v", ckpt.Name, err)
			return false
		}
		defer tmpClient.Close()
		dbVersion, err := utils.GetDBVersion(tmpClient)
		if err != nil {
			LOG.Error("%s MongoCheckpoint get db version failed:%v", ckpt.Name, err)
			return false
		}
		version, err := semver.StrictNewVersion(dbVersion)
		if err != nil {
			LOG.Error("%s MongoCheckpoint failed to parse dbVersion: %v", ckpt.Name, err)
			return false
		}
		version36, _ := semver.StrictNewVersion(utils.VarMongoVersion36)
		rc := utils.ReadWriteConcernLocal
		if version.GreaterThanEqual(version36) {
			LOG.Info("%s MongoCheckpoint dbVersion %s >= %s, use readConcern:majority",
				ckpt.Name, version.String(), utils.VarMongoVersion36)
			rc = utils.ReadWriteConcernMajority
		} else {
			LOG.Info("%s MongoCheckpoint dbVersion %s < %s, use readConcern:local",
				ckpt.Name, version.String(), utils.VarMongoVersion36)
		}

		if client, err := utils.NewMongoCommunityConn(ckpt.URL, utils.VarMongoConnectModePrimary, true,
			rc, utils.ReadWriteConcernMajority,
			conf.Options.CheckpointStorageUrlMongoSslRootCaFile); err == nil {
			ckpt.client = client
		} else {
			LOG.Error("%s MongoCheckpoint connect to mongo cluster failed. %v", ckpt.Name, err)
			return false
		}
	}

	return true
}

func (ckpt *MongoCheckpoint) close() {
	ckpt.client.Close()
	ckpt.client = nil
}

func (ckpt *MongoCheckpoint) Get() (*CheckpointContext, bool) {
	if !ckpt.ensureNetwork() {
		LOG.Error("%s MongoCheckpoint ensureNetwork failed", ckpt.Name)
		return nil, false
	}

	var err error
	value := new(CheckpointContext)
	if err = ckpt.client.Client.Database(ckpt.DB).Collection(ckpt.Table).FindOne(nil,
		bson.M{CheckpointName: ckpt.Name}).Decode(value); err == nil {

		LOG.Info("%s Load exist checkpoint. content %v", ckpt.Name, value)
		return value, true
	} else if err.Error() == mongo.ErrNoDocuments.Error() {
		if InitCheckpoint > ckpt.Timestamp {
			ckpt.Timestamp = InitCheckpoint
		}
		value.Name = ckpt.Name
		value.Timestamp = ckpt.Timestamp
		value.Version = ckpt.Version
		value.OplogDiskQueue = ckpt.OplogDiskQueue
		value.OplogDiskQueueFinishTs = ckpt.OplogDiskQueueFinishTs
		LOG.Info("%s Regenerate checkpoint but won't persist. content: %s", ckpt.Name, value)
		return value, false
	}

	ckpt.close()
	LOG.Error("%s Reload ckpt find context fail. %v", ckpt.Name, err)
	return nil, false
}

func (ckpt *MongoCheckpoint) Insert(updates *CheckpointContext) error {
	if !ckpt.ensureNetwork() {
		LOG.Warn("%s Record ckpt ensure network failed. %v", ckpt.Name, ckpt.client)
		return fmt.Errorf("%s record ckpt network failed", ckpt.Name)
	}

	opts := options.Update().SetUpsert(true)
	filter := bson.M{CheckpointName: ckpt.Name}
	update := bson.M{"$set": updates}

	_, err := ckpt.client.Client.Database(ckpt.DB).Collection(ckpt.Table).UpdateOne(nil, filter, update, opts)
	if err != nil {
		LOG.Warn("%s Record checkpoint %v upsert error %v", ckpt.Name, updates, err)
		ckpt.close()
		return err
	}

	LOG.Info("%s Record new checkpoint in MongoDB success [%d]", ckpt.Name,
		utils.ExtractMongoTimestamp(updates.Timestamp))
	return nil
}

// HttpApiCheckpoint for http api
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
		LOG.Warn("%s Http api ckpt request failed, %v", ckpt.Name, err)
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
	if resp, err := http.Post(ckpt.URL, "application/json", bytes.NewReader(body));
		err != nil || resp.StatusCode != http.StatusOK {
		LOG.Warn("%s Context api manager write request failed, %v", ckpt.Name, err)
		return err
	}

	LOG.Info("%s Record new checkpoint in HttpApi success [%d]",
		ckpt.Name, utils.ExtractMongoTimestamp(insert.Timestamp))
	return nil
}
