package ckpt

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/Masterminds/semver/v3"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	l "github.com/alibaba/MongoShake/v2/pkg/log"
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
	// FullFinishTs is the source newest timestamp sampled when full sync
	// finished (#1047). It bounds the full/incr overlap window so the writer
	// can treat _id duplicate keys inside it as expected. 0 means unknown
	// (legacy checkpoint or no full sync): window-based handling stays off.
	FullFinishTs int64 `bson:"full_finish_ts" json:"full_finish_ts"`
}

func (cc *CheckpointContext) String() string {
	if ret, err := json.Marshal(cc); err != nil {
		return err.Error()
	} else {
		return string(ret)
	}
}

// UnmarshalBSON accepts both encodings of the two timestamp fields. Current builds
// write int64, but the mgo-based releases typed ckpt and
// oplog_disk_queue_apply_finish_ts as bson.MongoTimestamp, which mgo encodes as a
// BSON timestamp (0x11), and the driver refuses to decode that into an integer.
// Without this, a collector started over a checkpoint written by an older release
// finishes full sync and then exits at the incremental handover with "cannot decode
// timestamp into an integer type" followed by "current ckpt context is empty".
//
// Marshalling is deliberately left alone: the first update rewrites both fields as
// int64, so the legacy encoding disappears once the collector has run.
func (cc *CheckpointContext) UnmarshalBSON(data []byte) error {
	var aux struct {
		Name                   string        `bson:"name"`
		Timestamp              bson.RawValue `bson:"ckpt"`
		Version                int           `bson:"version"`
		FetchMethod            string        `bson:"fetch_method"`
		OplogDiskQueue         string        `bson:"oplog_disk_queue"`
		OplogDiskQueueFinishTs bson.RawValue `bson:"oplog_disk_queue_apply_finish_ts"`
		FullFinishTs           bson.RawValue `bson:"full_finish_ts"`
	}
	if err := bson.Unmarshal(data, &aux); err != nil {
		return err
	}
	ts, err := decodeCheckpointTimestamp(aux.Timestamp)
	if err != nil {
		return fmt.Errorf("ckpt: %v", err)
	}
	finishTs, err := decodeCheckpointTimestamp(aux.OplogDiskQueueFinishTs)
	if err != nil {
		return fmt.Errorf("oplog_disk_queue_apply_finish_ts: %v", err)
	}
	fullFinishTs, err := decodeCheckpointTimestamp(aux.FullFinishTs)
	if err != nil {
		return fmt.Errorf("full_finish_ts: %v", err)
	}
	*cc = CheckpointContext{
		Name:                   aux.Name,
		Timestamp:              ts,
		Version:                aux.Version,
		FetchMethod:            aux.FetchMethod,
		OplogDiskQueue:         aux.OplogDiskQueue,
		OplogDiskQueueFinishTs: finishTs,
		FullFinishTs:           fullFinishTs,
	}
	return nil
}

// decodeCheckpointTimestamp reads ckpt from either encoding. An absent field keeps
// the zero value; anything that cannot denote a timestamp is still an error rather
// than a silent 0, which would restart replication from the beginning.
func decodeCheckpointTimestamp(v bson.RawValue) (int64, error) {
	if v.IsZero() { // field absent from the document
		return 0, nil
	}
	switch v.Type {
	case bson.TypeTimestamp:
		t, i := v.Timestamp()
		return utils.TimeStampToInt64(primitive.Timestamp{T: t, I: i}), nil
	case bson.TypeInt64:
		return v.Int64(), nil
	case bson.TypeInt32:
		return int64(v.Int32()), nil
	case bson.TypeNull, bson.TypeUndefined:
		return 0, nil
	default:
		return 0, fmt.Errorf("unsupported ckpt BSON type[%v]", v.Type)
	}
}

type CheckpointOperation interface {
	// Get read checkpoint from remote storage. and encapsulation
	// with CheckpointContext struct
	// bool means whether exists on remote
	//
	// Contract relied on by CheckpointManager.FlushStaged (#1050 R3):
	// a nil context means the READ FAILED; a non-nil context with
	// exist=false means no durable document exists (regenerated context).
	// Implementations must preserve this distinction.
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
			l.Logger.Errorf("%s MongoCheckpoint create client failed:%v", ckpt.Name, err)
			return false
		}
		defer tmpClient.Close()
		dbVersion, err := utils.GetDBVersion(tmpClient)
		if err != nil {
			l.Logger.Errorf("%s MongoCheckpoint get db version failed:%v", ckpt.Name, err)
			return false
		}
		version, err := semver.StrictNewVersion(dbVersion)
		if err != nil {
			l.Logger.Errorf("%s MongoCheckpoint failed to parse dbVersion: %v", ckpt.Name, err)
			return false
		}
		version36, _ := semver.StrictNewVersion(utils.VarMongoVersion36)
		rc := utils.ReadWriteConcernLocal
		if version.GreaterThanEqual(version36) {
			l.Logger.Infof("%s MongoCheckpoint dbVersion %s >= %s, use readConcern:majority",
				ckpt.Name, version.String(), utils.VarMongoVersion36)
			rc = utils.ReadWriteConcernMajority
		} else {
			l.Logger.Infof("%s MongoCheckpoint dbVersion %s < %s, use readConcern:local",
				ckpt.Name, version.String(), utils.VarMongoVersion36)
		}

		if client, err := utils.NewMongoCommunityConn(ckpt.URL, utils.VarMongoConnectModePrimary, true,
			rc, utils.ReadWriteConcernMajority,
			conf.Options.CheckpointStorageUrlMongoSslRootCaFile); err == nil {
			ckpt.client = client
		} else {
			l.Logger.Errorf("%s MongoCheckpoint connect to mongo cluster failed. %v", ckpt.Name, err)
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
		l.Logger.Errorf("%s MongoCheckpoint ensureNetwork failed", ckpt.Name)
		return nil, false
	}

	var err error
	value := new(CheckpointContext)
	if err = ckpt.client.Client.Database(ckpt.DB).Collection(ckpt.Table).FindOne(nil,
		bson.M{CheckpointName: ckpt.Name}).Decode(value); err == nil {

		l.Logger.Infof("%s Load exist checkpoint. content %v", ckpt.Name, value)
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
		value.FullFinishTs = ckpt.FullFinishTs
		l.Logger.Infof("%s Regenerate checkpoint but won't persist. content: %s", ckpt.Name, value)
		return value, false
	}

	ckpt.close()
	l.Logger.Errorf("%s Reload ckpt find context fail. %v", ckpt.Name, err)
	return nil, false
}

func (ckpt *MongoCheckpoint) Insert(updates *CheckpointContext) error {
	if !ckpt.ensureNetwork() {
		l.Logger.Warnf("%s Record ckpt ensure network failed. %v", ckpt.Name, ckpt.client)
		return fmt.Errorf("%s record ckpt network failed", ckpt.Name)
	}

	opts := options.Update().SetUpsert(true)
	filter := bson.M{CheckpointName: ckpt.Name}
	update := bson.M{"$set": updates}

	_, err := ckpt.client.Client.Database(ckpt.DB).Collection(ckpt.Table).UpdateOne(nil, filter, update, opts)
	if err != nil {
		l.Logger.Warnf("%s Record checkpoint %v upsert error %v", ckpt.Name, updates, err)
		ckpt.close()
		return err
	}

	l.Logger.Infof("%s Record new checkpoint in MongoDB success [%d]", ckpt.Name,
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
		l.Logger.Warnf("%s Http api ckpt request failed, %v", ckpt.Name, err)
		return nil, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		l.Logger.Warnf("%s Http api ckpt request returned status %s", ckpt.Name, resp.Status)
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
		value.FullFinishTs = ckpt.FullFinishTs
		return value, false
	}
	return value, true
}

func (ckpt *HttpApiCheckpoint) Insert(insert *CheckpointContext) error {
	body, _ := json.Marshal(insert)
	resp, err := http.Post(ckpt.URL, "application/json", bytes.NewReader(body))
	if err != nil {
		l.Logger.Warnf("%s Context api manager write request failed, %v", ckpt.Name, err)
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("checkpoint api returned status %s", resp.Status)
		l.Logger.Warnf("%s Context api manager write request failed, %v", ckpt.Name, err)
		return err
	}

	l.Logger.Infof("%s Record new checkpoint in HttpApi success [%d]",
		ckpt.Name, utils.ExtractMongoTimestamp(insert.Timestamp))
	return nil
}
