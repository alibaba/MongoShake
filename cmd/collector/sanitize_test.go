package main

import (
	"testing"

	"github.com/stretchr/testify/assert"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
)

func TestNormalizeFilterOpTypes(t *testing.T) {
	opTypes, err := normalizeFilterOpTypes([]string{" d ", "I", "", "  "})
	assert.NoError(t, err, "should be equal")
	assert.Equal(t, []string{"d", "i"}, opTypes, "should be equal")
}

func TestNormalizeFilterOpTypesRejectNoop(t *testing.T) {
	opTypes, err := normalizeFilterOpTypes([]string{"n"})
	assert.Nil(t, opTypes, "should be equal")
	assert.EqualError(t, err, `filter.op_types: unsupported op type "n"; noop oplogs are already filtered by the built-in NoopFilter`)
}

func TestCheckDefaultValueInvalidFilterOpTypes(t *testing.T) {
	origin := conf.Options
	defer func() {
		conf.Options = origin
	}()

	conf.Options = conf.Configuration{
		FilterOpTypes: []string{"delete"},
	}

	err := checkDefaultValue()
	assert.EqualError(t, err, `filter.op_types: unknown op type "delete", must be one of {i, u, d, c}`)
}

func TestCheckDefaultValueKeepDisabledHTTPPorts(t *testing.T) {
	origin := conf.Options
	defer func() {
		conf.Options = origin
	}()

	conf.Options = conf.Configuration{
		FullSyncHTTPListenPort: -1,
		IncrSyncHTTPListenPort: 0,
		PromHTTPListenPort:     0,
		MongoUrls:              []string{"mongodb://source-a"},
	}

	err := checkDefaultValue()
	assert.NoError(t, err, "should be equal")
	assert.Equal(t, -1, conf.Options.FullSyncHTTPListenPort, "should be equal")
	assert.Equal(t, 0, conf.Options.IncrSyncHTTPListenPort, "should be equal")
	assert.Equal(t, 0, conf.Options.PromHTTPListenPort, "should be equal")
}

func TestCheckDefaultValueRejectChangeStreamDiskSpool(t *testing.T) {
	origin := conf.Options
	defer func() {
		conf.Options = origin
	}()

	conf.Options = conf.Configuration{
		MongoUrls:                           []string{"mongodb://source-a"},
		SyncMode:                            utils.VarSyncModeAll,
		FullSyncReaderOplogStoreDisk:        true,
		IncrSyncMongoFetchMethod:            utils.VarIncrSyncMongoFetchMethodChangeStream,
		FullSyncReaderOplogStoreDiskMaxSize: 1,
	}

	err := checkDefaultValue()
	assert.EqualError(t, err, "full_sync.reader.oplog_store_disk currently supports incr_sync.mongo_fetch_method=oplog only")
}

func TestCheckDefaultValueAllowsChangeStreamDiskSpoolOutsideAllMode(t *testing.T) {
	origin := conf.Options
	defer func() {
		conf.Options = origin
	}()

	conf.Options = conf.Configuration{
		MongoUrls:                    []string{"mongodb://source-a"},
		SyncMode:                     utils.VarSyncModeIncr,
		FullSyncReaderOplogStoreDisk: true,
		IncrSyncMongoFetchMethod:     utils.VarIncrSyncMongoFetchMethodChangeStream,
	}

	err := checkDefaultValue()
	assert.NoError(t, err, "should be equal")
}

func TestCheckDefaultValueRequireMasterQuorumElectionID(t *testing.T) {
	origin := conf.Options
	defer func() {
		conf.Options = origin
	}()

	conf.Options = conf.Configuration{
		MasterQuorum: true,
		MongoUrls:    []string{"mongodb://source-a"},
	}

	err := checkDefaultValue()
	assert.EqualError(t, err, "master_quorum.election_id should be given while master election enabled")
}

func TestCheckDefaultValueRejectInvalidMasterQuorumElectionID(t *testing.T) {
	origin := conf.Options
	defer func() {
		conf.Options = origin
	}()

	conf.Options = conf.Configuration{
		MasterQuorum:           true,
		MasterQuorumElectionID: "invalid-object-id",
		MongoUrls:              []string{"mongodb://source-a"},
	}

	err := checkDefaultValue()
	assert.Error(t, err, "should be equal")
	assert.Contains(t, err.Error(), "master_quorum.election_id should be a valid ObjectID")
}

func TestCheckDefaultValueAcceptMasterQuorumElectionID(t *testing.T) {
	origin := conf.Options
	defer func() {
		conf.Options = origin
	}()

	conf.Options = conf.Configuration{
		MasterQuorum:           true,
		MasterQuorumElectionID: "66e3ffdd9df1af8e2308fb53",
		MongoUrls:              []string{"mongodb://source-a"},
	}

	err := checkDefaultValue()
	assert.NoError(t, err, "should be equal")
	assert.Equal(t, utils.VarCheckpointStorageDatabase, conf.Options.CheckpointStorage, "should be equal")
}

func TestCheckConflictRejectPrometheusHTTPPortConflict(t *testing.T) {
	origin := conf.Options
	defer func() {
		conf.Options = origin
	}()

	conf.Options = conf.Configuration{
		FullSyncHTTPListenPort: 9101,
		IncrSyncHTTPListenPort: 9100,
		PromHTTPListenPort:     9101,
		MongoUrls:              []string{"mongodb://source-a"},
	}

	err := checkConflict()
	assert.EqualError(t, err, "prom.http_port should not equal to full_sync.http_port")
}

func TestCheckConflictRejectPrometheusSystemProfilePortConflict(t *testing.T) {
	origin := conf.Options
	defer func() {
		conf.Options = origin
	}()

	conf.Options = conf.Configuration{
		FullSyncHTTPListenPort: -1,
		IncrSyncHTTPListenPort: 0,
		PromHTTPListenPort:     9200,
		SystemProfilePort:      9200,
		MongoUrls:              []string{"mongodb://source-a"},
	}

	err := checkConflict()
	assert.EqualError(t, err, "system_profile_port should not equal to prom.http_port")
}
