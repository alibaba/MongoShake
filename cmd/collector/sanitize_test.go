package main

import (
	"sort"
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
	assert.Equal(t, int64(256000), conf.Options.FullSyncReaderOplogStoreDiskMaxSize, "should be equal")
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

func TestParseFieldWhitelists(t *testing.T) {
	type in struct {
		full  []string
		incr  []string
		white []string
		fetch string
	}
	cases := []struct {
		name     string
		in       in
		wantErr  string // "" means no error
		wantFull map[string][]string
	}{
		{
			name:     "empty is no-op",
			in:       in{},
			wantErr:  "",
			wantFull: map[string][]string{},
		},
		{
			name:     "full only, covered by exact white",
			in:       in{full: []string{"db1.c1:a, b", "db1.c1:c"}, white: []string{"db1.c1"}},
			wantErr:  "",
			wantFull: map[string][]string{"db1.c1": {"a", "b", "c"}},
		},
		{
			name:     "covered by db-level white",
			in:       in{full: []string{"db1.c1:a"}, white: []string{"db1"}},
			wantErr:  "",
			wantFull: map[string][]string{"db1.c1": {"a"}},
		},
		{
			name:    "missing namespace.white",
			in:      in{full: []string{"db1.c1:a"}},
			wantErr: "field whitelist requires filter.namespace.white to be set",
		},
		{
			name:    "ns not covered by white",
			in:      in{full: []string{"db1.c1:a"}, white: []string{"db2.c2"}},
			wantErr: "field whitelist namespace(s) [db1.c1] not covered by filter.namespace.white",
		},
		{
			name:    "dotted field rejected",
			in:      in{full: []string{"db1.c1:a.b"}, white: []string{"db1.c1"}},
			wantErr: "full_sync.field.whitelist does not support nested/dotted field [a.b] in v1",
		},
		{
			name:    "ns without dot rejected",
			in:      in{full: []string{"db1:a"}, white: []string{"db1"}},
			wantErr: "full_sync.field.whitelist namespace should be db.collection; got [db1:a]",
		},
		{
			name:    "incr without change_stream rejected",
			in:      in{incr: []string{"db1.c1:a"}, white: []string{"db1.c1"}, fetch: "oplog"},
			wantErr: "incr_sync.field.whitelist requires incr_sync.mongo_fetch_method = change_stream",
		},
		{
			name:    "full/incr mismatch rejected",
			in:      in{full: []string{"db1.c1:a,b"}, incr: []string{"db1.c1:a"}, white: []string{"db1.c1"}, fetch: "change_stream"},
			wantErr: "field whitelist mismatch for ns[db1.c1]: full_sync=[a b], incr_sync=[a]",
		},
		{
			name:     "full/incr match ok",
			in:       in{full: []string{"db1.c1:a,b"}, incr: []string{"db1.c1:b,a"}, white: []string{"db1.c1"}, fetch: "change_stream"},
			wantErr:  "",
			wantFull: map[string][]string{"db1.c1": {"a", "b"}},
		},
	}

	origin := conf.Options
	defer func() { conf.Options = origin }()

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			conf.Options = conf.Configuration{
				FullSyncFieldWhitelist: c.in.full,
				IncrSyncFieldWhitelist: c.in.incr,
				FilterNamespaceWhite:   c.in.white,
			}
			if c.in.fetch != "" {
				conf.Options.IncrSyncMongoFetchMethod = c.in.fetch
			} else {
				conf.Options.IncrSyncMongoFetchMethod = utils.VarIncrSyncMongoFetchMethodChangeStream
			}

			err := parseFieldWhitelists()
			if c.wantErr == "" {
				assert.NoError(t, err)
				for ns, fields := range c.wantFull {
					got := make([]string, 0, len(conf.Options.FullSyncFieldWhitelistMap[ns]))
					for f := range conf.Options.FullSyncFieldWhitelistMap[ns] {
						got = append(got, f)
					}
					sort.Strings(got)
					assert.Equal(t, fields, got)
				}
			} else {
				assert.EqualError(t, err, c.wantErr)
			}
		})
	}
}
