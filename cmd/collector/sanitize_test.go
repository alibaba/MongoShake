package main

import (
	"math"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

func TestCheckDefaultValueWriteThread(t *testing.T) {
	origin := conf.Options
	defer func() {
		conf.Options = origin
	}()

	cases := []struct {
		name        string
		tunnel      string
		worker      int
		writeThread int
		wantErr     string // "" means no error
		wantThread  int    // expected write_thread after sanitize when no error
	}{
		// write_thread is unused by the direct tunnel, so a non-multiple value must not
		// block startup (the customer hit a silent exit raising worker 8 -> 16).
		{"direct non-multiple allowed", utils.VarTunnelDirect, 16, 8, "", 8},
		{"unset tunnel defaults direct, non-multiple allowed", "", 16, 8, "", 8},
		{"direct multiple allowed", utils.VarTunnelDirect, 8, 16, "", 16},
		{"kafka multiple allowed", utils.VarTunnelKafka, 8, 16, "", 16},
		{"unset write_thread defaults to worker", utils.VarTunnelKafka, 16, 0, "", 16},
		{"kafka non-multiple rejected", utils.VarTunnelKafka, 16, 8,
			"must be a positive integer multiple of incr_sync.worker[16]", 0},
		{"kafka negative rejected", utils.VarTunnelKafka, 16, -16,
			"must be a positive integer multiple of incr_sync.worker[16]", 0},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			conf.Options = conf.Configuration{
				Tunnel:                    c.tunnel,
				MongoUrls:                 []string{"mongodb://source-a"},
				IncrSyncWorker:            c.worker,
				IncrSyncTunnelWriteThread: c.writeThread,
			}
			err := checkDefaultValue()
			if c.wantErr == "" {
				assert.NoError(t, err)
				assert.Equal(t, c.wantThread, conf.Options.IncrSyncTunnelWriteThread)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), c.wantErr)
				assert.Contains(t, err.Error(), "tunnel=kafka")
			}
		})
	}
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
			wantErr: "field whitelist/blacklist requires filter.namespace.white to be set",
		},
		{
			name:    "ns not covered by white",
			in:      in{full: []string{"db1.c1:a"}, white: []string{"db2.c2"}},
			wantErr: "field whitelist/blacklist namespace(s) [db1.c1] not covered by filter.namespace.white",
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
			wantErr: "incr_sync.field.whitelist/blacklist requires incr_sync.mongo_fetch_method = change_stream",
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

			err := parseFieldLists()
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

func TestParseFieldBlacklists(t *testing.T) {
	origin := conf.Options
	defer func() { conf.Options = origin }()

	cases := []struct {
		name    string
		fullWL  []string
		incrWL  []string
		fullBL  []string
		incrBL  []string
		white   []string
		fetch   string
		wantErr string
	}{
		{
			name:   "blacklist ok",
			fullBL: []string{"db1.c1:big,blob"}, incrBL: []string{"db1.c1:blob,big"},
			white: []string{"db1.c1"}, fetch: "change_stream",
		},
		{
			name:    "blacklist _id rejected",
			fullBL:  []string{"db1.c1:_id,big"},
			white:   []string{"db1.c1"},
			wantErr: "full_sync.field.blacklist cannot exclude _id (it is always kept); got [db1.c1:_id,big]",
		},
		{
			name:    "full whitelist+blacklist same ns rejected",
			fullWL:  []string{"db1.c1:a"},
			fullBL:  []string{"db1.c1:big"},
			white:   []string{"db1.c1"},
			wantErr: "ns[db1.c1] set in both full_sync.field.whitelist and full_sync.field.blacklist",
		},
		{
			name:   "incr whitelist+blacklist same ns rejected",
			incrWL: []string{"db1.c1:a"},
			incrBL: []string{"db1.c1:big"},
			white:  []string{"db1.c1"}, fetch: "change_stream",
			wantErr: "ns[db1.c1] set in both incr_sync.field.whitelist and incr_sync.field.blacklist",
		},
		{
			name:   "cross-phase mode conflict fullWL incrBL rejected",
			fullWL: []string{"db1.c1:a"},
			incrBL: []string{"db1.c1:big"},
			white:  []string{"db1.c1"}, fetch: "change_stream",
			wantErr: "ns[db1.c1] mode conflict: full_sync.field.whitelist vs incr_sync.field.blacklist",
		},
		{
			name:   "cross-phase mode conflict fullBL incrWL rejected",
			fullBL: []string{"db1.c1:big"},
			incrWL: []string{"db1.c1:a"},
			white:  []string{"db1.c1"}, fetch: "change_stream",
			wantErr: "ns[db1.c1] mode conflict: full_sync.field.blacklist vs incr_sync.field.whitelist",
		},
		{
			name:   "full/incr blacklist mismatch rejected",
			fullBL: []string{"db1.c1:big,blob"},
			incrBL: []string{"db1.c1:big"},
			white:  []string{"db1.c1"}, fetch: "change_stream",
			wantErr: "field blacklist mismatch for ns[db1.c1]: full_sync=[big blob], incr_sync=[big]",
		},
		{
			name:   "incr blacklist without change_stream rejected",
			incrBL: []string{"db1.c1:big"},
			white:  []string{"db1.c1"}, fetch: "oplog",
			wantErr: "incr_sync.field.whitelist/blacklist requires incr_sync.mongo_fetch_method = change_stream",
		},
		{
			name:    "blacklist ns not covered rejected",
			fullBL:  []string{"db1.c1:big"},
			white:   []string{"db2.c2"},
			wantErr: "field whitelist/blacklist namespace(s) [db1.c1] not covered by filter.namespace.white",
		},
		{
			name:   "different ns different modes ok",
			fullWL: []string{"db1.c1:a"}, fullBL: []string{"db2.c2:big"},
			white: []string{"db1.c1", "db2.c2"},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			conf.Options = conf.Configuration{
				FullSyncFieldWhitelist: c.fullWL,
				IncrSyncFieldWhitelist: c.incrWL,
				FullSyncFieldBlacklist: c.fullBL,
				IncrSyncFieldBlacklist: c.incrBL,
				FilterNamespaceWhite:   c.white,
			}
			if c.fetch != "" {
				conf.Options.IncrSyncMongoFetchMethod = c.fetch
			} else {
				conf.Options.IncrSyncMongoFetchMethod = utils.VarIncrSyncMongoFetchMethodChangeStream
			}
			err := parseFieldLists()
			if c.wantErr == "" {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, c.wantErr)
			}
		})
	}
}

func TestCheckIneffectiveOptionsBarrierOrdering(t *testing.T) {
	origin := conf.Options
	defer func() {
		conf.Options = origin
	}()

	replicaSet := []string{"mongodb://source-a"}
	shardCluster := []string{"mongodb://source-a", "mongodb://source-b"}

	cases := []struct {
		desc      string
		tunnel    string
		enable    bool
		mongoUrls []string
		want      int
	}{
		{"direct + on, replica set", utils.VarTunnelDirect, true, replicaSet, 0},
		{"kafka + on, replica set", utils.VarTunnelKafka, true, replicaSet, 1},
		{"direct + on, shard cluster", utils.VarTunnelDirect, true, shardCluster, 1},
		{"kafka + on, shard cluster", utils.VarTunnelKafka, true, shardCluster, 2},
		{"kafka + off, shard cluster", utils.VarTunnelKafka, false, shardCluster, 0},
	}

	for _, c := range cases {
		conf.Options = conf.Configuration{
			Tunnel:                        c.tunnel,
			IncrSyncBarrierOrderingEnable: c.enable,
			MongoUrls:                     c.mongoUrls,
		}

		warnings := checkIneffectiveOptions()
		assert.Len(t, warnings, c.want, c.desc)
		for _, warning := range warnings {
			assert.Contains(t, warning, "incr_sync.barrier.ordering_enable", c.desc)
		}
	}
}

func TestCheckDefaultValueFullSyncTPS(t *testing.T) {
	original := conf.Options
	t.Cleanup(func() { conf.Options = original })
	for _, rate := range []int64{-1, 0, 10, 1000, 1_000_000, 1_000_001, math.MaxInt64} {
		conf.Options = conf.Configuration{MongoUrls: []string{"mongodb://source-a"}, SyncMode: utils.VarSyncModeFull, FullSyncTPS: rate}
		err := checkDefaultValue()
		if rate < 0 || rate > 1_000_000 {
			assert.ErrorContains(t, err, "full_sync.tps")
		} else {
			assert.NoError(t, err)
			assert.Equal(t, rate, conf.Options.FullSyncTPS)
		}
	}
}
