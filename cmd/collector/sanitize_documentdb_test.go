package main

import (
	"testing"

	"github.com/stretchr/testify/assert"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
)

// TestCheckDefaultValueSpecialTargetDBFlag covers the config gate added for #1041.
// DocumentDB support only applies to a direct tunnel; a typo must not silently
// fall back to the MongoDB path, which is what made the original failure confusing.
func TestCheckDefaultValueSpecialTargetDBFlag(t *testing.T) {
	origin := conf.Options
	defer func() { conf.Options = origin }()

	cases := []struct {
		desc    string
		flag    string
		tunnel  string
		wantErr string
	}{
		{"empty flag accepted", "", utils.VarTunnelDirect, ""},
		{"documentdb + direct accepted", utils.VarSpecialTargetDBFlagDocumentDB, utils.VarTunnelDirect, ""},
		{"documentdb + kafka rejected", utils.VarSpecialTargetDBFlagDocumentDB, utils.VarTunnelKafka,
			"requires tunnel = 'direct'"},
		{"unknown flag rejected", "docdb", utils.VarTunnelDirect, "should be empty or 'documentdb'"},
		{"mongodb is not a valid flag", "mongodb", utils.VarTunnelDirect, "should be empty or 'documentdb'"},
	}
	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			conf.Options = conf.Configuration{
				MongoUrls:                []string{"mongodb://source-a"},
				SyncMode:                 utils.VarSyncModeAll,
				IncrSyncMongoFetchMethod: utils.VarIncrSyncMongoFetchMethodOplog,
				SpecialTargetDBFlag:      c.flag,
				Tunnel:                   c.tunnel,
			}
			err := checkDefaultValue()
			if c.wantErr == "" {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, c.wantErr)
			}
		})
	}
}

func TestIsDocumentDBTarget(t *testing.T) {
	origin := conf.Options
	defer func() { conf.Options = origin }()

	conf.Options.SpecialTargetDBFlag = ""
	assert.False(t, conf.Options.IsDocumentDBTarget())
	conf.Options.SpecialTargetDBFlag = utils.VarSpecialTargetDBFlagDocumentDB
	assert.True(t, conf.Options.IsDocumentDBTarget())
}

// TestDisableDocumentDBRetryWrites covers the URI normalization for #1041. The
// checkpoint URL is matched against the *sources*, so a target URI spelled
// differently from tunnel.address is still covered - an exact-match rule would
// silently leave retryable writes on and freeze the checkpoint on engine <8.0.2.
func TestDisableDocumentDBRetryWrites(t *testing.T) {
	origin := conf.Options
	defer func() { conf.Options = origin }()

	const (
		source = "mongodb://src:27017"
		target = "mongodb://docdb:10260/?tls=true"
	)
	cases := []struct {
		desc     string
		flag     string
		ckpt     string
		mongoS   string
		wantCkpt string
	}{
		{"flag off leaves checkpoint alone", "", target, "", target},
		{"empty checkpoint left to later defaulting", utils.VarSpecialTargetDBFlagDocumentDB, "", "", ""},
		{"checkpoint on source untouched", utils.VarSpecialTargetDBFlagDocumentDB, source, "", source},
		{"checkpoint on mongos untouched", utils.VarSpecialTargetDBFlagDocumentDB, "mongodb://mongos:27017",
			"mongodb://mongos:27017", "mongodb://mongos:27017"},
		{"checkpoint on target normalized", utils.VarSpecialTargetDBFlagDocumentDB, target, "",
			target + "&retryWrites=false"},
		{"differently spelled target still normalized", utils.VarSpecialTargetDBFlagDocumentDB,
			"mongodb://docdb:10260?tls=true", "", "mongodb://docdb:10260?tls=true&retryWrites=false"},
		{"explicit checkpoint choice respected", utils.VarSpecialTargetDBFlagDocumentDB,
			target + "&retryWrites=true", "", target + "&retryWrites=true"},
	}
	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			conf.Options = conf.Configuration{
				MongoUrls:            []string{source},
				MongoSUrl:            c.mongoS,
				Tunnel:               utils.VarTunnelDirect,
				TunnelAddress:        []string{target},
				CheckpointStorageUrl: c.ckpt,
				SpecialTargetDBFlag:  c.flag,
			}
			disableDocumentDBRetryWrites()
			assert.Equal(t, c.wantCkpt, conf.Options.CheckpointStorageUrl)
			if c.flag == "" {
				assert.Equal(t, []string{target}, conf.Options.TunnelAddress)
			} else {
				assert.Equal(t, []string{target + "&retryWrites=false"}, conf.Options.TunnelAddress)
			}
		})
	}
}
