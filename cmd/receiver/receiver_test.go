package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	utils "github.com/alibaba/MongoShake/v2/common"
	conf "github.com/alibaba/MongoShake/v2/receiver/configure"
)

func TestSanitizeOptionsFillsDefaults(t *testing.T) {
	saved := conf.Options
	t.Cleanup(func() { conf.Options = saved })

	conf.Options = conf.Configuration{}
	conf.Options.Tunnel = utils.VarTunnelTcp
	conf.Options.TunnelAddress = "127.0.0.1:1"
	require.NoError(t, sanitizeOptions())
	assert.Equal(t, "logs", conf.Options.LogDirectory)
	assert.Equal(t, utils.VarLogLevelInfo, conf.Options.LogLevel)
	assert.Equal(t, "receiver.log", conf.Options.LogFileName)
	assert.Equal(t, 20, conf.Options.LogMaxSizeMb)
	assert.Equal(t, 7, conf.Options.LogMaxAge)
}

func TestSanitizeOptionsKeepsExplicitValues(t *testing.T) {
	saved := conf.Options
	t.Cleanup(func() { conf.Options = saved })

	conf.Options = conf.Configuration{}
	conf.Options.LogDirectory = "mylogs"
	conf.Options.LogLevel = "debug"
	conf.Options.LogFileName = "r.log"
	conf.Options.LogMaxSizeMb = 5
	conf.Options.LogMaxAge = 3
	conf.Options.Tunnel = utils.VarTunnelTcp
	conf.Options.TunnelAddress = "127.0.0.1:1"
	require.NoError(t, sanitizeOptions())
	assert.Equal(t, "mylogs", conf.Options.LogDirectory)
	assert.Equal(t, "debug", conf.Options.LogLevel)
	assert.Equal(t, "r.log", conf.Options.LogFileName)
	assert.Equal(t, 5, conf.Options.LogMaxSizeMb)
	assert.Equal(t, 3, conf.Options.LogMaxAge)
}

func TestSanitizeOptionsRejectsMissingTunnel(t *testing.T) {
	saved := conf.Options
	t.Cleanup(func() { conf.Options = saved })

	conf.Options = conf.Configuration{}
	conf.Options.TunnelAddress = "127.0.0.1:1"
	assert.Error(t, sanitizeOptions(), "empty tunnel must be rejected")

	conf.Options = conf.Configuration{}
	conf.Options.Tunnel = utils.VarTunnelTcp
	assert.Error(t, sanitizeOptions(), "empty tunnel address must be rejected")
}
