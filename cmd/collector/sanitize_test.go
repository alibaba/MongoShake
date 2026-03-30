package main

import (
	"testing"

	"github.com/stretchr/testify/assert"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
)

func TestNormalizeFilterCmds(t *testing.T) {
	cmds, err := normalizeFilterCmds([]string{" d ", "I", "", "  "})
	assert.NoError(t, err, "should be equal")
	assert.Equal(t, []string{"d", "i"}, cmds, "should be equal")
}

func TestNormalizeFilterCmdsRejectNoop(t *testing.T) {
	cmds, err := normalizeFilterCmds([]string{"n"})
	assert.Nil(t, cmds, "should be equal")
	assert.EqualError(t, err, `filter.cmds: unsupported op type "n"; noop oplogs are already filtered by the built-in NoopFilter`)
}

func TestCheckDefaultValueInvalidFilterCmds(t *testing.T) {
	origin := conf.Options
	defer func() {
		conf.Options = origin
	}()

	conf.Options = conf.Configuration{
		FilterCmds: []string{"delete"},
	}

	err := checkDefaultValue()
	assert.EqualError(t, err, `filter.cmds: unknown op type "delete", must be one of {i, u, d, c}`)
}
