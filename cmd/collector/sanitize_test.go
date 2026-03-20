package main

import (
	"testing"

	"github.com/stretchr/testify/assert"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
)

func TestNormalizeFilterCmds(t *testing.T) {
	cmds, err := normalizeFilterCmds([]string{" d ", "I", "", "  ", "n"})
	assert.NoError(t, err, "should be equal")
	assert.Equal(t, []string{"d", "i", "n"}, cmds, "should be equal")
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
	assert.EqualError(t, err, "filter.cmds[delete] should in {i, u, d, c, n}")
}
