package main

import (
	"testing"

	"github.com/stretchr/testify/assert"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
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
