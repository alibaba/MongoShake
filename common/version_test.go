package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetDBVersionAndCompare(t *testing.T) {
	if testUrl == "" {
		t.Skip("MONGOSHAKE_TEST_URL not set")
	}
	conn, err := NewMongoCommunityConn(testUrl, VarMongoConnectModePrimary, true,
		ReadWriteConcernDefault, ReadWriteConcernDefault, "")
	require.NoError(t, err)
	defer conn.Close()

	v, err := GetDBVersion(conn)
	require.NoError(t, err)
	assert.NotEmpty(t, v)

	ok, err := GetAndCompareVersion(conn, "3.0.0", "")
	require.NoError(t, err)
	assert.True(t, ok, "local server is newer than 3.0")

	ok, err = GetAndCompareVersion(conn, "99.0.0", "")
	// below-threshold is reported as (false, error), not (false, nil)
	assert.False(t, ok, "local server is older than 99.0")
	assert.Error(t, err)
}
