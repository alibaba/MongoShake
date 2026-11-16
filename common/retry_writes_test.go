package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
)

// TestDisableRetryWrites covers the target-side URI normalization for #1041.
// Amazon DocumentDB rejects retryable writes before engine 8.0.2 (code 301) while
// the driver turns them on by default, so the flag has to reach the URI. An
// explicit user choice must survive untouched in either direction.
//
// Every output is fed through the driver's own URI parser, not just compared as a
// string: an earlier version of this test asserted "mongodb://h:1?retryWrites=false"
// as correct, but the driver rejects it with "must have a / before the query ?",
// which is exactly what broke a real DocumentDB deployment.
func TestDisableRetryWrites(t *testing.T) {
	cases := []struct {
		desc      string
		uri       string
		want      string
		wantRetry bool // expected effective retryWrites after parsing
	}{
		{"no path, no query", "mongodb://h:1", "mongodb://h:1/?retryWrites=false", false},
		{"trailing slash, no query", "mongodb://h:1/", "mongodb://h:1/?retryWrites=false", false},
		{"auth db path, no query", "mongodb://h:1/admin", "mongodb://h:1/admin?retryWrites=false", false},
		{"empty query string", "mongodb://h:1/?", "mongodb://h:1/?retryWrites=false", false},
		{"existing query kept", "mongodb://h:1/?tls=true", "mongodb://h:1/?tls=true&retryWrites=false", false},
		{"credentials and query kept", "mongodb://u:p@h:1/admin?tls=true&tlsInsecure=true",
			"mongodb://u:p@h:1/admin?tls=true&tlsInsecure=true&retryWrites=false", false},
		{"already false", "mongodb://h:1/?retryWrites=false", "mongodb://h:1/?retryWrites=false", false},
		{"explicit true respected", "mongodb://h:1/?retryWrites=true", "mongodb://h:1/?retryWrites=true", true},
		{"option name is case insensitive", "mongodb://h:1/?RetryWrites=true", "mongodb://h:1/?RetryWrites=true", true},
		{"not the last param", "mongodb://h:1/?retryWrites=false&tls=true",
			"mongodb://h:1/?retryWrites=false&tls=true", false},
		// userinfo may contain '?', '/' and '@'; it ends at the last '@'
		{"question mark in password", "mongodb://u:pa?ss@h:1", "mongodb://u:pa?ss@h:1/?retryWrites=false", false},
		{"encoded slash in password", "mongodb://u:pa%2Fss@h:1", "mongodb://u:pa%2Fss@h:1/?retryWrites=false", false},
		{"question mark in password with query", "mongodb://u:pa?ss@h:1/?tls=true",
			"mongodb://u:pa?ss@h:1/?tls=true&retryWrites=false", false},
		{"question mark in password already set", "mongodb://u:pa?ss@h:1/?retryWrites=true",
			"mongodb://u:pa?ss@h:1/?retryWrites=true", true},
		{"multiple hosts, no path", "mongodb://h1:1,h2:2", "mongodb://h1:1,h2:2/?retryWrites=false", false},
	}
	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			got := DisableRetryWrites(c.uri)
			assert.Equal(t, c.want, got)

			// validate with the driver's own URI parser, which is what rejects a
			// missing '/' before the query in production
			cs, err := connstring.ParseAndValidate(got)
			require.NoError(t, err, "driver rejected the normalized URI %q", got)
			assert.Equal(t, c.wantRetry, cs.RetryWrites, "effective retryWrites for %q", got)
		})
	}
}

// TestDisableRetryWritesOutputParses is the regression anchor for the DocumentDB
// deployment failure: a bare host URI with neither path nor query must still yield
// a URI the driver accepts.
func TestDisableRetryWritesOutputParses(t *testing.T) {
	const bare = "mongodb://root:secret@docdb-test.cluster-abc.sa-east-1.docdb.amazonaws.com:27017"
	got := DisableRetryWrites(bare)
	cs, err := connstring.ParseAndValidate(got)
	require.NoError(t, err, "bare DocumentDB endpoint must normalize to a parseable URI, got %q", got)
	assert.False(t, cs.RetryWrites)
	assert.True(t, cs.RetryWritesSet, "retryWrites must be explicitly set")
	assert.Equal(t, []string{"docdb-test.cluster-abc.sa-east-1.docdb.amazonaws.com:27017"}, cs.Hosts,
		"the host must not be polluted by the appended option")
}

// TestDisableRetryWritesIdempotent guards against appending the option twice when
// sanitize runs more than once over the same address.
func TestDisableRetryWritesIdempotent(t *testing.T) {
	for _, uri := range []string{
		"mongodb://h:1",
		"mongodb://h:1/",
		"mongodb://h:1/?tls=true",
		"mongodb://u:pa?ss@h:1",
	} {
		once := DisableRetryWrites(uri)
		assert.Equal(t, once, DisableRetryWrites(once), "not idempotent for %q", uri)
	}
}
