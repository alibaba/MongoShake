package unit_test_common

import "os"

// Test connection info is loaded from environment variables so the repo
// never ships real endpoints or credentials. When an env var is unset the
// value falls back to a localhost placeholder (or empty string for inputs
// that have no safe default such as TLS URIs and CA paths); connectivity
// tests should `t.Skip` when they read an empty value.
//
// Recognised env vars:
//   MONGOSHAKE_TEST_URL                - replica set URI
//   MONGOSHAKE_TEST_URL_SSL            - SSL/TLS URI (no default)
//   MONGOSHAKE_TEST_URL_CONFIG_SERVER  - sharded cluster config server URI (no default)
//   MONGOSHAKE_TEST_URL_SERVERLESS     - serverless tenant URI (no default)
//   MONGOSHAKE_TEST_URL_SHARDING       - mongos URI (no default)
//   MONGOSHAKE_TEST_CA_PEM             - filesystem path to CA chain pem (no default)
var (
	TestUrl                 = envOr("MONGOSHAKE_TEST_URL", "mongodb://localhost:27017")
	TestUrlSsl              = os.Getenv("MONGOSHAKE_TEST_URL_SSL")
	TestUrlConfigServer     = os.Getenv("MONGOSHAKE_TEST_URL_CONFIG_SERVER")
	TestUrlServerlessTenant = os.Getenv("MONGOSHAKE_TEST_URL_SERVERLESS")
	TestUrlSharding         = os.Getenv("MONGOSHAKE_TEST_URL_SHARDING")
	TestCaPem               = os.Getenv("MONGOSHAKE_TEST_CA_PEM")
)

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
