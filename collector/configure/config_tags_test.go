package conf

import (
	"reflect"
	"testing"
)

// TestConfigTagsAreUnique asserts no two fields of Configuration claim the
// same `config:"..."` key.
//
// The loader resolves a conf line to a field by its tag, so a duplicate means
// one of the two fields can never be set from its own key. MongoSslClientCaFile
// carried the tag mongo_ssl_root_ca_file -- the same one MongoSslRootCaFile
// already uses -- so there was no key that could ever hand it a client CA path.
// The field was also unread, which is the only reason nothing broke: the
// declaration sat directly below a working "// add v2.6.2" option and read as
// supported functionality to anyone auditing the SSL options.
//
// Fixing the tag alone would have been worse than deleting the field, since it
// would leave an unread option behind a plausible-looking key: setting
// mongo_ssl_client_ca_file in a conf would then be silently ignored rather than
// visibly absent from the template.
func TestConfigTagsAreUnique(t *testing.T) {
	typ := reflect.TypeOf(Configuration{})
	owner := make(map[string]string, typ.NumField())

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		tag := field.Tag.Get("config")
		if tag == "" {
			continue
		}
		if prev, dup := owner[tag]; dup {
			t.Errorf("config tag %q is claimed by both %s and %s; only one of them can ever be "+
				"populated from a conf file, so the other is unreachable configuration", tag, prev, field.Name)
			continue
		}
		owner[tag] = field.Name
	}
}
