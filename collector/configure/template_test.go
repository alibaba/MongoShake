package conf

import (
	"os"
	"testing"

	nimo "github.com/gugemichael/nimo4go"

	utils "github.com/alibaba/MongoShake/v2/common"
)

// TestShippedTemplateParses guards conf/collector.conf against values the loader
// cannot decode.
//
// The template is what every user starts from and nimo rejects the whole file on
// the first bad value, so one wrong line makes collector exit before it ever
// connects anywhere. `tunnel.kafka.producer.max_message_bytes = 18*1024*1024` did
// exactly that — "integer key ... wrong format 18*1024*1024" — and shipped in
// v2.8.7 and v2.8.8 (#998). Nothing else in the tree loads the template, so the
// breakage was invisible to the rest of the test suite.
func TestShippedTemplateParses(t *testing.T) {
	file, err := os.Open("../../conf/collector.conf")
	if err != nil {
		t.Fatalf("open shipped template: %v", err)
	}
	defer file.Close()

	// Load into a local copy rather than the global Options: other tests in this
	// package set conf.Options directly and would be clobbered. Seeding from
	// DefaultConfiguration mirrors main, since nimo does not clear the struct
	// first and omitted keys keep their seeded value.
	opts := DefaultConfiguration()
	loader := nimo.NewConfigLoader(file)
	loader.SetDateFormat(utils.GolangSecurityTime)
	if err := loader.Load(&opts); err != nil {
		t.Fatalf("shipped conf/collector.conf does not parse, so collector cannot start with it: %v", err)
	}

	// A parse alone is not enough: an integer key written as something nimo
	// coerces to 0 would load fine and still be wrong. This one has to stay
	// positive — tunnel/kafka/common.go only overrides sarama when it is > 0, and
	// sarama's own default is 1000000 bytes, so a 0 here silently caps every
	// message at 1MB and large oplog batches get rejected.
	if opts.KafkaProducerMaxMessage <= 0 {
		t.Errorf("tunnel.kafka.producer.max_message_bytes parsed as %d; it must be a positive "+
			"integer, otherwise sarama's 1MB default applies and large oplog batches are rejected",
			opts.KafkaProducerMaxMessage)
	}
}
