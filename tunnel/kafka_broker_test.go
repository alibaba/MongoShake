package tunnel

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
)

// These tests exercise the real producer/consumer wire path, so they need a
// broker. MSHAKE_KAFKA_URL carries "broker1,broker2"; unset means skip, keeping
// the suite runnable on machines without Kafka.
func kafkaBrokers(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("MSHAKE_KAFKA_URL")
	if addr == "" {
		t.Skip("MSHAKE_KAFKA_URL not set; Kafka broker unavailable")
	}
	return addr
}

// kafkaConf pins the options the real path reads. The code default for
// TunnelKafkaVersion is the legacy 0.10.0.0, which a KRaft broker will not
// serve; set a version the broker actually speaks.
func kafkaConf(t *testing.T) {
	t.Helper()
	saved := conf.Options
	savedFlag := unitTestWriteKafkaFlag
	t.Cleanup(func() {
		conf.Options = saved
		unitTestWriteKafkaFlag = savedFlag
	})
	conf.Options.IncrSyncTunnelWriteThread = 2
	conf.Options.IncrSyncWorker = 1
	conf.Options.TunnelKafkaVersion = "2.1.0"
	conf.Options.TunnelKafkaCompression = ""
	conf.Options.IncrSyncTunnelKafkaDebug = ""
	conf.Options.TunnelMongoSslRootCaFile = ""
	// encode() panics on an empty tunnel.message; raw passes RawLogs through
	// unchanged, which is what the round-trip assertion compares.
	conf.Options.TunnelMessage = "raw"
	// writer_test.go sets this true and never restores it; under -shuffle the
	// real path would otherwise be silently skipped.
	unitTestWriteKafkaFlag = false
}

func TestKafkaWriterReaderRoundTrip(t *testing.T) {
	brokers := kafkaBrokers(t)
	kafkaConf(t)
	// KafkaReader consumes from OffsetOldest, so a shared topic would replay
	// earlier runs into these assertions.
	topic := fmt.Sprintf("mshake-it-%d", time.Now().UnixNano())
	addr := topic + "@" + brokers

	w := &KafkaWriter{RemoteAddr: addr, PartitionId: 0}
	require.True(t, w.Prepare(), "real SyncWriter must start against the broker")
	t.Cleanup(func() {
		if w.writer != nil {
			_ = w.writer.Close()
		}
	})

	// raw mode emits one framed message per RawLog entry, each carrying the whole
	// batch (see kafka_writer.go raw branch). A single entry keeps the round trip
	// unambiguous; the per-entry duplication is noted in the PR, not pinned here.
	logs := [][]byte{[]byte("kafka-entry-1")}
	msg := &WMessage{TMessage: &TMessage{RawLogs: logs, Checksum: 42, Shard: 0, Compress: 0}}
	require.Equal(t, int64(0), w.Send(msg))

	rep := &captureReplayer{reply: ReplyOK}
	r := &KafkaReader{address: addr}
	require.NoError(t, r.Link([]Replayer{rep}))

	got := waitReplay(t, rep, 1)
	require.Len(t, got, 1, "writer->broker->reader round trip must deliver the batch")
	assert.Equal(t, logs, got[0].RawLogs)
	assert.Equal(t, uint32(42), got[0].Checksum)
	assert.Equal(t, uint32(0), got[0].Compress)
}

func TestKafkaWriterSkipsProbeAndEmpty(t *testing.T) {
	brokers := kafkaBrokers(t)
	kafkaConf(t)
	topic := fmt.Sprintf("mshake-it-probe-%d", time.Now().UnixNano())
	w := &KafkaWriter{RemoteAddr: topic + "@" + brokers, PartitionId: 0}
	require.True(t, w.Prepare())
	t.Cleanup(func() {
		if w.writer != nil {
			_ = w.writer.Close()
		}
	})

	assert.Equal(t, int64(0), w.Send(&WMessage{TMessage: &TMessage{}}))
	assert.Equal(t, int64(0), w.Send(&WMessage{TMessage: &TMessage{
		RawLogs: [][]byte{{1}}, Tag: MsgProbe,
	}}))
	assert.False(t, w.AckRequired())
	assert.False(t, w.ParsedLogsRequired())
	assert.Equal(t, "kafka", w.Name())
}

func TestKafkaUnreachableBrokerFailsCleanly(t *testing.T) {
	kafkaConf(t)
	// port 1 on loopback refuses connections; Prepare must report false rather
	// than hang or panic.
	w := &KafkaWriter{RemoteAddr: "mshake-unreachable@127.0.0.1:1", PartitionId: 0}
	assert.False(t, w.Prepare())

	r := &KafkaReader{address: "mshake-unreachable@127.0.0.1:1"}
	assert.Error(t, r.Link([]Replayer{&captureReplayer{}}))
}
