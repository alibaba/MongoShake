package tunnel

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
)

func TestTMessageBytesRoundTrip(t *testing.T) {
	logs := [][]byte{[]byte("alpha"), []byte("beta-longer"), {0x00, 0xff}}
	for _, order := range []binary.ByteOrder{binary.BigEndian, binary.LittleEndian} {
		src := &TMessage{Checksum: 7, Tag: 0, Shard: 3, Compress: 1, RawLogs: logs}
		buf := src.ToBytes(order)
		var dst TMessage
		dst.FromBytes(buf, order)
		assert.Equal(t, src.Checksum, dst.Checksum)
		assert.Equal(t, src.Shard, dst.Shard)
		assert.Equal(t, src.Compress, dst.Compress)
		assert.Equal(t, logs, dst.RawLogs)
		assert.Equal(t, src.ApproximateSize(), dst.ApproximateSize())
	}
}

func TestTMessageProbeHasNoPayload(t *testing.T) {
	src := &TMessage{Tag: MsgProbe}
	buf := src.ToBytes(binary.BigEndian)
	var dst TMessage
	dst.FromBytes(buf, binary.BigEndian)
	assert.Empty(t, dst.RawLogs)
	assert.NotZero(t, dst.Tag&MsgProbe)
}

func TestTMessageStringAndSize(t *testing.T) {
	m := &TMessage{Checksum: 1, Tag: 2, Shard: 3, Compress: 4, RawLogs: [][]byte{{1, 2}, {3}}}
	assert.Contains(t, m.String(), "cksum:1")
	assert.Contains(t, m.String(), "logs_len:2")
	assert.Equal(t, uint64(3), m.ApproximateSize())
	assert.Equal(t, uint64(0), (&TMessage{}).ApproximateSize())
}

func TestWriterFactoryCreate(t *testing.T) {
	// the kafka branch divides by TunnelKafkaPartitionNumber; production sets it
	// in sanitize, a bare conf.Options leaves it 0 and would divide by zero.
	saved := conf.Options.TunnelKafkaPartitionNumber
	conf.Options.TunnelKafkaPartitionNumber = 4
	t.Cleanup(func() { conf.Options.TunnelKafkaPartitionNumber = saved })

	addr := []string{"topic@127.0.0.1:9092"}
	for _, tc := range []struct {
		name string
		want interface{}
	}{
		{utils.VarTunnelKafka, &KafkaWriter{}},
		{utils.VarTunnelTcp, &TCPWriter{}},
		{utils.VarTunnelRpc, &RPCWriter{}},
		{utils.VarTunnelMock, &MockWriter{}},
		{utils.VarTunnelFile, &FileWriter{}},
		{utils.VarTunnelDirect, &DirectWriter{}},
	} {
		got := (&WriterFactory{Name: tc.name}).Create(addr, 0)
		require.NotNil(t, got, tc.name)
		assert.IsType(t, tc.want, got, tc.name)
	}
	assert.Nil(t, (&WriterFactory{Name: "nope"}).Create(addr, 0))

	// kafka partition is workerId modulo the configured partition count
	kw := (&WriterFactory{Name: utils.VarTunnelKafka}).Create(addr, 7).(*KafkaWriter)
	assert.Equal(t, 3, kw.PartitionId)
}

func TestReaderFactoryCreate(t *testing.T) {
	for _, tc := range []struct {
		name string
		want interface{}
	}{
		{utils.VarTunnelKafka, &KafkaReader{}},
		{utils.VarTunnelTcp, &TCPReader{}},
		{utils.VarTunnelRpc, &RPCReader{}},
		{utils.VarTunnelMock, &MockReader{}},
		{utils.VarTunnelFile, &FileReader{}},
	} {
		got := (&ReaderFactory{Name: tc.name}).Create("127.0.0.1:1")
		require.NotNil(t, got, tc.name)
		assert.IsType(t, tc.want, got, tc.name)
	}
	// direct mode has no reader side
	assert.Nil(t, (&ReaderFactory{Name: utils.VarTunnelDirect}).Create("x"))
	assert.Nil(t, (&ReaderFactory{Name: "nope"}).Create("x"))
}
