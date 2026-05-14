package kafka

import (
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
)

func TestNewConfig_DefaultVersion(t *testing.T) {
	conf.Options.TunnelKafkaVersion = ""
	conf.Options.TunnelKafkaSaslEnable = false
	conf.Options.TunnelKafkaCompression = ""
	conf.Options.KafkaProducerMaxMessage = 0

	c, err := NewConfig("")
	assert.NoError(t, err)
	assert.Equal(t, sarama.V0_10_0_0, c.Config.Version)
}

func TestNewConfig_CustomVersion(t *testing.T) {
	conf.Options.TunnelKafkaVersion = "2.6.0"
	conf.Options.TunnelKafkaSaslEnable = false
	conf.Options.TunnelKafkaCompression = ""
	conf.Options.KafkaProducerMaxMessage = 0

	c, err := NewConfig("")
	assert.NoError(t, err)
	assert.Equal(t, sarama.V2_6_0_0, c.Config.Version)
}

func TestNewConfig_InvalidVersion(t *testing.T) {
	conf.Options.TunnelKafkaVersion = "invalid"
	conf.Options.TunnelKafkaSaslEnable = false
	conf.Options.TunnelKafkaCompression = ""
	conf.Options.KafkaProducerMaxMessage = 0

	c, err := NewConfig("")
	assert.Error(t, err)
	assert.Nil(t, c)
	assert.Contains(t, err.Error(), "invalid tunnel.kafka.version")
}

func TestNewConfig_MetadataRefreshFrequency(t *testing.T) {
	conf.Options.TunnelKafkaVersion = "2.1.0"
	conf.Options.TunnelKafkaSaslEnable = false
	conf.Options.TunnelKafkaCompression = ""
	conf.Options.KafkaProducerMaxMessage = 0

	c, err := NewConfig("")
	assert.NoError(t, err)
	assert.Equal(t, 3*time.Minute, c.Config.Metadata.RefreshFrequency)
}

func TestNewConfig_ProducerRetry(t *testing.T) {
	conf.Options.TunnelKafkaVersion = "2.1.0"
	conf.Options.TunnelKafkaSaslEnable = false
	conf.Options.TunnelKafkaCompression = ""
	conf.Options.KafkaProducerMaxMessage = 0

	c, err := NewConfig("")
	assert.NoError(t, err)
	assert.Equal(t, 10, c.Config.Producer.Retry.Max)
	assert.Equal(t, 500*time.Millisecond, c.Config.Producer.Retry.Backoff)
}

func TestNewConfig_ProducerPartitioner(t *testing.T) {
	conf.Options.TunnelKafkaVersion = "2.1.0"
	conf.Options.TunnelKafkaSaslEnable = false
	conf.Options.TunnelKafkaCompression = ""
	conf.Options.KafkaProducerMaxMessage = 0

	c, err := NewConfig("")
	assert.NoError(t, err)
	assert.True(t, c.Config.Producer.Return.Errors)
	assert.True(t, c.Config.Producer.Return.Successes)
}

func TestParse(t *testing.T) {
	topic, brokers, err := parse("my-topic@broker1:9092,broker2:9092,broker3:9092")
	assert.NoError(t, err)
	assert.Equal(t, "my-topic", topic)
	assert.Equal(t, []string{"broker1:9092", "broker2:9092", "broker3:9092"}, brokers)

	topic, brokers, err = parse("broker1:9092,broker2:9092")
	assert.NoError(t, err)
	assert.Equal(t, "mongoshake", topic)
	assert.Equal(t, []string{"broker1:9092", "broker2:9092"}, brokers)

	_, _, err = parse("a@b@c")
	assert.Error(t, err)
}

func TestGetKafkaCompression(t *testing.T) {
	assert.Equal(t, sarama.CompressionNone, getKafkaCompression("none"))
	assert.Equal(t, sarama.CompressionGZIP, getKafkaCompression("gzip"))
	assert.Equal(t, sarama.CompressionSnappy, getKafkaCompression("snappy"))
	assert.Equal(t, sarama.CompressionLZ4, getKafkaCompression("lz4"))
	assert.Equal(t, sarama.CompressionZSTD, getKafkaCompression("zstd"))
	assert.Equal(t, sarama.CompressionNone, getKafkaCompression("unknown"))
}
