package kafka

import (
	"fmt"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/rcrowley/go-metrics"
	"mongoshake/common"
)

var (
	topicDefault           = "mongoshake"
	topicSplitter          = "@"
	brokersSplitter        = ","
	defaultPartition int32 = 0
)

type Message struct {
	Key       []byte
	Value     []byte
	Offset    int64
	TimeStamp time.Time
}

type Config struct {
	Config *sarama.Config
}

func NewConfig() *Config {
	config := sarama.NewConfig()
	config.Version = sarama.V0_10_0_0
	config.MetricRegistry = metrics.NewRegistry()

	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewManualPartitioner
	config.Producer.MaxMessageBytes = 16 * utils.MB + 2 * utils.MB // 2MB for the reserve gap

	return &Config{
		Config: config,
	}
}

// parse the address (topic@broker1,broker2,...)
func parse(address string) (string, []string, error) {
	arr := strings.Split(address, topicSplitter)
	l := len(arr)
	if l == 0 || l > 2 {
		return "", nil, fmt.Errorf("address format error")
	}

	topic := topicDefault
	if l == 2 {
		topic = arr[0]
	}

	brokers := strings.Split(arr[l-1], brokersSplitter)
	return topic, brokers, nil
}
