package kafka

import (
	"compress/gzip"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	LOG "github.com/vinllen/log4go"
	"io/ioutil"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/rcrowley/go-metrics"
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

func NewConfig(rootCaFile string) (*Config, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V0_10_0_0
	config.MetricRegistry = metrics.NewRegistry()

	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewManualPartitioner
	config.Producer.MaxMessageBytes = 16*utils.MB + 2*utils.MB // 2MB for the reserve gap
	if conf.Options.KafkaProducerMaxMessage > 0 {
		config.Producer.MaxMessageBytes = conf.Options.KafkaProducerMaxMessage
	}
	config.Producer.Compression = getKafkaCompression(conf.Options.TunnelKafkaCompression) // conf.Options.TunnelKafkaCompression
	if config.Producer.Compression == sarama.CompressionGZIP {
		config.Producer.CompressionLevel = gzip.BestCompression
	}
	// ssl
	if rootCaFile != "" {
		sslConfig := &tls.Config{
			InsecureSkipVerify: true,
		}
		caCert, err := ioutil.ReadFile(rootCaFile)
		if err != nil {
			LOG.Critical("failed to load the ca cert file[%s]: %s failed: %s", rootCaFile, err.Error())
			return nil, err
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		sslConfig.RootCAs = caCertPool
		config.Net.TLS.Config = sslConfig
		config.Net.TLS.Enable = true
	}
	if conf.Options.TunnelKafkaSaslEnable {
		user, pwd, _ := parseAuth(conf.Options.TunnelKafkaSaslAuth)
		config.Metadata.Full = false
		config.Net.SASL.Enable = true
		config.Net.SASL.User = user
		config.Net.SASL.Password = pwd
		saslMechanismOption := conf.Options.TunnelKafkaSaslMechanism
		if saslMechanismOption != "" {
			saslMechanism := sarama.SASLMechanism(saslMechanismOption)
			config.Net.SASL.Mechanism = saslMechanism
			switch saslMechanism {
			case sarama.SASLTypeSCRAMSHA256:
				//sarama.SASLTypeSCRAMSHA256
				config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
					return &XDGSCRAMClient{HashGeneratorFcn: SHA256}
				}
			case sarama.SASLTypeSCRAMSHA512:
				config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
					return &XDGSCRAMClient{HashGeneratorFcn: SHA512}
				}
			}

		}

	}

	return &Config{
		Config: config,
	}, nil
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

// parse the auth (user@pwd)
func parseAuth(auth string) (string, string, error) {
	arr := strings.Split(auth, topicSplitter)
	l := len(arr)
	if l != 2 {
		return "", "", fmt.Errorf("auth format error")
	}
	return arr[0], arr[1], nil
}

//getKafkaCompression 根据kafkaCompression值获取对应的枚举
func getKafkaCompression(compression string) sarama.CompressionCodec {
	compressions := map[string]sarama.CompressionCodec{
		"none":   sarama.CompressionNone,
		"gzip":   sarama.CompressionGZIP,
		"snappy": sarama.CompressionSnappy,
		"lz4":    sarama.CompressionLZ4,
		"zstd":   sarama.CompressionZSTD,
	}
	if result, ok := compressions[compression]; !ok {
		return sarama.CompressionNone
	} else {
		return result
	}
}
