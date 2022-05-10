package utils

import (
	"fmt"
	"github.com/alibaba/MongoShake/v2/unit_test_common"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	testUrl    = unit_test_common.TestUrl
	testUrlSsl = unit_test_common.TestUrlSsl
)

func TestBlockMongoUrlPassword(t *testing.T) {
	var nr int
	{
		fmt.Printf("TestBlockMongoUrlPassword case %d.\n", nr)
		nr++

		output := BlockMongoUrlPassword("mongodb://username:password@address", "***")
		assert.Equal(t, output, "mongodb://username:***@address", "should be equal")
	}

	{
		fmt.Printf("TestBlockMongoUrlPassword case %d.\n", nr)
		nr++

		output := BlockMongoUrlPassword("username:password@address", "***")
		assert.Equal(t, output, "username:***@address", "should be equal")
	}

	{
		fmt.Printf("TestBlockMongoUrlPassword case %d.\n", nr)
		nr++

		output := BlockMongoUrlPassword("username:", "***")
		assert.Equal(t, output, "username:", "should be equal")
	}

	{
		fmt.Printf("TestBlockMongoUrlPassword case %d.\n", nr)
		nr++

		output := BlockMongoUrlPassword("mongodb://username:@", "***")
		assert.Equal(t, output, "mongodb://username:@", "should be equal")
	}

	{
		fmt.Printf("TestBlockMongoUrlPassword case %d.\n", nr)
		nr++

		output := BlockMongoUrlPassword("mongodb://username:password@address", "***********")
		assert.Equal(t, output, "mongodb://username:***********@address", "should be equal")
	}
}

func TestMongoConn(t *testing.T) {
	var nr int
	{
		fmt.Printf("TestMongoConn case %d.\n", nr)
		nr++

		conn, err := NewMongoCommunityConn(testUrl, VarMongoConnectModePrimary, true, "", "", "")
		assert.Equal(t, err, nil, "should be equal")
		assert.Equal(t, conn != nil, true, "should be equal")
	}

	{
		fmt.Printf("TestMongoConn case %d.\n", nr)
		nr++

		conn, err := NewMongoCommunityConn(testUrlSsl, VarMongoConnectModePrimary, true, "", "", "/Users/vinllen-ali/code/mongo-shake-github/MongoShake/ApsaraDB-CA-Chain.pem")
		assert.Equal(t, err, nil, "should be equal")
		assert.Equal(t, conn != nil, true, "should be equal")
	}
}
