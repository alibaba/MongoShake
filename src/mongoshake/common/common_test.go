package utils

import (
	"testing"
	"fmt"

	"github.com/stretchr/testify/assert"
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