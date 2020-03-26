package sharding

import (
	"testing"
	"fmt"
	"reflect"

	"github.com/stretchr/testify/assert"
)

const (
	testCsAddress = "mongodb://xxxx"
)

func TestSharding(t *testing.T) {
	stopBalancer, err := GetBalancerStatusByUrl(testCsAddress)
	assert.Equal(t, nil, err, "should be equal")
	assert.Equal(t, true, stopBalancer, "should be equal")

	mp, err := GetChunkMapByUrl(testCsAddress)
	assert.Equal(t, nil, err, "should be equal")
	for key, val := range mp["test-replica-set"] {
		fmt.Printf("%v -> key[%v] type[%v] chunks[%v]\n", key, val.Keys, val.ShardType, val.Chunks)
		if val.Chunks != nil {
			for _, chunk := range val.Chunks {
				fmt.Printf("  [%v, %v]\n", chunk.Mins, chunk.Maxs)
				if len(chunk.Mins) > 0 {
					fmt.Println(reflect.TypeOf(chunk.Mins[0]))
				}
			}
		}
	}
}