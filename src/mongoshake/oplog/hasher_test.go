package oplog

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDistributeOplogByMod(t *testing.T) {
	// test DistributeOplogByMod

	var nr int

	// TableHasher
	// only for print
	{
		fmt.Printf("TestDistributeOplogByMod case %d.\n", nr)
		nr++

		th := new(TableHasher)

		log1 := &PartialLog{
			ParsedLog: ParsedLog{
				Namespace: "test.h4",
			},
		}
		hashVal1 := th.DistributeOplogByMod(log1, 3)

		log2 := &PartialLog{
			ParsedLog: ParsedLog{
				Namespace: "shard_collection.g4",
			},
		}
		hashVal2 := th.DistributeOplogByMod(log2, 8)
		fmt.Println(hashVal1, hashVal2)

		assert.NotEqual(t, hashVal1, hashVal2, "should be equal")
	}
}