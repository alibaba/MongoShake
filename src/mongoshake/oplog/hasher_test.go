package oplog

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vinllen/mgo/bson"
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

	// PrimaryKeyHasher
	{
		fmt.Printf("TestDistributeOplogByMod case %d.\n", nr)
		nr++

		pkh := new(PrimaryKeyHasher)

		log1 := &PartialLog{
			ParsedLog: ParsedLog{
				Namespace: "test.h4",
				Query: bson.M{
					"_id": 123,
				},
				Operation: "u",
			},
		}

		log2 := &PartialLog{
			ParsedLog: ParsedLog{
				Namespace: "test.h4",
				Query: bson.M{
					"_id": 1230,
				},
				Operation: "u",
			},
		}

		assert.NotEqual(t, pkh.DistributeOplogByMod(log2, 10000), pkh.DistributeOplogByMod(log1, 10000), "should be equal")
	}

	// WhiteListObjectIdHasher
	{
		fmt.Printf("TestDistributeOplogByMod case %d.\n", nr)
		nr++

		wloi := NewWhiteListObjectIdHasher([]string{"white1", "white5"})

		log1 := &PartialLog{
			ParsedLog: ParsedLog{
				Namespace: "test.h4",
				Query: bson.M{
					"_id": 123,
				},
				Operation: "u",
			},
		}

		log2 := &PartialLog{
			ParsedLog: ParsedLog{
				Namespace: "test.h4",
				Query: bson.M{
					"_id": 1230,
				},
				Operation: "u",
			},
		}
		assert.Equal(t, true, wloi.DistributeOplogByMod(log2, 10000) == wloi.DistributeOplogByMod(log1, 10000), "should be equal")

		log3 := &PartialLog{
			ParsedLog: ParsedLog{
				Namespace: "white1",
				Query: bson.M{
					"_id": 123,
				},
				Operation: "u",
			},
		}

		log4 := &PartialLog{
			ParsedLog: ParsedLog{
				Namespace: "white1",
				Query: bson.M{
					"_id": 1230,
				},
				Operation: "u",
			},
		}

		assert.Equal(t, false, wloi.DistributeOplogByMod(log3, 10000) == wloi.DistributeOplogByMod(log4, 10000), "should be equal")

		log5 := &PartialLog{
			ParsedLog: ParsedLog{
				Namespace: "white5",
				Query: bson.M{
					"_id": 1230,
				},
				Operation: "u",
			},
		}

		assert.Equal(t, true, wloi.DistributeOplogByMod(log4, 10000) == wloi.DistributeOplogByMod(log5, 10000), "should be equal")
	}
}