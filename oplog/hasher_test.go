package oplog

import (
	"fmt"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
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
				Query: bson.D{
					{"_id", 123},
				},
				Operation: "u",
			},
		}

		log2 := &PartialLog{
			ParsedLog: ParsedLog{
				Namespace: "test.h4",
				Query: bson.D{
					{"_id", 1230},
				},
				Operation: "u",
			},
		}

		assert.NotEqual(t, pkh.DistributeOplogByMod(log2, 10000), pkh.DistributeOplogByMod(log1, 10000), "should be equal")

		log3 := &PartialLog{
			ParsedLog: ParsedLog{
				Namespace: "test.h4",
				Query: bson.D{
					{"_id", int32(123)},
				},
				Operation: "u",
			},
		}

		log4 := &PartialLog{
			ParsedLog: ParsedLog{
				Namespace: "test.h4",
				Query: bson.D{
					{"_id", int32(1230)},
				},
				Operation: "u",
			},
		}

		assert.NotEqual(t, pkh.DistributeOplogByMod(log3, 10000), pkh.DistributeOplogByMod(log4, 10000), "should be equal")

		log5 := &PartialLog{
			ParsedLog: ParsedLog{
				Namespace: "test.h4",
				Query: bson.D{
					{"_id", "123"},
				},
				Operation: "u",
			},
		}

		log6 := &PartialLog{
			ParsedLog: ParsedLog{
				Namespace: "test.h4",
				Query: bson.D{
					{"_id", "1230"},
				},
				Operation: "u",
			},
		}

		assert.NotEqual(t, pkh.DistributeOplogByMod(log5, 10000), pkh.DistributeOplogByMod(log6, 10000), "should be equal")

		log7 := &PartialLog{
			ParsedLog: ParsedLog{
				Namespace: "test.h4",
				Query: bson.D{
					{"_id", int64(123456789012345678)},
				},
				Operation: "u",
			},
		}

		log8 := &PartialLog{
			ParsedLog: ParsedLog{
				Namespace: "test.h4",
				Query: bson.D{
					{"_id", int64(987654321098765432)},
				},
				Operation: "u",
			},
		}

		assert.NotEqual(t, pkh.DistributeOplogByMod(log7, 10000), pkh.DistributeOplogByMod(log8, 10000), "should be equal")

		oid1, _ := primitive.ObjectIDFromHex("684cde2b4aa2121c8b5907da")
		log9 := &PartialLog{
			ParsedLog: ParsedLog{
				Namespace: "test.h4",
				Query: bson.D{
					{"_id", oid1},
				},
				Operation: "u",
			},
		}
		oid2, _ := primitive.ObjectIDFromHex("684cde2b4aa2121c8b5907db")
		log10 := &PartialLog{
			ParsedLog: ParsedLog{
				Namespace: "test.h4",
				Query: bson.D{
					{"_id", oid2},
				},
				Operation: "u",
			},
		}

		assert.NotEqual(t, pkh.DistributeOplogByMod(log9, 10000), pkh.DistributeOplogByMod(log10, 10000), "should be equal")
	}

	// WhiteListObjectIdHasher
	{
		fmt.Printf("TestDistributeOplogByMod case %d.\n", nr)
		nr++

		wloi := NewWhiteListObjectIdHasher([]string{"white1", "white5"})

		log1 := &PartialLog{
			ParsedLog: ParsedLog{
				Namespace: "test.h4",
				Query: bson.D{
					{"_id", 123},
				},
				Operation: "u",
			},
		}

		log2 := &PartialLog{
			ParsedLog: ParsedLog{
				Namespace: "test.h4",
				Query: bson.D{
					{"_id", 1230},
				},
				Operation: "u",
			},
		}
		assert.Equal(t, true, wloi.DistributeOplogByMod(log2, 10000) == wloi.DistributeOplogByMod(log1, 10000), "should be equal")

		log3 := &PartialLog{
			ParsedLog: ParsedLog{
				Namespace: "white1",
				Query: bson.D{
					{"_id", 123},
				},
				Operation: "u",
			},
		}

		log4 := &PartialLog{
			ParsedLog: ParsedLog{
				Namespace: "white1",
				Query: bson.D{
					{"_id", 1230},
				},
				Operation: "u",
			},
		}

		assert.Equal(t, false, wloi.DistributeOplogByMod(log3, 10000) == wloi.DistributeOplogByMod(log4, 10000), "should be equal")

		log5 := &PartialLog{
			ParsedLog: ParsedLog{
				Namespace: "white5",
				Query: bson.D{
					{"_id", 1230},
				},
				Operation: "u",
			},
		}

		assert.Equal(t, true, wloi.DistributeOplogByMod(log4, 10000) == wloi.DistributeOplogByMod(log5, 10000), "should be equal")
	}
}
