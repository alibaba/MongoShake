package oplog

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestHash(t *testing.T) {
	var nr int

	// primitive.ObjectID
	{
		t.Logf("TestHash case %d.\n", nr)
		nr++

		objectID := primitive.NewObjectID()
		hashVal := Hash(objectID)

		assert.NotEqual(t, DefaultHashValue, hashVal, "Hash value for ObjectID should not be default")
	}

	// string
	{
		t.Logf("TestHash case %d.\n", nr)
		nr++

		str := "test string"
		hashVal := Hash(str)

		assert.NotEqual(t, DefaultHashValue, hashVal, "Hash value for string should not be default")
	}

	// int64
	{
		t.Logf("TestHash case %d.\n", nr)
		nr++

		var i64 int64 = 1234567890123456789
		hashVal := Hash(i64)

		assert.Equal(t, uint32(i64), hashVal, "Hash value for int64 should be truncated correctly")
	}

	// int32
	{
		t.Logf("TestHash case %d.\n", nr)
		nr++

		var i32 int32 = 1234567890
		hashVal := Hash(i32)

		assert.Equal(t, uint32(i32), hashVal, "Hash value for int32 should be converted correctly")
	}

	// int
	{
		t.Logf("TestHash case %d.\n", nr)
		nr++

		i := 1234567890
		hashVal := Hash(i)

		assert.Equal(t, uint32(i), hashVal, "Hash value for int should be converted correctly")
	}

	// uint
	{
		t.Logf("TestHash case %d.\n", nr)
		nr++

		var ui uint = 1234567890
		hashVal := Hash(ui)

		assert.Equal(t, uint32(ui), hashVal, "Hash value for uint should be converted correctly")
	}

	// uint64
	{
		t.Logf("TestHash case %d.\n", nr)
		nr++

		var ui64 uint64 = 1234567890123456789
		hashVal := Hash(ui64)

		assert.Equal(t, uint32(ui64), hashVal, "Hash value for uint64 should be truncated correctly")
	}

	// uint32
	{
		t.Logf("TestHash case %d.\n", nr)
		nr++

		var ui32 uint32 = 1234567890
		hashVal := Hash(ui32)

		assert.Equal(t, ui32, hashVal, "Hash value for uint32 should be returned as is")
	}

	// nil
	{
		t.Logf("TestHash case %d.\n", nr)
		nr++

		hashVal := Hash(nil)

		assert.Equal(t, DefaultHashValue, hashVal, "Hash value for nil should be default")
	}

	// types which do not support like float64
	{
		t.Logf("TestHash case %d.\n", nr)
		nr++

		f := 3.14159
		hashVal := Hash(f)

		assert.Equal(t, DefaultHashValue, hashVal, "Hash value for unsupported type should be default")
	}

	// diff ObjectID
	{
		t.Logf("TestHash case %d.\n", nr)
		nr++

		objectID1 := primitive.NewObjectID()
		objectID2, _ := primitive.ObjectIDFromHex("5e4fa224a6717632d6ee2e85")

		hashVal1 := Hash(objectID1)
		hashVal2 := Hash(objectID2)

		assert.NotEqual(t, hashVal1, hashVal2, "Different ObjectIDs should produce different hash values")
	}

	// same string
	{
		t.Logf("TestHash case %d.\n", nr)
		nr++

		str1 := "same string"
		str2 := "same string"

		hashVal1 := Hash(str1)
		hashVal2 := Hash(str2)

		assert.Equal(t, hashVal1, hashVal2, "Same strings should produce same hash values")
	}
}
func TestDistributeOplogByMod(t *testing.T) {
	// test DistributeOplogByMod

	var nr int
	oid, _ := primitive.ObjectIDFromHex("5e4fa224a6717632d6ee2e85")

	// TableHasher
	// only for print
	{
		fmt.Printf("TestDistributeOplogByMod case %d for TableHasher.\n", nr)
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
		fmt.Printf("TestDistributeOplogByMod case %d for PrimaryKeyHasher.\n", nr)
		nr++

		pkh := new(PrimaryKeyHasher)
		multiOpType := []int{0, 1}

		log1 := &PartialLog{
			ParsedLog: ParsedLog{
				Namespace: "test.h4",
				Query:     bson.D{{"_id", 123}},
				Operation: "u",
			},
		}

		log2 := &PartialLog{
			ParsedLog: ParsedLog{
				Namespace: "test.h4",
				Query:     bson.D{{"_id", 1230}},
				Operation: "u",
			},
		}

		assert.NotEqual(t, pkh.DistributeOplogByMod(log2, 10000), pkh.DistributeOplogByMod(log1, 10000), "shouldn't be equal")

		log3 := &PartialLog{
			ParsedLog: ParsedLog{
				Namespace: "test.h4",
				Query:     bson.D{{"_id", oid}},
				Object:    bson.D{{"_id", oid}, {"x", 1}},
				Operation: "u",
			},
		}

		log4 := &PartialLog{
			ParsedLog: ParsedLog{
				Namespace: "test.h4",
				Object:    bson.D{{"_id", oid}},
				Operation: "d",
			},
		}

		assert.Equal(t, pkh.DistributeOplogByMod(log3, 8), pkh.DistributeOplogByMod(log4, 8), "should be equal")

		// DDL
		log5 := &PartialLog{
			ParsedLog: ParsedLog{
				Namespace: "test.$cmd",
				Operation: "c",
				Object: bson.D{
					{"createIndexes", "y"},
					{"unique", true},
					{"v", 2},
					{"name", "x_1"},
					{"key", bson.M{"x": 1}},
				},
			},
		}
		assert.Equal(t, Hash("test.$cmd")%8, pkh.DistributeOplogByMod(log5, 8), "should be equal")

		// txn
		log6 := &PartialLog{
			ParsedLog: ParsedLog{
				Timestamp: primitive.Timestamp{T: 1234, I: 1},
				LSID:      bson.Raw{0, 0, 0, 0, 1},
				Operation: "c",
				Namespace: "admin.$cmd",
				Object: bson.D{
					{"applyOps", bson.A{
						bson.D{{"op", "n"}},
						bson.D{{"op", "i"}, {"o", bson.D{{"_id", oid}}}},
						bson.D{{"op", "d"}, {"o", bson.D{{"x", 1}}}},
					}},
					{"partialTxn", true},
				},
			},
		}

		assert.Equal(t, Hash("admin.$cmd")%8, pkh.DistributeOplogByMod(log6, 8), "should be equal")

		// vectored insert
		prevOpTime, _ := bson.Marshal(bson.D{{"ts", primitive.Timestamp{T: 1234, I: 1}}})
		log7 := &PartialLog{
			ParsedLog: ParsedLog{
				Timestamp: primitive.Timestamp{T: 1234, I: 1},
				LSID:      bson.Raw{0, 0, 0, 0, 1},
				Operation: "c",
				Namespace: "admin.$cmd",
				Object: bson.D{
					{"applyOps", bson.A{
						bson.D{{"op", "i"}, {"o", bson.D{{"_id", "1234"}}}, {"stmtId", 1}},
						bson.D{{"op", "i"}, {"o", bson.D{{"_id", "1235"}}}, {"stmtId", 2}},
						bson.D{{"op", "i"}, {"o", bson.D{{"_id", "1236"}}}, {"stmtId", 3}},
					}},
				},
				Version:     2,
				PrevOpTime:  []byte(emptyPrev),
				MultiOpType: &multiOpType[1],
			},
		}

		log8 := &PartialLog{
			ParsedLog: ParsedLog{
				Timestamp: primitive.Timestamp{T: 1234, I: 2},
				LSID:      bson.Raw{0, 0, 0, 0, 1},
				Operation: "c",
				Namespace: "admin.$cmd",
				Object: bson.D{
					{"applyOps", bson.A{
						bson.D{{"op", "i"}, {"o", bson.D{{"_id", "1237"}}}, {"stmtId", 4}},
						bson.D{{"op", "i"}, {"o", bson.D{{"_id", "1238"}}}, {"stmtId", 5}},
						bson.D{{"op", "i"}, {"o", bson.D{{"_id", "1239"}}}, {"stmtId", 6}},
					}},
				},
				Version:     2,
				PrevOpTime:  prevOpTime,
				MultiOpType: &multiOpType[1],
			},
		}

		assert.Equal(t, Hash(1234+1)%8, pkh.DistributeOplogByMod(log7, 8), "should be equal")
		assert.Equal(t, Hash(1234+2)%8, pkh.DistributeOplogByMod(log8, 8), "should be equal")
		assert.NotEqual(t, pkh.DistributeOplogByMod(log7, 8), pkh.DistributeOplogByMod(log8, 8), "shouldn't be equal")
	}

	// WhiteListObjectIdHasher
	{
		fmt.Printf("TestDistributeOplogByMod case %d for WhiteListObjectIdHasher.\n", nr)
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
				Query:     bson.D{{"_id", 123}},
				Operation: "u",
			},
		}

		log4 := &PartialLog{
			ParsedLog: ParsedLog{
				Namespace: "white1",
				Query:     bson.D{{"_id", 1230}},
				Operation: "u",
			},
		}

		assert.Equal(t, false, wloi.DistributeOplogByMod(log3, 10000) == wloi.DistributeOplogByMod(log4, 10000), "should be equal")

		log5 := &PartialLog{
			ParsedLog: ParsedLog{
				Namespace: "white5",
				Query:     bson.D{{"_id", 1230}},
				Operation: "u",
			},
		}

		assert.Equal(t, true, wloi.DistributeOplogByMod(log4, 10000) == wloi.DistributeOplogByMod(log5, 10000), "should be equal")
	}
}
