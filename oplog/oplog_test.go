package oplog

import (
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRemoveFiled(t *testing.T) {
	// test RemoveFiled

	var nr int
	{
		fmt.Printf("TestRemoveFiled case %d.\n", nr)
		nr++

		input := bson.D{
			bson.E{
				Key:   "w",
				Value: 1,
			},
			bson.E{
				Key:   "$v",
				Value: 2,
			},
			bson.E{
				Key:   "a",
				Value: 3,
			},
		}

		ret := RemoveFiled(input, "w")
		assert.Equal(t, bson.D{
			bson.E{
				Key:   "$v",
				Value: 2,
			},
			bson.E{
				Key:   "a",
				Value: 3,
			},
		}, ret, "should be equal")
	}

	{
		fmt.Printf("TestRemoveFiled case %d.\n", nr)
		nr++

		input := bson.D{
			bson.E{
				Key:   "w",
				Value: 1,
			},
			bson.E{
				Key:   "$v",
				Value: 2,
			},
			bson.E{
				Key:   "a",
				Value: 3,
			},
		}

		ret := RemoveFiled(input, "$v")
		assert.Equal(t, bson.D{
			bson.E{
				Key:   "w",
				Value: 1,
			},
			bson.E{
				Key:   "a",
				Value: 3,
			},
		}, ret, "should be equal")
	}

	{
		fmt.Printf("TestRemoveFiled case %d.\n", nr)
		nr++

		input := bson.D{
			bson.E{
				Key:   "w",
				Value: 1,
			},
			bson.E{
				Key:   "$v",
				Value: 2,
			},
			bson.E{
				Key:   "a",
				Value: 3,
			},
		}

		ret := RemoveFiled(input, "a")
		assert.Equal(t, bson.D{
			bson.E{
				Key:   "w",
				Value: 1,
			},
			bson.E{
				Key:   "$v",
				Value: 2,
			},
		}, ret, "should be equal")
	}

	{
		fmt.Printf("TestRemoveFiled case %d.\n", nr)
		nr++

		input := bson.D{
			bson.E{
				Key:   "w",
				Value: 1,
			},
			bson.E{
				Key:   "$v",
				Value: 2,
			},
			bson.E{
				Key:   "a",
				Value: 3,
			},
		}

		ret := RemoveFiled(input, "aff")
		assert.Equal(t, input, ret, "should be equal")
	}

	{
		fmt.Printf("TestRemoveFiled case %d.\n", nr)
		nr++

		input := bson.D{
			bson.E{
				Key:   "$v",
				Value: 1,
			},
			bson.E{
				Key: "$set",
				Value: bson.D{
					bson.E{
						Key:   "web_list.0.utime",
						Value: "2019-12-24 17:05:41",
					},
				},
			},
			bson.E{
				Key: "$set",
				Value: bson.M{
					"web_list.0.utime": "2019-12-24 17:05:41",
				},
			},
			bson.E{
				Key: "$set",
				Value: bson.D{
					bson.E{
						Key:   "web_list.0.utime",
						Value: "2019-12-24 17:05:41",
					},
				},
			},
		}

		ret := RemoveFiled(input, "$v")
		assert.Equal(t, ret, bson.D{
			bson.E{
				Key: "$set",
				Value: bson.D{
					bson.E{
						Key:   "web_list.0.utime",
						Value: "2019-12-24 17:05:41",
					},
				},
			},
			bson.E{
				Key: "$set",
				Value: bson.M{
					"web_list.0.utime": "2019-12-24 17:05:41",
				},
			},
			bson.E{
				Key: "$set",
				Value: bson.D{
					bson.E{
						Key:   "web_list.0.utime",
						Value: "2019-12-24 17:05:41",
					},
				},
			},
		}, "should be equal")
	}

	{
		fmt.Printf("TestRemoveFiled case %d.\n", nr)
		nr++

		input := bson.D{
			bson.E{
				Key:   "$v",
				Value: 1,
			},
			bson.E{
				Key: "$set",
				Value: bson.D{
					bson.E{
						Key:   "web_list.0.utime",
						Value: "2019-12-24 17:05:41",
					},
				},
			},
		}

		ret := RemoveFiled(input, "$v2")
		assert.Equal(t, ret, input, "should be equal")
	}

	{
		fmt.Printf("TestRemoveFiled case %d.\n", nr)
		nr++

		input := bson.D{
			bson.E{
				Key:   "$v",
				Value: 1,
			},
			bson.E{
				Key: "$set",
				Value: bson.D{
					bson.E{
						Key:   "workFlowNodeList.3.nodePersonList.0.attitude",
						Value: "2",
					},
				},
			},
		}

		ret := RemoveFiled(input, "$v")
		assert.Equal(t, ret, bson.D{
			bson.E{
				Key: "$set",
				Value: bson.D{
					bson.E{
						Key:   "workFlowNodeList.3.nodePersonList.0.attitude",
						Value: "2",
					},
				},
			},
		}, "should be equal")
	}
}

func TestGatherApplyOps(t *testing.T) {
	nr := 0

	{
		fmt.Printf("TestGatherApplyOps case %d.\n", nr)
		nr++

		input := []*PartialLog{
			{
				ParsedLog: ParsedLog{
					Timestamp: primitive.DateTime(1),
					Operation: "i",
					Namespace: "db1.c1",
					Object: bson.D{
						bson.E{
							Key:   "x",
							Value: 1,
						},
						bson.E{
							Key:   "y",
							Value: 2,
						},
					},
				},
			},
			{
				ParsedLog: ParsedLog{
					Timestamp: primitive.DateTime(1),
					Operation: "i",
					Namespace: "db1.c2",
					Object: bson.D{
						bson.E{
							Key:   "x",
							Value: 10,
						},
						bson.E{
							Key:   "y",
							Value: 20,
						},
					},
				},
			},
			{
				ParsedLog: ParsedLog{
					Timestamp: primitive.DateTime(1),
					Operation: "i",
					Namespace: "db2.c2",
					Object: bson.D{
						bson.E{
							Key:   "x",
							Value: 100,
						},
						bson.E{
							Key:   "y",
							Value: 200,
						},
					},
				},
			},
			{
				ParsedLog: ParsedLog{
					Timestamp: primitive.DateTime(1),
					Operation: "u",
					Namespace: "db3.c3",
					Object: bson.D{
						bson.E{
							Key:   "x",
							Value: 100,
						},
						bson.E{
							Key:   "y",
							Value: 200,
						},
					},
					Query: bson.D{
						{"x", 1},
						{"y", 1},
					},
				},
			},
		}

		// hit the 4 in logsQ[0]
		gather, err := GatherApplyOps(input)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, len(gather.Raw) > 0, "should be equal")
		assert.Equal(t, primitive.DateTime(1), gather.Parsed.Timestamp, "should be equal")
		assert.Equal(t, "admin.$cmd", gather.Parsed.Namespace, "should be equal")
		assert.Equal(t, "c", gather.Parsed.Operation, "should be equal")
		assert.Equal(t, bson.D(nil), gather.Parsed.Query, "should be equal")
		assert.Equal(t, "applyOps", gather.Parsed.Object[0].Key, "should be equal")
		assert.Equal(t, 4, len(gather.Parsed.Object[0].Value.([]bson.M)), "should be equal")
		assert.Equal(t, "i", gather.Parsed.Object[0].Value.([]bson.M)[0]["op"], "should be equal")
		assert.Equal(t, "db1.c1", gather.Parsed.Object[0].Value.([]bson.M)[0]["ns"], "should be equal")
		fmt.Println(gather.Parsed.Object[0])
	}
}

func TestPartialLog(t *testing.T) {
	nr := 0

	{
		fmt.Printf("TestPartialLog case %d.\n", nr)
		nr++

		input := bson.M{
			"ts": primitive.DateTime(1),
			"ns": "a.b",
			"o": bson.D{
				bson.E{
					Key:   "key1",
					Value: "value1",
				},
				bson.E{
					Key:   "key2",
					Value: "value2",
				},
			},
			"o2": bson.D{
				{"_id", "123"},
			},
			"useless": "can't see me",
		}

		output := NewPartialLog(input)
		assert.Equal(t, &PartialLog{
			ParsedLog: ParsedLog{
				Timestamp: primitive.DateTime(1),
				Namespace: "a.b",
				Object: bson.D{
					bson.E{
						Key:   "key1",
						Value: "value1",
					},
					bson.E{
						Key:   "key2",
						Value: "value2",
					},
				},
				Query: bson.D{
					{"_id", "123"},
				},
			},
		}, output, "should be equal")

		output.RawSize = 1 // shouldn't appear

		// test dump
		bsonDOutput := output.Dump(map[string]struct{}{
			"ts": {},
			"o":  {},
			"o2": {},
		}, false)
		assert.Equal(t, bson.D{
			bson.E{
				Key:   "ts",
				Value: primitive.DateTime(1),
			},
			bson.E{
				Key: "o",
				Value: bson.D{
					bson.E{
						Key:   "key1",
						Value: "value1",
					},
					bson.E{
						Key:   "key2",
						Value: "value2",
					},
				},
			},
			bson.E{
				Key: "o2",
				Value: bson.D{
					{"_id", "123"},
				},
			},
		}, bsonDOutput, "should be equal")
	}
}

func TestGetKey(t *testing.T) {
	nr := 0

	{
		fmt.Printf("TestGetKey case %d.\n", nr)
		nr++

		input := bson.D{
			bson.E{
				Key:   "_id",
				Value: "value1",
			},
			bson.E{
				Key:   "key2",
				Value: "value2",
			},
		}
		assert.Equal(t, "value1", GetKey(input, ""), "should be equal")
		assert.Equal(t, "value2", GetKey(input, "key2"), "should be equal")
		assert.Equal(t, nil, GetKey(input, "unknown"), "should be equal")
	}
}

func TestConvertBson(t *testing.T) {
	nr := 0

	{
		fmt.Printf("TestConvertBson case %d.\n", nr)
		nr++

		input := bson.M{
			"k1": "b",
			"k2": 12,
			"k3": []string{"1", "2", "3"},
			"k4": map[string]int{
				"hello": 1,
				"world": 2,
			},
		}

		m := ConvertBsonM2D(input)
		output, _ := ConvertBsonD2M(m)
		assert.Equal(t, input, output, "should be equal")
	}

	{
		fmt.Printf("TestConvertBson case %d.\n", nr)
		nr++

		input := bson.M{
			"k1": "b",
			"k2": 12,
			"k3": []string{"1", "2", "3"},
			"k4": map[string]int{
				"hello": 1,
				"world": 2,
			},
			"r": bson.D{
				{
					Key:   "xxx",
					Value: "yyy",
				},
				{
					Key:   "aaa",
					Value: "bbb",
				},
			},
		}

		m := ConvertBsonM2D(input)
		output, _ := ConvertBsonD2M(m)
		assert.Equal(t, output, bson.M{
			"k1": "b",
			"k2": 12,
			"k3": []string{"1", "2", "3"},
			"k4": map[string]int{
				"hello": 1,
				"world": 2,
			},
			"r": bson.M{
				"xxx": "yyy",
				"aaa": "bbb",
			},
		}, "should be equal")
	}

	{
		fmt.Printf("TestConvertBson case %d.\n", nr)
		nr++

		input := bson.M{
			"k1": "b",
			"k2": 12,
			"k3": []string{"1", "2", "3"},
			"k4": map[string]int{
				"hello": 1,
				"world": 2,
			},
			"r": bson.D{
				{
					Key:   "xxx",
					Value: "yyy",
				},
				{
					Key:   "aaa",
					Value: "bbb",
				},
			},
		}

		m := ConvertBsonM2D(input)
		output, _ := ConvertBsonD2MExcept(m, nil)
		assert.Equal(t, output, bson.M{
			"k1": "b",
			"k2": 12,
			"k3": []string{"1", "2", "3"},
			"k4": map[string]int{
				"hello": 1,
				"world": 2,
			},
			"r": bson.M{
				"xxx": "yyy",
				"aaa": "bbb",
			},
		}, "should be equal")

		output, _ = ConvertBsonD2MExcept(m, map[string]struct{}{
			"r": {},
		})
		assert.Equal(t, output, input, "should be equal")
	}
}
