package oplog

import (
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
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
					Timestamp: primitive.Timestamp{T: 0, I: 1},
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
					Timestamp: primitive.Timestamp{T: 0, I: 1},
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
					Timestamp: primitive.Timestamp{T: 0, I: 1},
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
					Timestamp: primitive.Timestamp{T: 0, I: 1},
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
		assert.Equal(t, primitive.Timestamp{T: 0, I: 1}, gather.Parsed.Timestamp, "should be equal")
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
			"ts": primitive.Timestamp{T: 0, I: 1},
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
				Timestamp: primitive.Timestamp{T: 0, I: 1},
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
				Value: primitive.Timestamp{T: 0, I: 1},
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

func TestDelteOplog(t *testing.T) {
	/*
		{
		    "op": "u",
		    "ns": "test.arr5",
		    "ui": UUID("8ba5a264-1475-4686-ae59-3c331a5655f3"),
		    "o": {
		        "$v": 2,
		        "diff": {
		            "d": {
		                "info": false
		            },
		            "i": {
		                "extra": "c"
		            },
		            "sarrname": {
		                "a": true,
		                "s0": {
		                    "u": {
		                        "count": 5,
		                        "nm": "c"
		                    },
		                    "i": {
		                        "extra": "ps"
		                    }
		                },
		                "s1": {
		                    "u": {
		                        "count": 6
		                    }
		                }
		            },
		            "snestobj": {
		                "sm1": {
		                    "d": {
		                        "n": false
		                    }
		                },
		                "sn1": {
		                    "i": {
		                        "1": 2
		                    }
		                }
		            }
		        }
		    },
		    "o2": {
		        "_id": ObjectId("642295e8bd4ab3cbd9632f7e")
		    },
		    "ts": Timestamp(1679988204,
		    1),
		    "t": NumberLong(1),
		    "v": NumberLong(2),
		    "wall": ISODate("2023-03-28T07:23:24.902Z")
		}
	*/
	object := bson.D{
		bson.E{Key: "$v", Value: 2},
		bson.E{
			Key: "diff",
			Value: bson.D{
				bson.E{Key: "d", Value: bson.E{Key: "info", Value: false}},
				bson.E{Key: "i", Value: bson.E{Key: "extra", Value: "c"}},
				bson.E{Key: "sarrname", Value: bson.D{
					bson.E{Key: "a", Value: true},
					bson.E{Key: "s0", Value: bson.D{
						bson.E{Key: "u", Value: bson.D{
							bson.E{Key: "count", Value: 5},
							bson.E{Key: "nm", Value: "c"},
						}},
						bson.E{Key: "i", Value: bson.D{
							bson.E{Key: "extra", Value: "ps"},
						}},
					}},
					bson.E{Key: "s1", Value: bson.D{
						bson.E{Key: "u", Value: bson.D{
							bson.E{Key: "count", Value: 6},
						}},
					}},
				}},
				bson.E{Key: "sarrname", Value: bson.D{
					bson.E{Key: "sm1", Value: bson.D{
						bson.E{Key: "d", Value: bson.D{
							bson.E{Key: "n", Value: false},
						}},
					}},
					bson.E{Key: "sn1", Value: bson.D{
						bson.E{Key: "i", Value: bson.D{
							bson.E{Key: "l", Value: 2},
						}},
					}},
				}},
				bson.E{Key: "sarrname", Value: bson.D{
					bson.E{Key: "a", Value: true},
					bson.E{Key: "u0", Value: bson.D{
						bson.E{Key: "count", Value: 10},
						bson.E{Key: "nm", Value: "e"},
					}},
				}},
			},
		},
	}

	resultInterface, err := DiffUpdateOplogToNormal(object)
	result := resultInterface.(bson.D)
	fmt.Printf("result:%v\n", result)
	assert.Equal(t, nil, err, "should be equal")
	assert.Equal(t, "$unset", result[0].Key, "should be equal")
	assert.Equal(t, bson.E{"info", false}, result[0].Value, "should be equal")
	assert.Equal(t, "$set", result[1].Key, "should be equal")
	assert.Equal(t, bson.E{"extra", "c"}, result[1].Value, "should be equal")
	assert.Equal(t, "$set", result[2].Key, "should be equal")
	assert.Equal(t, bson.D{
		{"arrname.0.count", 5},
		{"arrname.0.nm", "c"}}, result[2].Value, "should be equal")
	assert.Equal(t, "$set", result[3].Key, "should be equal")
	assert.Equal(t, bson.D{{"arrname.0.extra", "ps"}}, result[3].Value, "should be equal")
	assert.Equal(t, "$set", result[4].Key, "should be equal")
	assert.Equal(t, bson.D{{"arrname.1.count", 6}}, result[4].Value, "should be equal")
	assert.Equal(t, "$unset", result[5].Key, "should be equal")
	assert.Equal(t, bson.D{{"arrname.m1.n", false}}, result[5].Value, "should be equal")
	assert.Equal(t, "$set", result[6].Key, "should be equal")
	assert.Equal(t, bson.D{{"arrname.n1.l", 2}}, result[6].Value, "should be equal")
	assert.Equal(t, "$set", result[7].Key, "should be equal")
	assert.Equal(t, bson.D{{"arrname.0", bson.D{
		{"count", 10},
		{"nm", "e"},
	}}}, result[7].Value, "should be equal")

	object1 := bson.D{
		bson.E{Key: "$v", Value: 2},
		bson.E{
			Key: "diff",
			Value: bson.D{
				bson.E{Key: "sarrname", Value: bson.D{
					bson.E{Key: "a", Value: true},
					bson.E{Key: "l", Value: 1},
				}},
			},
		},
	}
	resultInterface1, err := DiffUpdateOplogToNormal(object1)
	result1 := resultInterface1.(mongo.Pipeline)
	fmt.Printf("result:%v\n", result1)

	assert.Equal(t, mongo.Pipeline{
		{{"$set", bson.D{
			{"arrname", bson.D{
				{"$slice", []interface{}{"$arrname", 1}},
			}},
		}}},
	}, result1, "should be equal")
}
