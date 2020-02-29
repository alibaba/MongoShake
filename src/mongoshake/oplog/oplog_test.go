package oplog

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vinllen/mgo/bson"
)

func TestRemoveFiled(t *testing.T) {
	// test RemoveFiled

	var nr int
	{
		fmt.Printf("TestMergeToGroups case %d.\n", nr)
		nr++

		input := bson.D{
			bson.DocElem{
				Name:  "w",
				Value: 1,
			},
			bson.DocElem{
				Name:  "$v",
				Value: 2,
			},
			bson.DocElem{
				Name:  "a",
				Value: 3,
			},
		}

		ret := RemoveFiled(input, "w")
		assert.Equal(t, bson.D{
			bson.DocElem{
				Name:  "$v",
				Value: 2,
			},
			bson.DocElem{
				Name:  "a",
				Value: 3,
			},
		}, ret, "should be equal")
	}

	{
		fmt.Printf("TestMergeToGroups case %d.\n", nr)
		nr++

		input := bson.D{
			bson.DocElem{
				Name:  "w",
				Value: 1,
			},
			bson.DocElem{
				Name:  "$v",
				Value: 2,
			},
			bson.DocElem{
				Name:  "a",
				Value: 3,
			},
		}

		ret := RemoveFiled(input, "$v")
		assert.Equal(t, bson.D{
			bson.DocElem{
				Name:  "w",
				Value: 1,
			},
			bson.DocElem{
				Name:  "a",
				Value: 3,
			},
		}, ret, "should be equal")
	}

	{
		fmt.Printf("TestMergeToGroups case %d.\n", nr)
		nr++

		input := bson.D{
			bson.DocElem{
				Name:  "w",
				Value: 1,
			},
			bson.DocElem{
				Name:  "$v",
				Value: 2,
			},
			bson.DocElem{
				Name:  "a",
				Value: 3,
			},
		}

		ret := RemoveFiled(input, "a")
		assert.Equal(t, bson.D{
			bson.DocElem{
				Name:  "w",
				Value: 1,
			},
			bson.DocElem{
				Name:  "$v",
				Value: 2,
			},
		}, ret, "should be equal")
	}

	{
		fmt.Printf("TestMergeToGroups case %d.\n", nr)
		nr++

		input := bson.D{
			bson.DocElem{
				Name:  "w",
				Value: 1,
			},
			bson.DocElem{
				Name:  "$v",
				Value: 2,
			},
			bson.DocElem{
				Name:  "a",
				Value: 3,
			},
		}

		ret := RemoveFiled(input, "aff")
		assert.Equal(t, input, ret, "should be equal")
	}

	{
		fmt.Printf("TestMergeToGroups case %d.\n", nr)
		nr++

		input := bson.D{
			bson.DocElem{
				Name:  "$v",
				Value: 1,
			},
			bson.DocElem{
				Name: "$set",
				Value: bson.D{
					bson.DocElem{
						Name:  "web_list.0.utime",
						Value: "2019-12-24 17:05:41",
					},
				},
			},
			bson.DocElem{
				Name: "$set",
				Value: bson.M{
					"web_list.0.utime": "2019-12-24 17:05:41",
				},
			},
			bson.DocElem{
				Name: "$set",
				Value: bson.D{
					bson.DocElem{
						Name:  "web_list.0.utime",
						Value: "2019-12-24 17:05:41",
					},
				},
			},
		}

		ret := RemoveFiled(input, "$v")
		assert.Equal(t, ret, bson.D{
			bson.DocElem{
				Name: "$set",
				Value: bson.D{
					bson.DocElem{
						Name:  "web_list.0.utime",
						Value: "2019-12-24 17:05:41",
					},
				},
			},
			bson.DocElem{
				Name: "$set",
				Value: bson.M{
					"web_list.0.utime": "2019-12-24 17:05:41",
				},
			},
			bson.DocElem{
				Name: "$set",
				Value: bson.D{
					bson.DocElem{
						Name:  "web_list.0.utime",
						Value: "2019-12-24 17:05:41",
					},
				},
			},
		}, "should be equal")
	}

	{
		fmt.Printf("TestMergeToGroups case %d.\n", nr)
		nr++

		input := bson.D{
			bson.DocElem{
				Name:  "$v",
				Value: 1,
			},
			bson.DocElem{
				Name: "$set",
				Value: bson.D{
					bson.DocElem{
						Name:  "web_list.0.utime",
						Value: "2019-12-24 17:05:41",
					},
				},
			},
		}

		ret := RemoveFiled(input, "$v2")
		assert.Equal(t, ret, input, "should be equal")
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
					Timestamp: bson.MongoTimestamp(1),
					Operation: "i",
					Namespace: "db1.c1",
					Object: bson.D{
						bson.DocElem{
							Name: "x",
							Value: 1,
						},
						bson.DocElem{
							Name: "y",
							Value: 2,
						},
					},
				},
			},
			{
				ParsedLog: ParsedLog{
					Timestamp: bson.MongoTimestamp(1),
					Operation: "i",
					Namespace: "db1.c2",
					Object: bson.D{
						bson.DocElem{
							Name: "x",
							Value: 10,
						},
						bson.DocElem{
							Name: "y",
							Value: 20,
						},
					},
				},
			},
			{
				ParsedLog: ParsedLog{
					Timestamp: bson.MongoTimestamp(1),
					Operation: "i",
					Namespace: "db2.c2",
					Object: bson.D{
						bson.DocElem{
							Name: "x",
							Value: 100,
						},
						bson.DocElem{
							Name: "y",
							Value: 200,
						},
					},
				},
			},
			{
				ParsedLog: ParsedLog{
					Timestamp: bson.MongoTimestamp(1),
					Operation: "u",
					Namespace: "db3.c3",
					Object: bson.D{
						bson.DocElem{
							Name: "x",
							Value: 100,
						},
						bson.DocElem{
							Name: "y",
							Value: 200,
						},
					},
					Query: bson.M{
						"x": 1,
						"y": 1,
					},
				},
			},
		}

		// hit the 4 in logsQ[0]
		gather, err := GatherApplyOps(input)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, len(gather.Raw) > 0, "should be equal")
		assert.Equal(t, bson.MongoTimestamp(1), gather.Parsed.Timestamp, "should be equal")
		assert.Equal(t, "admin.$cmd", gather.Parsed.Namespace, "should be equal")
		assert.Equal(t, "c", gather.Parsed.Operation, "should be equal")
		assert.Equal(t, bson.M(nil), gather.Parsed.Query, "should be equal")
		assert.Equal(t, "applyOps", gather.Parsed.Object[0].Name, "should be equal")
		assert.Equal(t, 4, len(gather.Parsed.Object[0].Value.([]bson.M)), "should be equal")
		assert.Equal(t, "i", gather.Parsed.Object[0].Value.([]bson.M)[0]["op"], "should be equal")
		assert.Equal(t, "db1.c1", gather.Parsed.Object[0].Value.([]bson.M)[0]["ns"], "should be equal")
		fmt.Println(gather.Parsed.Object[0])
	}
}
