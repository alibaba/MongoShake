package filter

import (
	"fmt"
	"testing"
	"time"

	"github.com/getlantern/deepcopy"
	"github.com/mongodb/mongo-tools-common/json"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/x/mongo/driver/uuid"

	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
)

func TestNamespaceFilter(t *testing.T) {
	// test NamespaceFilter

	var nr int
	{
		fmt.Printf("TestNamespaceFilter case %d.\n", nr)
		nr++

		filter := NewNamespaceFilter([]string{"gogo.test1", "gogo.test2"}, nil)
		log := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "gogo.$cmd",
			},
		}
		assert.Equal(t, false, filter.Filter(log), "should be equal")
	}

	{
		fmt.Printf("TestNamespaceFilter case %d.\n", nr)
		nr++

		filter := NewNamespaceFilter(nil, nil)
		log := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "zz.mm",
				Operation: "i",
			},
		}

		assert.Equal(t, false, filter.Filter(log), "should be equal")
	}

	{
		fmt.Printf("TestNamespaceFilter case %d.\n", nr)
		nr++

		filter := NewNamespaceFilter(nil, []string{"zz", "cc.x"})
		log1 := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "zz.mm",
				Operation: "i",
			},
		}
		assert.Equal(t, true, filter.Filter(log1), "should be equal")

		log2 := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "cc.$cmd",
				Operation: "i",
			},
		}
		assert.Equal(t, false, filter.Filter(log2), "should be equal")

		log3 := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "cc.x",
				Operation: "i",
			},
		}
		assert.Equal(t, true, filter.Filter(log3), "should be equal")

		log4 := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "cc.y",
				Operation: "i",
			},
		}
		assert.Equal(t, false, filter.Filter(log4), "should be equal")
	}

	{
		fmt.Printf("TestNamespaceFilter case %d.\n", nr)
		nr++

		filter := NewNamespaceFilter(nil, nil)
		log := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "admin.$cmd",
				Operation: "c",
			},
		}

		assert.Equal(t, false, filter.Filter(log), "should be equal")
	}

	// applyOps
	{
		fmt.Printf("TestNamespaceFilter case %d.\n", nr)
		nr++

		filter := NewNamespaceFilter([]string{"zz"}, nil)
		log := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "admin.$cmd",
				Operation: "c",
				Object: bson.D{
					{
						Key: "applyOps",
						Value: []bson.D{
							{
								bson.E{Key: "op", Value: "i"},
								bson.E{Key: "ns", Value: "zz.mmm"},
								bson.E{Key: "o", Value: bson.D{
									bson.E{Key: "a", Value: 1},
									bson.E{Key: "_id", Value: "xxx"},
								}},
							},
							{
								bson.E{Key: "op", Value: "i"},
								bson.E{Key: "ns", Value: "zz.x"},
								bson.E{Key: "o", Value: bson.D{
									bson.E{Key: "xyz", Value: "ff"},
									bson.E{Key: "_id", Value: "yyy"},
								}},
							},
						},
					},
				},
			},
		}
		assert.Equal(t, false, filter.Filter(log), "should be equal")
		assert.Equal(t, 2, len(log.Object[0].Value.(bson.A)), "should be equal")

		log1 := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "admin.$cmd",
				Operation: "c",
				Object: bson.D{
					{
						Key: "applyOps",
						Value: []bson.D{
							{
								bson.E{Key: "op", Value: "i"},
								bson.E{Key: "ns", Value: "zl.mmm"},
								bson.E{Key: "o", Value: bson.D{
									bson.E{Key: "a", Value: 1},
									bson.E{Key: "_id", Value: "xxx"},
								}},
							},
							{
								bson.E{Key: "op", Value: "i"},
								bson.E{Key: "ns", Value: "zl.x"},
								bson.E{Key: "o", Value: bson.D{
									bson.E{Key: "xyz", Value: "ff"},
									bson.E{Key: "_id", Value: "yyy"},
								}},
							},
						},
					},
				},
			},
		}
		assert.Equal(t, true, filter.Filter(log1), "should be equal")
	}

	// applyOps with black list and rewrite 'o.applyOps' field
	{
		fmt.Printf("TestNamespaceFilter case %d.\n", nr)
		nr++

		filter := NewNamespaceFilter([]string{"zz.mmm"}, nil)
		log := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "admin.$cmd",
				Operation: "c",
				Object: bson.D{
					{
						Key: "applyOps",
						Value: []bson.D{
							{
								bson.E{Key: "op", Value: "i"},
								bson.E{Key: "ns", Value: "zz.mmm"},
								bson.E{Key: "o", Value: bson.D{
									bson.E{Key: "a", Value: 1},
									bson.E{Key: "_id", Value: "xxx"},
								}},
							},
							{
								bson.E{Key: "op", Value: "i"},
								bson.E{Key: "ns", Value: "zz.x"},
								bson.E{Key: "o", Value: bson.D{
									bson.E{Key: "xyz", Value: "ff"},
									bson.E{Key: "_id", Value: "yyy"},
								}},
							},
						},
					},
				},
			},
		}
		assert.Equal(t, false, filter.Filter(log), "should be equal")
		assert.Equal(t, 1, len(log.Object[0].Value.(bson.A)), "should be equal")
	}

	{
		fmt.Printf("TestNamespaceFilter case %d.\n", nr)
		nr++

		filter := NewNamespaceFilter(nil, []string{"ff"})
		log := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "admin.$cmd",
				Operation: "c",
				Object: bson.D{
					{
						Key: "applyOps",
						Value: []bson.D{
							{
								bson.E{Key: "op", Value: "i"},
								bson.E{Key: "ns", Value: "zz.mmm"},
								bson.E{Key: "o", Value: bson.D{
									bson.E{Key: "a", Value: 1},
									bson.E{Key: "_id", Value: "xxx"},
								}},
							},
							{
								bson.E{Key: "op", Value: "i"},
								bson.E{Key: "ns", Value: "ff.x"},
								bson.E{Key: "o", Value: bson.D{
									bson.E{Key: "xyz", Value: "ff"},
									bson.E{Key: "_id", Value: "yyy"},
								}},
							},
						},
					},
				},
			},
		}
		assert.Equal(t, false, filter.Filter(log), "should be equal")
		assert.Equal(t, 1, len(log.Object[0].Value.(bson.A)), "should be equal")
	}

	{
		fmt.Printf("TestNamespaceFilter case %d.\n", nr)
		nr++

		filter := NewNamespaceFilter([]string{"zz.mmm"}, nil)
		log := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "admin.$cmd",
				Operation: "c",
				Object: bson.D{
					{
						Key: "applyOps",
						Value: []any{
							bson.D{
								bson.E{Key: "op", Value: "i"},
								bson.E{Key: "ns", Value: "zz.mmm"},
								bson.E{Key: "o", Value: bson.D{
									bson.E{Key: "a", Value: 1},
									bson.E{Key: "_id", Value: "xxx"},
								}},
							},
							bson.D{
								bson.E{Key: "op", Value: "i"},
								bson.E{Key: "ns", Value: "zz.x"},
								bson.E{Key: "o", Value: bson.D{
									bson.E{Key: "xyz", Value: "ff"},
									bson.E{Key: "_id", Value: "yyy"},
								}},
							},
						},
					},
				},
			},
		}
		assert.Equal(t, false, filter.Filter(log), "should be equal")
		assert.Equal(t, 1, len(log.Object[0].Value.(bson.A)), "should be equal")
		assert.Equal(t, "zz.mmm", oplog.GetKey(log.Object[0].Value.(bson.A)[0].(bson.D), "ns"), "should be equal")
	}

	{
		fmt.Printf("TestNamespaceFilter case %d.\n", nr)
		nr++

		filter := NewNamespaceFilter([]string{"zz.mmm"}, nil)
		log := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "admin.$cmd",
				Operation: "c",
				Object: bson.D{
					{
						Key:   "applyOps",
						Value: []any{"illegal"},
					},
				},
			},
		}
		assert.NotPanics(t, func() {
			assert.Equal(t, false, filter.Filter(log), "should be equal")
		}, "should be equal")
		assert.Equal(t, []any{"illegal"}, log.Object[0].Value, "should be equal")
	}

	// applyOps with inner delete ops for 'config.system.sessions'
	// NamespaceFilter will not handle it, it's handled by AutologousFilter
	{
		fmt.Printf("TestNamespaceFilter case %d.\n", nr)
		nr++
		filter := NewNamespaceFilter(nil, nil)
		log := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "admin.$cmd",
				Operation: "c",
				Object: bson.D{
					{
						Key: "applyOps",
						Value: []bson.D{
							{
								bson.E{Key: "op", Value: "d"},
								bson.E{Key: "ns", Value: "config.system.sessions"},
								bson.E{Key: "ui", Value: primitive.Binary{
									Subtype: 4,
									Data:    []byte{0, 1, 3, 4, 5, 6, 7},
								}},
								bson.E{Key: "o", Value: bson.D{
									bson.E{Key: "_id", Value: "xxx"},
								}},
							},
							{
								bson.E{Key: "op", Value: "d"},
								bson.E{Key: "ns", Value: "config.system.sessions"},
								bson.E{Key: "ui", Value: primitive.Binary{
									Subtype: 4,
									Data:    []byte{0, 1, 3, 4, 5, 6, 7},
								}},
								bson.E{Key: "o", Value: bson.D{
									bson.E{Key: "_id", Value: "yyy"},
								}},
							},
						},
					},
				},
			},
		}
		assert.Equal(t, false, filter.Filter(log), "should be equal")
		assert.Equal(t, 2, len(log.Object[0].Value.(bson.A)), "should be equal")
	}
	// applyOps with {multiOpType:1}
	{
		fmt.Printf("TestNamespaceFilter case %d.\n", nr)
		nr++
		filter := NewNamespaceFilter([]string{"zz"}, nil)
		txnN := []int64{0, 1}
		term := []int64{1}
		multiOpType := []int{0, 1}
		log := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Operation: "c",
				Namespace: "admin.$cmd",
				TxnNumber: &txnN[0],
				Object: bson.D{
					bson.E{
						Key: "applyOps",
						Value: []bson.D{
							{
								bson.E{Key: "ns", Value: "zz.y"},
								bson.E{Key: "op", Value: "i"},
								bson.E{Key: "ui", Value: primitive.Binary{
									Subtype: 4,
									Data:    []byte{0, 1, 3, 4, 5, 6, 7},
								}},
								bson.E{Key: "o", Value: bson.M{
									"_id": int64(4),
									"x":   json.NumberLong(-20),
									"y":   json.NumberLong(5)}},
							},
							{
								bson.E{Key: "ns", Value: "zz.y"},
								bson.E{Key: "op", Value: "i"},
								bson.E{Key: "ui", Value: primitive.Binary{
									Subtype: 4,
									Data:    []byte{0, 1, 3, 4, 5, 6, 7},
								}},
								bson.E{Key: "o", Value: bson.D{
									bson.E{Key: "_id", Value: int64(5)},
									bson.E{Key: "x", Value: json.NumberLong(-30)},
									bson.E{Key: "y", Value: json.NumberLong(11)},
								}},
							},
						},
					},
				},
				Timestamp:   utils.TimeToTimestamp(time.Now().Unix()),
				Term:        &term[0],
				Version:     2,
				PrevOpTime:  utils.MarshalData(bson.D{{"ts", utils.Int64ToTimestamp(0)}}),
				MultiOpType: &multiOpType[1], // 1 for vectored insert oplog format
			},
		}
		assert.Equal(t, false, filter.Filter(log), "should be equal")
		assert.Equal(t, 2, len(log.Object[0].Value.(bson.A)), "should be equal")
		log1 := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Operation: "c",
				Namespace: "admin.$cmd",
				TxnNumber: &txnN[0],
				Object: bson.D{
					bson.E{
						Key: "applyOps",
						Value: []bson.D{
							{
								bson.E{Key: "ns", Value: "zl.y"},
								bson.E{Key: "op", Value: "i"},
								bson.E{Key: "ui", Value: primitive.Binary{
									Subtype: 4,
									Data:    []byte{0, 1, 3, 4, 5, 6, 7},
								}},
								bson.E{Key: "o", Value: bson.M{
									"_id": int64(4),
									"x":   json.NumberLong(-20),
									"y":   json.NumberLong(5)}},
							},
							{
								bson.E{Key: "ns", Value: "zl.y"},
								bson.E{Key: "op", Value: "i"},
								bson.E{Key: "ui", Value: primitive.Binary{
									Subtype: 4,
									Data:    []byte{0, 1, 3, 4, 5, 6, 7},
								}},
								bson.E{Key: "o", Value: bson.D{
									bson.E{Key: "_id", Value: int64(5)},
									bson.E{Key: "x", Value: json.NumberLong(-30)},
									bson.E{Key: "y", Value: json.NumberLong(11)},
								}},
							},
						},
					},
				},
				Timestamp:   utils.TimeToTimestamp(time.Now().Unix()),
				Term:        &term[0],
				Version:     2,
				PrevOpTime:  utils.MarshalData(bson.D{{"ts", utils.Int64ToTimestamp(0)}}),
				MultiOpType: &multiOpType[1], // 1 for vectored insert oplog format
			},
		}
		assert.Equal(t, true, filter.Filter(log1), "should be equal")

		log2 := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Operation: "c",
				Namespace: "admin.$cmd",
				TxnNumber: &txnN[0],
				Object: bson.D{
					bson.E{
						Key: "applyOps",
						Value: []any{
							bson.D{
								bson.E{Key: "ns", Value: "zl.y"},
								bson.E{Key: "op", Value: "i"},
								bson.E{Key: "ui", Value: primitive.Binary{
									Subtype: 4,
									Data:    []byte{0, 1, 3, 4, 5, 6, 7},
								}},
								bson.E{Key: "o", Value: bson.M{
									"_id": int64(4),
									"x":   json.NumberLong(-20),
									"y":   json.NumberLong(5)}},
							},
							bson.D{
								bson.E{Key: "ns", Value: "zl.y"},
								bson.E{Key: "op", Value: "i"},
								bson.E{Key: "ui", Value: primitive.Binary{
									Subtype: 4,
									Data:    []byte{0, 1, 3, 4, 5, 6, 7},
								}},
								bson.E{Key: "o", Value: bson.D{
									bson.E{Key: "_id", Value: int64(5)},
									bson.E{Key: "x", Value: json.NumberLong(-30)},
									bson.E{Key: "y", Value: json.NumberLong(11)},
								}},
							},
						},
					},
				},
				Timestamp:   utils.TimeToTimestamp(time.Now().Unix()),
				Term:        &term[0],
				Version:     2,
				PrevOpTime:  utils.MarshalData(bson.D{{"ts", utils.Int64ToTimestamp(0)}}),
				MultiOpType: &multiOpType[1], // 1 for vectored insert oplog format
			},
		}
		assert.Equal(t, true, filter.Filter(log2), "should be equal")
	}

	// applyOps with inner delete ops for 'config.system.preimages'
	// NamespaceFilter will not handle it, it's handled by AutologousFilter
	{
		fmt.Printf("TestNamespaceFilter case %d.\n", nr)
		nr++
		filter := NewNamespaceFilter(nil, nil)
		log := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "admin.$cmd",
				Operation: "c",
				Object: bson.D{
					{
						Key: "applyOps",
						Value: []bson.D{
							{
								bson.E{Key: "op", Value: "d"},
								bson.E{Key: "ns", Value: "config.system.preimages"},
								bson.E{Key: "ui", Value: primitive.Binary{
									Subtype: 4,
									Data:    []byte{0, 1, 3, 4, 5, 6, 7},
								}},
								bson.E{Key: "o", Value: bson.D{
									bson.E{Key: "_id", Value: bson.D{
										bson.E{Key: "nsUUID", Value: primitive.Binary{
											Subtype: 4,
											Data:    []byte{0, 1, 3, 4, 5, 6, 7},
										}},
										bson.E{Key: "ts", Value: primitive.Timestamp{T: 1758155667, I: 24}},
										bson.E{Key: "applyOpsIndex", Value: 0},
									}},
								}},
							},
							{
								bson.E{Key: "op", Value: "d"},
								bson.E{Key: "ns", Value: "config.system.preimages"},
								bson.E{Key: "ui", Value: primitive.Binary{
									Subtype: 4,
									Data:    []byte{0, 1, 3, 4, 5, 6, 7},
								}},
								bson.E{Key: "o", Value: bson.D{
									bson.E{Key: "_id", Value: bson.D{
										bson.E{Key: "nsUUID", Value: primitive.Binary{
											Subtype: 4,
											Data:    []byte{0, 1, 3, 4, 5, 6, 7},
										}},
										bson.E{Key: "ts", Value: primitive.Timestamp{T: 175855668, I: 2}},
										bson.E{Key: "applyOpsIndex", Value: 0},
									}},
								}},
							},
						},
					},
				},
			},
		}
		assert.Equal(t, false, filter.Filter(log), "should be equal")
		assert.Equal(t, 2, len(log.Object[0].Value.(bson.A)), "should be equal")
	}
}

func TestGidFilter(t *testing.T) {
	// test GidFilter

	var nr int
	{
		fmt.Printf("TestGidFilter case %d.\n", nr)
		nr++

		filter := NewGidFilter([]string{})
		log := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Gid: "1",
			},
		}
		assert.Equal(t, false, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{}
		assert.Equal(t, false, filter.Filter(log), "should be equal")
	}

	{
		fmt.Printf("TestGidFilter case %d.\n", nr)
		nr++

		filter := NewGidFilter([]string{"5", "6", "7"})
		log := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Gid: "1",
			},
		}
		assert.Equal(t, true, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{}
		assert.Equal(t, true, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Gid: "5",
			},
		}
		assert.Equal(t, false, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Gid: "8",
			},
		}
		assert.Equal(t, true, filter.Filter(log), "should be equal")
	}
}

func TestCmdFilter(t *testing.T) {
	// test CmdFilter

	var nr int
	newApplyOpsLog := func(ops []bson.D) *oplog.PartialLog {
		return &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "admin.$cmd",
				Operation: "c",
				Object: bson.D{
					{
						Key:   "applyOps",
						Value: ops,
					},
				},
			},
		}
	}

	{
		fmt.Printf("TestCmdFilter case %d.\n", nr)
		nr++

		filter := NewCmdFilter(nil)
		log := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Operation: "d",
			},
		}
		assert.Equal(t, false, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{}
		assert.Equal(t, false, filter.Filter(log), "should be equal")
	}

	{
		fmt.Printf("TestCmdFilter case %d.\n", nr)
		nr++

		filter := NewCmdFilter([]string{"d", "i"})

		log := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Operation: "d",
			},
		}
		assert.Equal(t, true, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Operation: "i",
			},
		}
		assert.Equal(t, true, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Operation: "u",
			},
		}
		assert.Equal(t, false, filter.Filter(log), "should be equal")
	}

	{
		fmt.Printf("TestCmdFilter case %d.\n", nr)
		nr++

		filter := NewCmdFilter([]string{"d"})
		log := newApplyOpsLog([]bson.D{
			{
				bson.E{Key: "op", Value: "d"},
				bson.E{Key: "ns", Value: "zz.mmm"},
				bson.E{Key: "o", Value: bson.D{
					bson.E{Key: "_id", Value: "xxx"},
				}},
			},
			{
				bson.E{Key: "op", Value: "d"},
				bson.E{Key: "ns", Value: "zz.mmm"},
				bson.E{Key: "o", Value: bson.D{
					bson.E{Key: "_id", Value: "yyy"},
				}},
			},
		})

		assert.Equal(t, true, filter.Filter(log), "should be equal")
		assert.Equal(t, 0, len(log.Object[0].Value.(bson.A)), "should be equal")
	}

	{
		fmt.Printf("TestCmdFilter case %d.\n", nr)
		nr++

		filter := NewCmdFilter([]string{"i"})
		log := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "admin.$cmd",
				Operation: "c",
				Object: bson.D{
					{
						Key: "applyOps",
						Value: []any{
							bson.D{
								bson.E{Key: "op", Value: "i"},
								bson.E{Key: "ns", Value: "zz.mmm"},
								bson.E{Key: "o", Value: bson.D{
									bson.E{Key: "_id", Value: "generic-slice-doc"},
								}},
							},
						},
					},
				},
			},
		}

		assert.Equal(t, true, filter.Filter(log), "should be equal")
		assert.Equal(t, 0, len(log.Object[0].Value.(bson.A)), "should be equal")
	}

	{
		fmt.Printf("TestCmdFilter case %d.\n", nr)
		nr++

		filter := NewCmdFilter([]string{"d"})
		log := newApplyOpsLog([]bson.D{
			{
				bson.E{Key: "op", Value: "d"},
				bson.E{Key: "ns", Value: "zz.mmm"},
				bson.E{Key: "o", Value: bson.D{
					bson.E{Key: "_id", Value: "xxx"},
				}},
			},
			{
				bson.E{Key: "op", Value: "i"},
				bson.E{Key: "ns", Value: "zz.mmm"},
				bson.E{Key: "o", Value: bson.D{
					bson.E{Key: "_id", Value: "zzz"},
				}},
			},
		})

		assert.Equal(t, false, filter.Filter(log), "should be equal")
		assert.Equal(t, 1, len(log.Object[0].Value.(bson.A)), "should be equal")
		assert.Equal(t, "i", oplog.GetKey(log.Object[0].Value.(bson.A)[0].(bson.D), "op"), "should be equal")
	}

	{
		fmt.Printf("TestCmdFilter case %d.\n", nr)
		nr++

		filter := NewCmdFilter([]string{"i"})
		log := newApplyOpsLog([]bson.D{
			{
				bson.E{Key: "op", Value: "i"},
				bson.E{Key: "ns", Value: "zz.mmm"},
				bson.E{Key: "o", Value: bson.D{
					bson.E{Key: "_id", Value: "xxx"},
				}},
			},
			{
				bson.E{Key: "op", Value: "i"},
				bson.E{Key: "ns", Value: "zz.mmm"},
				bson.E{Key: "o", Value: bson.D{
					bson.E{Key: "_id", Value: "zzz"},
				}},
			},
		})

		assert.Equal(t, true, filter.Filter(log), "should be equal")
		assert.Equal(t, 0, len(log.Object[0].Value.(bson.A)), "should be equal")
	}

	{
		fmt.Printf("TestCmdFilter case %d.\n", nr)
		nr++

		filter := NewCmdFilter([]string{"i"})
		log := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "admin.$cmd",
				Operation: "c",
				Object: bson.D{
					{
						Key: "applyOps",
						Value: primitive.A{
							bson.D{
								bson.E{Key: "op", Value: "i"},
								bson.E{Key: "ns", Value: "zz.mmm"},
								bson.E{Key: "o", Value: bson.D{
									bson.E{Key: "_id", Value: "primitive-array-doc"},
								}},
							},
						},
					},
				},
			},
		}

		assert.Equal(t, true, filter.Filter(log), "should be equal")
		assert.Equal(t, 0, len(log.Object[0].Value.(bson.A)), "should be equal")
	}

	{
		fmt.Printf("TestCmdFilter case %d.\n", nr)
		nr++

		filter := NewCmdFilter([]string{"c", "d"})
		log := newApplyOpsLog([]bson.D{
			{
				bson.E{Key: "op", Value: "d"},
				bson.E{Key: "ns", Value: "zz.mmm"},
				bson.E{Key: "o", Value: bson.D{
					bson.E{Key: "_id", Value: "xxx"},
				}},
			},
			{
				bson.E{Key: "op", Value: "i"},
				bson.E{Key: "ns", Value: "zz.mmm"},
				bson.E{Key: "o", Value: bson.D{
					bson.E{Key: "_id", Value: "zzz"},
				}},
			},
		})

		assert.Equal(t, true, filter.Filter(log), "should be equal")
		assert.Equal(t, 2, len(log.Object[0].Value.([]bson.D)), "should be equal")
	}

	{
		fmt.Printf("TestCmdFilter case %d.\n", nr)
		nr++

		filter := NewCmdFilter([]string{"u"})
		log := newApplyOpsLog([]bson.D{
			{
				bson.E{Key: "op", Value: "i"},
				bson.E{Key: "ns", Value: "zz.mmm"},
				bson.E{Key: "o", Value: bson.D{
					bson.E{Key: "_id", Value: "insert-doc"},
				}},
			},
			{
				bson.E{Key: "op", Value: "u"},
				bson.E{Key: "ns", Value: "zz.mmm"},
				bson.E{Key: "o2", Value: bson.D{
					bson.E{Key: "_id", Value: "update-doc"},
				}},
				bson.E{Key: "o", Value: bson.D{
					bson.E{Key: "$set", Value: bson.D{{Key: "x", Value: 1}}},
				}},
			},
			{
				bson.E{Key: "op", Value: "d"},
				bson.E{Key: "ns", Value: "zz.mmm"},
				bson.E{Key: "o", Value: bson.D{
					bson.E{Key: "_id", Value: "delete-doc"},
				}},
			},
		})

		assert.Equal(t, false, filter.Filter(log), "should be equal")
		assert.Equal(t, 2, len(log.Object[0].Value.(bson.A)), "should be equal")
		assert.Equal(t, "i", oplog.GetKey(log.Object[0].Value.(bson.A)[0].(bson.D), "op"), "should be equal")
		assert.Equal(t, "d", oplog.GetKey(log.Object[0].Value.(bson.A)[1].(bson.D), "op"), "should be equal")
	}

	{
		fmt.Printf("TestCmdFilter case %d.\n", nr)
		nr++

		filter := NewCmdFilter([]string{"i", "u"})
		log := newApplyOpsLog([]bson.D{
			{
				bson.E{Key: "op", Value: "i"},
				bson.E{Key: "ns", Value: "zz.mmm"},
				bson.E{Key: "o", Value: bson.D{
					bson.E{Key: "_id", Value: "insert-doc"},
				}},
			},
			{
				bson.E{Key: "op", Value: "u"},
				bson.E{Key: "ns", Value: "zz.mmm"},
				bson.E{Key: "o2", Value: bson.D{
					bson.E{Key: "_id", Value: "update-doc"},
				}},
				bson.E{Key: "o", Value: bson.D{
					bson.E{Key: "$set", Value: bson.D{{Key: "x", Value: 1}}},
				}},
			},
			{
				bson.E{Key: "op", Value: "d"},
				bson.E{Key: "ns", Value: "zz.mmm"},
				bson.E{Key: "o", Value: bson.D{
					bson.E{Key: "_id", Value: "delete-doc"},
				}},
			},
		})

		assert.Equal(t, false, filter.Filter(log), "should be equal")
		assert.Equal(t, 1, len(log.Object[0].Value.(bson.A)), "should be equal")
		assert.Equal(t, "d", oplog.GetKey(log.Object[0].Value.(bson.A)[0].(bson.D), "op"), "should be equal")
	}

	{
		fmt.Printf("TestCmdFilter case %d.\n", nr)
		nr++

		filter := NewCmdFilter([]string{"i"})
		log := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "admin.$cmd",
				Operation: "c",
				Object: bson.D{
					{
						Key:   "applyOps",
						Value: []any{"illegal"},
					},
				},
			},
		}

		assert.Equal(t, false, filter.Filter(log), "should be equal")
		assert.Equal(t, []any{"illegal"}, log.Object[0].Value, "should be equal")
	}
}

func TestAutologousFilter(t *testing.T) {
	// test AutologousFilter

	var nr int
	{
		fmt.Printf("TestAutologousFilter case %d.\n", nr)
		nr++

		filter := new(AutologousFilter)
		log := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "a.b",
			},
		}
		assert.Equal(t, false, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{}
		assert.Equal(t, false, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "mongoshake.x",
			},
		}
		assert.Equal(t, true, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "mongoshake_conflict.x",
			},
		}
		assert.Equal(t, true, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "local.x.z.y",
			},
		}
		assert.Equal(t, true, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "a.system.views",
			},
		}
		assert.Equal(t, false, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "a.system.view",
			},
		}
		assert.Equal(t, false, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "admin.x",
			},
		}
		assert.Equal(t, true, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "config.system.sessions",
			},
		}
		assert.Equal(t, true, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "config.cache.databases",
			},
		}
		assert.Equal(t, true, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "config.transactions",
			},
		}
		assert.Equal(t, true, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "a.system.profile",
			},
		}
		assert.Equal(t, true, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "config.system.preimages",
			},
		}
		assert.Equal(t, true, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "config.migrationCoordinators",
			},
		}
		assert.Equal(t, true, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "config.rangeDeletions",
			},
		}
		assert.Equal(t, true, filter.Filter(log), "should be equal")
	}

	rec := make(map[string]bool)
	_ = deepcopy.Copy(&rec, &NsShouldBeIgnore)

	{
		fmt.Printf("TestAutologousFilter case %d.\n", nr)
		nr++

		InitNs([]string{"admin", "system.views"})

		filter := new(AutologousFilter)
		log := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "a.b",
			},
		}
		assert.Equal(t, false, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{}
		assert.Equal(t, false, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "mongoshake.x",
			},
		}
		assert.Equal(t, true, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "local.x.z.y",
			},
		}
		assert.Equal(t, true, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "a.system.views",
			},
		}
		assert.Equal(t, false, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "a.system.view",
			},
		}
		assert.Equal(t, false, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "admin.x",
			},
		}
		assert.Equal(t, false, filter.Filter(log), "should be equal")
	}

	fmt.Println(rec, NsShouldBeIgnore)

	// test cmd
	{
		fmt.Printf("TestAutologousFilter case %d.\n", nr)
		nr++

		InitNs([]string{})
		filter := new(AutologousFilter)

		log := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "zz.$cmd",
				Operation: "c",
				Object: bson.D{
					{
						Key:   "drop",
						Value: "xxx",
					},
				},
			},
		}
		assert.Equal(t, false, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "zz.$cmd",
				Operation: "c",
				Object: bson.D{
					{
						Key:   "startIndexBuild",
						Value: "xxx",
					},
				},
			},
		}
		assert.Equal(t, true, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "zz.$cmd",
				Operation: "c",
				Object: bson.D{
					{
						Key:   "abortIndexBuild",
						Value: "xxx",
					},
				},
			},
		}
		assert.Equal(t, true, filter.Filter(log), "should be equal")
	}

	// test transaction
	{
		fmt.Printf("TestAutologousFilter case %d.\n", nr)
		nr++

		_ = deepcopy.Copy(&NsShouldBeIgnore, &rec)

		InitNs([]string{})
		filter := new(AutologousFilter)
		log := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "admin.$cmd",
				Operation: "c",
			},
		}
		assert.Equal(t, false, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "admin.xx",
				Operation: "c",
			},
		}
		assert.Equal(t, true, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "adminx",
				Operation: "d",
			},
		}
		assert.Equal(t, false, filter.Filter(log), "should be equal")

		// txn with config.system.sessions
		log = &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "admin.$cmd",
				Operation: "c",
				Object: bson.D{
					{
						Key: "applyOps",
						Value: bson.A{
							bson.D{
								bson.E{Key: "op", Value: "d"},
								bson.E{Key: "ns", Value: "config.system.sessions"},
								bson.E{Key: "o", Value: bson.D{
									bson.E{Key: "_id", Value: uuid.UUID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}},
								}},
							},
							bson.D{
								bson.E{Key: "op", Value: "d"},
								bson.E{Key: "ns", Value: "config.system.sessions"},
								bson.E{Key: "o", Value: bson.D{
									bson.E{Key: "_id", Value: "xxx"},
								}},
							},
						},
					},
				},
			},
		}
		ns, err := oplog.ExtractInnerNs(&log.ParsedLog)
		assert.NoError(t, err, "should be equal")
		assert.Equal(t, "config.system.sessions", ns, "should be equal")
		assert.Equal(t, true, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "admin.$cmd",
				Operation: "c",
				Object: bson.D{
					{
						Key: "applyOps",
						Value: bson.A{
							bson.D{
								bson.E{Key: "op", Value: "i"},
								bson.E{Key: "ns", Value: "zz.mmm"},
								bson.E{Key: "o", Value: bson.D{
									bson.E{Key: "_id", Value: "xxx"},
								}},
							},
							bson.D{
								bson.E{Key: "op", Value: "d"},
								bson.E{Key: "ns", Value: "config.system.sessions"},
								bson.E{Key: "o", Value: bson.D{
									bson.E{Key: "_id", Value: "xxx"},
								}},
							},
						},
					},
				},
			},
		}
		ns, err = oplog.ExtractInnerNs(&log.ParsedLog)
		assert.NoError(t, err, "should be equal")
		assert.Equal(t, "zz.mmm", ns, "should be equal")
		assert.Equal(t, false, filter.Filter(log), "should be equal")
	}
}

// only for print
func TestComputeHash(t *testing.T) {
	// test ComputeHash

	var nr int
	{
		fmt.Printf("TestComputeHash case %d.\n", nr)
		nr++

		v1 := ComputeHash(106402199)
		v2 := ComputeHash(106296614)
		fmt.Println(v1, v2)
	}
}
