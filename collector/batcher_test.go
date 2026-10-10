package collector

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	"github.com/alibaba/MongoShake/v2/collector/filter"
	sourceReader "github.com/alibaba/MongoShake/v2/collector/reader"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
)

func mockSyncer() *OplogSyncer {
	length := 3
	syncer := &OplogSyncer{
		PendingQueue:           make([]chan [][]byte, length),
		logsQueue:              make([]chan []*oplog.GenericOplog, length),
		hasher:                 &oplog.PrimaryKeyHasher{},
		fullSyncFinishPosition: utils.TimeToTimestamp(0), // disable in current test
		replMetric:             utils.NewMetric("test", "", 0),
	}
	for i := 0; i < length; i++ {
		syncer.logsQueue[i] = make(chan []*oplog.GenericOplog, 100)
	}
	return syncer
}

/*
 * return oplogs array with length=input length.
 * ddlGiven array marks the ddl.
 * noopGiven array marks the noop.
 * sameTsGiven array marks the index that ts is the same with before.
 */
func mockOplogs(length int, ddlGiven []int, noopGiven []int, txnGiven []int, startTs int64) []*oplog.GenericOplog {
	output := make([]*oplog.GenericOplog, length)
	ddlIndex := 0
	noopIndex := 0
	txnIndex := 0
	txnN := []int64{0, 1}
	for i := 0; i < length; i++ {
		op := "u"
		if noopIndex < len(noopGiven) && noopGiven[noopIndex] == i {
			op = "n"
			noopIndex++
		} else if ddlIndex < len(ddlGiven) && ddlGiven[ddlIndex] == i {
			op = "c"
			ddlIndex++
		}
		output[i] = &oplog.GenericOplog{
			Parsed: &oplog.PartialLog{
				ParsedLog: oplog.ParsedLog{
					Namespace: "a.b",
					Operation: op,
					Timestamp: utils.TimeToTimestamp(startTs + int64(i)),
					TxnNumber: &txnN[0],
					LSID: utils.MarshalData(bson.D{
						{"id", primitive.Binary{4, []byte{0, 1, 3, 4, 5, 6, 7}}},
						{"uid", []byte{9, 8, 7, 6, 5, 4, 3, 2, 1}},
					}),
				},
			},
		}
		if txnIndex < len(txnGiven) && txnGiven[txnIndex] == i {
			// single oplog transaction
			output[i] = &oplog.GenericOplog{
				Parsed: &oplog.PartialLog{
					ParsedLog: oplog.ParsedLog{
						Timestamp: utils.TimeToTimestamp(startTs + int64(i)),
						Operation: "c",
						Namespace: "admin.$cmd",
						Object: bson.D{
							bson.E{
								Key: "applyOps",
								Value: bson.A{
									bson.D{
										bson.E{"op", "i"},
										bson.E{"ns", "txntest.c1"},
										bson.E{"o", bson.D{
											bson.E{"_id", 0},
											bson.E{"x", startTs + int64(i)},
										}},
										bson.E{"ui", primitive.Binary{
											3,
											[]byte{0, 1, 3, 4, 5, 6, 7},
										}},
									},
									bson.D{
										bson.E{"op", "u"},
										bson.E{"ns", "txntest.c2"},
										bson.E{"o", bson.D{
											bson.E{"$set", bson.D{
												bson.E{"x", 1},
											}},
										}},
										bson.E{"o2", bson.D{
											{"_id", 0},
										}},
										bson.E{"ui", primitive.Binary{
											3,
											[]byte{0, 1, 3, 4, 5, 6, 7},
										}},
									},
									bson.D{
										bson.E{"op", "d"},
										bson.E{"ns", "txntest.c3"},
										bson.E{"o", bson.D{
											bson.E{"_id", 1},
										}},
										bson.E{"ui", primitive.Binary{
											3,
											[]byte{0, 1, 3, 4, 5, 6, 7},
										}},
									},
								},
							},
						},
						LSID: utils.MarshalData(
							bson.D{
								{"id", primitive.Binary{4, []byte{0, 1, 3, 4, 5, 6, byte(startTs + int64(i))}}},
								{"uid", []byte{8, 7, 6, 5, 4, 3, 2, 1, byte(startTs + int64(i))}},
							}),
						TxnNumber:  &txnN[0],
						PrevOpTime: utils.MarshalData(bson.D{{"ts", utils.Int64ToTimestamp(0)}}),
					},
				},
			}

			txnIndex++
		}

		// fmt.Println(output[i].Parsed.Timestamp, output[i].Parsed.Operation)
	}
	// fmt.Println("--------------")
	return output
}

// rawOplogRaw round-trips a GenericOplog through bson, the way the persister hands raw
// bytes to the deserializer in production.
func rawOplogRaw(log *oplog.GenericOplog) []byte {
	raw, err := bson.Marshal(&log.Parsed.ParsedLog)
	if err != nil {
		return nil
	}
	return raw
}

func mockTxnPartialOplogs(startTs int64, normalOplog bool) []*oplog.GenericOplog {

	incr := 0
	if normalOplog {
		incr = 1
	}
	output := make([]*oplog.GenericOplog, 3+incr)
	txnN := []int64{0, 1}

	output[0] = &oplog.GenericOplog{
		Parsed: &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Timestamp: utils.TimeToTimestamp(startTs + 0),
				Operation: "c",
				Namespace: "admin.$cmd",
				Object: bson.D{
					bson.E{
						Key: "applyOps",
						Value: bson.A{
							bson.D{
								bson.E{"op", "i"},
								bson.E{"ns", "txntest.c11"},
								bson.E{"o", bson.D{
									bson.E{"_id", 0},
									bson.E{"x", startTs + 0},
								}},
								bson.E{"ui", primitive.Binary{
									3,
									[]byte{0, 1, 3, 4, 5, 6, 7},
								}},
							},
							bson.D{
								bson.E{"op", "u"},
								bson.E{"ns", "txntest.c12"},
								bson.E{"o", bson.D{
									bson.E{"$set", bson.D{
										bson.E{"x", 1},
									}},
								}},
								bson.E{"o2", bson.D{
									{"_id", 0},
								}},
								bson.E{"ui", primitive.Binary{
									3,
									[]byte{0, 1, 3, 4, 5, 6, 7},
								}},
							},
							bson.D{
								bson.E{"op", "d"},
								bson.E{"ns", "txntest.c13"},
								bson.E{"o", bson.D{
									bson.E{"_id", 1},
								}},
								bson.E{"ui", primitive.Binary{
									3,
									[]byte{0, 1, 3, 4, 5, 6, 7},
								}},
							},
						},
					},
					bson.E{
						Key:   "partialTxn",
						Value: interface{}(true),
					},
				},
				LSID: utils.MarshalData(
					bson.D{
						{"id", primitive.Binary{Subtype: 4, Data: []byte{0, 1, 3, 4, 5, 6, byte(startTs)}}},
						{"uid", []byte{8, 7, 6, 5, 4, 3, 2, 1, byte(startTs)}},
					}),
				TxnNumber:  &txnN[0],
				PrevOpTime: utils.MarshalData(bson.D{{"ts", utils.Int64ToTimestamp(0)}}),
			},
		},
	}

	output[1] = &oplog.GenericOplog{
		Parsed: &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Timestamp: utils.TimeToTimestamp(startTs + 1),
				Operation: "c",
				Namespace: "admin.$cmd",
				Object: bson.D{
					bson.E{
						Key: "applyOps",
						Value: bson.A{
							bson.D{
								bson.E{"op", "i"},
								bson.E{"ns", "txntest.c21"},
								bson.E{"o", bson.D{
									bson.E{"_id", 0},
									bson.E{"x", startTs + 1},
								}},
								bson.E{"ui", primitive.Binary{
									3,
									[]byte{0, 1, 3, 4, 5, 6, 7},
								}},
							},
							bson.D{
								bson.E{"op", "u"},
								bson.E{"ns", "txntest.c22"},
								bson.E{"o", bson.D{
									bson.E{"$set", bson.D{
										bson.E{"x", 1},
									}},
								}},
								bson.E{"o2", bson.D{
									{"_id", 0},
								}},
								bson.E{"ui", primitive.Binary{
									3,
									[]byte{0, 1, 3, 4, 5, 6, 7},
								}},
							},
							bson.D{
								bson.E{"op", "d"},
								bson.E{"ns", "txntest.c23"},
								bson.E{"o", bson.D{
									bson.E{"_id", 1},
								}},
								bson.E{"ui", primitive.Binary{
									3,
									[]byte{0, 1, 3, 4, 5, 6, 7},
								}},
							},
						},
					},
					bson.E{
						Key:   "partialTxn",
						Value: interface{}(true),
					},
				},
				LSID: utils.MarshalData(
					bson.D{
						{"id", primitive.Binary{Subtype: 4, Data: []byte{0, 1, 3, 4, 5, 6, byte(startTs)}}},
						{"uid", []byte{8, 7, 6, 5, 4, 3, 2, 1, byte(startTs)}},
					}),
				TxnNumber:  &txnN[0],
				PrevOpTime: utils.MarshalData(bson.D{{"ts", utils.TimeToTimestamp(startTs + 0)}}),
			},
		},
	}

	if incr == 1 {
		output[2] = &oplog.GenericOplog{
			Parsed: &oplog.PartialLog{
				ParsedLog: oplog.ParsedLog{
					Namespace: "a.b",
					Operation: "i",
					Timestamp: utils.TimeToTimestamp(startTs + 2),
					Object: bson.D{
						bson.E{
							Key:   "_id",
							Value: interface{}(0),
						},
					},
				},
			},
		}
	}

	output[2+incr] = &oplog.GenericOplog{
		Parsed: &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Timestamp: utils.TimeToTimestamp(startTs + 2 + int64(incr)),
				Operation: "c",
				Namespace: "admin.$cmd",
				Object: bson.D{
					bson.E{
						Key: "applyOps",
						Value: bson.A{
							bson.D{
								bson.E{"op", "i"},
								bson.E{"ns", "txntest.c31"},
								bson.E{"o", bson.D{
									bson.E{"_id", 0},
									bson.E{"x", startTs + 2},
								}},
								bson.E{"ui", primitive.Binary{
									3,
									[]byte{0, 1, 3, 4, 5, 6, 7},
								}},
							},
							bson.D{
								bson.E{"op", "u"},
								bson.E{"ns", "txntest.c32"},
								bson.E{"o", bson.D{
									bson.E{"$set", bson.D{
										bson.E{"x", 1},
									}},
								}},
								bson.E{"o2", bson.D{
									{"_id", 0},
								}},
								bson.E{"ui", primitive.Binary{
									3,
									[]byte{0, 1, 3, 4, 5, 6, 7},
								}},
							},
							bson.D{
								bson.E{"op", "d"},
								bson.E{"ns", "txntest.c33"},
								bson.E{"o", bson.D{
									bson.E{"_id", 1},
								}},
								bson.E{"ui", primitive.Binary{
									3,
									[]byte{0, 1, 3, 4, 5, 6, 7},
								}},
							},
						},
					},
					bson.E{
						Key:   "count",
						Value: interface{}(9),
					},
				},
				LSID: utils.MarshalData(
					bson.D{
						{"id", primitive.Binary{Subtype: 4, Data: []byte{0, 1, 3, 4, 5, 6, byte(startTs)}}},
						{"uid", []byte{8, 7, 6, 5, 4, 3, 2, 1, byte(startTs)}},
					}),
				TxnNumber:  &txnN[0],
				PrevOpTime: utils.MarshalData(bson.D{{"ts", utils.TimeToTimestamp(startTs + 1)}}),
			},
		},
	}

	return output
}

func mockDisTxnOplogs(startTs int64, normalOplog bool, isCommit bool) []*oplog.GenericOplog {
	incr := 0
	if normalOplog {
		incr = 1
	}
	output := make([]*oplog.GenericOplog, 2+incr)
	txnN := []int64{0, 1}

	output[0] = &oplog.GenericOplog{
		Parsed: &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Timestamp: utils.TimeToTimestamp(startTs + 0),
				Operation: "c",
				Namespace: "admin.$cmd",
				Object: bson.D{
					bson.E{
						Key: "applyOps",
						Value: bson.A{
							bson.D{
								bson.E{"op", "i"},
								bson.E{"ns", "txntest.c11"},
								bson.E{"o", bson.D{
									bson.E{"_id", 0},
									bson.E{"x", startTs + 0},
								}},
								bson.E{"ui", primitive.Binary{
									3,
									[]byte{0, 1, 3, 4, 5, 6, 7},
								}},
							},
							bson.D{
								bson.E{"op", "u"},
								bson.E{"ns", "txntest.c12"},
								bson.E{"o", bson.D{
									bson.E{"$set", bson.D{
										bson.E{"x", 1},
									}},
								}},
								bson.E{"o2", bson.D{
									{"_id", 0},
								}},
								bson.E{"ui", primitive.Binary{
									3,
									[]byte{0, 1, 3, 4, 5, 6, 7},
								}},
							},
							bson.D{
								bson.E{"op", "d"},
								bson.E{"ns", "txntest.c13"},
								bson.E{"o", bson.D{
									bson.E{"_id", 1},
								}},
								bson.E{"ui", primitive.Binary{
									3,
									[]byte{0, 1, 3, 4, 5, 6, 7},
								}},
							},
						},
					},
					bson.E{
						Key:   "prepare",
						Value: interface{}(true),
					},
				},
				LSID: utils.MarshalData(
					bson.D{
						{"id", primitive.Binary{4, []byte{0, 1, 3, 4, 5, 6, byte(startTs)}}},
						{"uid", []byte{8, 7, 6, 5, 4, 3, 2, 1, byte(startTs)}},
					}),
				TxnNumber:  &txnN[0],
				PrevOpTime: utils.MarshalData(bson.D{{"ts", utils.Int64ToTimestamp(0)}}),
			},
		},
	}

	if incr == 1 {
		output[1] = &oplog.GenericOplog{
			Parsed: &oplog.PartialLog{
				ParsedLog: oplog.ParsedLog{
					Namespace: "a.b",
					Operation: "i",
					Timestamp: utils.TimeToTimestamp(startTs + 1),
					Object: bson.D{
						bson.E{
							Key:   "_id",
							Value: interface{}(0),
						},
					},
				},
			},
		}
	}

	var tmpO bson.D
	if isCommit {
		tmpO = bson.D{
			bson.E{
				Key:   "commitTransaction",
				Value: interface{}(1),
			},
			bson.E{
				Key:   "commitTimestamp",
				Value: interface{}(utils.TimeToTimestamp(startTs + 10)),
			},
		}
	} else {
		tmpO = bson.D{
			bson.E{
				Key:   "abortTransaction",
				Value: interface{}(1),
			},
		}
	}
	output[1+incr] = &oplog.GenericOplog{
		Parsed: &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Timestamp: utils.TimeToTimestamp(startTs + 1 + int64(incr)),
				Operation: "c",
				Namespace: "admin.$cmd",
				Object:    tmpO,
				LSID: utils.MarshalData(
					bson.D{
						{"id", primitive.Binary{4, []byte{0, 1, 3, 4, 5, 6, byte(startTs)}}},
						{"uid", []byte{8, 7, 6, 5, 4, 3, 2, 1, byte(startTs)}},
					}),
				TxnNumber:  &txnN[0],
				PrevOpTime: utils.MarshalData(bson.D{{"ts", utils.TimeToTimestamp(startTs + 0)}}),
			},
		},
	}

	return output
}

func mockDisTxnPartialOplogs(startTs int64, normalOplog bool, isCommit bool) []*oplog.GenericOplog {
	incr := 0
	if normalOplog {
		incr = 1
	}
	output := make([]*oplog.GenericOplog, 3+incr)
	txnN := []int64{0, 1}

	output[0] = &oplog.GenericOplog{
		Parsed: &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Timestamp: utils.TimeToTimestamp(startTs + 0),
				Operation: "c",
				Namespace: "admin.$cmd",
				Object: bson.D{
					bson.E{
						Key: "applyOps",
						Value: bson.A{
							bson.D{
								bson.E{"op", "i"},
								bson.E{"ns", "txntest.c11"},
								bson.E{"o", bson.D{
									bson.E{"_id", 0},
									bson.E{"x", startTs + 0},
								}},
								bson.E{"ui", primitive.Binary{
									3,
									[]byte{0, 1, 3, 4, 5, 6, 7},
								}},
							},
							bson.D{
								bson.E{"op", "u"},
								bson.E{"ns", "txntest.c12"},
								bson.E{"o", bson.D{
									bson.E{"$set", bson.D{
										bson.E{"x", 1},
									}},
								}},
								bson.E{"o2", bson.D{
									{"_id", 0},
								}},
								bson.E{"ui", primitive.Binary{
									3,
									[]byte{0, 1, 3, 4, 5, 6, 7},
								}},
							},
							bson.D{
								bson.E{"op", "d"},
								bson.E{"ns", "txntest.c13"},
								bson.E{"o", bson.D{
									bson.E{"_id", 1},
								}},
								bson.E{"ui", primitive.Binary{
									3,
									[]byte{0, 1, 3, 4, 5, 6, 7},
								}},
							},
						},
					},
					bson.E{
						Key:   "partialTxn",
						Value: interface{}(true),
					},
				},
				LSID: utils.MarshalData(
					bson.D{
						{"id", primitive.Binary{4, []byte{0, 1, 3, 4, 5, 6, byte(startTs)}}},
						{"uid", []byte{8, 7, 6, 5, 4, 3, 2, 1, byte(startTs)}},
					}),
				TxnNumber:  &txnN[0],
				PrevOpTime: utils.MarshalData(bson.D{{"ts", utils.Int64ToTimestamp(0)}}),
			},
		},
	}

	output[1] = &oplog.GenericOplog{
		Parsed: &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Timestamp: utils.TimeToTimestamp(startTs + 1),
				Operation: "c",
				Namespace: "admin.$cmd",
				Object: bson.D{
					bson.E{
						Key: "applyOps",
						Value: bson.A{
							bson.D{
								bson.E{"op", "i"},
								bson.E{"ns", "txntest.c21"},
								bson.E{"o", bson.D{
									bson.E{"_id", 0},
									bson.E{"x", startTs + 1},
								}},
								bson.E{"ui", primitive.Binary{
									3,
									[]byte{0, 1, 3, 4, 5, 6, 7},
								}},
							},
							bson.D{
								bson.E{"op", "u"},
								bson.E{"ns", "txntest.c22"},
								bson.E{"o", bson.D{
									bson.E{"$set", bson.D{
										bson.E{"x", 1},
									}},
								}},
								bson.E{"o2", bson.D{
									{"_id", 0},
								}},
								bson.E{"ui", primitive.Binary{
									3,
									[]byte{0, 1, 3, 4, 5, 6, 7},
								}},
							},
							bson.D{
								bson.E{"op", "d"},
								bson.E{"ns", "txntest.c23"},
								bson.E{"o", bson.D{
									bson.E{"_id", 1},
								}},
								bson.E{"ui", primitive.Binary{
									3,
									[]byte{0, 1, 3, 4, 5, 6, 7},
								}},
							},
						},
					},
					bson.E{
						Key:   "prepare",
						Value: interface{}(true),
					},
				},
				LSID: utils.MarshalData(
					bson.D{
						{"id", primitive.Binary{4, []byte{0, 1, 3, 4, 5, 6, byte(startTs)}}},
						{"uid", []byte{8, 7, 6, 5, 4, 3, 2, 1, byte(startTs)}},
					}),
				TxnNumber:  &txnN[0],
				PrevOpTime: utils.MarshalData(bson.D{{"ts", utils.TimeToTimestamp(startTs + 0)}}),
			},
		},
	}

	if incr == 1 {
		output[2] = &oplog.GenericOplog{
			Parsed: &oplog.PartialLog{
				ParsedLog: oplog.ParsedLog{
					Namespace: "a.b",
					Operation: "i",
					Timestamp: utils.TimeToTimestamp(startTs + 2),
					Object: bson.D{
						bson.E{
							Key:   "_id",
							Value: interface{}(0),
						},
					},
				},
			},
		}
	}

	var tmpO bson.D
	if isCommit {
		tmpO = bson.D{
			bson.E{
				Key:   "commitTransaction",
				Value: interface{}(1),
			},
			bson.E{
				Key:   "commitTimestamp",
				Value: interface{}(utils.TimeToTimestamp(startTs + 10)),
			},
		}
	} else {
		tmpO = bson.D{
			bson.E{
				Key:   "abortTransaction",
				Value: interface{}(1),
			},
		}
	}
	output[2+incr] = &oplog.GenericOplog{
		Parsed: &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Timestamp: utils.TimeToTimestamp(startTs + 2 + int64(incr)),
				Operation: "c",
				Namespace: "admin.$cmd",
				Object:    tmpO,
				LSID: utils.MarshalData(
					bson.D{
						{"id", primitive.Binary{4, []byte{0, 1, 3, 4, 5, 6, byte(startTs)}}},
						{"uid", []byte{8, 7, 6, 5, 4, 3, 2, 1, byte(startTs)}},
					}),
				TxnNumber:  &txnN[0],
				PrevOpTime: utils.MarshalData(bson.D{{"ts", utils.TimeToTimestamp(startTs + 1)}}),
			},
		},
	}

	return output
}

func TestBatchMoreApplyOpsInheritsSourceTime(t *testing.T) {
	utils.InitialLogger("", "", "debug", true, 1)

	syncer := mockSyncer()
	defer syncer.replMetric.Close()

	filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
	batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

	conf.Options.IncrSyncAdaptiveBatchingMaxSize = 100
	conf.Options.FilterDDLEnable = false

	sourceTime := time.Unix(1_700_000_400, 0).UTC()
	syncer.logsQueue[0] <- []*oplog.GenericOplog{
		{
			SourceTime: sourceTime,
			Parsed: &oplog.PartialLog{
				ParsedLog: oplog.ParsedLog{
					Timestamp: utils.TimeToTimestamp(100),
					Operation: "c",
					Namespace: "admin.$cmd",
					Object: bson.D{
						bson.E{
							Key: "applyOps",
							Value: bson.A{
								bson.D{
									bson.E{"op", "i"},
									bson.E{"ns", "txntest.c1"},
									bson.E{"o", bson.D{{"_id", 1}}},
								},
								bson.D{
									bson.E{"op", "i"},
									bson.E{"ns", "txntest.c2"},
									bson.E{"o", bson.D{{"_id", 2}}},
								},
								bson.D{
									bson.E{"op", "d"},
									bson.E{"ns", "txntest.c3"},
									bson.E{"o", bson.D{{"_id", 3}}},
								},
							},
						},
					},
				},
			},
		},
	}

	batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()

	assert.Equal(t, false, barrier, "should be equal")
	assert.Equal(t, false, allEmpty, "should be equal")
	assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
	for _, log := range batchedOplog[0] {
		assert.Equal(t, sourceTime, log.SourceTime, "should be equal")
	}
}

func TestHandleTransactionInheritsSourceTime(t *testing.T) {
	utils.InitialLogger("", "", "debug", true, 1)

	syncer := mockSyncer()
	defer syncer.replMetric.Close()

	filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
	batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

	logs := mockTxnPartialOplogs(100, false)
	sourceTimes := []time.Time{
		time.Unix(1_700_000_500, 0).UTC(),
		time.Unix(1_700_000_510, 0).UTC(),
		time.Unix(1_700_000_520, 0).UTC(),
	}
	for i := range logs {
		logs[i].SourceTime = sourceTimes[i]
	}

	for i, log := range logs {
		txnMeta, ok := batcher.isTransaction(log.Parsed)
		assert.Equal(t, true, ok, "should be equal")

		isRet, _, deliveredOps := batcher.handleTransaction(txnMeta, log)
		if i < len(logs)-1 {
			assert.Equal(t, false, isRet, "should be equal")
			assert.Equal(t, 0, len(deliveredOps), "should be equal")
			continue
		}

		assert.Equal(t, true, isRet, "should be equal")
		assert.Equal(t, 9, len(deliveredOps), "should be equal")
		for idx, op := range deliveredOps {
			assert.Equal(t, sourceTimes[idx/3], op.SourceTime, "should be equal")
		}
	}
}

func TestBatchMore(t *testing.T) {
	// test BatchMore

	utils.InitialLogger("", "", "debug", true, 1)

	// This branch's ordering contract is direct-only and needs incr_sync.barrier.ordering_enable,
	// and the expectations below pin it: a barrier round reports allEmpty=false so startBatcher's
	// `!allEmpty` guard cannot skip the barrier branch. The rounds where the contract does not apply
	// -- another tunnel, or the switch off -- are asserted separately, on the same round shape, in
	// the "barrier is the first element of mergeBatch" case.
	origTunnel, origOrderingEnable := conf.Options.Tunnel, conf.Options.IncrSyncBarrierOrderingEnable
	defer func() {
		conf.Options.Tunnel = origTunnel
		conf.Options.IncrSyncBarrierOrderingEnable = origOrderingEnable
	}()
	conf.Options.Tunnel = utils.VarTunnelDirect
	conf.Options.IncrSyncBarrierOrderingEnable = true

	var nr int
	// normal
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 100
		conf.Options.FilterDDLEnable = false

		syncer.logsQueue[0] <- mockOplogs(5, nil, nil, nil, 0)
		syncer.logsQueue[1] <- mockOplogs(6, nil, nil, nil, 100)
		syncer.logsQueue[2] <- mockOplogs(7, nil, nil, nil, 200)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 18, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(206), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		syncer.logsQueue[0] <- mockOplogs(1, nil, nil, nil, 300)
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(300), batcher.lastOplog.Parsed.Timestamp, "should be equal")
	}

	// split by `conf.Options.IncrSyncAdaptiveBatchingMaxSize`
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 10
		conf.Options.FilterDDLEnable = false

		syncer.logsQueue[0] <- mockOplogs(5, nil, nil, nil, 0)
		syncer.logsQueue[1] <- mockOplogs(6, nil, nil, nil, 100)
		syncer.logsQueue[2] <- mockOplogs(7, nil, nil, nil, 200)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 11, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(105), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 7, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(206), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		// test the last flush oplog
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(206), batcher.lastOplog.Parsed.Timestamp, "should be equal")
	}

	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 10
		conf.Options.FilterDDLEnable = false

		syncer.logsQueue[0] <- mockOplogs(5, []int{0, 1, 2, 3, 4}, nil, nil, 0)
		syncer.logsQueue[1] <- mockOplogs(6, []int{0, 1, 2, 3, 4, 5}, nil, nil, 100)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(105), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog, batcher.lastOplog, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(105), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog, batcher.lastOplog, "should be equal")
	}

	// has ddl
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 100
		conf.Options.FilterDDLEnable = true

		syncer.logsQueue[0] <- mockOplogs(5, nil, nil, nil, 0)
		syncer.logsQueue[1] <- mockOplogs(6, []int{2}, nil, nil, 100)
		syncer.logsQueue[2] <- mockOplogs(7, nil, nil, nil, 200)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 7, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 10, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 1, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(101), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 10, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(102), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 10, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(206), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(206), batcher.lastOplog.Parsed.Timestamp, "should be equal")
	}

	// barrier is the first element of mergeBatch: batchGroup ends up empty. With the ordering
	// guarantee in force the round still reports allEmpty=false, so startBatcher's `!allEmpty`
	// guard cannot skip the wait for the oplogs dispatched ahead of the barrier. Without it --
	// another tunnel, an unset tunnel, or direct with incr_sync.barrier.ordering_enable off --
	// allEmpty=true is allowed to skip that wait, and that must not change.
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		origTunnel, origOrderingEnable := conf.Options.Tunnel, conf.Options.IncrSyncBarrierOrderingEnable
		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 100
		conf.Options.FilterDDLEnable = true

		for _, tc := range []struct {
			tunnel   string
			enable   bool
			allEmpty bool
		}{
			{utils.VarTunnelDirect, true, false},
			{utils.VarTunnelDirect, false, true}, // direct alone is not enough: the switch defaults to off
			{"", true, true},                     // an unset tunnel counts as non-direct, so it keeps the original verdict
			{utils.VarTunnelKafka, true, true},
		} {
			conf.Options.Tunnel = tc.tunnel
			conf.Options.IncrSyncBarrierOrderingEnable = tc.enable

			desc := fmt.Sprintf("tunnel %q, ordering_enable %v", tc.tunnel, tc.enable)

			syncer := mockSyncer()
			filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
			batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

			// ts 0 is the ddl, so the barrier is mergeBatch[0] and nothing reaches batchGroup
			syncer.logsQueue[0] <- mockOplogs(5, []int{0}, nil, nil, 0)
			syncer.logsQueue[1] <- mockOplogs(6, nil, nil, nil, 100)
			syncer.logsQueue[2] <- mockOplogs(7, nil, nil, nil, 200)

			batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
			assert.Equal(t, true, barrier, "%s should report a barrier", desc)
			assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
			assert.Equal(t, tc.allEmpty, allEmpty, "%s should report allEmpty=%v", desc, tc.allEmpty)
			assert.Equal(t, 1, len(batcher.barrierOplogs), "should be equal")
			assert.Equal(t, 17, len(batcher.remainLogs), "should be equal")
		}

		conf.Options.Tunnel = origTunnel
		conf.Options.IncrSyncBarrierOrderingEnable = origOrderingEnable
	}

	// has several ddl
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 100
		conf.Options.FilterDDLEnable = true

		syncer.logsQueue[0] <- mockOplogs(5, []int{3}, nil, nil, 0)
		syncer.logsQueue[1] <- mockOplogs(6, []int{2}, nil, nil, 100)
		syncer.logsQueue[2] <- mockOplogs(7, []int{4, 5}, nil, nil, 200)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 14, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 1, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(2), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		// 3 in logsQ[0]
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 14, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(3), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 10, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 1, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(101), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		// 2 in logsQ[1]
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 10, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(102), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 7, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 2, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 1, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(203), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		// 4 in logsQ[2]
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 2, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(204), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		// 5 in logsQ[2]
		//
		// A round that carries a barrier always reports allEmpty=false, even when batchGroup comes
		// out empty: the barrier is parked in barrierOplogs to be dispatched on the next round, so
		// whether any DML happened to precede it used to be what decided the flag. startBatcher's
		// `!allEmpty` guard reads that flag, so the rounds where it came out empty skipped the
		// barrier wait and the forced checkpoint with it. Every assertion below that pairs
		// barrier=true with an empty batchGroup pins the new contract.
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 1, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 1, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(204), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 1, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(205), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		// test the last flush oplog
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(206), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(206), batcher.lastOplog.Parsed.Timestamp, "should be equal")
	}

	// first one and last one are ddl
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 100
		conf.Options.FilterDDLEnable = true

		syncer.logsQueue[0] <- mockOplogs(5, []int{0}, nil, nil, 0)
		syncer.logsQueue[1] <- mockOplogs(6, nil, nil, nil, 100)
		syncer.logsQueue[2] <- mockOplogs(7, []int{6}, nil, nil, 200)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 17, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 1, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog, batcher.lastOplog, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 17, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(0), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 16, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 1, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(205), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(206), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		// push again
		syncer.logsQueue[0] <- mockOplogs(80, nil, nil, nil, 300)

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 80, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(379), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		// test the last flush oplog
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(379), batcher.lastOplog.Parsed.Timestamp, "should be equal")
	}

	// all ddl
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 100
		conf.Options.FilterDDLEnable = true

		syncer.logsQueue[0] <- mockOplogs(3, []int{0, 1, 2}, nil, nil, 0)
		syncer.logsQueue[1] <- mockOplogs(1, []int{0}, nil, nil, 100)
		syncer.logsQueue[2] <- mockOplogs(1, []int{0}, nil, nil, 200)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 4, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 1, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog, batcher.lastOplog, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 4, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(0), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 3, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 1, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(0), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 3, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(1), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 2, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 1, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(1), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 2, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(2), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		// push again
		syncer.logsQueue[0] <- mockOplogs(80, nil, nil, nil, 300)

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 1, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 1, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(2), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 1, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(100), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 1, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(100), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(200), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 80, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(379), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(379), batcher.lastOplog.Parsed.Timestamp, "should be equal")
	}

	// the edge of `IncrSyncAdaptiveBatchingMaxSize` is ddl
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 8
		conf.Options.FilterDDLEnable = true

		syncer.logsQueue[0] <- mockOplogs(5, nil, nil, nil, 0)
		syncer.logsQueue[1] <- mockOplogs(6, []int{5}, nil, nil, 100) // last is ddl
		syncer.logsQueue[2] <- mockOplogs(7, []int{3}, nil, nil, 200)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 10, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 1, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(104), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(105), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 3, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 1, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(202), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 3, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(203), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(206), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(206), batcher.lastOplog.Parsed.Timestamp, "should be equal")
	}

	// test all transaction(only 1)
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 100
		conf.Options.FilterDDLEnable = true

		syncer.logsQueue[0] <- mockOplogs(1, nil, nil, []int{0}, 100)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 3, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, fakeOplog, batcher.lastOplog, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(100), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(100), batcher.lastOplog.Parsed.Timestamp, "should be equal")
	}

	// test transaction(head,middle,end) and normal oplog
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 100
		conf.Options.FilterDDLEnable = true

		syncer.logsQueue[0] <- mockOplogs(5, nil, nil, []int{0, 4}, 10)
		syncer.logsQueue[1] <- mockOplogs(6, nil, nil, nil, 100)
		// at the end of queue
		syncer.logsQueue[2] <- mockOplogs(7, nil, nil, []int{5, 6}, 200)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 17, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 3, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, fakeOplog, batcher.lastOplog, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 17, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(10), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		// inject more
		syncer.logsQueue[0] <- mockOplogs(5, nil, nil, []int{1}, 300)

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 13, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 3, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(13), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 13, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(14), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 11, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 1, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 3, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(204), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 1, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(205), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 3, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(205), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(206), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 3, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 3, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(300), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 3, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(301), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(304), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(304), batcher.lastOplog.Parsed.Timestamp, "should be equal")
	}

	// test large unprepared transaction(applyOps + partialTxn)
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 100
		conf.Options.FilterDDLEnable = true

		syncer.logsQueue[0] <- mockOplogs(9, nil, nil, []int{2, 6}, 0)
		syncer.logsQueue[1] <- mockTxnPartialOplogs(100, false)
		syncer.logsQueue[2] <- mockOplogs(5, nil, nil, nil, 200)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 2, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 14, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 3, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(1), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 14, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(2), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 10, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 3, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(5), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 10, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(6), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 2, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 5, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 9, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(8), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 9, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 5, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(102), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 5, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(204), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(204), batcher.lastOplog.Parsed.Timestamp, "should be equal")
	}

	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 100
		conf.Options.FilterDDLEnable = true

		syncer.logsQueue[0] <- mockOplogs(9, nil, nil, []int{2, 6}, 0)
		syncer.logsQueue[1] <- mockTxnPartialOplogs(100, false)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 2, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 9, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 3, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(1), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 9, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(2), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 5, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 3, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(5), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 5, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(6), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 2, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 9, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(8), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 9, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(102), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(102), batcher.lastOplog.Parsed.Timestamp, "should be equal")
	}

	// test large unprepared transaction(applyOps + partialTxn)
	// normal oplog between partialTxn Transactions, normal oplog will execute first.
	// transaction oplog buffer in batcher.txnBuffer
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 100
		conf.Options.FilterDDLEnable = true

		syncer.logsQueue[0] <- mockOplogs(9, nil, nil, []int{2, 6}, 0)
		syncer.logsQueue[1] <- mockTxnPartialOplogs(100, true)
		syncer.logsQueue[2] <- mockOplogs(5, nil, nil, nil, 200)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 2, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 15, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 3, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(1), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 15, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(2), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 11, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 3, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(5), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 11, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(6), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 5, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 9, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(102), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 9, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 5, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(103), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 5, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(204), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(204), batcher.lastOplog.Parsed.Timestamp, "should be equal")
	}

	// distributed transaction(prepared, committed)
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 100
		conf.Options.FilterDDLEnable = true

		syncer.logsQueue[0] <- mockOplogs(5, nil, nil, nil, 0)
		syncer.logsQueue[1] <- mockDisTxnOplogs(100, false, true)
		syncer.logsQueue[2] <- mockOplogs(5, nil, nil, nil, 200)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 5, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 5, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 3, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(4), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 5, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(100), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 5, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(204), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(204), batcher.lastOplog.Parsed.Timestamp, "should be equal")
	}

	// distributed transaction(prepared, committed), have normal oplog between transaction
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 100
		conf.Options.FilterDDLEnable = true

		syncer.logsQueue[0] <- mockOplogs(5, nil, nil, nil, 0)
		syncer.logsQueue[1] <- mockDisTxnOplogs(100, true, true)
		syncer.logsQueue[2] <- mockOplogs(5, nil, nil, nil, 200)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 6, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 5, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 3, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(101), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 5, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(101), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 5, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(204), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(204), batcher.lastOplog.Parsed.Timestamp, "should be equal")
	}

	// distributed transaction(prepared, abroted)
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 100
		conf.Options.FilterDDLEnable = true

		syncer.logsQueue[0] <- mockOplogs(5, nil, nil, nil, 0)
		syncer.logsQueue[1] <- mockDisTxnOplogs(100, false, false)
		syncer.logsQueue[2] <- mockOplogs(5, nil, nil, nil, 200)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 10, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(101), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(204), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(101), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(204), batcher.lastOplog.Parsed.Timestamp, "should be equal")
	}

	// distributed transaction(prepared + partial, committed)
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 100
		conf.Options.FilterDDLEnable = true

		syncer.logsQueue[0] <- mockOplogs(5, nil, nil, nil, 0)
		syncer.logsQueue[1] <- mockDisTxnPartialOplogs(100, false, true)
		syncer.logsQueue[2] <- mockOplogs(5, nil, nil, nil, 200)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 5, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 5, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 6, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(4), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 6, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 5, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(101), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 5, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(204), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(204), batcher.lastOplog.Parsed.Timestamp, "should be equal")
	}

	// distributed transaction(prepared + partial, committed), have normal oplog between transaction
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 100
		conf.Options.FilterDDLEnable = true

		syncer.logsQueue[0] <- mockOplogs(5, nil, nil, nil, 0)
		syncer.logsQueue[1] <- mockDisTxnPartialOplogs(100, true, true)
		syncer.logsQueue[2] <- mockOplogs(5, nil, nil, nil, 200)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 6, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 5, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 6, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(102), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 6, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 5, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(102), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 5, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(204), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(204), batcher.lastOplog.Parsed.Timestamp, "should be equal")
	}

	// distributed transaction(prepared + partial, abroted)
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 100
		conf.Options.FilterDDLEnable = true

		syncer.logsQueue[0] <- mockOplogs(5, nil, nil, nil, 0)
		syncer.logsQueue[1] <- mockDisTxnPartialOplogs(100, true, false)
		syncer.logsQueue[2] <- mockOplogs(5, nil, nil, nil, 200)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 11, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(103), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(204), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(103), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(204), batcher.lastOplog.Parsed.Timestamp, "should be equal")
	}

	// test transaction and filter(noop) mix
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 100
		conf.Options.FilterDDLEnable = true

		syncer.logsQueue[0] <- mockOplogs(9, nil, []int{3, 4, 7, 8}, []int{2, 6}, 0)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 2, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 6, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 3, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(1), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 6, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(2), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 2, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 3, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(4), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(5), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 2, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(4), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(6), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(8), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(6), batcher.lastOplog.Parsed.Timestamp, "should be equal")
	}

	// test transaction, DDL, filter(noop) mix
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 100
		conf.Options.FilterDDLEnable = true

		syncer.logsQueue[0] <- mockOplogs(9, []int{0, 7}, []int{3, 4}, []int{2, 6}, 0)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 8, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 1, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, fakeOplog, batcher.lastOplog, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 8, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(0), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 6, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 3, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(1), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 6, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(2), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 2, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 3, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(4), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(5), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 2, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(4), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(6), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 1, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 1, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(4), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(6), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 1, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(4), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(7), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(4), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(8), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(4), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(8), batcher.lastOplog.Parsed.Timestamp, "should be equal")
	}

	// complicate test!
	// test transaction, DDL, filter(noop) mix.
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 10
		conf.Options.FilterDDLEnable = true

		syncer.logsQueue[0] <- mockOplogs(6, []int{5}, []int{0, 1, 2}, []int{4}, 0)
		syncer.logsQueue[1] <- mockOplogs(7, []int{0, 1}, []int{2, 3, 4, 5, 6}, nil, 100)
		syncer.logsQueue[2] <- mockOplogs(8, []int{0}, []int{1, 7}, []int{4, 5, 6}, 200)

		// hit the 4 in logsQ[0]
		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 8, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 3, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(2), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(3), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		// after 4 in logsQ[0]
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 8, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(2), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(4), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 7, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 1, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(2), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(4), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 7, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(2), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(5), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 6, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 1, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(2), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(5), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 6, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(2), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(100), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 5, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 1, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(2), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(100), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 5, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(2), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(101), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(106), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(101), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 7, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 1, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(106), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(101), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 7, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(106), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(200), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 2, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 3, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 3, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(201), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(203), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 3, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(201), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(204), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 2, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 3, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(201), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(204), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 2, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(201), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(205), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 1, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 3, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(201), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(205), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 1, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(201), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(206), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(207), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(206), batcher.lastOplog.Parsed.Timestamp, "should be equal")
	}

	// test simple case which run failed in sync test
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.IncrSyncAdaptiveBatchingMaxSize = 10
		conf.Options.FilterDDLEnable = true

		syncer.logsQueue[0] <- mockOplogs(4, []int{2}, []int{0, 1}, nil, 0)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 1, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 1, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(1), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, fakeOplog, batcher.lastOplog, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 1, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(1), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(2), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(1), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(3), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(1), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(3), batcher.lastOplog.Parsed.Timestamp, "should be equal")
	}

	// test DDL on the last
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.FilterDDLEnable = true

		syncer.logsQueue[0] <- mockOplogs(6, []int{5}, nil, nil, 0)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 5, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 1, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(4), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(5), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(5), batcher.lastOplog.Parsed.Timestamp, "should be equal")
	}

	// test transaction on the last
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.FilterDDLEnable = true

		syncer.logsQueue[0] <- mockOplogs(6, nil, nil, []int{1, 2, 3, 4, 5}, 0)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 1, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 4, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 3, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(0), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 4, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(1), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 3, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 3, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(1), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 3, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(2), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 2, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 3, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(2), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 2, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(3), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 1, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 3, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(3), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 1, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(4), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 3, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(4), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 3, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(5), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(5), batcher.lastOplog.Parsed.Timestamp, "should be equal")
	}

	// test empty
	{
		fmt.Printf("TestBatchMore case %d.\n", nr)
		nr++

		syncer := mockSyncer()
		filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
		batcher := NewBatcher(syncer, filterList, syncer, []*Worker{new(Worker)})

		conf.Options.FilterDDLEnable = true

		// syncer.logsQueue[0] <- mockOplogs(6, nil, nil, []int{1, 2, 3, 4, 5}, 0)

		batchedOplog, barrier, allEmpty, _ := batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, fakeOplog.Parsed, batcher.lastFilterOplog, "should be equal")
		assert.Equal(t, fakeOplog, batcher.lastOplog, "should be equal")

		// all filtered
		syncer.logsQueue[0] <- mockOplogs(3, nil, []int{0, 1, 2}, nil, 0)
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(2), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, fakeOplog, batcher.lastOplog, "should be equal")

		// inject one
		syncer.logsQueue[1] <- mockOplogs(5, nil, nil, nil, 100)
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 5, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, false, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(2), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(104), batcher.lastOplog.Parsed.Timestamp, "should be equal")

		// get the last one
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, false, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
		assert.Equal(t, 0, len(batcher.remainLogs), "should be equal")
		assert.Equal(t, 0, len(batcher.barrierOplogs), "should be equal")
		assert.Equal(t, 0, batcher.txnBuffer.Size(), "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(2), batcher.lastFilterOplog.Timestamp, "should be equal")
		assert.Equal(t, utils.TimeToTimestamp(104), batcher.lastOplog.Parsed.Timestamp, "should be equal")
	}
}

/*
 * Regression for the stall a real run hit: the pipeline stopped with the ingest gate closed and the
 * whole sync waiting forever.
 *
 * Buffer has exactly one writer -- the poll goroutine -- and the gate keeps that goroutine from
 * fetching anything that could trip the buffer size threshold again. So an oplog left in Buffer
 * when the gate closes is never dispatched, rawInFlight never returns to zero (it counts an oplog
 * from the moment it entered Buffer), and the quiesce wait spins on a batch nobody is left to push.
 *
 * The flush therefore belongs to Buffer's owner: the poll goroutine pushes it forward on its way to
 * the gate. This test drives that path: one op is fetched and buffered while the gate is open, the
 * gate closes, and the next() that parks on it has to push the op forward first.
 */
func TestNextFlushesBufferBeforeParkingOnGate(t *testing.T) {
	utils.InitialLogger("", "", "debug", true, 1)

	origCapacity := conf.Options.IncrSyncFetcherBufferCapacity
	defer func() {
		conf.Options.IncrSyncFetcherBufferCapacity = origCapacity
	}()
	// keep the buffer from dispatching on size, so the op really is parked in Buffer
	conf.Options.IncrSyncFetcherBufferCapacity = 100

	syncer := mockSyncer()
	syncer.PendingQueue[0] = make(chan [][]byte, 10)
	syncer.batcher = &Batcher{syncer: syncer}
	syncer.persister = NewPersister("test", syncer)

	dml := mockOplogs(1, nil, nil, nil, 50)
	raw := rawOplogRaw(dml[0])
	if raw == nil {
		t.Fatalf("marshal raw dml failed")
	}
	syncer.reader = &stubReader{ops: [][]byte{raw}}

	// gate open: next() fetches the op and buffers it
	assert.Equal(t, true, syncer.next(), "should be equal")
	assert.Equal(t, 1, len(syncer.persister.Buffer), "the op should be parked in the persister buffer")
	assert.Equal(t, int64(1), atomic.LoadInt64(&syncer.rawInFlight), "the buffered op should be counted in flight")
	assert.Equal(t, 0, len(syncer.PendingQueue[0]), "nothing should have reached the pending queue yet")

	// barrier round: the gate closes, so the next() call must flush before it parks
	syncer.batcher.setIngestGate()
	parked := make(chan struct{})
	go func() {
		defer close(parked)
		syncer.next()
	}()

	select {
	case batch := <-syncer.PendingQueue[0]:
		assert.Equal(t, 1, len(batch), "the buffered op should have been pushed forward")
		assert.Equal(t, raw, batch[0], "should be equal")
	case <-time.After(nextGateWaitTimeout):
		t.Fatal("next() parked on the closed gate without flushing the persister buffer: " +
			"the op would keep rawInFlight > 0 and stall the quiesce wait forever")
	}
	// the op is now on its way to a logsQueue, so only the deserializer releases its count
	assert.Equal(t, int64(1), atomic.LoadInt64(&syncer.rawInFlight), "the op should still be in flight")

	syncer.batcher.clearIngestGate()
	select {
	case <-parked:
	case <-time.After(nextGateWaitTimeout):
		t.Fatal("next() did not resume after the gate was released")
	}
	// read Buffer only after the parked next() has returned (the channel close orders the two):
	// reading it right after the queue receive would race with the reset dispatchBuffer does
	// after its send, and the last Inject(nil) of the resumed call dispatches nothing
	assert.Equal(t, 0, len(syncer.persister.Buffer), "the buffer should be empty after the flush")
}

// nextGateWaitTimeout has to outlast one park-loop yield: next() sleeps barrierQuiescePollIntervalMs
// between gate checks, so a released gate is only noticed on the following round.
const nextGateWaitTimeout = 2 * SyncerFetchErrorRetryMs * time.Millisecond

// entranceWaitTimeout is the budget for the entrance's quiesce wait, which needs two deserializer
// and batcher round trips plus the confirm gap.
const entranceWaitTimeout = 5 * time.Second

// TestIsEntryBarrierClassifiesDDLOnly pins the entrance's classification: a DDL is gated, and the
// cheap screen in front of the parse may only skip oplogs that provably cannot be one.
func TestIsEntryBarrierClassifiesDDLOnly(t *testing.T) {
	utils.InitialLogger("", "", "debug", true, 1)

	origTunnel := conf.Options.Tunnel
	origOrderingEnable := conf.Options.IncrSyncBarrierOrderingEnable
	origDDL := conf.Options.FilterDDLEnable
	origFetchMethod := conf.Options.IncrSyncMongoFetchMethod
	defer func() {
		conf.Options.Tunnel = origTunnel
		conf.Options.IncrSyncBarrierOrderingEnable = origOrderingEnable
		conf.Options.FilterDDLEnable = origDDL
		conf.Options.IncrSyncMongoFetchMethod = origFetchMethod
	}()
	conf.Options.Tunnel = utils.VarTunnelDirect
	conf.Options.IncrSyncBarrierOrderingEnable = true
	conf.Options.FilterDDLEnable = true
	conf.Options.IncrSyncMongoFetchMethod = utils.VarIncrSyncMongoFetchMethodOplog

	syncer := &OplogSyncer{}
	rawOf := func(d bson.D) []byte {
		raw, err := bson.Marshal(d)
		if err != nil {
			t.Fatalf("marshal failed: %v", err)
		}
		return raw
	}
	ts := func() bson.E { return bson.E{Key: "ts", Value: utils.Int64ToTimestamp(100)} }

	ddlCommand := rawOf(bson.D{ts(), {Key: "op", Value: "c"}, {Key: "ns", Value: "a.$cmd"},
		{Key: "o", Value: bson.D{{Key: "drop", Value: "b"}}}})
	cases := []struct {
		name string
		raw  []byte
		want bool
	}{
		{"ddl command", ddlCommand, true},
		{"applyOps is not a barrier", rawOf(bson.D{ts(), {Key: "op", Value: "c"}, {Key: "ns", Value: "admin.$cmd"},
			{Key: "o", Value: bson.D{{Key: "applyOps", Value: bson.A{}}}}}), false},
		{"plain insert", rawOf(bson.D{ts(), {Key: "op", Value: "i"}, {Key: "ns", Value: "a.b"},
			{Key: "o", Value: bson.D{{Key: "_id", Value: 1}}}}), false},
		{"system.indexes write", rawOf(bson.D{ts(), {Key: "op", Value: "i"}, {Key: "ns", Value: "a.system.indexes"},
			{Key: "o", Value: bson.D{{Key: "v", Value: 2}}}}), true},
		{"empty raw", nil, false},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, syncer.isEntryBarrier(tc.raw), tc.name)
	}

	// the screen keeps only what it can read for sure: a change stream event has no "op" field, so
	// every operation type but the four data ones has to fall through to the parse
	event := func(operationType string) bson.Raw {
		return bson.Raw(rawOf(bson.D{
			{Key: "operationType", Value: operationType},
			{Key: "ns", Value: bson.D{{Key: "db", Value: "a"}, {Key: "coll", Value: "b"}}},
		}))
	}
	assert.Equal(t, false, mayBeDDL(event("insert")), "an insert event cannot be a DDL")
	assert.Equal(t, false, mayBeDDL(event("update")), "an update event cannot be a DDL")
	assert.Equal(t, true, mayBeDDL(event("drop")), "a drop event has to be parsed")
	assert.Equal(t, true, mayBeDDL(event("invalidate")), "an unrecognised event type has to be parsed")

	// scope: the guarantee needs tunnel=direct *and* the switch, and no DDL barrier is configured at
	// all unless FilterDDLEnable says so
	conf.Options.Tunnel = utils.VarTunnelKafka
	assert.Equal(t, false, syncer.isEntryBarrier(ddlCommand), "non-direct must not gate")
	conf.Options.Tunnel = ""
	assert.Equal(t, false, syncer.isEntryBarrier(ddlCommand), "an unset tunnel counts as non-direct")
	conf.Options.Tunnel = utils.VarTunnelDirect
	conf.Options.IncrSyncBarrierOrderingEnable = false
	assert.Equal(t, false, syncer.isEntryBarrier(ddlCommand), "ordering_enable off must not gate")
	conf.Options.IncrSyncBarrierOrderingEnable = true
	conf.Options.FilterDDLEnable = false
	assert.Equal(t, false, syncer.isEntryBarrier(ddlCommand), "FilterDDLEnable off means no DDL barrier")
}

/*
 * The regression the entrance gate exists for: getBatch concatenates logs queues by index, not by
 * timestamp, and persister hands batches out round robin. A queue that fell behind therefore holds
 * oplogs older than the DDL the batcher is about to park, and without the gate the batcher parks
 * the DDL and applies those older DMLs in a later round -- the DDL running before the DML that was
 * written ahead of it.
 *
 * The gate closes before the DDL is injected, so what this test can assert is the observable that
 * buys: by the time the DDL reached the pipeline, every oplog written before it had already left
 * the in-flight region and been taken by the batcher.
 */
func TestNextGatesDDLUntilPipelineQuiesced(t *testing.T) {
	utils.InitialLogger("", "", "debug", true, 1)

	origTunnel := conf.Options.Tunnel
	origOrderingEnable := conf.Options.IncrSyncBarrierOrderingEnable
	origCapacity := conf.Options.IncrSyncFetcherBufferCapacity
	origDDL := conf.Options.FilterDDLEnable
	origFetchMethod := conf.Options.IncrSyncMongoFetchMethod
	defer func() {
		conf.Options.Tunnel = origTunnel
		conf.Options.IncrSyncBarrierOrderingEnable = origOrderingEnable
		conf.Options.IncrSyncFetcherBufferCapacity = origCapacity
		conf.Options.FilterDDLEnable = origDDL
		conf.Options.IncrSyncMongoFetchMethod = origFetchMethod
	}()
	conf.Options.Tunnel = utils.VarTunnelDirect
	conf.Options.IncrSyncBarrierOrderingEnable = true
	conf.Options.FilterDDLEnable = true
	conf.Options.IncrSyncMongoFetchMethod = utils.VarIncrSyncMongoFetchMethodOplog
	conf.Options.IncrSyncFetcherBufferCapacity = 100 // keep Buffer from flushing on size

	syncer := mockSyncer()
	// dispatchBuffer hands batches out round robin, so *every* queue has to exist: leaving the
	// second one nil would block that dispatch forever on the nil channel, and the gate with it.
	// One queue is all this test needs.
	syncer.PendingQueue = []chan [][]byte{make(chan [][]byte, 10)}
	syncer.batcher = NewBatcher(syncer, filter.OplogFilterChain{}, syncer, []*Worker{new(Worker)})
	syncer.persister = NewPersister("test", syncer)

	rawOf := func(ts int64, ddl bool) []byte {
		var ddlGiven []int
		if ddl {
			ddlGiven = []int{0}
		}
		raw := rawOplogRaw(mockOplogs(1, ddlGiven, nil, nil, ts)[0])
		if raw == nil {
			t.Fatalf("marshal raw oplog failed")
		}
		return raw
	}
	syncer.reader = &stubReader{ops: [][]byte{rawOf(50, false), rawOf(51, false), rawOf(100, true)}}

	// the resident deserializer, as production runs it: take a pending batch, push it into
	// logsQueue, then release its share of rawInFlight. Both loops run for the length of the test
	// and are left blocked on their channel when it ends.
	go func() {
		for batch := range syncer.PendingQueue[0] {
			logs := make([]*oplog.GenericOplog, 0, len(batch))
			for _, raw := range batch {
				log, err := parseRawOplog(raw)
				if err != nil {
					t.Errorf("parse failed: %v", err)
					continue
				}
				logs = append(logs, &oplog.GenericOplog{Raw: raw, Parsed: log})
			}
			syncer.logsQueue[0] <- logs
			atomic.AddInt64(&syncer.rawInFlight, -int64(len(logs)))
		}
	}()

	// the batcher side: take whatever reaches logsQueue, so an empty queue -- the quiesce
	// condition -- is reachable exactly as it is in production
	consumed := make(chan int64, 10)
	go func() {
		for logs := range syncer.logsQueue[0] {
			for _, log := range logs {
				// mockOplogs stamps ts through utils.TimeToTimestamp, which carries the value in T,
				// so read it back the same way rather than through TimeStampToInt64.
				consumed <- int64(log.Parsed.Timestamp.T)
			}
		}
	}()

	// two plain DMLs go in ungated
	assert.Equal(t, true, syncer.next(), "should be equal")
	assert.Equal(t, true, syncer.next(), "should be equal")
	assert.Nil(t, syncer.batcher.ingestGate.Load(), "plain DMLs must not gate ingest")

	// the DDL: next() has to hold it back until both DMLs have left the in-flight region
	injected := make(chan bool, 1)
	go func() { injected <- syncer.next() }()
	select {
	case <-injected:
	case <-time.After(entranceWaitTimeout):
		t.Fatal("next() did not return on the DDL: the entrance wait never saw a quiesced pipeline")
	}

	assert.Equal(t, 2, len(consumed), "both DMLs must have reached the batcher before the DDL was injected")
	assert.Equal(t, int64(1), atomic.LoadInt64(&syncer.rawInFlight), "only the DDL should still be in flight")
	assert.NotNil(t, syncer.batcher.ingestGate.Load(), "the gate stays closed until the DDL has been taken too")

	// one more round: the parked next() flushes the DDL forward, waits for it to be taken, and only
	// then reopens the gate
	resumed := make(chan struct{})
	go func() {
		defer close(resumed)
		syncer.next()
	}()
	deadline := time.Now().Add(entranceWaitTimeout)
	for syncer.batcher.ingestGate.Load() != nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	assert.Nil(t, syncer.batcher.ingestGate.Load(), "the gate must reopen once the pipeline is quiescent")

	// Let that call finish before the deferred config restore runs: it reads conf.Options on its way
	// through the reader, and restoring underneath a live goroutine is a data race. Bounded, since
	// the reader is exhausted and the call returns as soon as the gate is open.
	select {
	case <-resumed:
	case <-time.After(entranceWaitTimeout):
		t.Fatal("next() did not return after the gate was released")
	}

	// The gate opens as soon as the DDL has left the in-flight region, which is a moment before the
	// batcher-side goroutine has read it off the channel, so wait for that handover instead of
	// sampling it. What the gate promises is exactly this: delivery into a logs queue, no later.
	deadline = time.Now().Add(entranceWaitTimeout)
	for len(consumed) < 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	var order []int64
	for len(consumed) > 0 {
		order = append(order, <-consumed)
	}
	assert.Equal(t, []int64{50, 51, 100}, order, "the DMLs must be dispatched before the DDL they precede")
}

// TestNextDoesNotGateOutsideDirect pins the scope of the whole mechanism: outside direct -- and
// inside direct with the incr_sync.barrier.ordering_enable switch left off -- next() injects a DDL
// straight through and touches no gate. The switch defaults to false, so it has to be the second
// case that proves the gate needs both conditions and neither one alone.
func TestNextDoesNotGateOutsideDirect(t *testing.T) {
	utils.InitialLogger("", "", "debug", true, 1)

	origTunnel := conf.Options.Tunnel
	origOrderingEnable := conf.Options.IncrSyncBarrierOrderingEnable
	origCapacity := conf.Options.IncrSyncFetcherBufferCapacity
	origDDL := conf.Options.FilterDDLEnable
	origFetchMethod := conf.Options.IncrSyncMongoFetchMethod
	defer func() {
		conf.Options.Tunnel = origTunnel
		conf.Options.IncrSyncBarrierOrderingEnable = origOrderingEnable
		conf.Options.IncrSyncFetcherBufferCapacity = origCapacity
		conf.Options.FilterDDLEnable = origDDL
		conf.Options.IncrSyncMongoFetchMethod = origFetchMethod
	}()
	conf.Options.FilterDDLEnable = true
	conf.Options.IncrSyncMongoFetchMethod = utils.VarIncrSyncMongoFetchMethodOplog
	conf.Options.IncrSyncFetcherBufferCapacity = 100

	for _, tc := range []struct {
		tunnel string
		enable bool
	}{
		{utils.VarTunnelKafka, true},
		{"", true},                     // an unset tunnel is non-direct
		{utils.VarTunnelDirect, false}, // the switch is off by default: direct alone must not gate
	} {
		conf.Options.Tunnel = tc.tunnel
		conf.Options.IncrSyncBarrierOrderingEnable = tc.enable
		desc := fmt.Sprintf("tunnel %q, ordering_enable %v", tc.tunnel, tc.enable)

		syncer := mockSyncer()
		syncer.PendingQueue[0] = make(chan [][]byte, 10)
		syncer.batcher = NewBatcher(syncer, filter.OplogFilterChain{}, syncer, []*Worker{new(Worker)})
		syncer.persister = NewPersister("test", syncer)

		raw := rawOplogRaw(mockOplogs(1, []int{0}, nil, nil, 100)[0])
		if raw == nil {
			t.Fatalf("marshal raw ddl failed")
		}
		syncer.reader = &stubReader{ops: [][]byte{raw}}

		done := make(chan bool, 1)
		go func() { done <- syncer.next() }()
		select {
		case ok := <-done:
			assert.Equal(t, true, ok, "%s: next() should return", desc)
		case <-time.After(nextGateWaitTimeout):
			t.Fatalf("%s: next() stalled on a DDL: the ordering gate must not engage", desc)
		}
		assert.Nil(t, syncer.batcher.ingestGate.Load(), "%s: no gate", desc)
		assert.Equal(t, 1, len(syncer.persister.Buffer), "%s: the DDL should have been injected as usual", desc)
	}
}

// stubReader hands out a fixed list of raw oplogs and then reports a timeout, the way a real
// reader waits for the source to produce something.
type stubReader struct {
	ops   [][]byte
	index int
}

func (r *stubReader) Name() string                         { return "stub" }
func (r *stubReader) StartFetcher()                        {}
func (r *stubReader) SetQueryTimestampOnEmpty(interface{}) {}
func (r *stubReader) UpdateQueryTimestamp(int64)           {}
func (r *stubReader) EnsureNetwork() error                 { return nil }
func (r *stubReader) FetchNewestTimestamp() (interface{}, error) {
	return nil, nil
}

func (r *stubReader) Next() ([]byte, error) {
	if r.index >= len(r.ops) {
		return nil, sourceReader.TimeoutError
	}
	op := r.ops[r.index]
	r.index++
	return op, nil
}

func mockBatcher(nsWhite []string, nsBlack []string) *Batcher {
	filterList := filter.OplogFilterChain{new(filter.AutologousFilter), new(filter.NoopFilter)}
	// namespace filter
	if len(nsWhite) != 0 || len(nsBlack) != 0 {
		namespaceFilter := filter.NewNamespaceFilter(nsWhite, nsBlack)
		filterList = append(filterList, namespaceFilter)
	}
	return &Batcher{
		syncer: &OplogSyncer{
			fullSyncFinishPosition: utils.TimeToTimestamp(0),
		},
		filterList: filterList,
	}
}

func TestDispatchBatches(t *testing.T) {
	// test dispatchBatches

	var nr int

	// 1. input array is empty
	{
		fmt.Printf("TestDispatchBatches case %d.\n", nr)
		nr++

		batcher := &Batcher{
			workerGroup: []*Worker{
				{
					queue: make(chan []*oplog.GenericOplog, 100),
				},
			},
		}
		utils.IncrSentinelOptions.TargetDelay = -1
		conf.Options.IncrSyncTargetDelay = 0
		ret := batcher.dispatchBatches(nil)
		assert.Equal(t, false, ret, "should be equal")
	}
}

func TestGetTargetDelay(t *testing.T) {
	var nr int

	{
		fmt.Printf("TestGetTargetDelay case %d.\n", nr)
		nr++

		utils.IncrSentinelOptions.TargetDelay = -1
		conf.Options.IncrSyncTargetDelay = 10
		assert.Equal(t, int64(10), getTargetDelay(), "should be equal")
	}

	{
		fmt.Printf("TestGetTargetDelay case %d.\n", nr)
		nr++

		utils.IncrSentinelOptions.TargetDelay = 12
		conf.Options.IncrSyncTargetDelay = 10
		assert.Equal(t, int64(12), getTargetDelay(), "should be equal")
	}
}

func TestGetBatchWithDelay(t *testing.T) {
	// test getBatchWithDelay

	var nr int

	utils.InitialLogger("", "", "info", true, 1)

	// reset to default
	utils.IncrSentinelOptions.TargetDelay = -1
	conf.Options.IncrSyncTargetDelay = 0

	// 1. input is nil
	{
		fmt.Printf("TestGetBatchWithDelay case %d.\n", nr)
		nr++

		batcher := &Batcher{
			syncer: mockSyncer(),
		}
		batcher.syncer.fullSyncFinishPosition = utils.TimeToTimestamp(1)

		batcher.utBatchesDelay.flag = true
		batcher.utBatchesDelay.injectBatch = nil
		batcher.utBatchesDelay.delay = 0

		ret, exit := batcher.getBatchWithDelay()
		assert.Equal(t, 0, len(ret), "should be equal")
		assert.Equal(t, false, exit, "should be equal")
	}

	// 2. normal case: input array is not empty
	{
		fmt.Printf("TestGetBatchWithDelay case %d.\n", nr)
		nr++

		batcher := &Batcher{
			syncer: mockSyncer(),
		}
		batcher.syncer.fullSyncFinishPosition = utils.TimeToTimestamp(1)

		batcher.utBatchesDelay.flag = true
		batcher.utBatchesDelay.injectBatch = mockOplogs(20, nil, nil, nil, time.Now().Unix())
		batcher.utBatchesDelay.delay = 0
		utils.IncrSentinelOptions.ExitPoint = (time.Now().Unix() + 1000000) << 32

		ret, exit := batcher.getBatchWithDelay()
		assert.Equal(t, 20, len(ret), "should be equal")
		assert.Equal(t, int64(0), getTargetDelay(), "should be equal")
		assert.Equal(t, 0, batcher.utBatchesDelay.delay, "should be equal")
		assert.Equal(t, false, exit, "should be equal")
	}

	// 3. delay == 1s
	{
		fmt.Printf("TestGetBatchWithDelay case %d.\n", nr)
		nr++

		batcher := &Batcher{
			syncer: mockSyncer(),
		}
		batcher.syncer.fullSyncFinishPosition = utils.TimeToTimestamp(1)

		batcher.utBatchesDelay.flag = true
		batcher.utBatchesDelay.injectBatch = mockOplogs(20, nil, nil, nil, time.Now().Unix())
		batcher.utBatchesDelay.delay = 0
		utils.IncrSentinelOptions.ExitPoint = 0

		utils.IncrSentinelOptions.TargetDelay = -1
		conf.Options.IncrSyncTargetDelay = 1

		ret, exit := batcher.getBatchWithDelay()
		assert.Equal(t, 20, len(ret), "should be equal")
		assert.Equal(t, 0, batcher.utBatchesDelay.delay, "should be equal")
		assert.Equal(t, false, exit, "should be equal")
	}

	// 4. delay == 10s
	{
		fmt.Printf("TestGetBatchWithDelay case %d.\n", nr)
		nr++

		batcher := &Batcher{
			syncer: mockSyncer(),
		}
		batcher.syncer.fullSyncFinishPosition = utils.TimeToTimestamp(1)

		nowTs := time.Now().Unix()
		batcher.utBatchesDelay.flag = true
		batcher.utBatchesDelay.injectBatch = mockOplogs(20, nil, nil, nil, nowTs)
		batcher.utBatchesDelay.delay = 0

		utils.IncrSentinelOptions.ExitPoint = (nowTs + 5) << 32
		utils.IncrSentinelOptions.TargetDelay = -1
		conf.Options.IncrSyncTargetDelay = 10

		ret, exit := batcher.getBatchWithDelay()
		fmt.Println(batcher.utBatchesDelay.delay)
		assert.Equal(t, 6, len(ret), "should be equal")
		assert.Equal(t, true, batcher.utBatchesDelay.delay == 0, "should be equal")
		assert.Equal(t, true, exit, "should be equal")
	}

	// 5. delay == 10s, but before fullSyncFinishPosition
	{
		fmt.Printf("TestGetBatchWithDelay case %d.\n", nr)
		nr++

		batcher := &Batcher{
			syncer: mockSyncer(),
		}
		batcher.syncer.fullSyncFinishPosition = utils.TimeToTimestamp(time.Now().Unix() + 100)

		nowTs := time.Now().Unix()
		batcher.utBatchesDelay.flag = true
		batcher.utBatchesDelay.injectBatch = mockOplogs(20, nil, nil, nil, nowTs)
		batcher.utBatchesDelay.delay = 0

		utils.IncrSentinelOptions.ExitPoint = (nowTs + 5) << 32
		utils.IncrSentinelOptions.TargetDelay = -1
		conf.Options.IncrSyncTargetDelay = 10

		ret, exit := batcher.getBatchWithDelay()
		fmt.Println(batcher.utBatchesDelay.delay)
		assert.Equal(t, 20, len(ret), "should be equal")
		assert.Equal(t, 0, batcher.utBatchesDelay.delay, "should be equal")
		assert.Equal(t, false, exit, "should be equal")
	}

	// 6. no delay, exit at middle
	{
		fmt.Printf("TestGetBatchWithDelay case %d.\n", nr)
		nr++

		batcher := &Batcher{
			syncer: mockSyncer(),
		}
		batcher.syncer.fullSyncFinishPosition = utils.TimeToTimestamp(1)

		nowTs := time.Now().Unix()
		batcher.utBatchesDelay.flag = true
		batcher.utBatchesDelay.injectBatch = mockOplogs(20, nil, nil, nil, nowTs)
		batcher.utBatchesDelay.delay = 0
		utils.IncrSentinelOptions.ExitPoint = (nowTs + 5) << 32

		utils.IncrSentinelOptions.TargetDelay = -1
		conf.Options.IncrSyncTargetDelay = 0

		ret, exit := batcher.getBatchWithDelay()
		assert.Equal(t, 6, len(ret), "should be equal")
		assert.Equal(t, 0, batcher.utBatchesDelay.delay, "should be equal")
		assert.Equal(t, true, exit, "should be equal")
	}

	// 7. delay == 60s
	{
		fmt.Printf("TestGetBatchWithDelay case %d.\n", nr)
		nr++

		batcher := &Batcher{
			syncer: mockSyncer(),
		}
		batcher.syncer.fullSyncFinishPosition = utils.TimeToTimestamp(1)

		nowTs := time.Now().Unix()
		batcher.utBatchesDelay.flag = true
		batcher.utBatchesDelay.injectBatch = mockOplogs(20, nil, nil, nil, nowTs)
		batcher.utBatchesDelay.delay = 0

		utils.IncrSentinelOptions.TargetDelay = 60
		conf.Options.IncrSyncTargetDelay = 10
		utils.IncrSentinelOptions.ExitPoint = (nowTs + 50) << 32

		ret, exit := batcher.getBatchWithDelay()
		fmt.Println(batcher.utBatchesDelay.delay)
		assert.Equal(t, 20, len(ret), "should be equal")
		assert.Equal(t, true, batcher.utBatchesDelay.delay > 11, "should be equal")
		assert.Equal(t, false, exit, "should be equal")
	}

	// 8. delay == 1s, no delay
	{
		fmt.Printf("TestGetBatchWithDelay case %d.\n", nr)
		nr++

		batcher := &Batcher{
			syncer: mockSyncer(),
		}
		batcher.syncer.fullSyncFinishPosition = utils.TimeToTimestamp(1)

		batcher.utBatchesDelay.flag = true
		batcher.utBatchesDelay.injectBatch = mockOplogs(20, nil, nil, nil, time.Now().Unix())
		batcher.utBatchesDelay.delay = 0

		utils.IncrSentinelOptions.TargetDelay = 1
		conf.Options.IncrSyncTargetDelay = 10
		utils.IncrSentinelOptions.ExitPoint = -1

		ret, exit := batcher.getBatchWithDelay()
		fmt.Println(batcher.utBatchesDelay.delay)
		assert.Equal(t, 20, len(ret), "should be equal")
		assert.Equal(t, 0, batcher.utBatchesDelay.delay, "should be equal")
		assert.Equal(t, false, exit, "should be equal")
	}

	// 9. delay == 60s, exit at an old time
	{
		fmt.Printf("TestGetBatchWithDelay case %d.\n", nr)
		nr++

		batcher := &Batcher{
			syncer: mockSyncer(),
		}
		batcher.syncer.fullSyncFinishPosition = utils.TimeToTimestamp(1)

		nowTs := time.Now().Unix()
		batcher.utBatchesDelay.flag = true
		batcher.utBatchesDelay.injectBatch = mockOplogs(20, nil, nil, nil, nowTs)
		batcher.utBatchesDelay.delay = 0

		utils.IncrSentinelOptions.TargetDelay = 60
		conf.Options.IncrSyncTargetDelay = 10
		utils.IncrSentinelOptions.ExitPoint = (nowTs - 1000) << 32

		ret, exit := batcher.getBatchWithDelay()
		fmt.Println(batcher.utBatchesDelay.delay)
		assert.Equal(t, 0, len(ret), "should be equal")
		assert.Equal(t, true, batcher.utBatchesDelay.delay == 0, "should be equal")
		assert.Equal(t, true, exit, "should be equal")
	}
}
