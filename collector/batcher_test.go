package collector

import (
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"testing"
	"time"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	"github.com/alibaba/MongoShake/v2/collector/filter"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"

	"github.com/stretchr/testify/assert"
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

func marshalData(input bson.D) bson.Raw {
	var dataRaw bson.Raw
	if data, err := bson.Marshal(input); err != nil {
		return nil
	} else {
		dataRaw = data[:]
	}

	return dataRaw
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
					LSID: marshalData(bson.D{
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
						LSID: marshalData(
							bson.D{
								{"id", primitive.Binary{4, []byte{0, 1, 3, 4, 5, 6, byte(startTs + int64(i))}}},
								{"uid", []byte{8, 7, 6, 5, 4, 3, 2, 1, byte(startTs + int64(i))}},
							}),
						TxnNumber:  &txnN[0],
						PrevOpTime: marshalData(bson.D{{"ts", utils.Int64ToTimestamp(0)}}),
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
				LSID: marshalData(
					bson.D{
						{"id", primitive.Binary{Subtype: 4, Data: []byte{0, 1, 3, 4, 5, 6, byte(startTs)}}},
						{"uid", []byte{8, 7, 6, 5, 4, 3, 2, 1, byte(startTs)}},
					}),
				TxnNumber:  &txnN[0],
				PrevOpTime: marshalData(bson.D{{"ts", utils.Int64ToTimestamp(0)}}),
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
				LSID: marshalData(
					bson.D{
						{"id", primitive.Binary{Subtype: 4, Data: []byte{0, 1, 3, 4, 5, 6, byte(startTs)}}},
						{"uid", []byte{8, 7, 6, 5, 4, 3, 2, 1, byte(startTs)}},
					}),
				TxnNumber:  &txnN[0],
				PrevOpTime: marshalData(bson.D{{"ts", utils.TimeToTimestamp(startTs + 0)}}),
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
				LSID: marshalData(
					bson.D{
						{"id", primitive.Binary{Subtype: 4, Data: []byte{0, 1, 3, 4, 5, 6, byte(startTs)}}},
						{"uid", []byte{8, 7, 6, 5, 4, 3, 2, 1, byte(startTs)}},
					}),
				TxnNumber:  &txnN[0],
				PrevOpTime: marshalData(bson.D{{"ts", utils.TimeToTimestamp(startTs + 1)}}),
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
				LSID: marshalData(
					bson.D{
						{"id", primitive.Binary{4, []byte{0, 1, 3, 4, 5, 6, byte(startTs)}}},
						{"uid", []byte{8, 7, 6, 5, 4, 3, 2, 1, byte(startTs)}},
					}),
				TxnNumber:  &txnN[0],
				PrevOpTime: marshalData(bson.D{{"ts", utils.Int64ToTimestamp(0)}}),
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
				LSID: marshalData(
					bson.D{
						{"id", primitive.Binary{4, []byte{0, 1, 3, 4, 5, 6, byte(startTs)}}},
						{"uid", []byte{8, 7, 6, 5, 4, 3, 2, 1, byte(startTs)}},
					}),
				TxnNumber:  &txnN[0],
				PrevOpTime: marshalData(bson.D{{"ts", utils.TimeToTimestamp(startTs + 0)}}),
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
				LSID: marshalData(
					bson.D{
						{"id", primitive.Binary{4, []byte{0, 1, 3, 4, 5, 6, byte(startTs)}}},
						{"uid", []byte{8, 7, 6, 5, 4, 3, 2, 1, byte(startTs)}},
					}),
				TxnNumber:  &txnN[0],
				PrevOpTime: marshalData(bson.D{{"ts", utils.Int64ToTimestamp(0)}}),
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
				LSID: marshalData(
					bson.D{
						{"id", primitive.Binary{4, []byte{0, 1, 3, 4, 5, 6, byte(startTs)}}},
						{"uid", []byte{8, 7, 6, 5, 4, 3, 2, 1, byte(startTs)}},
					}),
				TxnNumber:  &txnN[0],
				PrevOpTime: marshalData(bson.D{{"ts", utils.TimeToTimestamp(startTs + 0)}}),
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
				LSID: marshalData(
					bson.D{
						{"id", primitive.Binary{4, []byte{0, 1, 3, 4, 5, 6, byte(startTs)}}},
						{"uid", []byte{8, 7, 6, 5, 4, 3, 2, 1, byte(startTs)}},
					}),
				TxnNumber:  &txnN[0],
				PrevOpTime: marshalData(bson.D{{"ts", utils.TimeToTimestamp(startTs + 1)}}),
			},
		},
	}

	return output
}

func TestBatchMore(t *testing.T) {
	// test BatchMore

	utils.InitialLogger("", "", "debug", true, 1)

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
		batchedOplog, barrier, allEmpty, _ = batcher.BatchMore()
		assert.Equal(t, true, barrier, "should be equal")
		assert.Equal(t, 0, len(batchedOplog[0]), "should be equal")
		assert.Equal(t, true, allEmpty, "should be equal")
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
		assert.Equal(t, true, allEmpty, "should be equal")
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
		assert.Equal(t, true, allEmpty, "should be equal")
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
		assert.Equal(t, true, allEmpty, "should be equal")
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
		assert.Equal(t, true, allEmpty, "should be equal")
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
		assert.Equal(t, true, allEmpty, "should be equal")
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
		assert.Equal(t, true, allEmpty, "should be equal")
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
		assert.Equal(t, true, allEmpty, "should be equal")
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
		assert.Equal(t, true, allEmpty, "should be equal")
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
		assert.Equal(t, true, allEmpty, "should be equal")
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
		assert.Equal(t, true, allEmpty, "should be equal")
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
		assert.Equal(t, true, allEmpty, "should be equal")
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
		assert.Equal(t, true, allEmpty, "should be equal")
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
		assert.Equal(t, true, allEmpty, "should be equal")
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
		assert.Equal(t, true, allEmpty, "should be equal")
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
		assert.Equal(t, true, allEmpty, "should be equal")
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
		assert.Equal(t, true, allEmpty, "should be equal")
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
		assert.Equal(t, true, allEmpty, "should be equal")
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
		assert.Equal(t, true, allEmpty, "should be equal")
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
		assert.Equal(t, true, allEmpty, "should be equal")
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
		assert.Equal(t, true, allEmpty, "should be equal")
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
		assert.Equal(t, true, allEmpty, "should be equal")
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
		assert.Equal(t, true, allEmpty, "should be equal")
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
