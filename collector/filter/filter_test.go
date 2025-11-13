package filter

import (
	"fmt"
	"testing"

	"github.com/getlantern/deepcopy"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/x/mongo/driver/uuid"

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

	// test applyOps
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
