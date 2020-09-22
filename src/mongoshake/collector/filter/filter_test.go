package filter

import (
	"fmt"
	"testing"
	"mongoshake/oplog"

	"github.com/stretchr/testify/assert"
	"github.com/getlantern/deepcopy"
	"github.com/vinllen/mgo/bson"
)

func TestNamespaceFilter(t *testing.T) {
	// test NamespaceFilter

	var nr int
	{
		fmt.Printf("TestNamespaceFilter case %d.\n", nr)
		nr++

		filter := NewNamespaceFilter([]string{"gogo.test1", "gogo.test2"}, nil)
		log := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog {
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
			ParsedLog: oplog.ParsedLog {
				Namespace: "admin.$cmd",
				Operation: "c",
				Object: bson.D{
					{
						Name: "applyOps",
						Value: []bson.D{
							{
								bson.DocElem{"op", "i"},
								bson.DocElem{"ns", "zz.mmm"},
								bson.DocElem{"o", bson.D{
									bson.DocElem{"a", 1},
									bson.DocElem{"_id", "xxx"},
								}},
							},
							{
								bson.DocElem{"op", "i"},
								bson.DocElem{"ns", "zz.x"},
								bson.DocElem{"o", bson.D{
									bson.DocElem{"xyz", "ff"},
									bson.DocElem{"_id", "yyy"},
								}},
							},
						},
					},
				},
			},
		}
		assert.Equal(t, false, filter.Filter(log), "should be equal")
		assert.Equal(t, 2, len(log.Object[0].Value.([]interface{})), "should be equal")
	}

	{
		fmt.Printf("TestNamespaceFilter case %d.\n", nr)
		nr++

		filter := NewNamespaceFilter(nil, []string{"ff"})
		log := &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog {
				Namespace: "admin.$cmd",
				Operation: "c",
				Object: bson.D{
					{
						Name: "applyOps",
						Value: []bson.D{
							{
								bson.DocElem{"op", "i"},
								bson.DocElem{"ns", "zz.mmm"},
								bson.DocElem{"o", bson.D{
									bson.DocElem{"a", 1},
									bson.DocElem{"_id", "xxx"},
								}},
							},
							{
								bson.DocElem{"op", "i"},
								bson.DocElem{"ns", "ff.x"},
								bson.DocElem{"o", bson.D{
									bson.DocElem{"xyz", "ff"},
									bson.DocElem{"_id", "yyy"},
								}},
							},
						},
					},
				},
			},
		}
		assert.Equal(t, false, filter.Filter(log), "should be equal")
		assert.Equal(t, 1, len(log.Object[0].Value.([]interface{})), "should be equal")
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
				Namespace: "local.x.z.y",
			},
		}
		assert.Equal(t, true, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			ParsedLog: oplog.ParsedLog{
				Namespace: "a.system.views",
			},
		}
		assert.Equal(t, true, filter.Filter(log), "should be equal")

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
	}

	rec := make(map[string]bool)
	deepcopy.Copy(&rec, &NsShouldBeIgnore)

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

	// test transaction
	{
		fmt.Printf("TestAutologousFilter case %d.\n", nr)
		nr++

		deepcopy.Copy(&NsShouldBeIgnore, &rec)

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
		// assert.Equal(t, false, filter.Filter(log), "should be equal")
		fmt.Println(v1, v2)
	}
}