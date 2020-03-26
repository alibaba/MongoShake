package filter

import (
	"fmt"
	"testing"
	"mongoshake/oplog"

	"github.com/stretchr/testify/assert"
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
}