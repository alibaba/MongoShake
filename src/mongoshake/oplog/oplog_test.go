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
				Name:  "$set",
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
				Name:  "$set",
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
