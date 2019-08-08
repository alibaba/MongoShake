package utils

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vinllen/mgo/bson"
)

func TestAdjustDBRef(t *testing.T) {
	var nr int
	{
		fmt.Printf("TestAdjustDBRef case %d.\n", nr)
		nr++

		input := bson.M{
			"a": 1,
		}

		output := AdjustDBRef(input, true)
		assert.Equal(t, input, output, "should be equal")
	}

	{
		fmt.Printf("TestAdjustDBRef case %d.\n", nr)
		nr++

		input := bson.M{}

		output := AdjustDBRef(input, true)
		assert.Equal(t, input, output, "should be equal")
	}

	{
		fmt.Printf("TestAdjustDBRef case %d.\n", nr)
		nr++

		input := bson.M{
			"a": bson.M{
				"$db":  "xxx",
				"$id":  "1234",
				"$ref": "a.b",
			},
		}

		output := AdjustDBRef(input, false)
		assert.Equal(t, input, output, "should be equal")
	}

	{
		fmt.Printf("TestAdjustDBRef case %d.\n", nr)
		nr++

		input := bson.M{
			"a": bson.M{
				"$db":  "xxx",
				"$id":  1234,
				"$ref": "a.b",
			},
		}

		output := AdjustDBRef(input, true)
		assert.Equal(t, "$ref", output["a"].(bson.D)[0].Name, "should be equal")
		assert.Equal(t, "a.b", output["a"].(bson.D)[0].Value, "should be equal")
		assert.Equal(t, "$id", output["a"].(bson.D)[1].Name, "should be equal")
		assert.Equal(t, 1234, output["a"].(bson.D)[1].Value, "should be equal")
		assert.Equal(t, "$db", output["a"].(bson.D)[2].Name, "should be equal")
		assert.Equal(t, "xxx", output["a"].(bson.D)[2].Value, "should be equal")
	}

	{
		fmt.Printf("TestAdjustDBRef case %d.\n", nr)
		nr++

		input := bson.M{
			"a": bson.M{
				"$db":  "xxx",
				"$id":  1234,
				"$ref": "a.b",
			},
			"b": bson.M{
				"c": bson.M{
					"$id":  5678,
					"$db":  "yyy",
					"$ref": "c.d",
				},
				"d": "hello-world",
				"e": bson.M{
					"$id":  910,
					"$db":  "zzz",
					"$ref": "e.f",
				},
				"f": 1,
			},
		}

		output := AdjustDBRef(input, true)
		assert.Equal(t, "$ref", output["a"].(bson.D)[0].Name, "should be equal")
		assert.Equal(t, "a.b", output["a"].(bson.D)[0].Value, "should be equal")
		assert.Equal(t, "$id", output["a"].(bson.D)[1].Name, "should be equal")
		assert.Equal(t, 1234, output["a"].(bson.D)[1].Value, "should be equal")
		assert.Equal(t, "$db", output["a"].(bson.D)[2].Name, "should be equal")
		assert.Equal(t, "xxx", output["a"].(bson.D)[2].Value, "should be equal")

		assert.Equal(t, "hello-world", output["b"].(bson.M)["d"], "should be equal")
		assert.Equal(t, 1, output["b"].(bson.M)["f"], "should be equal")

		assert.Equal(t, "$ref", output["b"].(bson.M)["c"].(bson.D)[0].Name, "should be equal")
		assert.Equal(t, "c.d", output["b"].(bson.M)["c"].(bson.D)[0].Value, "should be equal")
		assert.Equal(t, "$id", output["b"].(bson.M)["c"].(bson.D)[1].Name, "should be equal")
		assert.Equal(t, 5678, output["b"].(bson.M)["c"].(bson.D)[1].Value, "should be equal")
		assert.Equal(t, "$db", output["b"].(bson.M)["c"].(bson.D)[2].Name, "should be equal")
		assert.Equal(t, "yyy", output["b"].(bson.M)["c"].(bson.D)[2].Value, "should be equal")

		assert.Equal(t, "$ref", output["b"].(bson.M)["e"].(bson.D)[0].Name, "should be equal")
		assert.Equal(t, "e.f", output["b"].(bson.M)["e"].(bson.D)[0].Value, "should be equal")
		assert.Equal(t, "$id", output["b"].(bson.M)["e"].(bson.D)[1].Name, "should be equal")
		assert.Equal(t, 910, output["b"].(bson.M)["e"].(bson.D)[1].Value, "should be equal")
		assert.Equal(t, "$db", output["b"].(bson.M)["e"].(bson.D)[2].Name, "should be equal")
		assert.Equal(t, "zzz", output["b"].(bson.M)["e"].(bson.D)[2].Value, "should be equal")
	}

	{
		fmt.Printf("TestAdjustDBRef case %d.\n", nr)
		nr++

		input := bson.M{
			"p": bson.D{
				{
					Name: "x",
					Value: bson.M{
						"$id":    10,
						"$db":    "zzz",
						"$ref":   "www",
						"others": "aaa",
						"fuck":   "hello",
					},
				},
				{
					Name: "y",
					Value: bson.M{
						"$id":    20,
						"$db":    "po",
						"$ref":   "po2",
						"others": "bbb",
						"fuck":   "world",
					},
				},
			},
		}

		output := AdjustDBRef(input, true)
		assert.Equal(t, "x", output["p"].(bson.D)[0].Name, "should be equal")

		assert.Equal(t, "$ref", output["p"].(bson.D)[0].Value.(bson.D)[0].Name, "should be equal")
		assert.Equal(t, "www", output["p"].(bson.D)[0].Value.(bson.D)[0].Value, "should be equal")
		assert.Equal(t, "$id", output["p"].(bson.D)[0].Value.(bson.D)[1].Name, "should be equal")
		assert.Equal(t, 10, output["p"].(bson.D)[0].Value.(bson.D)[1].Value, "should be equal")
		assert.Equal(t, "$db", output["p"].(bson.D)[0].Value.(bson.D)[2].Name, "should be equal")
		assert.Equal(t, "zzz", output["p"].(bson.D)[0].Value.(bson.D)[2].Value, "should be equal")
		assert.Equal(t, "others", output["p"].(bson.D)[0].Value.(bson.D)[3].Name, "should be equal")
		assert.Equal(t, "aaa", output["p"].(bson.D)[0].Value.(bson.D)[3].Value, "should be equal")
		assert.Equal(t, "fuck", output["p"].(bson.D)[0].Value.(bson.D)[4].Name, "should be equal")
		assert.Equal(t, "hello", output["p"].(bson.D)[0].Value.(bson.D)[4].Value, "should be equal")
	}

	{
		fmt.Printf("TestAdjustDBRef case %d.\n", nr)
		nr++

		input := bson.M{
			"ts": 1560588963,
			"t":  8,
			"h":  -4461630918490158108,
			"v":  2,
			"op": "u",
			"ns": "test.zzz",
			"o2": bson.M{
				"_id": "5d04b02c27d5888ce0224fc8",
			},
			"o": bson.M{
				"_id": "5d04b02c27d5888ce0224fc8",
				"b":   7,
				"user": bson.M{
					"$ref": "xxx",
					"$id":  "40b6d79e507b2c613615f15d",
				},
			},
		}

		output := AdjustDBRef(input["o"].(bson.M), true)

		assert.Equal(t, "$ref", output["user"].(bson.D)[0].Name, "should be equal")
		assert.Equal(t, "xxx", output["user"].(bson.D)[0].Value, "should be equal")
		assert.Equal(t, "$id", output["user"].(bson.D)[1].Name, "should be equal")
		assert.Equal(t, "40b6d79e507b2c613615f15d", output["user"].(bson.D)[1].Value, "should be equal")

	}
}
