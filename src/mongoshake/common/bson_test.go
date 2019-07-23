package utils

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/vinllen/mgo/bson"
	"testing"
)

type MockKey struct {
	Id        interface{} `bson:"docId"`
	Namespace string      `bson:"namespace"`
}


func TestStruct2Map(t *testing.T) {
	var nr int
	{
		fmt.Printf("TestStruct2Map case %d.\n", nr)
		nr++

		p := &MockKey{Id: bson.ObjectId("aaa"), Namespace:"aaa"}
		data, err := Struct2Map(p, "bson")
		assert.Equal(t, nil, err)
		assert.Equal(t, map[string]interface {}{"docId":bson.ObjectId("aaa"), "namespace":"aaa"}, data)
	}
}

func TestMap2Struct(t *testing.T) {
	var nr int
	{
		fmt.Printf("TestMap2Struct case %d.\n", nr)
		nr++

		data := map[string]interface {}{"docId":bson.ObjectId("aaa"), "namespace":"aaa"}
		k := MockKey{}
		err := Map2Struct(data, "bson", &k)
		assert.Equal(t, nil, err)
		assert.Equal(t, MockKey{Id: bson.ObjectId("aaa"), Namespace:"aaa"}, k)
	}
}