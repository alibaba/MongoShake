package utils

import (
	"go.mongodb.org/mongo-driver/bson"
)

func GetKey(log bson.D, wanted string) interface{} {
	ret, _ := GetKeyWithIndex(log, wanted)
	return ret
}

func GetKeyWithIndex(log bson.D, wanted string) (interface{}, int) {
	if wanted == "" {
		wanted = "_id"
	}

	// "_id" is always the first field
	for id, ele := range log {
		if ele.Key == wanted {
			return ele.Value, id
		}
	}

	return nil, 0
}

func SetFiled(input bson.D, key string, value interface{}, upsert bool) {
	for i, ele := range input {
		if ele.Key == key {
			input[i].Value = value
		}
	}

	if upsert {
		input = append(input, bson.E{
			Key:   key,
			Value: value})
	}
}
