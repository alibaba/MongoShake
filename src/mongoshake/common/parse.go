package utils

import (
	bson2 "github.com/vinllen/mongo-go-driver/bson"
)

func GetKey(log bson2.D, wanted string) interface{} {
	ret, _ := GetKeyWithIndex(log, wanted)
	return ret
}

func GetKeyWithIndex(log bson2.D, wanted string) (interface{}, int) {
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

func SetFiled(input bson2.D, key string, value interface{}, upsert bool) {
	for i, ele := range input {
		if ele.Key == key {
			input[i].Value = value
		}
	}

	if upsert {
		input = append(input, bson2.E{
			Key:   key,
			Value: value})
	}
}
