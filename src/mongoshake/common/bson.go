package utils

import (
	"encoding/json"
	"fmt"
	"github.com/vinllen/mgo/bson"
	"reflect"
)

func Struct2Map(ptr interface{}, tag string) (map[string]interface{}, error) {
	ptrValue := reflect.ValueOf(ptr)
	if ptrValue.Kind() != reflect.Ptr || ptrValue.IsNil() {
		return nil, fmt.Errorf("Struct2Map ptr[%v] is not a valid pointer", ptr)
	}
	ptrType := reflect.TypeOf(ptr)
	objType := ptrType.Elem()
	data := make(map[string]interface{})
	for i := 0; i < objType.NumField(); i++ {
		if tagName, ok := objType.Field(i).Tag.Lookup(tag); ok {
			v := reflect.ValueOf(ptr).Elem().Field(i).Interface()
			switch v.(type) {
			case bson.D:
				if vr, err := json.Marshal(v); err != nil {
					return nil, fmt.Errorf("Struct2Map marshal field[%v] failed. %v", v, err)
				} else {
					data[tagName] = string(vr)
				}
			default:
				data[tagName] = v
			}
		}
	}
	return data, nil
}

func Map2Struct(data map[string]interface{}, tag string, ptr interface{}) error {
	ptrValue := reflect.ValueOf(ptr)
	if ptrValue.Kind() != reflect.Ptr || ptrValue.IsNil() {
		return fmt.Errorf("Map2Struct ptr[%v] is not a valid pointer", ptr)
	}
	ptrType := reflect.TypeOf(ptr)
	objType := ptrType.Elem()
	for i := 0; i < objType.NumField(); i++ {
		tagName := objType.Field(i).Tag.Get(tag)
		if v, ok := data[tagName]; ok {
			field := reflect.ValueOf(ptr).Elem().Field(i)
			switch field.Interface().(type) {
			case bson.D:
				if vr, ok := v.(string); !ok {
					return fmt.Errorf("Map2Struct data[%v] of bson.D field is not string", v)
				} else {
					value := bson.D{}
					if err := json.Unmarshal([]byte(vr), &value); err != nil {
						return fmt.Errorf("Map2Struct unmarshal field[%v] failed. %v", v, err)
					}
					field.Set(reflect.ValueOf(value))
				}
			default:
				if v != nil {
					field.Set(reflect.ValueOf(v))
				}
			}
		}
	}
	return nil
}
