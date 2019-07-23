package utils

import (
	"fmt"
	"reflect"
)

func Struct2Map(ptr interface{}, tag string) (map[string]interface{}, error) {
	ptrValue := reflect.ValueOf(ptr)
	if ptrValue.Kind() != reflect.Ptr || ptrValue.IsNil() {
		return nil, fmt.Errorf("ptr[%v] is not a valid pointer", ptr)
	}
	ptrType := reflect.TypeOf(ptr)
	objType := ptrType.Elem()
	data := make(map[string]interface{})
	for i := 0; i < objType.NumField(); i++ {
		if tagName, ok := objType.Field(i).Tag.Lookup(tag); ok {
			data[tagName] = reflect.ValueOf(ptr).Elem().Field(i).Interface()
		}
	}
	return data, nil
}

func Map2Struct(data map[string]interface{}, tag string, ptr interface{}) error {
	ptrValue := reflect.ValueOf(ptr)
	if ptrValue.Kind() != reflect.Ptr || ptrValue.IsNil() {
		return fmt.Errorf("ptr[%v] is not a valid pointer", ptr)
	}
	ptrType := reflect.TypeOf(ptr)
	objType := ptrType.Elem()
	for i := 0; i < objType.NumField(); i++ {
		tagName := objType.Field(i).Tag.Get(tag)
		if v, ok := data[tagName]; ok {
			reflect.ValueOf(ptr).Elem().Field(i).Set(reflect.ValueOf(v))
		}
	}
	return nil
}
