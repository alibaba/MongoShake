package unit_test_common

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func FetchAllDocumentbsonD(client *mongo.Client, testDb string, testCollection string,
	opts *options.FindOptions) ([]bson.D, error) {
	cursor, _ := client.Database(testDb).Collection(testCollection).Find(nil, bson.M{}, opts)

	result := make([]bson.D, 0)
	for cursor.Next(nil) {
		var doc bson.D
		err := cursor.Decode(&doc)
		if err != nil {
			return nil, err
		}
		result = append(result, doc)
	}
	return result, nil
}

func FetchAllDocumentbsonM(client *mongo.Client, testDb string, testCollection string,
	opts *options.FindOptions) ([]bson.M, error) {

	cursor, _ := client.Database(testDb).Collection(testCollection).Find(nil, bson.M{}, opts)

	result := make([]bson.M, 0)
	for cursor.Next(nil) {
		var doc bson.M // doc is ptr, change content also change result content
		err := cursor.Decode(&doc)
		if err != nil {
			return nil, err
		}
		result = append(result, doc)
	}
	return result, nil
}
