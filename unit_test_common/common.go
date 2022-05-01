package unit_test_common

import (
	utils "github.com/alibaba/MongoShake/v2/common"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func FetchAllDocumentbsonD(conn *utils.MongoCommunityConn, testDb string, testCollection string,
	opts *options.FindOptions) ([]bson.D, error) {
	cursor, _ := conn.Client.Database(testDb).Collection(testCollection).Find(nil, bson.M{}, opts)

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

func FetchAllDocumentbsonM(conn *utils.MongoCommunityConn, testDb string, testCollection string,
	opts *options.FindOptions) ([]bson.M, error) {

	cursor, _ := conn.Client.Database(testDb).Collection(testCollection).Find(nil, bson.M{}, opts)

	result := make([]bson.M, 0)
	for cursor.Next(nil) {
		var doc bson.M // doc is ptr
		err := cursor.Decode(&doc)
		if err != nil {
			return nil, err
		}
		result = append(result, doc)
	}
	return result, nil
}
