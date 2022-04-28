package unit_test_common

import (
	utils "github.com/alibaba/MongoShake/v2/common"
	bson2 "go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func FetchAllDocumentbsonD(conn *utils.MongoCommunityConn, testDb string, testCollection string,
	opts *options.FindOptions) ([]bson2.D, error) {
	cursor, _ := conn.Client.Database(testDb).Collection(testCollection).Find(nil, bson2.M{}, opts)

	doc := new(bson2.D)
	result := make([]bson2.D, 0)
	for cursor.Next(nil) {
		err := cursor.Decode(doc)
		if err != nil {
			return nil, err
		}
		result = append(result, *doc)
	}
	return result, nil
}

func FetchAllDocumentbsonM(conn *utils.MongoCommunityConn, testDb string, testCollection string,
	opts *options.FindOptions) ([]bson2.M, error) {
	cursor, _ := conn.Client.Database(testDb).Collection(testCollection).Find(nil, bson2.M{}, opts)

	doc := new(bson2.M)
	result := make([]bson2.M, 0)
	for cursor.Next(nil) {
		err := cursor.Decode(doc)
		if err != nil {
			return nil, err
		}
		result = append(result, *doc)
	}
	return result, nil
}
