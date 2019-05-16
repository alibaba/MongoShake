package utils

import (
	"fmt"
	"mongoshake/dbpool"
	"strconv"
	"strings"

	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
)

var (
	QueryTs = "ts"
	localDB = "local"
)

type MongoSource struct {
	URL         string
	ReplicaName string
	Gid         string
}


// get db version, return string with format like "3.0.1"
func GetDBVersion(session *mgo.Session) (string, error) {
	var result bson.M
	err := session.Run(bson.D{{"buildInfo", 1}}, &result)
	if err != nil {
		return "", err
	}

	if version, ok := result["version"]; ok {
		if s, ok := version.(string); ok {
			return s, nil
		}
		return "", fmt.Errorf("version type assertion error[%v]", version)
	}
	return "", fmt.Errorf("version not found")
}

// get current db version and compare to threshold. Return whether the result
// is bigger or equal to the input threshold.
func GetAndCompareVersion(session *mgo.Session, threshold string) bool {
	compare, err := GetDBVersion(session)
	if err != nil {
		return false
	}

	compareArr := strings.Split(compare, ".")
	thresholdArr := strings.Split(threshold, ".")
	if len(compareArr) < 2 || len(thresholdArr) < 2 {
		return false
	}

	for i := 0; i < 2; i++ {
		compareEle, errC := strconv.Atoi(compareArr[i])
		thresholdEle, errT := strconv.Atoi(thresholdArr[i])
		if errC != nil || errT != nil || compareEle < thresholdEle {
			return false
		}
	}
	return true
}

// get newest oplog
func GetNewestTimestamp(session *mgo.Session) (bson.MongoTimestamp, error) {
	var retMap map[string]interface{}
	err := session.DB(localDB).C(dbpool.OplogNS).Find(bson.M{}).Sort("-$natural").Limit(1).One(&retMap)
	if err != nil {
		return 0, err
	}
	return retMap[QueryTs].(bson.MongoTimestamp), nil
}

// get oldest oplog
func GetOldestTimestamp(session *mgo.Session) (bson.MongoTimestamp, error) {
	var retMap map[string]interface{}
	err := session.DB(localDB).C(dbpool.OplogNS).Find(bson.M{}).Limit(1).One(&retMap)
	if err != nil {
		return 0, err
	}
	return retMap[QueryTs].(bson.MongoTimestamp), nil
}
