package utils

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
)

const (
	DBRefRef = "$ref"
	DBRefId  = "$id"
	DBRefDb  = "$db"

	LocalDB  = "local"
	ConfigDB = "config"

	QueryTs     = "ts"
	SettingsCol = "settings"
	ShardCol    = "shards"
	ChunkCol    = "chunks"
	CollectionCol = "collections"

	HashedShard = "hashed"
	RangedShard = "ranged"
)

type MongoSource struct {
	URL     string
	Replset string
	Gids        []string
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
func GetAndCompareVersion(session *mgo.Session, threshold string) (bool, error) {
	compare, err := GetDBVersion(session)
	if err != nil {
		return false, err
	}

	compareArr := strings.Split(compare, ".")
	thresholdArr := strings.Split(threshold, ".")
	if len(compareArr) < 2 || len(thresholdArr) < 2 {
		return false, err
	}

	for i := 0; i < 2; i++ {
		compareEle, errC := strconv.Atoi(compareArr[i])
		thresholdEle, errT := strconv.Atoi(thresholdArr[i])
		if errC != nil || errT != nil {
			return false, fmt.Errorf("errC:[%v], errT:[%v]", errC, errT)
		}

		if compareEle > thresholdEle {
			return true, nil
		} else if compareEle < thresholdEle {
			return false, fmt.Errorf("compareEle[%v] < thresholdEle[%v]", compareEle, thresholdEle)
		}
	}
	return true, nil
}

// get balancer status from config server
func GetBalancerStatusByUrl(url string) (bool, error) {
	var conn *MongoConn
	var err error
	if conn, err = NewMongoConn(url, ConnectModePrimary, true); conn == nil || err != nil {
		return true, err
	}
	defer conn.Close()

	var retMap map[string]interface{}
	err = conn.Session.DB(ConfigDB).C(SettingsCol).Find(bson.M{"_id": "balancer"}).Limit(1).One(&retMap)
	if err != nil {
		return true, err
	}
	return !retMap["stopped"].(bool), nil
}

type ChunkRange struct {
	Min interface{}
	Max interface{}
}

type ShardCollection struct {
	Chunks []*ChunkRange
	Key string
	ShardType string
}

// {replset: {namespace: []ChunkRange} }
type ShardingChunkMap map[string]map[string]*ShardCollection

type DBChunkMap map[string]*ShardCollection

func GetChunkMapByUrl(url string) (ShardingChunkMap, error) {
	var conn *MongoConn
	var err error
	if conn, err = NewMongoConn(url, ConnectModePrimary, true); conn == nil || err != nil {
		return nil, err
	}
	defer conn.Close()

	chunkMap := make(ShardingChunkMap)
	type ShardDoc struct {
		Tag  string `bson:"_id"`
		Host string `bson:"host"`
	}
	shardMap := make(map[string]string)
	var shardDoc ShardDoc
	shardIter := conn.Session.DB(ConfigDB).C(ShardCol).Find(bson.M{}).Iter()
	for shardIter.Next(&shardDoc) {
		replset := strings.Split(shardDoc.Host, "/")[0]
		shardMap[shardDoc.Tag] = replset
		chunkMap[replset] = make(DBChunkMap)
	}

	type ChunkDoc struct {
		Ns    string    `bson:"ns"`
		Min   *bson.Raw `bson:"min"`
		Max   *bson.Raw `bson:"max"`
		Shard string    `bson:"shard"`
	}
	var chunkDoc ChunkDoc
	chunkIter := conn.Session.DB(ConfigDB).C(ChunkCol).Find(bson.M{}).Sort("min").Iter()
	for chunkIter.Next(&chunkDoc) {
		replset := shardMap[chunkDoc.Shard]
		if _, ok := chunkMap[replset][chunkDoc.Ns]; !ok {
			key, shardType, err := GetColShardType(conn.Session, chunkDoc.Ns)
			if err != nil {
				return nil ,err
			}
			chunkMap[replset][chunkDoc.Ns] = &ShardCollection{Key: key, ShardType: shardType}
		}
		var minD, maxD bson.D
		err1 := bson.Unmarshal(chunkDoc.Min.Data, &minD)
		err2 := bson.Unmarshal(chunkDoc.Max.Data, &maxD)
		if err1 != nil || err2 != nil || len(minD) != 1 || len(maxD) != 1 || minD[0].Name != maxD[0].Name {
			return nil, fmt.Errorf("GetChunkMapByUrl get illegal chunk doc min[%v] max[%v]",
				chunkDoc.Min, chunkDoc.Max)
		}
		chunkRange := &ChunkRange{Min: minD[0].Value, Max: maxD[0].Value}
		shardCol := chunkMap[replset][chunkDoc.Ns]
		shardCol.Chunks = append(shardCol.Chunks, chunkRange)
	}
	return chunkMap, nil
}

func GetColShardType(session *mgo.Session, namespace string) (string, string, error) {
	colDoc := make(map[string]interface{})
	if err := session.DB(ConfigDB).C(CollectionCol).Find(bson.M{"_id": namespace}).One(&colDoc); err != nil {
		return "", "", err
	}
	for key, shardType := range colDoc["key"].(map[string]interface {}) {
		switch shardType.(type) {
		case string:
			return key, HashedShard, nil
		case float64:
			return key, RangedShard, nil
		}
		break
	}
	return "", "", fmt.Errorf("GetColShardType meet unknown ShakeKey type %v", colDoc["key"])
}

func IsNotFound(err error) bool {
	return err.Error() == mgo.ErrNotFound.Error()
}

func ApplyOpsFilter(key string) bool {
	// convert to map if has more later
	k := strings.TrimSpace(key)
	if k == "$db" {
		// 40621, $db is not allowed in OP_QUERY requests
		return true
	} else if k == "ui" {
		return true
	}

	return false
}

// get newest oplog
func GetNewestTimestampBySession(session *mgo.Session) (bson.MongoTimestamp, error) {
	var retMap map[string]interface{}
	err := session.DB(LocalDB).C(OplogNS).Find(bson.M{}).Sort("-$natural").Limit(1).One(&retMap)
	if err != nil {
		return 0, err
	}
	return retMap[QueryTs].(bson.MongoTimestamp), nil
}

// get oldest oplog
func GetOldestTimestampBySession(session *mgo.Session) (bson.MongoTimestamp, error) {
	var retMap map[string]interface{}
	err := session.DB(LocalDB).C(OplogNS).Find(bson.M{}).Limit(1).One(&retMap)
	if err != nil {
		return 0, err
	}
	return retMap[QueryTs].(bson.MongoTimestamp), nil
}

func GetNewestTimestampByUrl(url string) (bson.MongoTimestamp, error) {
	var conn *MongoConn
	var err error
	if conn, err = NewMongoConn(url, ConnectModeSecondaryPreferred, true); conn == nil || err != nil {
		return 0, err
	}
	defer conn.Close()

	return GetNewestTimestampBySession(conn.Session)
}

func GetOldestTimestampByUrl(url string) (bson.MongoTimestamp, error) {
	var conn *MongoConn
	var err error
	if conn, err = NewMongoConn(url, ConnectModeSecondaryPreferred, true); conn == nil || err != nil {
		return 0, err
	}
	defer conn.Close()

	return GetOldestTimestampBySession(conn.Session)
}

type TimestampNode struct {
	Oldest bson.MongoTimestamp
	Newest bson.MongoTimestamp
}

/*
 * get all newest timestamp
 * return:
 *     map: whole timestamp map, key: replset name, value: struct that includes the newest and oldest timestamp
 *     bson.MongoTimestamp: the biggest of the newest timestamp
 *     bson.MongoTimestamp: the smallest of the newest timestamp
 *     error: error
 */
func GetAllTimestamp(sources []*MongoSource) (map[string]TimestampNode, bson.MongoTimestamp,
	bson.MongoTimestamp, bson.MongoTimestamp, bson.MongoTimestamp, error) {
	smallestNew := bson.MongoTimestamp(math.MaxInt64)
	biggestNew := bson.MongoTimestamp(0)
	smallestOld := bson.MongoTimestamp(math.MaxInt64)
	biggestOld := bson.MongoTimestamp(0)
	tsMap := make(map[string]TimestampNode)
	for _, src := range sources {
		newest, err := GetNewestTimestampByUrl(src.URL)
		if err != nil {
			return nil, 0, 0, 0, 0, err
		}

		oldest, err := GetOldestTimestampByUrl(src.URL)
		if err != nil {
			return nil, 0, 0, 0, 0, err
		}
		tsMap[src.Replset] = TimestampNode{
			Oldest: oldest,
			Newest: newest,
		}

		if newest > biggestNew {
			biggestNew = newest
		}
		if newest < smallestNew {
			smallestNew = newest
		}
		if oldest > biggestOld {
			biggestOld = oldest
		}
		if oldest < smallestOld {
			smallestOld = oldest
		}
	}
	return tsMap, biggestNew, smallestNew, biggestOld, smallestOld, nil
}

// adjust dbRef order: $ref, $id, $db, others
func AdjustDBRef(input bson.M, dbRef bool) bson.M {
	if dbRef {
		doDfsDBRef(input)
	}
	return input
}

// change bson.M to bson.D when "$ref" find
func doDfsDBRef(input interface{}) {
	// only handle bson.M
	if _, ok := input.(bson.M); !ok {
		return
	}

	inputMap := input.(bson.M)

	for k, v := range inputMap {
		switch vr := v.(type) {
		case bson.M:
			doDfsDBRef(vr)
			if HasDBRef(vr) {
				inputMap[k] = SortDBRef(vr)
			}
		case bson.D:
			for id, ele := range vr {
				doDfsDBRef(ele)
				if HasDBRef(ele.Value.(bson.M)) {
					vr[id].Value = SortDBRef(ele.Value.(bson.M))
				}
			}
		}
	}
}

func HasDBRef(object bson.M) bool {
	_, hasRef := object[DBRefRef]
	_, hasId := object[DBRefId]
	if hasRef && hasId {
		return true
	}
	return false
}

func SortDBRef(input bson.M) bson.D {
	var output bson.D
	output = append(output, bson.DocElem{Name: DBRefRef, Value: input[DBRefRef]})

	if _, ok := input[DBRefId]; ok {
		output = append(output, bson.DocElem{Name: DBRefId, Value: input[DBRefId]})
	}
	if _, ok := input[DBRefDb]; ok {
		output = append(output, bson.DocElem{Name: DBRefDb, Value: input[DBRefDb]})
	}

	// add the others
	for key, val := range input {
		if key == DBRefRef || key == DBRefId || key == DBRefDb {
			continue
		}

		output = append(output, bson.DocElem{Name: key, Value: val})
	}
	return output
}
