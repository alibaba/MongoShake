package utils

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"sort"

	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
	bson2 "github.com/vinllen/mongo-go-driver/bson"
)

var (
	QueryTs = "ts"
	localDB = "local"
)

const (
	DBRefRef = "$ref"
	DBRefId  = "$id"
	DBRefDb  = "$db"

	CollectionCapped           = "CollectionScan died due to position in capped" // bigger than 3.0
	CollectionCappedLowVersion = "UnknownError"                                  // <= 3.0 version
)

// for UT only
var (
	GetAllTimestampInUTInput map[string]Pair // replicaSet/MongoS name => <oldest timestamp, newest timestamp>
)

/************************************************/

type MongoSource struct {
	URL         string
	ReplicaName string
	Gids        []string
}

func (ms *MongoSource) String() string {
	return fmt.Sprintf("url[%v], name[%v]", BlockMongoUrlPassword(ms.URL, "***"), ms.ReplicaName)
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
func GetAndCompareVersion(session *mgo.Session, threshold string, compare string) (bool, error) {
	var err error
	if compare == "" {
		if session == nil {
			return false, nil
		}

		compare, err = GetDBVersion(session)
		if err != nil {
			return false, err
		}
	}

	compareArr := strings.Split(compare, ".")
	thresholdArr := strings.Split(threshold, ".")
	if len(compareArr) < 2 || len(thresholdArr) < 2 {
		return false, nil
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
			return false, fmt.Errorf("compare[%v] < threshold[%v]", compare, threshold)
		}
	}
	return true, nil
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
	err := session.DB(localDB).C(OplogNS).Find(bson.M{}).Sort("-$natural").Limit(1).One(&retMap)
	if err != nil {
		return 0, err
	}
	return retMap[QueryTs].(bson.MongoTimestamp), nil
}

// get oldest oplog
func GetOldestTimestampBySession(session *mgo.Session) (bson.MongoTimestamp, error) {
	var retMap map[string]interface{}
	err := session.DB(localDB).C(OplogNS).Find(bson.M{}).Limit(1).One(&retMap)
	if err != nil {
		return 0, err
	}
	return retMap[QueryTs].(bson.MongoTimestamp), nil
}

func GetNewestTimestampByUrl(url string, fromMongoS bool, sslRootFile, sslPEMKeyFile string) (bson.MongoTimestamp, error) {
	var conn *MongoConn
	var err error
	if conn, err = NewMongoConn(url, VarMongoConnectModeSecondaryPreferred, true,
		ReadWriteConcernDefault, ReadWriteConcernDefault, sslRootFile, sslPEMKeyFile); conn == nil || err != nil {
		return 0, err
	}
	defer conn.Close()

	if fromMongoS {
		date := conn.CurrentDate()
		return date, nil
	}

	return GetNewestTimestampBySession(conn.Session)
}

func GetOldestTimestampByUrl(url string, fromMongoS bool, sslRootFile, sslPEMKeyFile string) (bson.MongoTimestamp, error) {
	if fromMongoS {
		return 0, nil
	}

	var conn *MongoConn
	var err error
	if conn, err = NewMongoConn(url, VarMongoConnectModeSecondaryPreferred, true,
		ReadWriteConcernDefault, ReadWriteConcernDefault, sslRootFile, sslPEMKeyFile); conn == nil || err != nil {
		return 0, err
	}
	defer conn.Close()

	return GetOldestTimestampBySession(conn.Session)
}

// deprecated
func IsFromMongos(url string) (bool, error) {
	var conn *MongoConn
	var err error
	if conn, err = NewMongoConn(url, VarMongoConnectModeSecondaryPreferred, true,
		ReadWriteConcernDefault, ReadWriteConcernDefault, "", ""); conn == nil || err != nil {
		return false, err
	}
	return conn.IsMongos(), nil
}

// record the oldest and newest timestamp of each mongod
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
func GetAllTimestamp(sources []*MongoSource, sslRootFile, sslPEMKeyFile string) (map[string]TimestampNode, bson.MongoTimestamp,
	bson.MongoTimestamp, bson.MongoTimestamp, bson.MongoTimestamp, error) {
	smallestNew := bson.MongoTimestamp(math.MaxInt64)
	biggestNew := bson.MongoTimestamp(0)
	smallestOld := bson.MongoTimestamp(math.MaxInt64)
	biggestOld := bson.MongoTimestamp(0)
	tsMap := make(map[string]TimestampNode)

	for _, src := range sources {
		newest, err := GetNewestTimestampByUrl(src.URL, false, sslRootFile, sslPEMKeyFile)
		if err != nil {
			return nil, 0, 0, 0, 0, err
		} else if newest == 0 {
			return nil, 0, 0, 0, 0, fmt.Errorf("illegal newest timestamp == 0")
		}

		oldest, err := GetOldestTimestampByUrl(src.URL, false, sslRootFile, sslPEMKeyFile)
		if err != nil {
			return nil, 0, 0, 0, 0, err
		}
		tsMap[src.ReplicaName] = TimestampNode{
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

// only used in unit test
func GetAllTimestampInUT() (map[string]TimestampNode, bson.MongoTimestamp,
	bson.MongoTimestamp, bson.MongoTimestamp, bson.MongoTimestamp, error) {
	smallestNew := bson.MongoTimestamp(math.MaxInt64)
	biggestNew := bson.MongoTimestamp(0)
	smallestOld := bson.MongoTimestamp(math.MaxInt64)
	biggestOld := bson.MongoTimestamp(0)
	tsMap := make(map[string]TimestampNode)
	for name, ele := range GetAllTimestampInUTInput {
		oldest := ele.First.(bson.MongoTimestamp)
		newest := ele.Second.(bson.MongoTimestamp)
		tsMap[name] = TimestampNode{
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

func IsCollectionCappedError(err error) bool {
	errMsg := err.Error()
	if strings.Contains(errMsg, CollectionCapped) || strings.Contains(errMsg, CollectionCappedLowVersion) {
		return true
	}
	return false
}

// deprecated
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

// used to handle bulk return error
func FindFirstErrorIndexAndMessage(error string) (int, string, bool) {
	subIndex := "index["
	subMsg := "msg["
	subDup := "dup["
	index := strings.Index(error, subIndex)
	if index == -1 {
		return index, "", false
	}

	indexVal := 0
	for i := index + len(subIndex); i < len(error) && error[i] != ']'; i++ {
		// fmt.Printf("%c %d\n", rune(error[i]), int(error[i] - '0'))
		indexVal = indexVal*10 + int(error[i]-'0')
	}

	index = strings.Index(error, subMsg)
	if index == -1 {
		return indexVal, "", false
	}

	i := index + len(subMsg)
	stack := 0
	for ; i < len(error); i++ {
		if error[i] == ']' {
			if stack == 0 {
				break
			} else {
				stack -= 1
			}
		} else if error[i] == '[' {
			stack += 1
		}
	}
	msg := error[index+len(subMsg) : i]

	index = strings.Index(error, subDup)
	if index == -1 {
		return indexVal, msg, false
	}
	i = index + len(subMsg)
	for ; i < len(error) && error[i] != ']'; i++ {
	}
	dupVal := error[index+len(subMsg) : i]

	return indexVal, msg, dupVal == "true"
}

func HasUniqueIndex(index []mgo.Index) bool {
	for _, idx := range index {
		if !strings.HasPrefix(idx.Name, "_id") && idx.Unique == true {
			return true
		}
	}
	return false
}

/**
 * return db namespace. return:
 *     @[]NS: namespace list, e.g., []{"a.b", "a.c"}
 *     @map[string][]string: db->collection map. e.g., "a"->[]string{"b", "c"}
 *     @error: error info
 */
func GetDbNamespace(url string, filterFunc func(name string) bool) ([]NS, map[string][]string, error) {
	var err error
	var conn *MongoCommunityConn
	if conn, err = NewMongoCommunityConn(url, VarMongoConnectModePrimary, true,
		ReadWriteConcernLocal, ReadWriteConcernDefault); conn == nil || err != nil {
		return nil, nil, err
	}
	defer conn.Close()

	var dbNames []string
	if dbNames, err = conn.Client.ListDatabaseNames(nil, bson2.M{}); err != nil {
		err = fmt.Errorf("get database names of mongodb[%s] error: %v", url, err)
		return nil, nil, err
	}
	// sort by db names
	sort.Strings(dbNames)

	nsList := make([]NS, 0, 128)
	for _, db := range dbNames {
		colNames, err := conn.Client.Database(db).ListCollectionNames(nil, bson2.M{})
		if err != nil {
			err = fmt.Errorf("get collection names of mongodb[%s] db[%v] error: %v", url, db, err)
			return nil, nil, err
		}

		// LOG.Info("db[%v] colNames: %v", db, colNames)
		for _, col := range colNames {
			ns := NS{Database: db, Collection: col}
			if strings.HasPrefix(col, "system.") {
				continue
			}
			if filterFunc != nil && filterFunc(ns.Str()) {
				// LOG.Debug("Namespace is filtered. %v", ns.Str())
				continue
			}
			nsList = append(nsList, ns)
		}
	}

	// copy, convert nsList to map
	nsMap := make(map[string][]string, 0)
	for _, ns := range nsList {
		if _, ok := nsMap[ns.Database]; !ok {
			nsMap[ns.Database] = make([]string, 0)
		}
		nsMap[ns.Database] = append(nsMap[ns.Database], ns.Collection)
	}

	return nsList, nsMap, nil
}

/**
 * return all namespace. return:
 *     @map[NS]struct{}: namespace set where key is the namespace while value is useless, e.g., "a.b"->nil, "a.c"->nil
 *     @map[string][]string: db->collection map. e.g., "a"->[]string{"b", "c"}
 *     @error: error info
 */
func GetAllNamespace(sources []*MongoSource, filterFunc func(name string) bool) (map[NS]struct{}, map[string][]string, error) {
	nsSet := make(map[NS]struct{})
	for _, src := range sources {
		nsList, _, err := GetDbNamespace(src.URL, filterFunc)
		if err != nil {
			return nil, nil, err
		}
		for _, ns := range nsList {
			nsSet[ns] = struct{}{}
		}
	}

	// copy
	nsMap := make(map[string][]string, len(sources))
	for ns := range nsSet {
		if _, ok := nsMap[ns.Database]; !ok {
			nsMap[ns.Database] = make([]string, 0)
		}
		nsMap[ns.Database] = append(nsMap[ns.Database], ns.Collection)
	}

	return nsSet, nsMap, nil
}
