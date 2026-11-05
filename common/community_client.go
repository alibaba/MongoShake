package utils

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

	l "github.com/alibaba/MongoShake/v2/pkg/log"
)

type MongoCommunityConn struct {
	Client *mongo.Client
	URL    string
	ctx    context.Context
}

func NewMongoCommunityConn(url string, connectMode string, timeout bool, readConcern,
	writeConcern string, sslRootFile string) (*MongoCommunityConn, error) {

	// URL encoding at first
	encodedURL, err := EncodeMongoURI(url)
	if err != nil {
		return nil, fmt.Errorf("failed to encode MongoDB URL: %v", err)
	}
	l.Logger.Debugf("encodedURL:[%v]", encodedURL)

	clientOps := options.Client().ApplyURI(encodedURL)
	//clientOps := options.Client().ApplyURI(url)

	// tls tlsInsecure + tlsCaFile
	if sslRootFile != "" {
		tlsConfig, err := NewChainVerifyingTLSConfig(sslRootFile)
		if err != nil {
			return nil, fmt.Errorf("load rootCaFile[%v] failed: %v", sslRootFile, err)
		}

		clientOps.SetTLSConfig(tlsConfig)
	}

	// read concern
	switch readConcern {
	case ReadWriteConcernDefault:
	default:
		clientOps.SetReadConcern(readconcern.New(readconcern.Level(readConcern)))
	}

	// write concern
	switch writeConcern {
	case ReadWriteConcernMajority:
		clientOps.SetWriteConcern(writeconcern.New(writeconcern.WMajority()))
	}

	// read pref
	readPreference := readpref.Primary()
	switch connectMode {
	case VarMongoConnectModePrimary:
		readPreference = readpref.Primary()
	case VarMongoConnectModeSecondaryPreferred:
		readPreference = readpref.SecondaryPreferred()
	case VarMongoConnectModeStandalone:
		clientOps.SetDirect(true)
		readPreference = readpref.Nearest()
	case VarMongoConnectModeNearset:
		readPreference = readpref.Nearest()
	default:
		readPreference = readpref.Primary()
	}
	clientOps.SetReadPreference(readPreference)

	// set timeout
	if !timeout {
		clientOps.SetConnectTimeout(0)
	} else {
		clientOps.SetConnectTimeout(20 * time.Minute)
	}

	//clientOps.SetMaxConnIdleTime(1 * time.Hour)

	// create default context
	ctx := context.Background()

	// connect
	client, err := mongo.NewClient(clientOps)
	if err != nil {
		return nil, fmt.Errorf("new client failed: %v", err)
	}
	if err := client.Connect(ctx); err != nil {
		disconnect(client)
		return nil, fmt.Errorf("connect to %s failed: %v", BlockMongoUrlPassword(url, "***"), err)
	}

	// ping
	if err = client.Ping(ctx, clientOps.ReadPreference); err != nil {
		disconnect(client)
		return nil, fmt.Errorf("ping to %v failed: %v\n"+
			"If mongo server is standalone(single node) or conn address is different with mongo server address"+
			" try a standalone mode by mongodb://ip:port/admin?directConnection=true",
			BlockMongoUrlPassword(url, "***"), err)
	}

	l.Logger.Infof("New session to %s successfully", BlockMongoUrlPassword(url, "***"))
	return &MongoCommunityConn{
		Client: client,
		URL:    url,
		ctx:    ctx,
	}, nil
}

func (conn *MongoCommunityConn) Close() {
	l.Logger.Infof("Close client with %s", BlockMongoUrlPassword(conn.URL, "***"))
	disconnect(conn.Client)
}

// disconnect bounds Disconnect so endSessions against a dead topology cannot block forever.
func disconnect(client *mongo.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = client.Disconnect(ctx)
}

func (conn *MongoCommunityConn) IsGood() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := conn.Client.Ping(ctx, nil); err != nil {
		return false
	}

	return true
}

func (conn *MongoCommunityConn) HasOplogNs(queryCondition bson.M) bool {
	if ns, err := conn.Client.Database("local").ListCollectionNames(nil, queryCondition); err == nil {
		for _, table := range ns {
			if table == OplogNS {
				return true
			}
		}
	}

	return false
}

func (conn *MongoCommunityConn) AcquireReplicaSetName() string {

	res, err := conn.Client.Database("admin").
		RunCommand(conn.ctx, bson.D{{"replSetGetStatus", 1}}).DecodeBytes()
	if err != nil {
		l.Logger.Warnf("Replica set name not found in system.replset: %v", err)
		return ""
	}

	id, ok := res.Lookup("set").StringValueOK()
	if !ok {
		l.Logger.Warnf("Replica set name not found, is empty")
		return ""
	}

	return id
}

func (conn *MongoCommunityConn) HasUniqueIndex(queryCondition bson.M) bool {
	nsList, err := conn.ListUniqueIndexNamespaces(queryCondition)
	if err != nil {
		l.Logger.Errorf("list unique index namespaces failed: %v", err)
		return false
	}
	return len(nsList) != 0
}

// ListUniqueIndexNamespaces returns the namespaces whose indexes constrain more
// than _id: the index is unique and its leading key field is not _id. It is the
// "auto" shard key input, so an unreadable namespace is reported as an error
// instead of being silently treated as safe.
func (conn *MongoCommunityConn) ListUniqueIndexNamespaces(queryCondition bson.M) ([]string, error) {
	checkNs := make([]NS, 0, 128)
	databases, err := conn.Client.ListDatabaseNames(nil, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("list databases: %v", err)
	}

	for _, db := range databases {
		if db == "admin" || db == "local" || db == "config" {
			continue
		}
		collections, err := conn.Client.Database(db).ListCollectionNames(nil, queryCondition)
		if err != nil {
			return nil, fmt.Errorf("list collections of db[%s]: %v", db, err)
		}
		for _, collection := range collections {
			if collection != "system.profile" {
				checkNs = append(checkNs, NS{Database: db, Collection: collection})
			}
		}
	}
	l.Logger.Infof("ListUniqueIndexNamespaces checkNs:%v", checkNs)

	nsList := make([]string, 0, len(checkNs))
	for _, ns := range checkNs {
		unique, err := conn.hasUniqueIndexBesidesID(ns.Database, ns.Collection)
		if err != nil {
			return nil, err
		}
		if unique {
			l.Logger.Infof("Found unique index besides _id on %s.%s in auto shard mode",
				ns.Database, ns.Collection)
			nsList = append(nsList, ns.Database+"."+ns.Collection)
		}
	}

	sort.Strings(nsList)
	return nsList, nil
}

// hasUniqueIndexBesidesID reports whether the collection carries a unique index
// that _id based dispatch cannot honour. A unique index led by _id adds no
// constraint beyond the primary key, so it is ignored. A collection dropped
// while the scan runs is reported as not constraining: it cannot receive more
// oplogs.
func (conn *MongoCommunityConn) hasUniqueIndexBesidesID(db, collection string) (bool, error) {
	cursor, err := conn.Client.Database(db).Collection(collection).Indexes().List(nil)
	if err != nil {
		if isIndexScanSkipError(err) {
			return false, nil
		}
		return false, fmt.Errorf("list indexes of %s.%s: %v", db, collection, err)
	}
	defer cursor.Close(nil)

	for cursor.Next(nil) {
		uniqueValue, err := cursor.Current.LookupErr("unique")
		if err != nil {
			continue
		}
		if unique, ok := uniqueValue.BooleanOK(); !ok || !unique {
			continue
		}
		keyValue, err := cursor.Current.LookupErr("key")
		if err != nil {
			continue
		}
		key, ok := keyValue.DocumentOK()
		if !ok {
			continue
		}
		leading, err := key.IndexErr(0)
		if err != nil || leading.Key() == "_id" {
			continue
		}
		return true, nil
	}

	if err := cursor.Err(); err != nil {
		if isIndexScanSkipError(err) {
			return false, nil
		}
		return false, fmt.Errorf("iterate indexes of %s.%s: %v", db, collection, err)
	}
	return false, nil
}

// isIndexScanSkipError reports the errors that mean the collection cannot carry
// a unique index the dispatch has to honour: it was dropped between
// listCollections and listIndexes (NamespaceNotFound), or the caller did not
// exclude views and it is one (CommandNotSupportedOnView).
func isIndexScanSkipError(err error) bool {
	var commandErr mongo.CommandError
	if errors.As(err, &commandErr) {
		return commandErr.Code == 26 || commandErr.Code == 166
	}
	return false
}

func (conn *MongoCommunityConn) CurrentDate() primitive.Timestamp {

	res, _ := conn.Client.Database("admin").
		RunCommand(conn.ctx, bson.D{{"replSetGetStatus", 1}}).DecodeBytes()

	t, i, ok := res.Lookup("operationTime").TimestampOK()
	if !ok {
		l.Logger.Warnf("Replica set operationTime not found, res[%v]", res)
		return primitive.Timestamp{T: uint32(time.Now().Unix()), I: 0}
	}

	return primitive.Timestamp{T: t, I: i}
}

func (conn *MongoCommunityConn) IsTimeSeriesCollection(dbName string, collName string) bool {
	res, _ := conn.Client.Database(dbName).
		RunCommand(conn.ctx, bson.D{{"collStats", collName}}).DecodeBytes()

	_, timeseries := res.Lookup("timeseries").DocumentOK()

	return timeseries
}

// EncodeMongoURI encodes MongoDB URIs, mainly performing URL encoding on the password part.
// expected to handle the following URI:
// 1) normal one: "mongodb://user:password@localhost:27017/admin"
// 2) with special chars: "mongodb://root:~!@#$^&*()_-=@localhost:27017/admin"
// 3) without path: "mongodb://user:password@localhost:27017"
// 4) auth disabled: "mongodb://localhost:27017"
func EncodeMongoURI(uri string) (string, error) {
	// split scheme and rest
	parts := strings.SplitN(uri, ":", 2)
	if len(parts) != 2 || !strings.HasPrefix(parts[1], "//") {
		return "", fmt.Errorf("invalid URI scheme")
	}
	scheme := parts[0]
	if scheme != "mongodb" {
		return "", fmt.Errorf("unsupported scheme: %s", scheme)
	}
	rest := parts[1][2:] // remove "//"

	// split user:pwd and hosts
	atIndex := strings.LastIndex(rest, "@")
	if atIndex == -1 { // coule be no-auth, do nothing
		//return "", fmt.Errorf("missing '@' in URI")
		return uri, nil
	}
	userInfoPart := rest[:atIndex]
	afterUserInfo := rest[atIndex+1:]

	// split user and password
	userPassParts := strings.SplitN(userInfoPart, ":", 2)
	if len(userPassParts) != 2 {
		return "", fmt.Errorf("missing ':' in username:password")
	}
	username := userPassParts[0]
	password := userPassParts[1]
	if username == "" || password == "" {
		return "", fmt.Errorf("missing username or password in username:password")
	}

	// split query string, then hosts and path
	afterUserInfo, rawQuery, _ := strings.Cut(afterUserInfo, "?")
	hostPathParts := strings.SplitN(afterUserInfo, "/", 2)
	host := hostPathParts[0]
	var path string
	if len(hostPathParts) > 1 {
		path = "/" + hostPathParts[1]
	} else {
		path = ""
	}

	// generate URL
	u := &url.URL{
		Scheme:   scheme,
		User:     url.UserPassword(username, password),
		Host:     host,
		Path:     path,
		RawQuery: rawQuery,
	}

	return u.String(), nil
}
