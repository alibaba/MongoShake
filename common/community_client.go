package utils

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"net/url"
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

func addCACertFromFile(cfg *tls.Config, file string) error {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}

	certBytes, err := loadCert(data)
	if err != nil {
		return err
	}

	cert, err := x509.ParseCertificate(certBytes)
	if err != nil {
		return err
	}

	if cfg.RootCAs == nil {
		cfg.RootCAs = x509.NewCertPool()
	}

	cfg.RootCAs.AddCert(cert)

	return nil
}

func loadCert(data []byte) ([]byte, error) {
	var certBlock *pem.Block

	for certBlock == nil {
		if data == nil || len(data) == 0 {
			return nil, fmt.Errorf(".pem file must have both a CERTIFICATE and an RSA PRIVATE KEY section")
		}
		block, rest := pem.Decode(data)
		if block == nil {
			return nil, fmt.Errorf("invalid .pem file")
		}

		switch block.Type {
		case "CERTIFICATE":
			certBlock = block
		}
		data = rest
	}

	return certBlock.Bytes, nil
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
		tlsConfig := new(tls.Config)

		err := addCACertFromFile(tlsConfig, sslRootFile)
		if err != nil {
			return nil, fmt.Errorf("load rootCaFile[%v] failed: %v", sslRootFile, err)
		}

		// not check hostname
		tlsConfig.InsecureSkipVerify = true

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
		return nil, fmt.Errorf("connect to %s failed: %v", BlockMongoUrlPassword(url, "***"), err)
	}

	// ping
	if err = client.Ping(ctx, clientOps.ReadPreference); err != nil {
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
	_ = conn.Client.Disconnect(conn.ctx)
}

func (conn *MongoCommunityConn) IsGood() bool {
	if err := conn.Client.Ping(nil, nil); err != nil {
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
	checkNs := make([]NS, 0, 128)
	var databases []string
	var err error
	if databases, err = conn.Client.ListDatabaseNames(nil, bson.M{}); err != nil {
		l.Logger.Criticalf("Couldn't get databases from remote server: %v", err)
		return false
	}

	for _, db := range databases {
		if db != "admin" && db != "local" && db != "config" {
			coll, _ := conn.Client.Database(db).ListCollectionNames(nil, queryCondition)
			for _, c := range coll {
				if c != "system.profile" {
					// push all collections
					checkNs = append(checkNs, NS{Database: db, Collection: c})
				}
			}
		}
	}
	l.Logger.Infof("HasUniqueIndex checkNs:%v", checkNs)

	for _, ns := range checkNs {
		cursor, _ := conn.Client.Database(ns.Database).Collection(ns.Collection).Indexes().List(nil)
		for cursor.Next(nil) {

			unique, uErr := cursor.Current.LookupErr("unique")
			if uErr == nil && unique.Boolean() == true {
				l.Logger.Infof("Found unique index %s on %s.%s in auto shard mode",
					cursor.Current.Lookup("name").StringValue(), ns.Database, ns.Collection)
				return true
			}
		}
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

	// split hosts and path
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
		Scheme: scheme,
		User:   url.UserPassword(username, password),
		Host:   host,
		Path:   path,
	}

	return u.String(), nil
}
