package utils

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	bson2 "github.com/vinllen/mongo-go-driver/bson"
	"io/ioutil"
	"time"

	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mongo-go-driver/mongo"
	"github.com/vinllen/mongo-go-driver/mongo/options"
	"github.com/vinllen/mongo-go-driver/mongo/readconcern"
	"github.com/vinllen/mongo-go-driver/mongo/readpref"
	"github.com/vinllen/mongo-go-driver/mongo/writeconcern"
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
			if certBlock != nil {
				return nil, fmt.Errorf("multiple CERTIFICATE sections in .pem file")
			}

			certBlock = block
		}

		data = rest
	}

	return certBlock.Bytes, nil
}

func NewMongoCommunityConn(url string, connectMode string, timeout bool, readConcern,
	writeConcern string, sslRootFile string) (*MongoCommunityConn, error) {

	clientOps := options.Client().ApplyURI(url)

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
		// TODO, no standalone, choose nearest
		fallthrough
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
		return nil, fmt.Errorf("ping to %v failed: %v", BlockMongoUrlPassword(url, "***"), err)
	}

	LOG.Info("New session to %s successfully", BlockMongoUrlPassword(url, "***"))
	return &MongoCommunityConn{
		Client: client,
		URL:    url,
		ctx: ctx,
	}, nil
}

func (conn *MongoCommunityConn) Close() {
	LOG.Info("Close client with %s", BlockMongoUrlPassword(conn.URL, "***"))
	conn.Client.Disconnect(conn.ctx)
}

func (conn *MongoCommunityConn) IsGood() bool {
	if err := conn.Client.Ping(nil, nil); err != nil {
		return false
	}

	return true
}

func (conn *MongoCommunityConn) HasOplogNs() bool {
	if ns, err := conn.Client.Database("local").ListCollectionNames(nil, bson2.M{}); err == nil {
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
		RunCommand(conn.ctx, bson2.D{{"replSetGetStatus", 1}}).DecodeBytes()
	if err != nil {
		LOG.Warn("Replica set name not found in system.replset: %v", err)
		return ""
	}

	id, ok := res.Lookup("set").StringValueOK()
	if !ok {
		LOG.Warn("Replica set name not found, is empty")
		return ""
	}

	return id
}

func (conn *MongoCommunityConn) HasUniqueIndex() (bool, string, string) {
	checkNs := make([]NS, 0, 128)
	var databases []string
	var err error
	if databases, err = conn.Client.ListDatabaseNames(nil, bson2.M{}); err != nil {
		LOG.Critical("Couldn't get databases from remote server: %v", err)
		return false, "", ""
	}

	for _, db := range databases {
		if db != "admin" && db != "local" {
			coll, _ := conn.Client.Database(db).ListCollectionNames(nil, bson2.M{})
			for _, c := range coll {
				if c != "system.profile" {
					// push all collections
					checkNs = append(checkNs, NS{Database: db, Collection: c})
				}
			}
		}
	}

	for _, ns := range checkNs {
		cursor, _ := conn.Client.Database(ns.Database).Collection(ns.Collection).Indexes().List(nil)
		for cursor.Next(nil) {


			unique, uerr := cursor.Current.LookupErr("unique")
			if uerr == nil && unique.Boolean() == true {
				LOG.Info("Found unique index %s on %s.%s in auto shard mode",
					cursor.Current.Lookup("name").StringValue(), ns.Database, ns.Collection)
				return true, ns.Database, ns.Collection
			}
		}
	}

	return false, "", ""
}

func (conn *MongoCommunityConn) CurrentDate() int64 {

	res, err := conn.Client.Database("admin").
		RunCommand(conn.ctx, bson2.D{{"replSetGetStatus", 1}}).DecodeBytes()
	if err != nil {
		LOG.Warn("Replica set operationTime not found in system.replset: %v", err)
		return 0
	}

	date, ok := res.Lookup("operationTime").DateTimeOK()
	if !ok {
		LOG.Warn("Replica set operationTime not found, is empty")
		return 0
	}

	return date
}