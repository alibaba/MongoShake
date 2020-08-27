package utils

import (
	"fmt"
	"context"

	LOG "github.com/vinllen/log4go"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

)

type MongoCommunityConn struct {
	Client *mongo.Client
	URL    string
	ctx    context.Context
}

func NewMongoCommunityConn(url string, connectMode string, timeout bool, readConcern, writeConcern string) (*MongoCommunityConn, error) {
	if connectMode == VarMongoConnectModeStandalone {
		url += "?connect=direct"
	}

	clientOps := options.Client().ApplyURI(url)

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
	if mode, err := readpref.ModeFromString(connectMode); err != nil {
		return nil, fmt.Errorf("create connectMode[%v] failed: %v", connectMode, err)
	} else if opts, err := readpref.New(mode); err != nil {
		return nil, fmt.Errorf("new mode with connectMode[%v] failed: %v", connectMode, err)
	} else {
		clientOps.SetReadPreference(opts)
	}

	// set timeout
	if !timeout {
		clientOps.SetConnectTimeout(0)
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
	}, nil
}
