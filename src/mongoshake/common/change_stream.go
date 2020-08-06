package utils

import (
	"context"
	"time"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo"
	LOG "github.com/vinllen/log4go"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
)

const (
	contextTimeout      = 60 * time.Second
	changeStreamTimeout = 24 // hours
)

type ChangeStreamConn struct {
	Client    *mongo.Client
	CsHandler *mongo.ChangeStream
	ctx       context.Context
}

func NewChangeStreamConn(src string, mode string, fulldoc bool, watchStartTime int64, batchSize int32) (*ChangeStreamConn, error) {
	// init client ops
	clientOps := options.Client().ApplyURI(src)

	// clientOps.ReadConcern = readconcern.Local()

	// read preference
	readPreference := readpref.Primary()
	switch mode {
	case VarMongoConnectModeSecondaryPreferred:
		readPreference = readpref.SecondaryPreferred()
	case VarMongoConnectModeStandalone:
		// TODO, no standalone, choose nearest here
		fallthrough
	default:
		readPreference = readpref.Nearest()
	}
	clientOps.SetReadPreference(readPreference)
	clientOps.ReadConcern = readconcern.Majority() // mandatory set read concern to majority for change stream

	// create client
	client, err := mongo.NewClient(clientOps)
	if err != nil {
		return nil, fmt.Errorf("create change stream client[%v] failed[%v]", src, err)
	}

	// ctx, _ := context.WithTimeout(context.Background(), contextTimeout)
	ctx := context.Background()

	if err = client.Connect(ctx); err != nil {
		return nil, fmt.Errorf("change stream connect src[%v] failed[%v]", src, err)
	}

	// ping
	if err = client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("client[%v] ping failed[%v]", src, err)
	}

	waitTime := time.Duration(changeStreamTimeout * time.Hour) // hours
	ops := &options.ChangeStreamOptions{
		StartAtOperationTime: &primitive.Timestamp{
			T: uint32(watchStartTime >> 32),
			I: uint32(watchStartTime & Int32max),
		},
		MaxAwaitTime: &waitTime,
		BatchSize:    &batchSize,
	}

	if fulldoc {
		ops.SetFullDocument(options.UpdateLookup)
	}

	csHandler, err := client.Watch(ctx, mongo.Pipeline{}, ops)
	if err != nil {
		return nil, fmt.Errorf("client[%v] create change stream handler failed[%v]", src, err)
	}

	LOG.Info("create change stream with start-time[%v], max-await-time[%v], batch-size[%v]",
		ops.StartAtOperationTime, waitTime, batchSize)

	return &ChangeStreamConn{
		Client:    client,
		CsHandler: csHandler,
		ctx:       ctx,
	}, nil
}

func (csc *ChangeStreamConn) Close() {
	if csc.CsHandler != nil {
		csc.CsHandler.Close(csc.ctx)
		csc.CsHandler = nil
	}
	csc.Client = nil
}

func (csc *ChangeStreamConn) IsNotNil() bool {
	return csc != nil && csc.Client != nil && csc.CsHandler != nil
}

func (csc *ChangeStreamConn) GetNext() (bool, []byte) {
	if ok := csc.CsHandler.Next(csc.ctx); !ok {
		return false, nil
	}
	return true, csc.CsHandler.Current
}
