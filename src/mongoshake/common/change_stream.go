package utils

import (
	"context"
	"time"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/bson/primitive"
	LOG "github.com/vinllen/log4go"
)

const (
	contextTimeout      = 60 * time.Second
	int32max            = (int64(1) << 33) - 1
	changeStreamTimeout = 24 // hours
)

type ChangeStreamConn struct {
	Client    *mongo.Client
	CsHandler *mongo.ChangeStream
	ctx       context.Context
}

func NewChangeStreamConn(src string, watchStartTime int64) (*ChangeStreamConn, error) {
	client, err := mongo.NewClient(options.Client().ApplyURI(src))
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
	csHandler, err := client.Watch(ctx, mongo.Pipeline{}, &options.ChangeStreamOptions{
		StartAtOperationTime: &primitive.Timestamp{
			T: uint32(watchStartTime >> 32),
			I: uint32(watchStartTime & int32max),
		},
		MaxAwaitTime: &waitTime,
	})
	if err != nil {
		return nil, fmt.Errorf("client[%v] create change stream handler failed[%v]", src, err)
	}

	LOG.Info("create change stream with timestamp[%v]", ExtractTimestampForLog(watchStartTime))

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
