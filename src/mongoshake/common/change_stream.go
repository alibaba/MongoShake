package utils

import (
	"go.mongodb.org/mongo-driver/mongo"
	"context"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
	"fmt"

	mdBson "go.mongodb.org/mongo-driver/bson/primitive"
)

const (
	contextTimeout = 60 * time.Second
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

	ctx, _ := context.WithTimeout(context.Background(), contextTimeout)

	if err = client.Connect(ctx); err != nil {
		return nil, fmt.Errorf("change stream connect src[%v] failed[%v]", src, err)
	}

	// ping
	if err = client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("client[%v] ping failed[%v]", src, err)
	}

	csHandler, err := client.Watch(ctx, mongo.Pipeline{
		mdBson.D {
			{
				Key:   "startAtOperationTime",
				Value: watchStartTime,
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("client[%v] create change stream handler failed[%v]", src, err)
	}

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
