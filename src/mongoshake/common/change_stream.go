package utils

import (
	"context"
	"time"
	"fmt"
	"strings"
	LOG "github.com/vinllen/log4go"

	"github.com/vinllen/mongo-go-driver/mongo/options"
	"github.com/vinllen/mongo-go-driver/mongo"
	"github.com/vinllen/mongo-go-driver/mongo/readpref"
	"github.com/vinllen/mongo-go-driver/mongo/readconcern"
	"github.com/vinllen/mongo-go-driver/bson/primitive"
)

const (
	contextTimeout      = 60 * time.Second
	changeStreamTimeout = 24 // hours
)

type ChangeStreamConn struct {
	Client    *mongo.Client
	CsHandler *mongo.ChangeStream
	Ops       *options.ChangeStreamOptions
	ctx       context.Context
}

func NewChangeStreamConn(src string,
	mode string,
	fullDoc bool,
	specialDb string,
	filterFunc func(name string) bool,
	watchStartTime interface{},
	batchSize int32) (*ChangeStreamConn, error) {
	// init client ops
	clientOps := options.Client().ApplyURI(src)

	// clientOps.ReadConcern = readconcern.Local()

	// read preference
	readPreference := readpref.Primary()
	switch mode {
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
		MaxAwaitTime: &waitTime,
		BatchSize:    &batchSize,
	}
	if watchStartTime != nil {
		if val, ok := watchStartTime.(int64); ok {
			if (val >> 32) > 1 {
				startTime := &primitive.Timestamp{
					T: uint32(val >> 32),
					I: uint32(val & Int32max),
				}
				ops.SetStartAtOperationTime(startTime)
			}
		} else {
			// ResumeToken
			ops.SetStartAfter(watchStartTime)
		}
	}

	if fullDoc {
		ops.SetFullDocument(options.UpdateLookup)
	}

	var csHandler *mongo.ChangeStream
	if specialDb == VarSpecialSourceDBFlagAliyunServerless {
		_, dbs, err := GetDbNamespace(src, filterFunc)
		if err != nil {
			return nil, fmt.Errorf("GetDbNamespace failed: %v", err)
		}
		dbList := make([]string, 0, len(dbs))
		for name := range dbs {
			dbList = append(dbList, name)
		}

		if len(dbList) == 0 {
			return nil, fmt.Errorf("db list is empty")
		}

		ops.SetMultiDbSelections("(" + strings.Join(dbList, "|") + ")")

		LOG.Info("change stream options with aliyun_serverless: %v", printCsOption(ops))
		// csHandler, err = client.Database("non-exist-database-shake").Watch(ctx, mongo.Pipeline{}, ops)
		csHandler, err = client.Database("serverless-shake-fake-db").Collection("serverless-shake-fake-collection").Watch(ctx, mongo.Pipeline{}, ops)
		if err != nil {
			return nil, fmt.Errorf("client[%v] create change stream handler failed[%v]", src, err)
		}
	} else {
		LOG.Info("change stream options: %v", printCsOption(ops))
		csHandler, err = client.Watch(ctx, mongo.Pipeline{}, ops)
		if err != nil {
			return nil, fmt.Errorf("client[%v] create change stream handler failed[%v]", src, err)
		}
	}

	return &ChangeStreamConn{
		Client:    client,
		CsHandler: csHandler,
		Ops:       ops,
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

func (csc *ChangeStreamConn) TryNext() (bool, []byte) {
	if ok := csc.CsHandler.TryNext(csc.ctx); !ok {
		return false, nil
	}
	return true, csc.CsHandler.Current
}

func (csc *ChangeStreamConn) ResumeToken() interface{} {
	out := csc.CsHandler.ResumeToken()
	if len(out) == 0 {
		return nil
	}
	return out
}

func printCsOption(ops *options.ChangeStreamOptions) string {
	var ret string
	if ops.BatchSize != nil {
		ret = fmt.Sprintf("%v BatchSize[%v]", ret, *ops.BatchSize)
	}
	if ops.Collation != nil {
		ret = fmt.Sprintf("%v Collation[%v]", ret, *ops.Collation)
	}
	if ops.FullDocument != nil {
		ret = fmt.Sprintf("%v FullDocument[%v]", ret, *ops.FullDocument)
	}
	if ops.MaxAwaitTime != nil {
		ret = fmt.Sprintf("%v MaxAwaitTime[%v]", ret, *ops.MaxAwaitTime)
	}
	if ops.ResumeAfter != nil {
		ret = fmt.Sprintf("%v ResumeAfter[%v]", ret, ops.ResumeAfter)
	}
	if ops.StartAtOperationTime != nil {
		ret = fmt.Sprintf("%v StartAtOperationTime[%v]", ret, *ops.StartAtOperationTime)
	}
	if ops.StartAfter != nil {
		ret = fmt.Sprintf("%v StartAfter[%v]", ret, ops.StartAfter)
	}
	if ops.MultiDbSelections != "" {
		ret = fmt.Sprintf("%v MultiDbSelections[%v]", ret, ops.MultiDbSelections)
	}

	return ret
}
