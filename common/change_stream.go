package utils

import (
	"context"
	"fmt"
	LOG "github.com/vinllen/log4go"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"

	"sort"
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
	conn      *MongoCommunityConn
}

func NewChangeStreamConn(src string,
	mode string,
	fullDoc bool,
	specialDb string,
	filterFunc func(name string) bool,
	watchStartTime interface{},
	batchSize int32,
	sourceDbversion string,
	sslRootFile string) (*ChangeStreamConn, error) {

	conn, err := NewMongoCommunityConn(src, mode, true, ReadWriteConcernMajority, "", sslRootFile)
	if err != nil {
		return nil, fmt.Errorf("NewChangeStreamConn source[%v %v] build connection failed: %v",
			src, mode, err)
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
			// ResumeToken，sourceDbversion >= 4.2 use StartAfter, < 4.2 use ResumeAfter
			if val_ver, _ := GetAndCompareVersion(nil, "4.2.0", sourceDbversion); val_ver {
				ops.SetStartAfter(watchStartTime)
			} else {
				ops.SetResumeAfter(watchStartTime)
			}
		}
	}

	if fullDoc {
		ops.SetFullDocument(options.UpdateLookup)
	}

	var csHandler *mongo.ChangeStream
	if specialDb == VarSpecialSourceDBFlagAliyunServerless {
		_, dbs, err := GetDbNamespace(src, filterFunc, sslRootFile)
		if err != nil {
			return nil, fmt.Errorf("GetDbNamespace failed: %v", err)
		}
		dbList := make([]string, 0, len(dbs))
		for name := range dbs {
			dbList = append(dbList, name)
		}
		sort.Strings(dbList)

		if len(dbList) == 0 {
			return nil, fmt.Errorf("db list is empty")
		}

		// TODO(jianyou) deprecate aliyun_serverless
		//ops.SetMultiDbSelections("(" + strings.Join(dbList, "|") + ")")

		LOG.Info("change stream options with aliyun_serverless: %v", printCsOption(ops))
		// csHandler, err = client.Database("non-exist-database-shake").Watch(ctx, mongo.Pipeline{}, ops)
		csHandler, err = conn.Client.Database("serverless-shake-fake-db").
			Collection("serverless-shake-fake-collection").
			Watch(conn.ctx, mongo.Pipeline{}, ops)
		// csHandler, err = client.Database(dbList[0]).Collection("serverless-shake-fake-collection").Watch(ctx, mongo.Pipeline{}, ops)
		if err != nil {
			return nil, fmt.Errorf("client[%v] create change stream handler failed[%v]", src, err)
		}
	} else {
		LOG.Info("new change stream with options: %v", printCsOption(ops))

		csHandler, err = conn.Client.Watch(conn.ctx, mongo.Pipeline{}, ops)
		if err != nil {
			if conn != nil {
				conn.Close()
			}
			LOG.Error("client[%v] create change stream handler failed[%v]", src, err)
			return nil, fmt.Errorf("client[%v] create change stream handler failed[%v]", src, err)
		}
	}

	return &ChangeStreamConn{
		Client:    conn.Client,
		CsHandler: csHandler,
		Ops:       ops,
		ctx:       conn.ctx,
		conn:      conn,
	}, nil
}

func (csc *ChangeStreamConn) Close() {
	if csc.CsHandler != nil {
		csc.CsHandler.Close(csc.ctx)
		csc.CsHandler = nil
	}

	// 关闭ChangeStream时，同时关闭NewMongoCommunityConn连接
	if csc.conn != nil {
		csc.conn.Close()
	}
	if csc.Client != nil {
		csc.Client = nil
	}
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
	//if ops.MultiDbSelections != "" {
	//	ret = fmt.Sprintf("%v MultiDbSelections[%v]", ret, ops.MultiDbSelections)
	//}

	return ret
}
