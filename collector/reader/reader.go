package sourceReader

// read from the source

import (
	"fmt"

	utils "github.com/alibaba/MongoShake/v2/common"
	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo/bson"
)

var (
	BatchSize       = 8192
	PrefetchPercent = 0.2
)

type Reader interface {
	Name() string                               // reader name
	StartFetcher()                              // start fetcher
	SetQueryTimestampOnEmpty(interface{})       // set query timestamp when first start
	UpdateQueryTimestamp(bson.MongoTimestamp)   // update query timestamp
	Next() ([]byte, error)                      // fetch next oplog/event
	EnsureNetwork() error                       // ensure network
	FetchNewestTimestamp() (interface{}, error) // only used in EventReader that fetch PBRT
}

// used in internal channel, include oplog or event
type retOplog struct {
	log []byte // log/event content
	err error  // error detail message
}

func CreateReader(fetchMethod string, src string, replset string) (Reader, error) {
	switch fetchMethod {
	case utils.VarIncrSyncMongoFetchMethodOplog:
		return NewOplogReader(src, replset), nil
	case utils.VarIncrSyncMongoFetchMethodChangeStream:
		return NewEventReader(src, replset), nil
	default:
		return nil, fmt.Errorf("unknown reader type[%v]", fetchMethod)
	}

	LOG.Critical("can't see me!")
	return nil, nil
}
