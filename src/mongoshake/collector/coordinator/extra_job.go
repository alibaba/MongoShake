package coordinator

import (
	"sync"
	"time"
	"mongoshake/collector/configure"
	"mongoshake/common"

	LOG "github.com/vinllen/log4go"

)

const (
	NameCheckUniqueIndexExistsJob = "CheckIndexExistsJob"
)

var (
	extraJobList = make(map[int][]extraJob)
	lock         sync.Mutex
)

type extraJob interface {
	Name() string
	Run()
}

func AddExtraJob(name string, interval int, input ...interface{}) {
	LOG.Info("start run extra job[%v] with interval[%v]", name, interval)

	lock.Lock()
	defer lock.Unlock()

	switch name {
	case NameCheckUniqueIndexExistsJob:
		collections := input[0].([]string)
		urls := input[1].([]*utils.MongoSource)
		extraJobList[interval] = append(extraJobList[interval], NewCheckUniqueIndexExistsJob(interval, collections, urls))
	}
}

func RunExtraJob(RealSourceIncrSync []*utils.MongoSource) error {
	if len(conf.Options.IncrSyncShardByObjectIdWhiteList) != 0 {
		AddExtraJob(NameCheckUniqueIndexExistsJob, 10, conf.Options.IncrSyncShardByObjectIdWhiteList, RealSourceIncrSync)
	}

	for _, jobList := range extraJobList {
		for _, job := range jobList {
			go job.Run()
		}
	}
	return nil
}

type CheckUniqueIndexExistsJob struct {
	interval    int
	collections []string
	urls        []*utils.MongoSource
}

func NewCheckUniqueIndexExistsJob(interval int, collections []string, urls []*utils.MongoSource) *CheckUniqueIndexExistsJob {
	return &CheckUniqueIndexExistsJob{
		interval:    interval,
		collections: collections,
		urls:        urls,
	}
}

func (cui *CheckUniqueIndexExistsJob) Name() string {
	return NameCheckUniqueIndexExistsJob
}

func (cui *CheckUniqueIndexExistsJob) Run() {
	var err error
	conns := make([]*utils.MongoConn, len(cui.urls))
	for i, source := range cui.urls {
		conns[i], err = utils.NewMongoConn(source.URL, utils.VarMongoConnectModeSecondaryPreferred, true,
			utils.ReadWriteConcernMajority, utils.ReadWriteConcernDefault)
		if err != nil {
			LOG.Error("extra job[%s] connect source[%v] failed: %v", cui.Name(), source.URL, err)
			return
		}
	}

	// parse collection to ns
	nsList := make([]utils.NS, 0, len(cui.collections))
	for _, c := range cui.collections {
		nsList = append(nsList, utils.NewNS(c))
	}

	for range time.NewTicker(time.Duration(cui.interval) * time.Second).C {
		LOG.Debug("extra job[%s] check", cui.Name())
		for i, source := range cui.urls {
			for _, ns := range nsList {
				index, err := conns[i].Session.DB(ns.Database).C(ns.Collection).Indexes()
				if err != nil {
					LOG.Warn("extra job[%s] with source[%v] query index[%v] failed: %v", cui.Name(), source.URL,
						ns.Str(), err)
				}

				if utils.HasUniqueIndex(index) {
					LOG.Crashf("extra job[%s] with source[%v] query index[%v] find unique index: %v",
						cui.Name(), source.URL, ns.Str(), index)
				}
			}
		}
	}
}
