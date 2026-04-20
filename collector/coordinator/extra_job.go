package coordinator

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	LOG "github.com/alibaba/MongoShake/v2/third_party/log4go"
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

func (cui *CheckUniqueIndexExistsJob) innerRun() error {
	var err error
	conns := make([]*utils.MongoCommunityConn, len(cui.urls))
	for i, source := range cui.urls {
		conns[i], err = utils.NewMongoCommunityConn(source.URL, utils.VarMongoConnectModeSecondaryPreferred, true,
			utils.ReadWriteConcernMajority, utils.ReadWriteConcernDefault, conf.Options.MongoSslRootCaFile)
		if err != nil {
			_ = LOG.Error("extra job[%s] connect source[%v] failed: %v",
				cui.Name(), utils.BlockMongoUrlPassword(source.URL, "***"), err)
			return nil
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

				LOG.Debug("extra job[%s] check[%v]", cui.Name(), ns)
				cursor, err := conns[i].Client.Database(ns.Database).Collection(ns.Collection).
					Indexes().List(context.Background())
				if err != nil {
					_ = LOG.Error("extra job[%s] list indexes for [%s.%s] failed: %v",
						cui.Name(), ns.Database, ns.Collection, err)
					continue
				}
				for cursor.Next(context.Background()) {
					nameVal, nErr := cursor.Current.LookupErr("name")
					uniqueVal, uErr := cursor.Current.LookupErr("unique")
					if nErr != nil || uErr != nil {
						continue
					}

					nameStr, nameOk := nameVal.StringValueOK()
					uniqueBool, uniqueOk := uniqueVal.BooleanOK()
					if nameOk && uniqueOk && uniqueBool &&
						!strings.HasPrefix(nameStr, "_id") {
						cursor.Close(context.Background())
						return fmt.Errorf("extra job[%s] with source[%v] query "+
							"collection[%s - %s] find unique[%v]", cui.Name(),
							utils.BlockMongoUrlPassword(source.URL, "***"),
							ns.Database, ns.Collection, cursor.Current)
					}
				}
				cursor.Close(context.Background())
			}
		}
	}

	return nil
}

func (cui *CheckUniqueIndexExistsJob) Run() {
	var err error
	err = cui.innerRun()
	if err != nil {
		LOG.Crashf("%v", err)
	}
}
