package coordinator

import (
	"errors"
	"fmt"

	nimo "github.com/gugemichael/nimo4go"

	"github.com/alibaba/MongoShake/v2/collector"
	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	l "github.com/alibaba/MongoShake/v2/pkg/log"
)

func (coordinator *ReplicationCoordinator) startOplogReplication(oplogStartPosition interface{},
	fullSyncFinishPosition int64,
	startTsMap map[string]int64) error {

	// prepare all syncer. only one syncer while source is ReplicaSet or mongos
	// otherwise one syncer connects to one shard
	l.Logger.Infof("start incr replication")
	for i, src := range coordinator.RealSourceIncrSync {
		var syncerTs interface{}

		if len(coordinator.MongoD) > 0 {
			// read from shard or replicaset
			if val, ok := oplogStartPosition.(int64); ok && val == 0 {
				if v, ok := startTsMap[src.ReplicaName]; !ok {
					return fmt.Errorf("replia[%v] not exists on startTsMap[%v]", src.ReplicaName, startTsMap)
				} else {
					syncerTs = v
				}
			} else {
				syncerTs = oplogStartPosition
			}
		} else {
			// read from mongos
			syncerTs = oplogStartPosition
			l.Logger.Infof("read from mongos src.ReplicaName:%s ts:%v", src.ReplicaName, startTsMap[src.ReplicaName])
			if len(conf.Options.MongoSUrl) > 0 && len(conf.Options.MongoCsUrl) == 0 && len(conf.Options.MongoUrls) == 0 {
				if v, ok := startTsMap[src.ReplicaName]; ok {
					syncerTs = v
					l.Logger.Infof("read from mongos and set ts src.ReplicaName:%s ts:%v", src.ReplicaName, startTsMap[src.ReplicaName])
				}
			}
		}

		l.Logger.Infof("RealSourceIncrSync[%d]: %s, startTimestamp[%v]", i, src, syncerTs)
		syncer := collector.NewOplogSyncer(src.ReplicaName, syncerTs, fullSyncFinishPosition, src.URL,
			src.Gids)
		// syncerGroup http api registry
		syncer.Init()
		coordinator.syncerGroup = append(coordinator.syncerGroup, syncer)
	}
	// set to group 0 as a leader
	coordinator.syncerGroup[0].SyncGroup = coordinator.syncerGroup

	// prepare worker routine and bind it to syncer
	for i := 0; i < conf.Options.IncrSyncWorker; i++ {
		syncer := coordinator.syncerGroup[i%len(coordinator.syncerGroup)]
		w := collector.NewWorker(syncer, uint32(i))
		if !w.Init() {
			return errors.New("worker initialize error")
		}
		w.SetInitSyncFinishTs(fullSyncFinishPosition)

		// syncer and worker are independent. the relationship between
		// them needs binding here. one worker definitely belongs to a specific
		// syncer. However individual syncer could bind multi workers (if source
		// of overall replication is single mongodb replica)
		syncer.Bind(w)
		go w.StartWorker()
	}

	for _, syncer := range coordinator.syncerGroup {
		go syncer.Start()
	}

	// start http server
	if utils.IsHTTPPortEnabled(conf.Options.IncrSyncHTTPListenPort) {
		nimo.GoRoutine(func() {
			if err := utils.IncrSyncHttpApi.Listen(); err != nil {
				l.Logger.Criticalf("start incr sync server with port[%v] failed: %v", conf.Options.IncrSyncHTTPListenPort,
					err)
			}
		})
	} else {
		l.Logger.Infof("incr sync http api disabled. port[%v]", conf.Options.IncrSyncHTTPListenPort)
	}

	return nil
}
