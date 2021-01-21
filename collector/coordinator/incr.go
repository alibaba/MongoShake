package coordinator

import (
	"errors"

	"github.com/alibaba/MongoShake/v2/collector"
	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"

	"fmt"

	nimo "github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
)

func (coordinator *ReplicationCoordinator) startOplogReplication(oplogStartPosition, fullSyncFinishPosition int64,
	startTsMap map[string]int64) error {
	// replicate speed limit on all syncer
	coordinator.rateController = nimo.NewSimpleRateController()

	// prepare all syncer. only one syncer while source is ReplicaSet
	// otherwise one syncer connects to one shard
	LOG.Info("start incr replication")
	for i, src := range coordinator.RealSourceIncrSync {
		var syncerTs int64
		if oplogStartPosition == 0 {
			if v, ok := startTsMap[src.ReplicaName]; !ok {
				return fmt.Errorf("replia[%v] not exists on startTsMap[%v]", src.ReplicaName, startTsMap)
			} else {
				syncerTs = v
			}
		} else {
			syncerTs = oplogStartPosition
		}

		LOG.Info("RealSourceIncrSync[%d]: %s, startTimestamp[%v]", i, src, syncerTs)
		syncer := collector.NewOplogSyncer(src.ReplicaName, syncerTs, fullSyncFinishPosition, src.URL,
			src.Gids, coordinator.rateController)
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
	nimo.GoRoutine(func() {
		if err := utils.IncrSyncHttpApi.Listen(); err != nil {
			LOG.Critical("start incr sync server with port[%v] failed: %v", conf.Options.IncrSyncHTTPListenPort,
				err)
		}
	})

	return nil
}
