package executor

import (
	"errors"
	"reflect"
	"strings"

	"mongoshake/dbpool"
	"mongoshake/oplog"
	"mongoshake/collector/configure"
	"mongoshake/common"

	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
	LOG "github.com/vinllen/log4go"
)

var ErrorsShouldSkip = map[int]string{
	61: "ShardKeyNotFound",
}

type CommandOperation struct {
	concernSyncData bool
}

var opsMap = map[string]*CommandOperation{
	"create":           {concernSyncData: false},
	"collMod":          {concernSyncData: false},
	"dropDatabase":     {concernSyncData: false},
	"drop":             {concernSyncData: false},
	"deleteIndex":      {concernSyncData: false},
	"deleteIndexes":    {concernSyncData: false},
	"dropIndex":        {concernSyncData: false},
	"dropIndexes":      {concernSyncData: false},
	"renameCollection": {concernSyncData: false},
	"convertToCapped":  {concernSyncData: false},
	"emptycapped":      {concernSyncData: false},
	"applyOps":         {concernSyncData: true},
}

func (exec *Executor) ensureConnection() bool {
	// reconnect if necessary
	if exec.session == nil {
		if conn, err := dbpool.NewMongoConn(exec.MongoUrl, true); err != nil {
			LOG.Critical("Connect to mongo cluster failed. %v", err)
			return false
		} else {
			exec.session = conn.Session
			exec.bulkInsert = utils.GetAndCompareVersion(exec.session, ThresholdVersion)
		}
	}

	return true
}

func (exec *Executor) dropConnection() {
	exec.session.Close()
	exec.session = nil
}

func (exec *Executor) execute(group *OplogsGroup) error {
	count := uint64(len(group.oplogRecords))
	lastOne := group.oplogRecords[count-1]

	if conf.Options.ReplayerDurable {
		if !exec.ensureConnection() {
			return errors.New("network connection lost . we would retry for next connecting")
		}
		// just use the first log. they has the same metadata
		metadata := buildMetadata(group.oplogRecords[0].original.partialLog)
		hasIndex := strings.Contains(group.ns, "system.indexes")
		dbWriter := NewDbWriter(exec.session, metadata, exec.bulkInsert && !hasIndex)
		var err error

		LOG.Debug("Replay-%d oplog collection ns [%s] with command [%s] batch count %d, metadata %v",
			exec.batchExecutor.ReplayerId, group.ns, strings.ToUpper(lookupOpName(group.op)), count, metadata)

		// for indexes
		if conf.Options.ReplayerDMLOnly && hasIndex {
			// exec.batchExecutor.ReplMetric.AddFilter(uint64(len(group.oplogRecords)))
		} else {
			// "0" -> database, "1" -> collection
			dc := strings.SplitN(group.ns, ".", 2)
			switch group.op {
			case "i":
				err = dbWriter.doInsert(dc[0], dc[1], metadata, group.oplogRecords,
					conf.Options.ReplayerExecutorInsertOnDupUpdate)
			case "u":
				err = dbWriter.doUpdate(dc[0], dc[1], metadata, group.oplogRecords,
					conf.Options.ReplayerExecutorUpsert)
			case "d":
				err = dbWriter.doDelete(dc[0], dc[1], metadata, group.oplogRecords)
			case "c":
				err = dbWriter.doCommand(dc[0], metadata, group.oplogRecords)
			case "n":
				// exec.batchExecutor.ReplMetric.AddFilter(count)
			default:
				LOG.Warn("Unknown type oplogs found. op '%s'", group.op)
			}

			// a few known error we can skip !! such as "ShardKeyNotFound" returned
			// if mongoshake connected to MongoS
			if exec.errorIgnore(err) {
				LOG.Info("Discard known error %v, It's acceptable", err)
				err = nil
			}

			if err != nil {
				LOG.Critical("Replayer-%d, executor-%d, oplog for [%s] op[%s] failed. (%v) [%v], logs %d. firstLog %v",
					exec.batchExecutor.ReplayerId, exec.id, group.ns, group.op, reflect.TypeOf(err), err.Error(), count,
					group.oplogRecords[0].original.partialLog)
				exec.dropConnection()

				return err
			}
		}
	}
	// exec.batchExecutor.ReplMetric.ReplStatus.Clear(utils.ReplicaExecBad)
	// wait for conflict break point if need
	if lastOne.wait != nil {
		lastOne.wait()
	}

	// group logs have the equivalent namespace
	//ns := group.logs[0].original.partialLog.Namespace
	//exec.replayer.ReplMetric.AddTableOps(ns, count)
	return nil
}

func (exec *Executor) errorIgnore(err error) bool {
	switch e := err.(type) {
	case *mgo.LastError:
		_, skip := ErrorsShouldSkip[e.Code]
		return skip
	}
	return false
}

func buildMetadata(oplog *oplog.PartialLog) bson.M {
	// with gid carried
	if len(oplog.Gid) != 0 {
		return bson.M{"g": oplog.Gid}
	}
	return bson.M{}
}

func extraCommandName(o bson.M) (string, bool) {
	for key := range o {
		if _, exist := opsMap[key]; exist {
			return key, true
		}
	}

	return "", false
}

func isSyncDataCommand(operation string) bool {
	if op, ok := opsMap[strings.TrimSpace(operation)]; ok {
		return op.concernSyncData
	}
	return false
}

func lookupOpName(op string) string {
	switch op {
	case "i":
		return "insert"
	case "u":
		return "update"
	case "d":
		return "delete"
	case "c":
		return "create"
	case "n":
		return "noop"
	default:
		return "unknown"
	}
}
