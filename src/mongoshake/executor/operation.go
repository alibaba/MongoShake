package executor

import (
	"errors"
	"reflect"
	"strings"

	"mongoshake/collector/configure"
	"mongoshake/common"
	"mongoshake/dbpool"
	"mongoshake/oplog"

	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
)

var ErrorsShouldSkip = map[int]string{
	61: "ShardKeyNotFound",
}

func (exec *Executor) ensureConnection() bool {
	// reconnect if necessary
	if exec.session == nil {
		if conn, err := dbpool.NewMongoConn(exec.MongoUrl, true, true); err != nil {
			LOG.Critical("Connect to mongo cluster failed. %v", err)
			return false
		} else {
			exec.session = conn.Session
			if exec.bulkInsert, err = utils.GetAndCompareVersion(exec.session, ThresholdVersion); err != nil {
				LOG.Info("compare version with return[%v], bulkInsert disable", err)
			}
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
	if count == 0 {
		// probe
		return nil
	}

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

		/*
		 * in the former version, we filter DDL here. But in current version, all messages that need filter
		 * have already removed in the collector(syncer). So here, we only need to write all oplogs.
		 */
		// for indexes
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
			LOG.Info("Discard known error[%v], It's acceptable", err)
			err = nil
		}

		if err != nil {
			LOG.Critical("Replayer-%d, executor-%d, oplog for namespace[%s] op[%s] failed. error type[%v]"+
				" error[%v], logs number[%d], firstLog: %v",
				exec.batchExecutor.ReplayerId, exec.id, group.ns, group.op, reflect.TypeOf(err), err.Error(), count,
				group.oplogRecords[0].original.partialLog)
			exec.dropConnection()

			return err
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
