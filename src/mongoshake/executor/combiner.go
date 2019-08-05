package executor

import (
	LOG "github.com/vinllen/log4go"
)

type OplogsGroup struct {
	ns           string
	op           string
	oplogRecords []*OplogRecord

	completionList []func()
}

func (group *OplogsGroup) completion() {
	for _, cb := range group.completionList {
		cb()
	}
}

type LogsGroupCombiner struct {
	maxGroupNr   int
	maxGroupSize int
}

func (combiner LogsGroupCombiner) mergeToGroups(logs []*OplogRecord) (groups []*OplogsGroup) {
	forceSplit := false
	sizeInGroup := 0

	for _, log := range logs {
		op := log.original.partialLog.Operation
		ns := log.original.partialLog.Namespace
		// the equivalent oplog.op and oplog.ns can be merged together
		last := len(groups) - 1

		if !forceSplit && // force split by log's wait function
			len(groups) > 0 && // have one group existing at least
			len(groups[last].oplogRecords) < combiner.maxGroupNr && // no more than max group number
			sizeInGroup+log.original.partialLog.RawSize < combiner.maxGroupSize && // no more than one group size
			groups[last].op == op && groups[last].ns == ns { // same op and ns
			// we can merge this oplog into the latest batched oplogRecords group
			combiner.merge(groups[len(groups)-1], log)
			sizeInGroup += log.original.partialLog.RawSize // add size
		} else {
			if sizeInGroup != 0 {
				LOG.Debug("mergeToGroups merge log with total size[%v]", sizeInGroup)
			}

			// new start of a group
			groups = append(groups, combiner.startNewGroup(log))
			sizeInGroup = log.original.partialLog.RawSize
		}

		// can't be merge more oplogRecords further. this log should be the end in this group
		forceSplit = log.wait != nil
	}

	LOG.Debug("mergeToGroups merge group with total number[%v]", len(groups))

	return
}

func (combiner *LogsGroupCombiner) merge(group *OplogsGroup, log *OplogRecord) {
	group.oplogRecords = append(group.oplogRecords, log)
	if log.original.callback != nil {
		group.completionList = append(group.completionList, log.original.callback)
	}
}

func (combiner *LogsGroupCombiner) startNewGroup(log *OplogRecord) *OplogsGroup {
	group := &OplogsGroup{
		op:           log.original.partialLog.Operation,
		ns:           log.original.partialLog.Namespace,
		oplogRecords: []*OplogRecord{log},
	}
	if log.original.callback == nil {
		group.completionList = []func(){}
	} else {
		group.completionList = []func(){log.original.callback}
	}
	return group
}
