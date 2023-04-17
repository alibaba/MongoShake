package oplog

import (
	"go.mongodb.org/mongo-driver/bson"
	"strings"
)

type CommandOperation struct {
	concernSyncData bool
	runOnAdmin      bool // some commands like `renameCollection` need run on admin database
	needFilter      bool // should be ignored in shake
}

var opsMap = map[string]*CommandOperation{
	"create":           {concernSyncData: false, runOnAdmin: false, needFilter: false},
	"createIndexes":    {concernSyncData: false, runOnAdmin: false, needFilter: false},
	"collMod":          {concernSyncData: false, runOnAdmin: false, needFilter: false},
	"dropDatabase":     {concernSyncData: false, runOnAdmin: false, needFilter: false},
	"drop":             {concernSyncData: false, runOnAdmin: false, needFilter: false},
	"deleteIndex":      {concernSyncData: false, runOnAdmin: false, needFilter: false},
	"deleteIndexes":    {concernSyncData: false, runOnAdmin: false, needFilter: false},
	"dropIndex":        {concernSyncData: false, runOnAdmin: false, needFilter: false},
	"dropIndexes":      {concernSyncData: false, runOnAdmin: false, needFilter: false},
	"renameCollection": {concernSyncData: false, runOnAdmin: true, needFilter: false},
	"convertToCapped":  {concernSyncData: false, runOnAdmin: false, needFilter: false},
	"emptycapped":      {concernSyncData: false, runOnAdmin: false, needFilter: false},
	"applyOps":         {concernSyncData: true, runOnAdmin: false, needFilter: false},
	"startIndexBuild":  {concernSyncData: false, runOnAdmin: false, needFilter: true},
	"commitIndexBuild": {concernSyncData: false, runOnAdmin: false, needFilter: false},
	"abortIndexBuild":  {concernSyncData: false, runOnAdmin: false, needFilter: true},
}

func ExtraCommandName(o bson.D) (string, bool) {
	// command name must be at the first position
	if len(o) > 0 {
		if _, exist := opsMap[o[0].Key]; exist {
			return o[0].Key, true
		}
	}

	return "", false
}

func IsSyncDataCommand(operation string) bool {
	if op, ok := opsMap[strings.TrimSpace(operation)]; ok {
		return op.concernSyncData
	}
	return false
}

func IsRunOnAdminCommand(operation string) bool {
	if op, ok := opsMap[strings.TrimSpace(operation)]; ok {
		return op.runOnAdmin
	}
	return false
}

func IsNeedFilterCommand(operation string) bool {
	if op, ok := opsMap[strings.TrimSpace(operation)]; ok {
		return op.needFilter
	}
	return false
}
