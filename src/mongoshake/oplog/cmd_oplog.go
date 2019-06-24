package oplog

import (
	"github.com/vinllen/mgo/bson"
	"strings"
)

type CommandOperation struct {
	concernSyncData bool
	runOnAdmin      bool // some commands like `renameCollection` need run on admin database
}

var opsMap = map[string]*CommandOperation{
	"create":           {concernSyncData: false, runOnAdmin: false},
	"createIndexes":    {concernSyncData: false, runOnAdmin: false},
	"collMod":          {concernSyncData: false, runOnAdmin: false},
	"dropDatabase":     {concernSyncData: false, runOnAdmin: false},
	"drop":             {concernSyncData: false, runOnAdmin: false},
	"deleteIndex":      {concernSyncData: false, runOnAdmin: false},
	"deleteIndexes":    {concernSyncData: false, runOnAdmin: false},
	"dropIndex":        {concernSyncData: false, runOnAdmin: false},
	"dropIndexes":      {concernSyncData: false, runOnAdmin: false},
	"renameCollection": {concernSyncData: false, runOnAdmin: true},
	"convertToCapped":  {concernSyncData: false, runOnAdmin: false},
	"emptycapped":      {concernSyncData: false, runOnAdmin: false},
	"applyOps":         {concernSyncData: true, runOnAdmin: false},
}

func ExtraCommandName(o bson.D) (string, bool) {
	// command name must be at the first position
	if len(o) > 0 {
		if _, exist := opsMap[o[0].Name]; exist {
			return o[0].Name, true
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
