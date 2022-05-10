package executor

import (
	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
)

// RecordDuplicatedOplog
// Write dup oplog in DB APPConflictDatabase
func RecordDuplicatedOplog(conn *utils.MongoCommunityConn, coll string, records []*OplogRecord) {
	for _, record := range records {
		log := record.original.partialLog
		switch conf.Options.IncrSyncConflictWriteTo {
		case DumpConflictToDB:
			// discard conflict again
			conn.Client.Database(utils.APPConflictDatabase).Collection(coll).InsertOne(nil, log.Object)
		case NoDumpConflict:
		}
	}
}
