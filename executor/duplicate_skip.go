package executor

import (
	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
)

func shouldSkipDupKeyOnInsert(database, collection string, err error) (bool, string) {
	if conf.Options.IncrSyncExecutorDupKeyStrategy != utils.VarIncrSyncExecutorDupKeyStrategySkip ||
		!utils.DuplicateKey(err) {
		return false, ""
	}

	indexName := parseDupKeyIndexName(err)
	if indexName == "" {
		return false, ""
	}

	// _id duplicates carry distinct semantics: an oplog being re-applied
	// after a checkpoint replay produces an _id E11000, and the writer
	// layer's already-applied detection is the right place to handle it
	// (it can compare the existing doc to the incoming one). Skipping
	// _id_ here — even under a "*" wildcard rule — would mask real data
	// divergence, so always defer _id back to the writer.
	if indexName == "_id_" {
		return false, indexName
	}

	return shouldSkipDupKeyIndex(database, collection, indexName), indexName
}

func shouldSkipDupKeyIndex(database, collection, indexName string) bool {
	if indexName == "" {
		return false
	}

	ns := database + "." + collection
	allowedIndexes, ok := conf.Options.IncrSyncExecutorDupKeySkipRulesMap[ns]
	if !ok {
		return false
	}
	if _, ok := allowedIndexes["*"]; ok {
		return true
	}
	_, ok = allowedIndexes[indexName]
	return ok
}
