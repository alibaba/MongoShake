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
