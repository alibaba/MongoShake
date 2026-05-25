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

	ns := database + "." + collection
	allowedIndexes, ok := conf.Options.IncrSyncExecutorDupKeySkipRulesMap[ns]
	if !ok {
		return false, ""
	}

	indexName := parseDupKeyIndexName(err)
	if indexName == "" {
		return false, ""
	}

	if _, ok := allowedIndexes["*"]; ok {
		return true, indexName
	}
	if _, ok := allowedIndexes[indexName]; ok {
		return true, indexName
	}

	return false, indexName
}
