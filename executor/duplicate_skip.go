package executor

import (
	"regexp"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
)

var duplicateKeyIndexRegexp = regexp.MustCompile(`index: ([^ ]+) dup key`)

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

	indexName := extractDuplicateKeyIndexName(err)
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

func extractDuplicateKeyIndexName(err error) string {
	if err == nil {
		return ""
	}

	matches := duplicateKeyIndexRegexp.FindStringSubmatch(err.Error())
	if len(matches) != 2 {
		return ""
	}
	return matches[1]
}
