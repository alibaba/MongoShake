package executor

import (
	"context"
	"regexp"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	l "github.com/alibaba/MongoShake/v2/pkg/log"
)

var dupKeyIndexRegex = regexp.MustCompile(`index:\s+(\S+)`)
var dupKeyContentRegex = regexp.MustCompile(`dup key:\s*\{\s*(.*?)\s*\}`)
var dupKeyFieldRegex = regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_.]*)\s*:`)

func parseDupKeyIndexName(err error) string {
	if err == nil {
		return ""
	}
	matches := dupKeyIndexRegex.FindStringSubmatch(err.Error())
	if len(matches) >= 2 {
		return matches[1]
	}
	return ""
}

// parseDupKeyFields extracts field names from the "dup key: { field1: val1, field2: val2 }" portion
// of an E11000 error message. This is more reliable than inferring fields from index names.
func parseDupKeyFields(err error) []string {
	if err == nil {
		return nil
	}
	contentMatch := dupKeyContentRegex.FindStringSubmatch(err.Error())
	if len(contentMatch) < 2 {
		return nil
	}
	fieldMatches := dupKeyFieldRegex.FindAllStringSubmatch(contentMatch[1], -1)
	if len(fieldMatches) == 0 {
		return nil
	}
	fields := make([]string, 0, len(fieldMatches))
	for _, m := range fieldMatches {
		fields = append(fields, m[1])
	}
	return fields
}

// resolveConflictFilter parses field names from the E11000 dup key error, then extracts
// corresponding values from the source document to build a conflict filter.
// Returns nil if fields cannot be parsed or values cannot be extracted.
func resolveConflictFilter(dupErr error, doc bson.D) bson.D {
	fields := parseDupKeyFields(dupErr)
	if len(fields) == 0 {
		return nil
	}

	filter := make(bson.D, 0, len(fields))
	for _, field := range fields {
		value, found := getFieldValue(doc, field)
		if !found {
			l.Logger.Warnf("resolveConflictFilter: field[%s] not found in doc, cannot build filter", field)
			return nil
		}
		filter = append(filter, bson.E{Key: field, Value: value})
	}
	return filter
}

// getFieldValue extracts a field value from a bson.D document, supporting dotted paths.
// Returns the value and whether the field was found (distinguishes nil value from missing field).
func getFieldValue(doc bson.D, field string) (interface{}, bool) {
	parts := splitDotted(field)
	var current interface{} = doc
	for _, part := range parts {
		switch d := current.(type) {
		case bson.D:
			found := false
			for _, elem := range d {
				if elem.Key == part {
					current = elem.Value
					found = true
					break
				}
			}
			if !found {
				return nil, false
			}
		default:
			return nil, false
		}
	}
	return current, true
}

func splitDotted(field string) []string {
	if field == "" {
		return nil
	}
	result := make([]string, 0, 2)
	start := 0
	for i := 0; i < len(field); i++ {
		if field[i] == '.' {
			result = append(result, field[start:i])
			start = i + 1
		}
	}
	result = append(result, field[start:])
	return result
}

// deleteConflictAndRetry attempts to resolve a non-_id duplicate key error by:
// 1. Parsing the conflicting field names from the E11000 error's "dup key" portion
// 2. Extracting values from the source document to build a conflict filter
// 3. Deleting the conflicting document
// Returns (resolved, error). resolved=true means the conflict was handled (caller should retry upsert).
func deleteConflictAndRetry(collection *mongo.Collection, updateFilter interface{},
	doc bson.D, dupErr error, opts *interface{}) (bool, error) {

	if conf.Options.IncrSyncExecutorDupKeyStrategy != utils.VarIncrSyncExecutorDupKeyStrategyDeleteAndRetry {
		return false, nil
	}
	if !utils.DuplicateKey(dupErr) {
		return false, nil
	}

	indexName := parseDupKeyIndexName(dupErr)
	if indexName == "_id_" {
		return false, nil
	}

	conflictFilter := resolveConflictFilter(dupErr, doc)
	if conflictFilter == nil {
		l.Logger.Warnf("deleteConflictAndRetry: cannot resolve conflict filter for index[%s], skip", indexName)
		return false, nil
	}

	l.Logger.Warnf("deleteConflictAndRetry: deleting conflict doc with filter[%v] for index[%s]",
		conflictFilter, indexName)
	if _, delErr := collection.DeleteOne(context.Background(), conflictFilter); delErr != nil {
		l.Logger.Errorf("deleteConflictAndRetry: delete failed with filter[%v]: %v", conflictFilter, delErr)
		return false, delErr
	}

	return true, nil
}
