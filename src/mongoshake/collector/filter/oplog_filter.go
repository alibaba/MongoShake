package filter

import (
	"fmt"
	"strings"

	"mongoshake/oplog"

	"github.com/vinllen/mgo/bson"
	LOG "github.com/vinllen/log4go"
)

// OplogFilter: AutologousFilter, NamespaceFilter, GidFilter, NoopFilter, DDLFilter
type OplogFilter interface {
	Filter(log *oplog.PartialLog) bool
}

type OplogFilterChain []OplogFilter

func (chain OplogFilterChain) IterateFilter(log *oplog.PartialLog) bool {
	for _, filter := range chain {
		if filter.Filter(log) {
			return true
		}
	}
	return false
}

type GidFilter struct {
	Gid string
}

func (filter *GidFilter) Filter(log *oplog.PartialLog) bool {
	// filter OplogGlobalId from others
	return len(filter.Gid) != 0 && log.Gid != filter.Gid
}

type AutologousFilter struct {
}

func (filter *AutologousFilter) Filter(log *oplog.PartialLog) bool {
	// for namespace. we filter noop operation and collection name
	// that are admin, local, mongoshake, mongoshake_conflict
	return filter.FilterNs(log.Namespace)
}

type NoopFilter struct {
}

func (filter *NoopFilter) Filter(log *oplog.PartialLog) bool {
	return log.Operation == "n"
}

type DDLFilter struct {
}

func (filter *DDLFilter) Filter(log *oplog.PartialLog) bool {
	return log.Operation == "c" || strings.HasSuffix(log.Namespace, "system.indexes")
}

type MigrateFilter struct {
}

func (filter *MigrateFilter) Filter(log *oplog.PartialLog) bool {
	return log.FromMigrate
}

// because regexp use the default perl engine which is not support inverse match, so
// use two rules to match
type NamespaceFilter struct {
	whiteRule      string
	blackRule      string
	whiteDBRuleMap map[string]bool
}

// convert input namespace filter to regex string
// e.g., namespace-fileter = []string{"db1", "db2.collection2"}
// return: ^(db1|db2.collection2)$|(db1\.|db2\.collection2\.).*$
func convertToRule(input []string) string {
	if len(input) == 0 {
		return ""
	}

	rule1 := strings.Join(input, "|")

	inputWithPrefix := make([]string, len(input))
	for i, s := range input {
		inputWithPrefix[i] = s + "."
	}
	rule2 := strings.Join(inputWithPrefix, "|")

	rule1R := strings.Replace(rule1, ".", "\\.", -1)
	rule2R := strings.Replace(rule2, ".", "\\.", -1)

	return fmt.Sprintf("^(%s)$|^(%s).*$", rule1R, rule2R)
}

func covertToWhiteDBRule(input []string) map[string]bool {
	whiteDBRuleMap := map[string]bool{}
	for _, ns := range input {
		db := strings.SplitN(ns, ".", 2)[0]
		whiteDBRuleMap[db] = true
	}
	return whiteDBRuleMap
}

func NewNamespaceFilter(white, black []string) *NamespaceFilter {
	whiteRule := convertToRule(white)
	blackRule := convertToRule(black)
	whiteDBRuleMap := covertToWhiteDBRule(white)

	return &NamespaceFilter{
		whiteRule:      whiteRule,
		blackRule:      blackRule,
		whiteDBRuleMap: whiteDBRuleMap,
	}
}

func (filter *NamespaceFilter) Filter(log *oplog.PartialLog) bool {
	var result bool
	db := strings.SplitN(log.Namespace, ".", 2)[0]
	if log.Operation != "c" {
		// DML
		// {"op" : "i", "ns" : "my.system.indexes", "o" : { "v" : 2, "key" : { "date" : 1 }, "name" : "date_1", "ns" : "my.tbl", "expireAfterSeconds" : 3600 }
		if strings.HasSuffix(log.Namespace, "system.indexes") {
			// DDL: change log.Namespace to ns of object, in order to do filter with real namespace
			ns := log.Namespace
			log.Namespace = oplog.GetKey(log.Object, "ns").(string)
			result = filter.filter(log)
			log.Namespace = ns
		} else {
			result = filter.filter(log)
		}
		return result
	} else {
		// DDL
		operation, found := oplog.ExtraCommandName(log.Object)
		if !found {
			LOG.Warn("extraCommandName meets type[%s] which is not implemented, ignore!", operation)
			return false
		}
		switch operation {
		case "create":
			fallthrough
		case "createIndexes":
			fallthrough
		case "collMod":
			fallthrough
		case "drop":
			fallthrough
		case "deleteIndex":
			fallthrough
		case "deleteIndexes":
			fallthrough
		case "dropIndex":
			fallthrough
		case "dropIndexes":
			fallthrough
		case "convertToCapped":
			fallthrough
		case "emptycapped":
			col, ok := oplog.GetKey(log.Object, operation).(string)
			if !ok {
				LOG.Warn("extraCommandName meets illegal %v oplog %v, ignore!", operation, log.Object)
				return false
			}
			log.Namespace = fmt.Sprintf("%s.%s", db, col)
			return filter.filter(log)
		case "renameCollection":
			// { "renameCollection" : "my.tbl", "to" : "my.my", "stayTemp" : false, "dropTarget" : false }
			ns, ok := oplog.GetKey(log.Object, operation).(string)
			if !ok {
				LOG.Warn("extraCommandName meets illegal %v oplog %v, ignore!", operation, log.Object)
				return false
			}
			log.Namespace = ns
			return filter.filter(log)
		case "applyOps":
			var filterOps []bson.D
			if ops := oplog.GetKey(log.Object, "applyOps").([]interface{}); ops != nil {
				for _, ele := range ops {
					eleD := ele.(bson.D)
					m, _ := oplog.ConvertBsonD2M(eleD)
					subLog := oplog.NewPartialLog(m)
					if ok := filter.Filter(subLog); !ok {
						filterOps = append(filterOps, eleD)
					}
				}
				oplog.SetFiled(log.Object, "applyOps", filterOps)
			}
			return len(filterOps) > 0
		default:
			// such as: dropDatabase
			return filter.filter(log)
		}
	}
}

func (filter *NamespaceFilter) filter(log *oplog.PartialLog) bool {
	// if whiteRule is db.col, then db.$cmd command will not be filtered
	if strings.HasSuffix(log.Namespace, ".$cmd") {
		db := strings.SplitN(log.Namespace, ".", 2)[0]
		if _, ok := filter.whiteDBRuleMap[db]; ok {
			return false
		}
	}
	return filter.FilterNs(log.Namespace)
}
