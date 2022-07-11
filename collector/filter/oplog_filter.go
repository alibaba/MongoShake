package filter

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/alibaba/MongoShake/v2/oplog"

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
			LOG.Debug("%v filter oplog[%v]", reflect.TypeOf(filter), log)
			return true
		}
	}
	return false
}

type GidFilter struct {
	gidMp map[string]struct{}
}

func NewGidFilter(gids []string) *GidFilter {
	mp := make(map[string]struct{}, len(gids))
	for _, gid := range gids {
		mp[gid] = struct{}{}
	}
	return &GidFilter{
		gidMp: mp,
	}
}

func (filter *GidFilter) Filter(log *oplog.PartialLog) bool {
	// filter OplogGlobalId from others
	if len(filter.gidMp) == 0 {
		// all pass if gid map is empty
		return false
	}
	if _, ok := filter.gidMp[log.Gid]; ok {
		// should match if gid map isn't empty
		return false
	}
	return true
}

type AutologousFilter struct {
}

func (filter *AutologousFilter) Filter(log *oplog.PartialLog) bool {

	// Filter out unnecessary commands
	if operation, found := oplog.ExtraCommandName(log.Object); found {
		if oplog.IsNeedFilterCommand(operation) {
			return true
		}
	}

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
	operation, _ := oplog.ExtraCommandName(log.Object)
	return log.Operation == "c" && operation != "applyOps" || strings.HasSuffix(log.Namespace, "system.indexes")
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

	LOG.Debug("NamespaceFilter check oplog:%v", log.Object)

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
		// startIndexBuild,abortIndexBuild,commitIndexBuild waw introduced in 4.4, first two command are ignored
		// commitIndexBuild may have mutil indexes which need change to CreateIndexes command
		case "startIndexBuild":
			fallthrough
		case "abortIndexBuild":
			return true
		case "commitIndexBuild":
			fallthrough
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
			return false

			// TODO move all extra and filter to individual func
			//var ops []bson.D
			//var remainOps []interface{} // return []interface{}
			//
			//LOG.Info("log.Object:%v , type:%v", log.Object, reflect.TypeOf(oplog.GetKey(log.Object, "applyOps")))
			//// it's very strange, some documents are []interface, some are []bson.D
			//switch v := oplog.GetKey(log.Object, "applyOps").(type) {
			//case []interface{}:
			//	LOG.Info("Fiter interface %v\n", v)
			//	for _, ele := range v {
			//		ops = append(ops, ele.(bson.D))
			//	}
			//case []bson.D:
			//	LOG.Info("Fiter bson.D %v\n", v)
			//	ops = v
			//case primitive.A:
			//	for i, ele := range v {
			//		ops = append(ops, ele.(bson.D))
			//
			//		LOG.Info("Fiter primitive.A type %v, ele:%v", reflect.TypeOf(ele), ele)
			//		is_filter := false
			//		for _, ele1 := range ele.(bson.D) {
			//			if ele1.Key == "ns" && filter.FilterNs(ele1.Value.(string)) {
			//				LOG.Info("Fiter primitive.A  fitler:%v", ele1.Value)
			//				is_filter = true
			//				break
			//			}
			//		}
			//		if is_filter {
			//			for j, ele1 := range ele.(bson.D) {
			//				if ele1.Key == "op" {
			//					//ele1.Value = "n"
			//					v[i].(bson.D)[j].Value = "n"
			//				}
			//			}
			//
			//			LOG.Info("Fiter zhangst  result:%v", ele)
			//		}
			//	}
			//default:
			//	LOG.Error("unknow applyOps type, log:%v", log.Object)
			//}
			//
			//LOG.Info("Fiter primitive.A ops:%v", ops)
			//// except field 'o'
			//except := map[string]struct{}{
			//	"o": {},
			//}
			//
			//for _, ele := range ops {
			//	m, _ := oplog.ConvertBsonD2MExcept(ele, except)
			//	subLog := oplog.NewPartialLog(m)
			//	if ok := filter.Filter(subLog); !ok {
			//		remainOps = append(remainOps, ele)
			//	}
			//}
			//oplog.SetFiled(log.Object, "applyOps", remainOps)
			//
			//LOG.Info("NamespaceFilter applyOps filter?[%v], remainOps: %v", len(remainOps) == 0, remainOps)
			//return len(remainOps) == 0
		default:
			// such as: dropDatabase
			return filter.filter(log)
		}
	}
}

func (filter *NamespaceFilter) filter(log *oplog.PartialLog) bool {
	return filter.FilterNs(log.Namespace)
}
