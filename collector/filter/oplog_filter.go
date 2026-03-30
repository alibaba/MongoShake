package filter

import (
	"fmt"
	"reflect"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/alibaba/MongoShake/v2/oplog"
	LOG "github.com/alibaba/MongoShake/v2/third_party/log4go"
)

// OplogFilter include: AutologousFilter, NamespaceFilter, GidFilter, NoopFilter, DDLFilter
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

// CmdFilter filters oplogs by oplog op type instead of only command oplogs.
type CmdFilter struct {
	cmdMp map[string]struct{}
}

// NewCmdFilter trusts sanitize to provide normalized op type values.
func NewCmdFilter(cmds []string) *CmdFilter {
	mp := make(map[string]struct{}, len(cmds))
	for _, cmd := range cmds {
		if cmd == "" {
			continue
		}
		mp[cmd] = struct{}{}
	}
	return &CmdFilter{
		cmdMp: mp,
	}
}

func (filter *CmdFilter) Filter(log *oplog.PartialLog) bool {
	if len(filter.cmdMp) == 0 {
		return false
	}

	operation := log.Operation
	// Command oplogs require a dedicated path:
	//
	//  1. Non-`c` operations keep the original behavior and are filtered only by
	//     their top-level `op`.
	//  2. If the current oplog is `c` and `filter.cmds=c` is configured, keep
	//     filtering the whole command oplog by its top-level `op`.
	//  3. Only when `c` is not configured, but one of `i/u/d` is configured and
	//     the command is `applyOps`, do we rewrite matching inner DML ops. This
	//     keeps top-level `c` filtering higher priority than partial `applyOps`
	//     rewriting.
	_, topLevelMatched := filter.cmdMp[operation]
	if operation != "c" || topLevelMatched {
		return topLevelMatched
	}

	if !filter.hasApplyOpsDMLFilter() {
		return false
	}

	command, found := oplog.ExtraCommandName(log.Object)
	if !found || command != "applyOps" {
		return false
	}

	remainOps, filteredCount, ok := filter.filterApplyOpsDML(log.Object)
	if !ok {
		return false
	}

	oplog.SetFiled(log.Object, "applyOps", remainOps)
	LOG.Info("CmdFilter filtered %d inner applyOps ops, drop oplog?[%v], after reorganize: %v",
		filteredCount, len(remainOps) == 0, log.Object)
	return len(remainOps) == 0
}

// hasApplyOpsDMLFilter reports whether inner applyOps rewriting is relevant for
// the current filter configuration.
func (filter *CmdFilter) hasApplyOpsDMLFilter() bool {
	for _, op := range []string{"i", "u", "d"} {
		if _, ok := filter.cmdMp[op]; ok {
			return true
		}
	}

	return false
}

// filterApplyOpsDML removes matching DML ops inside applyOps and returns the
// remaining inner ops.
func (filter *CmdFilter) filterApplyOpsDML(logObject bson.D) (bson.A, int, bool) {
	ops, ok := extractApplyOps(logObject)
	if !ok {
		return nil, 0, false
	}

	remainOps := make(bson.A, 0, len(ops))
	filteredCount := 0
	for _, ele := range ops {
		innerOperation, ok := oplog.GetKey(ele, "op").(string)
		if !ok {
			LOG.Warn("CmdFilter meets illegal applyOps inner op: %v", ele)
			return nil, 0, false
		}

		if filter.shouldFilterApplyOpsInnerOp(innerOperation) {
			LOG.Debug("CmdFilter filter inner %s op in applyOps: %v", innerOperation, ele)
			filteredCount++
			continue
		}

		remainOps = append(remainOps, ele)
	}

	return remainOps, filteredCount, true
}

// shouldFilterApplyOpsInnerOp reports whether the given inner applyOps op
// should be filtered by the current DML filter configuration.
func (filter *CmdFilter) shouldFilterApplyOpsInnerOp(operation string) bool {
	switch operation {
	case "i", "u", "d":
		_, ok := filter.cmdMp[operation]
		return ok
	default:
		return false
	}
}

// extractApplyOps extracts applyOps inner operations from the supported BSON
// representations already used in the project.
func extractApplyOps(logObject bson.D) ([]bson.D, bool) {
	var ops []bson.D

	switch v := oplog.GetKey(logObject, "applyOps").(type) {
	case []bson.D:
		ops = v
	case []any:
		ops = make([]bson.D, 0, len(v))
		for _, ele := range v {
			doc, ok := ele.(bson.D)
			if !ok {
				LOG.Error("unknown applyOps element type, filter can't handle. log:%+v", logObject)
				return nil, false
			}
			ops = append(ops, doc)
		}
	case primitive.A:
		ops = make([]bson.D, 0, len(v))
		for _, ele := range v {
			doc, ok := ele.(bson.D)
			if !ok {
				LOG.Error("unknown applyOps element type, filter can't handle. log:%+v", logObject)
				return nil, false
			}
			ops = append(ops, doc)
		}
	default:
		LOG.Error("unknown applyOps type, filter can't handle. log:%+v", logObject)
		return nil, false
	}

	return ops, true
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
	operation, found := oplog.ExtraCommandName(log.Object)
	if found {
		if oplog.IsNeedFilterCommand(operation) {
			return true
		}
	}

	// special case for txn oplog with inner ops for 'config.system.sessions'
	if log.Namespace == "admin.$cmd" && operation == "applyOps" {
		ns, err := oplog.ExtractInnerNs(&log.ParsedLog)
		if err != nil {
			LOG.Warn("ExtractInnerNs meets error:%v", err)
			return false
		}
		if ns == "config.system.sessions" || ns == "config.system.preimages" {
			return true
		}
	}

	// for namespace. we filter noop operation and collection name
	// that are admin, local, config, mongoshake, mongoshake_conflict
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

// NamespaceFilter
// because regexp use the default perl engine which is not support inverse match, so use two rules to match
type NamespaceFilter struct {
	whiteRule      string
	blackRule      string
	whiteDBRuleMap map[string]bool
}

// convert input namespace filter to regex string
// e.g., namespace-filter = []string{"db1", "db2.collection2"}
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
		// commitIndexBuild may have multi indexes which need change to CreateIndexes command
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
			// parse and reorganize all inner ops within the transaction,
			// also handle vectored insert oplog format with {multiOpType:1} introduced in 8.0
			var ops []bson.D
			var remainOps bson.A

			isVectoredInsert := false
			if log.MultiOpType != nil && *log.MultiOpType == 1 {
				isVectoredInsert = true
			}
			// it's very strange, some documents are []interface, some are []bson.D
			switch v := oplog.GetKey(log.Object, "applyOps").(type) {
			case []interface{}:
				for _, ele := range v {
					ops = append(ops, ele.(bson.D))
				}
			case []bson.D:
				ops = v
			case primitive.A:
				for _, ele := range v {
					ops = append(ops, ele.(bson.D))
				}
			default:
				LOG.Error("unknown applyOps type, filter can't handle. log:%+v", log.Object)
				return false
			}
			for _, ele := range ops {
				innerNs := oplog.GetKey(ele, "ns").(string)
				if filter.FilterNs(innerNs) {
					if isVectoredInsert {
						LOG.Info("filter vectored insert ops with ns:%v in o.applyOps oplog", innerNs)
						return true
					}
					LOG.Info("filter inner op with ns:%v in txn:%v", innerNs, log.Object)
					continue
				} else {
					remainOps = append(remainOps, ele)
				}
			}
			oplog.SetFiled(log.Object, "applyOps", remainOps)
			LOG.Info("NamespaceFilter applyOps filter?[%v], after reorganize: %v", len(remainOps) == 0, log.Object)
			return len(remainOps) == 0
		default:
			// such as: dropDatabase
			return filter.filter(log)
		}
	}
}

func (filter *NamespaceFilter) filter(log *oplog.PartialLog) bool {
	return filter.FilterNs(log.Namespace)
}
