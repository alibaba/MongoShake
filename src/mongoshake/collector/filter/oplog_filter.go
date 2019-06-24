package filter

import (
	"fmt"
	"mongoshake/oplog"
	"strings"
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
	whileDBRuleMap map[string]bool
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

func covertToWhileDBRule(input []string) map[string]bool {
	whileDBRuleMap := map[string]bool{}
	for _, ns := range input {
		db := strings.SplitN(ns, ".", 2)[0]
		whileDBRuleMap[db] = true
	}
	return whileDBRuleMap
}

func NewNamespaceFilter(white, black []string) *NamespaceFilter {
	whiteRule := convertToRule(white)
	blackRule := convertToRule(black)
	whileDBRuleMap := covertToWhileDBRule(white)

	return &NamespaceFilter{
		whiteRule:      whiteRule,
		blackRule:      blackRule,
		whileDBRuleMap: whileDBRuleMap,
	}
}

func (filter *NamespaceFilter) Filter(log *oplog.PartialLog) bool {
	// if white rule is db.col, then db.$cmd command will not be filtered
	if strings.HasSuffix(log.Namespace, ".$cmd") {
		db := strings.SplitN(log.Namespace, ".", 2)[0]
		if _, ok := filter.whileDBRuleMap[db]; ok {
			return false
		}
	}
	return filter.FilterNs(log.Namespace)
}
