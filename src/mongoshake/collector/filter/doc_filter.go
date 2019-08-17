package filter

import (
	"mongoshake/collector/configure"
	"mongoshake/common"
	"regexp"
	"strings"
	"fmt"
)

// key: ns, value: true means prefix, false means contain
var NsShouldBeIgnore = map[string]bool{
	"admin.":                        true,
	"local.":                        true,
	"config.":                       true,
	utils.AppDatabase + ".":         true,
	utils.APPConflictDatabase + ".": true,
	"system.views":                  false,
}

func InitNs(specialNsList []string) {
	for _, ns := range specialNsList {
		if _, ok := NsShouldBeIgnore[ns]; ok {
			delete(NsShouldBeIgnore, ns)
		}
		newNs := fmt.Sprintf("%s.", ns)
		if _, ok := NsShouldBeIgnore[newNs]; ok {
			delete(NsShouldBeIgnore, newNs)
		}
	}
}

// DocFilter: AutologousFilter, NamespaceFilter
type DocFilter interface {
	FilterNs(namespace string) bool
}

type DocFilterChain []DocFilter

func (chain DocFilterChain) IterateFilter(namespace string) bool {
	for _, filter := range chain {
		if filter.FilterNs(namespace) {
			return true
		}
	}
	return false
}

func (filter *AutologousFilter) FilterNs(namespace string) bool {
	// for namespace. we filter noop operation and collection name
	// that are admin, local, config, mongoshake, mongoshake_conflict
	for key, val := range NsShouldBeIgnore {
		if val == true && strings.HasPrefix(namespace, key) {
			return true
		}
		if val == false && strings.Contains(namespace, key) {
			return true
		}
	}
	return false
}

func (filter *NamespaceFilter) FilterNs(namespace string) bool {
	// if whiteRule is db.col, then db.$cmd command will not be filtered
	if strings.HasSuffix(namespace, ".$cmd") {
		db := strings.SplitN(namespace, ".", 2)[0]
		if _, ok := filter.whiteDBRuleMap[db]; ok {
			return false
		}
	}
	if filter.whiteRule != "" {
		if match, _ := regexp.MatchString(filter.whiteRule, namespace); !match {
			// filter
			return true
		}
	}
	if filter.blackRule != "" {
		if match, _ := regexp.MatchString(filter.blackRule, namespace); match {
			// filter
			return true
		}
	}
	return false
}

func NewDocFilterList() DocFilterChain {
	filterList := DocFilterChain{new(AutologousFilter)}
	if len(conf.Options.FilterNamespaceWhite) != 0 || len(conf.Options.FilterNamespaceBlack) != 0 {
		namespaceFilter := NewNamespaceFilter(conf.Options.FilterNamespaceWhite,
			conf.Options.FilterNamespaceBlack)
		filterList = append(filterList, namespaceFilter)
	}
	return filterList
}
