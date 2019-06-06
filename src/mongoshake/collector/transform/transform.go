package transform

import (
	"fmt"
	LOG "github.com/vinllen/log4go"
	"regexp"
	"strings"
)

type NamespaceTransform struct {
	ruleList [][2]string
}

func (transform *NamespaceTransform) Transform(namespace string) string {
	for _, rule_pair := range transform.ruleList {
		re := regexp.MustCompile(rule_pair[0])
		params := re.FindStringSubmatch(namespace)
		if len(params) > 0 {
			return rule_pair[1] + params[1]
		}
	}
	return namespace
}

func NewNamespaceTransform(transRule []string) *NamespaceTransform {
	ruleList := make([][2]string, 0)
	for _, rule := range transRule {
		rulePair := strings.SplitN(rule, ":", 2)
		if len(rulePair) != 2 ||
			len(strings.SplitN(rulePair[0], ".", 2)) != len(strings.SplitN(rulePair[1], ".", 2)) {
			LOG.Crashf("transform rule %v is illegal", rule)
		}
		fromRule := strings.Replace(rulePair[0], ".", "\\.", -1)
		fromPattern := fmt.Sprintf("^%s$|^%s(\\..*)$", fromRule, fromRule)
		ruleList = append(ruleList, [2]string{fromPattern, rulePair[1]})
	}
	return &NamespaceTransform{ruleList:ruleList}
}


type DBTransform struct {
	ruleMap map[string][]string
}

func (transform *DBTransform) Transform(db string) []string {
	if v, ok := transform.ruleMap[db]; ok {
		return v
	}
	return []string{db}
}

func NewDBTransform(transRule []string) *DBTransform {
	ruleMap := make(map[string][]string)
	for _, rule := range transRule {
		rulePair := strings.SplitN(rule, ":", 2)
		if len(rulePair) != 2 ||
			len(strings.SplitN(rulePair[0], ".", 2)) != len(strings.SplitN(rulePair[1], ".", 2)) {
			LOG.Crashf("transform rule %v is illegal", rule)
		}
		fromDB := strings.SplitN(rulePair[0], ".", 2)[0]
		toDB := strings.SplitN(rulePair[1], ".", 2)[0]
		if v, ok := ruleMap[fromDB]; ok {
			ruleMap[fromDB] = append(v, toDB)
		} else {
			ruleMap[fromDB] = []string{toDB}
		}
	}
	return &DBTransform{ruleMap:ruleMap}
}