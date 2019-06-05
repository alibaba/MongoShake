package transform

import (
	"fmt"
	"regexp"
	"strings"
)

type Transform interface {
	Transform(namespace string) string
}

type NamespaceTransform struct {
	ruleMap map[string]string
}

func (transform *NamespaceTransform) Transform(namespace string) string {
	for rule,to := range transform.ruleMap {
		re := regexp.MustCompile(rule)
		params := re.FindStringSubmatch(namespace)
		if len(params) > 0 {
			return to + params[1]
		}
	}
	return namespace
}

func NewNamespaceTransform(transRule []string) *NamespaceTransform {
	ruleMap := make(map[string]string)
	for _, rule := range transRule {
		pair := strings.SplitN(rule, ":", 2)
		fromRule := strings.Replace(pair[0], ".", "\\.", -1)
		fromPattern := fmt.Sprintf("^%s$|^%s(\\..*)$", fromRule, fromRule)
		ruleMap[fromPattern] = pair[1]
	}
	return &NamespaceTransform{ruleMap:ruleMap}
}


