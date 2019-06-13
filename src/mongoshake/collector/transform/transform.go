package transform

import (
	"fmt"
	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
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
	return &NamespaceTransform{ruleList: ruleList}
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
	return &DBTransform{ruleMap: ruleMap}
}

func TransformDBRef(logObject bson.M, db string, nsTrans *NamespaceTransform) bson.M {
	for k, v := range logObject {
		switch vr := v.(type) {
		case bson.M:
			if isDBRef(vr) {
				var ns string
				if _, ok := vr["$db"]; ok {
					ns = fmt.Sprintf("%s.%s", vr["$db"], vr["$ref"])
				} else {
					ns = fmt.Sprintf("%s.%s", db, vr["$ref"])
				}
				transformNs := nsTrans.Transform(ns)
				tuple := strings.SplitN(transformNs, ".", 2)
				logObject[k] = mgo.DBRef{
					Collection:tuple[1],
					Id:vr["$id"],
					Database: tuple[0],
				}
			} else {
				logObject[k] = TransformDBRef(vr, db, nsTrans)
			}
		default:
			logObject[k] = vr
		}
	}
	return logObject
}

func isDBRef(object bson.M) bool {
	_, hasRef := object["$ref"]
	_, hasId := object["$id"]
	if hasRef && hasId {
		return true
	}
	return false
}
