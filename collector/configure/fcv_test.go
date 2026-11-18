package conf

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"strings"
	"testing"

	utils "github.com/alibaba/MongoShake/v2/common"
)

const shippedTemplate = "../../conf/collector.conf"

// TestFcvCountersAgree guards the two configuration version counters against
// drifting apart again.
//
// conf/collector.conf declares conf.version, while common/fcv.go declares
// FcvConfiguration.CurrentVersion and maps every version number to the lowest
// MongoShake release that used it. Nothing enforced the relationship, so the
// template reached 14 in the v2.8.7 cycle (810acbf raised CurrentVersion 10->12
// while 0fbb387 moved the template 13->14) and LowestConfigurationVersion
// stopped at key 12.
//
// CheckFcv only compares against FeatureCompatibleVersion, so the mismatch is
// silent today. The map is what renders "please upgrade MongoShake to version
// >= X" in check.go, and a missing key yields the zero value -- an empty
// version string in the one message whose whole job is to name a version.
func TestFcvCountersAgree(t *testing.T) {
	// Reuse the production parser rather than re-reading the file. fcv=0 cannot
	// trip its "too old" branch, so this returns the declared version verbatim.
	shipped, err := CheckFcv(shippedTemplate, 0)
	if err != nil {
		t.Fatalf("parse conf.version out of %s: %v", shippedTemplate, err)
	}
	if shipped <= 0 {
		t.Fatalf("conf.version parsed as %d from %s; a missing or zero key would make "+
			"the comparison below pass vacuously", shipped, shippedTemplate)
	}

	if utils.FcvConfiguration.CurrentVersion != shipped {
		t.Errorf("FcvConfiguration.CurrentVersion = %d but %s declares conf.version = %d; "+
			"keep them equal -- CurrentVersion is meant to be the configuration version "+
			"this binary understands, and the shipped template is the one it ships with",
			utils.FcvConfiguration.CurrentVersion, shippedTemplate, shipped)
	}

	if utils.FcvConfiguration.FeatureCompatibleVersion > utils.FcvConfiguration.CurrentVersion {
		t.Errorf("FcvConfiguration.FeatureCompatibleVersion (%d) exceeds CurrentVersion (%d), "+
			"so every conf would be rejected as too old",
			utils.FcvConfiguration.FeatureCompatibleVersion, utils.FcvConfiguration.CurrentVersion)
	}
	if utils.FcvCheckpoint.FeatureCompatibleVersion > utils.FcvCheckpoint.CurrentVersion {
		t.Errorf("FcvCheckpoint.FeatureCompatibleVersion (%d) exceeds CurrentVersion (%d)",
			utils.FcvCheckpoint.FeatureCompatibleVersion, utils.FcvCheckpoint.CurrentVersion)
	}
}

// TestLowestVersionMapsAreTotal asserts both Lowest*Version maps cover every
// key from 0 to the corresponding CurrentVersion, with a non-empty value.
//
// ckpt_manager.go indexes LowestCheckpointVersion by CurrentVersion directly,
// and check.go indexes LowestConfigurationVersion by FeatureCompatibleVersion.
// Either lookup silently yields "" when a key is absent, which is how an
// upgrade hint ends up telling the user to upgrade to nothing.
func TestLowestVersionMapsAreTotal(t *testing.T) {
	for v := 0; v <= utils.FcvConfiguration.CurrentVersion; v++ {
		ver, ok := utils.LowestConfigurationVersion[v]
		if !ok {
			t.Errorf("LowestConfigurationVersion has no key %d, but FcvConfiguration.CurrentVersion is %d; "+
				"a FeatureCompatibleVersion of %d would render an empty version in the upgrade hint",
				v, utils.FcvConfiguration.CurrentVersion, v)
			continue
		}
		if strings.TrimSpace(ver) == "" {
			t.Errorf("LowestConfigurationVersion[%d] is empty", v)
		}
	}

	for v := 0; v <= utils.FcvCheckpoint.CurrentVersion; v++ {
		ver, ok := utils.LowestCheckpointVersion[v]
		if !ok {
			t.Errorf("LowestCheckpointVersion has no key %d, but FcvCheckpoint.CurrentVersion is %d; "+
				"ckpt_manager.go indexes this map by CurrentVersion", v, utils.FcvCheckpoint.CurrentVersion)
			continue
		}
		if strings.TrimSpace(ver) == "" {
			t.Errorf("LowestCheckpointVersion[%d] is empty", v)
		}
	}
}

// innerVariablesMarker is the comment in configure.go separating the options
// users are expected to set from the ones they are not.
const innerVariablesMarker = "inner variables, not open to user"

// TestUserFacingConfigTagsAppearInTemplate asserts every config tag declared
// above the inner-variables marker in configure.go is mentioned somewhere in
// the shipped template.
//
// The match must be unanchored: the template documents options that already
// have a usable default as commented examples ("# incr_sync.lazy_oplog_parse =
// true"), and an anchored grep reports those as missing. Tags below the marker
// are exempt -- all twelve currently-absent ones live there, and several are
// explicitly deprecated or marked "not open to user".
func TestUserFacingConfigTagsAppearInTemplate(t *testing.T) {
	tags, err := userFacingConfigTags("../../collector/configure/configure.go")
	if err != nil {
		t.Fatalf("collect config tags: %v", err)
	}
	if len(tags) == 0 {
		t.Fatal("collected no config tags; the marker comment or the struct tag key must have changed")
	}

	raw, err := os.ReadFile(shippedTemplate)
	if err != nil {
		t.Fatalf("read template: %v", err)
	}
	template := string(raw)

	for _, tag := range tags {
		if !strings.Contains(template, tag) {
			t.Errorf("config tag %q is declared as a user-facing option in configure.go but does not "+
				"appear anywhere in %s, so users cannot discover it from the template", tag, shippedTemplate)
		}
	}
}

// userFacingConfigTags returns every `config:"..."` value declared in the
// Configuration struct above the inner-variables marker comment.
func userFacingConfigTags(path string) ([]string, error) {
	src, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, src, parser.ParseComments)
	if err != nil {
		return nil, err
	}

	markerLine := 0
	for _, cg := range file.Comments {
		for _, c := range cg.List {
			if strings.Contains(c.Text, innerVariablesMarker) {
				markerLine = fset.Position(c.Pos()).Line
			}
		}
	}
	if markerLine == 0 {
		return nil, os.ErrNotExist
	}

	var tags []string
	ast.Inspect(file, func(n ast.Node) bool {
		field, ok := n.(*ast.Field)
		if !ok || field.Tag == nil {
			return true
		}
		if fset.Position(field.Pos()).Line > markerLine {
			return true
		}
		tag := reflect.StructTag(strings.Trim(field.Tag.Value, "`")).Get("config")
		if tag != "" {
			tags = append(tags, tag)
		}
		return true
	})
	return tags, nil
}
