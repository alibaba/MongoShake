package filter

import (
	"sort"
	"strings"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/alibaba/MongoShake/v2/oplog"
)

// firstComponent returns the part of a field path before the first dot, so a
// whitelisted top-level field also matches its sub-path updates (e.g. selecting
// "profile" keeps "profile.city"). See SDD 2026-09-22 section 3.
func firstComponent(path string) string {
	if i := strings.IndexByte(path, '.'); i >= 0 {
		return path[:i]
	}
	return path
}

// BuildInclusionProjection builds {_id:1, f1:1, ...} for server-side projection.
// Keys are sorted for deterministic output.
func BuildInclusionProjection(fields map[string]struct{}) bson.D {
	names := make([]string, 0, len(fields))
	for f := range fields {
		names = append(names, f)
	}
	sort.Strings(names)

	proj := make(bson.D, 0, len(fields)+1)
	proj = append(proj, bson.E{Key: "_id", Value: 1})
	for _, f := range names {
		proj = append(proj, bson.E{Key: f, Value: 1})
	}
	return proj
}

// ProjectDocument keeps _id and top-level fields whose name is exactly whitelisted.
// Used for insert / replace / looked-up full documents.
func ProjectDocument(doc bson.D, fields map[string]struct{}) bson.D {
	out := make(bson.D, 0, len(fields)+1)
	for _, e := range doc {
		if e.Key == "_id" {
			out = append(out, e)
			continue
		}
		if _, ok := fields[e.Key]; ok {
			out = append(out, e)
		}
	}
	return out
}

// FilterModifiers keeps only $set/$unset entries whose first path component is
// whitelisted. Returns the rebuilt modifier object and whether nothing remains
// (caller should drop the event in that case).
func FilterModifiers(obj bson.D, fields map[string]struct{}) (bson.D, bool) {
	out := make(bson.D, 0, len(obj))
	empty := true
	for _, e := range obj {
		switch e.Key {
		case "$set", "$unset":
			kept := filterModifierValue(e.Value, fields)
			if len(kept) == 0 {
				continue
			}
			empty = false
			out = append(out, bson.E{Key: e.Key, Value: kept})
		default:
			out = append(out, e)
		}
	}
	return out, empty
}

func filterModifierValue(val interface{}, fields map[string]struct{}) bson.M {
	kept := bson.M{}
	switch v := val.(type) {
	case bson.M:
		for k, mv := range v {
			if _, ok := fields[firstComponent(k)]; ok {
				kept[k] = mv
			}
		}
	case bson.D:
		for _, e := range v {
			if _, ok := fields[firstComponent(e.Key)]; ok {
				kept[e.Key] = e.Value
			}
		}
	}
	return kept
}

// IndexSpecCovered reports whether every non-_id field in the index key is
// whitelisted (all-or-nothing). See SDD 2026-09-22 section 8.
func IndexSpecCovered(indexSpec bson.D, fields map[string]struct{}) bool {
	keyDoc, ok := oplog.GetKey(indexSpec, "key").(bson.D)
	if !ok {
		return false
	}
	for _, e := range keyDoc {
		if e.Key == "_id" {
			continue
		}
		if _, ok := fields[firstComponent(e.Key)]; !ok {
			return false
		}
	}
	return true
}
