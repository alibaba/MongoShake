package filter

import (
	"sort"
	"strings"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/alibaba/MongoShake/v2/oplog"
	l "github.com/alibaba/MongoShake/v2/pkg/log"
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

// BuildExclusionProjection builds {big1:0, big2:0, ...} for server-side exclusion
// projection (blacklist). _id is intentionally omitted: exclusion projection keeps
// _id by default and cannot mix inclusion/exclusion values. Keys are sorted.
func BuildExclusionProjection(fields map[string]struct{}) bson.D {
	names := make([]string, 0, len(fields))
	for f := range fields {
		names = append(names, f)
	}
	sort.Strings(names)

	proj := make(bson.D, 0, len(fields))
	for _, f := range names {
		proj = append(proj, bson.E{Key: f, Value: 0})
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

// ExcludeDocument keeps _id and every top-level field NOT in the blacklist.
// Dual of ProjectDocument; used for insert / replace / looked-up full documents.
func ExcludeDocument(doc bson.D, fields map[string]struct{}) bson.D {
	out := make(bson.D, 0, len(doc))
	for _, e := range doc {
		if e.Key == "_id" {
			out = append(out, e)
			continue
		}
		if _, ok := fields[e.Key]; !ok {
			out = append(out, e)
		}
	}
	return out
}

// FilterModifiers keeps only $set/$unset/$push entries whose first path component is
// whitelisted. Returns the rebuilt modifier object and whether nothing remains
// (caller should drop the event in that case). $push carries change-stream array
// truncation (#986) and must be filtered too, else a pure truncation is dropped and
// a mixed event leaks non-whitelisted arrays.
func FilterModifiers(obj bson.D, fields map[string]struct{}) (bson.D, bool) {
	out := make(bson.D, 0, len(obj))
	for _, e := range obj {
		switch e.Key {
		case "$set", "$unset", "$push":
			kept := filterModifierValue(e.Value, fields)
			if len(kept) == 0 {
				continue
			}
			out = append(out, bson.E{Key: e.Key, Value: kept})
		default:
			out = append(out, e)
		}
	}
	return out, len(out) == 0
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

// FilterModifiersExclude keeps $set/$unset/$push entries whose first path component is
// NOT blacklisted. Dual of FilterModifiers; returns the rebuilt modifier and
// whether nothing remains (caller drops the event then). $push (change-stream array
// truncation, #986) must be filtered too: a pure non-blacklisted truncation must be
// kept, and a blacklisted array truncation must not leak into the target.
func FilterModifiersExclude(obj bson.D, fields map[string]struct{}) (bson.D, bool) {
	out := make(bson.D, 0, len(obj))
	for _, e := range obj {
		switch e.Key {
		case "$set", "$unset", "$push":
			kept := filterModifierValueExclude(e.Value, fields)
			if len(kept) == 0 {
				continue
			}
			out = append(out, bson.E{Key: e.Key, Value: kept})
		default:
			out = append(out, e)
		}
	}
	return out, len(out) == 0
}

func filterModifierValueExclude(val interface{}, fields map[string]struct{}) bson.M {
	kept := bson.M{}
	switch v := val.(type) {
	case bson.M:
		for k, mv := range v {
			if _, ok := fields[firstComponent(k)]; !ok {
				kept[k] = mv
			}
		}
	case bson.D:
		for _, e := range v {
			if _, ok := fields[firstComponent(e.Key)]; !ok {
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

// IndexSpecAllowed reports whether no non-_id key field is blacklisted (dual of
// IndexSpecCovered): an index referencing any blacklisted field is skipped entirely.
func IndexSpecAllowed(indexSpec bson.D, fields map[string]struct{}) bool {
	keyDoc, ok := oplog.GetKey(indexSpec, "key").(bson.D)
	if !ok {
		return false
	}
	for _, e := range keyDoc {
		if e.Key == "_id" {
			continue
		}
		if _, ok := fields[firstComponent(e.Key)]; ok {
			return false
		}
	}
	return true
}

// FieldFilter projects synced documents and indexes by a per-namespace top-level
// field whitelist or blacklist. Incremental (change stream) only. Each ns uses at
// most one mode (sanitize enforces mutual exclusion). It mutates log.Object in place
// and returns true to drop the event when nothing remains (mirrors OpTypeFilter's
// rewrite-and-maybe-drop pattern).
type FieldFilter struct {
	whitelist map[string]map[string]struct{}
	blacklist map[string]map[string]struct{}
}

func NewFieldFilter(whitelist, blacklist map[string]map[string]struct{}) *FieldFilter {
	return &FieldFilter{whitelist: whitelist, blacklist: blacklist}
}

func (f *FieldFilter) Filter(log *oplog.PartialLog) bool {
	if log.Operation == "c" {
		return f.filterCommand(log)
	}

	if fields, ok := f.whitelist[log.Namespace]; ok && len(fields) > 0 {
		return f.filterWhitelist(log, fields)
	}
	if fields, ok := f.blacklist[log.Namespace]; ok && len(fields) > 0 {
		return f.filterBlacklist(log, fields)
	}
	return false
}

func (f *FieldFilter) filterWhitelist(log *oplog.PartialLog, fields map[string]struct{}) bool {
	switch log.Operation {
	case "i":
		if err := log.MaterializeObject(); err != nil {
			l.Logger.Errorf("FieldFilter materialize object failed ns[%v]: %v", log.Namespace, err)
			return false
		}
		log.Object = ProjectDocument(log.Object, fields)
		return false
	case "u":
		if err := log.MaterializeObject(); err != nil {
			l.Logger.Errorf("FieldFilter materialize object failed ns[%v]: %v", log.Namespace, err)
			return false
		}
		if log.ObjectHasPrefix("$") {
			newObj, empty := FilterModifiers(log.Object, fields)
			if empty {
				return true
			}
			log.Object = newObj
			return false
		}
		log.Object = ProjectDocument(log.Object, fields)
		return false
	default: // "d" delete, "n" noop, others: leave untouched
		return false
	}
}

func (f *FieldFilter) filterBlacklist(log *oplog.PartialLog, fields map[string]struct{}) bool {
	switch log.Operation {
	case "i":
		if err := log.MaterializeObject(); err != nil {
			l.Logger.Errorf("FieldFilter materialize object failed ns[%v]: %v", log.Namespace, err)
			return false
		}
		log.Object = ExcludeDocument(log.Object, fields)
		return false
	case "u":
		if err := log.MaterializeObject(); err != nil {
			l.Logger.Errorf("FieldFilter materialize object failed ns[%v]: %v", log.Namespace, err)
			return false
		}
		if log.ObjectHasPrefix("$") {
			newObj, empty := FilterModifiersExclude(log.Object, fields)
			if empty {
				return true
			}
			log.Object = newObj
			return false
		}
		log.Object = ExcludeDocument(log.Object, fields)
		return false
	default: // "d" delete, "n" noop, others: leave untouched
		return false
	}
}

// filterCommand only handles createIndexes; all other commands pass through.
// change stream emits createIndexes on ns "db.$cmd", so rebuild the collection
// namespace from the command value to look up the whitelist/blacklist.
func (f *FieldFilter) filterCommand(log *oplog.PartialLog) bool {
	if err := log.MaterializeObject(); err != nil {
		l.Logger.Errorf("FieldFilter materialize object failed ns[%v]: %v", log.Namespace, err)
		return false
	}
	command, found := oplog.ExtraCommandName(log.Object)
	if !found || command != "createIndexes" {
		return false
	}
	coll, ok := oplog.GetKey(log.Object, "createIndexes").(string)
	if !ok {
		return false
	}
	db := strings.SplitN(log.Namespace, ".", 2)[0]
	ns := db + "." + coll

	var keep func(bson.D) bool
	if fields, ok := f.whitelist[ns]; ok && len(fields) > 0 {
		keep = func(spec bson.D) bool { return IndexSpecCovered(spec, fields) }
	} else if fields, ok := f.blacklist[ns]; ok && len(fields) > 0 {
		keep = func(spec bson.D) bool { return IndexSpecAllowed(spec, fields) }
	} else {
		return false
	}

	indexes, ok := oplog.GetKey(log.Object, "indexes").(bson.A)
	if !ok {
		return false
	}
	remain := make(bson.A, 0, len(indexes))
	for _, ele := range indexes {
		spec, ok := ele.(bson.D)
		if !ok {
			remain = append(remain, ele) // unknown shape, keep
			continue
		}
		if keep(spec) {
			remain = append(remain, ele)
		}
	}
	oplog.SetFiled(log.Object, "indexes", remain)
	return len(remain) == 0
}
