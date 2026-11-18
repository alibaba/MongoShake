package oplog

import (
	"sort"
	"strings"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	l "github.com/alibaba/MongoShake/v2/pkg/log"
)

const (
	ShardByID        = "id"
	ShardByNamespace = "collection"
	ShardAutomatic   = "auto"
)

const (
	DefaultHashValue = uint32(0)
)

type Hasher interface {
	DistributeOplogByMod(log *PartialLog, mod int) uint32
}

// TableHasher hash by namespace
type TableHasher struct {
	Hasher
}

func (collectionHasher *TableHasher) DistributeOplogByMod(log *PartialLog, mod int) uint32 {
	if mod == 1 {
		return 0
	}
	if len(log.Namespace) == 0 {
		return DefaultHashValue
	}

	// when oplog is DDL, go into worker 0.
	if log.Operation == "c" {
		return 0
	}

	return stringHashValue(log.Namespace) % uint32(mod)
}

// PrimaryKeyHasher will try to hash by _id firstly, {op:c} will use namespace instead
type PrimaryKeyHasher struct {
	Hasher
}

// DistributeOplogByMod
// We need to ensure that oplog entry will be sent to the same job[$hash] if they have the same ObjectID.
// thus we can consume the oplog entry sequentially
func (objectIdHasher *PrimaryKeyHasher) DistributeOplogByMod(log *PartialLog, mod int) uint32 {
	if mod == 1 {
		return 0
	}

	var hashObject interface{}

	switch log.Operation {
	case "i", "d", "u", "c":
		hashObject = GetIdOrNSFromOplog(log)
	case "n":
		return DefaultHashValue
	}

	if hashObject == nil {
		l.Logger.Warnf("Couldn't extract hash object. collector has mixed up. use Oplog.Namespace instead %v", log)
		hashObject = log.Namespace
	}

	return Hash(hashObject) % uint32(mod)
}

// WhiteListObjectIdHasher
// 1) hash by collection in general;
// 2) hash by objectId only when hit whitelist;
type WhiteListObjectIdHasher struct {
	Hasher

	TableHasher
	PrimaryKeyHasher

	whiteList map[string]struct{} // no need to add lock, only reading operation
}

func NewWhiteListObjectIdHasher(whiteList []string) *WhiteListObjectIdHasher {
	mp := make(map[string]struct{}, len(whiteList))
	for _, ele := range whiteList {
		mp[ele] = struct{}{}
	}

	return &WhiteListObjectIdHasher{
		TableHasher:      TableHasher{},
		PrimaryKeyHasher: PrimaryKeyHasher{},
		whiteList:        mp,
	}
}

func (wloi *WhiteListObjectIdHasher) DistributeOplogByMod(log *PartialLog, mod int) uint32 {
	ns := log.Namespace
	if len(ns) == 0 {
		return DefaultHashValue
	}

	if _, ok := wloi.whiteList[ns]; ok {
		return wloi.PrimaryKeyHasher.DistributeOplogByMod(log, mod)
	}
	return wloi.TableHasher.DistributeOplogByMod(log, mod)
}

// UniqueIndexAwareHasher is the "auto" shard key: a namespace that carries a
// unique index other than _id replays on a single worker (in source order),
// and every other namespace hashes by _id.
//
// _id level dispatch is unsafe once a namespace has a non-_id unique index. The
// oplog does not carry the previous value of an updated field, so an insert of
// a unique key value cannot be ordered against the update that releases it.
// When the insert wins, the target rejects it with E11000 and the default
// dup_key_strategy=ignore drops the document.
//
// Splitting inside such a namespace is not possible with a static hash either:
// routing inserts by unique key and updates by _id sends the operations of one
// document to different workers, and an update applied before the insert of the
// same document is lost. The whole namespace therefore stays on one worker.
//
// The set starts from the index metadata scanned at startup and only grows:
// a createIndexes oplog switches its namespace at the DDL barrier, and a
// renamed namespace inherits the constraint. Dropping the index does not switch
// back, which keeps the guarantee for the rest of the run.
type UniqueIndexAwareHasher struct {
	mu      sync.RWMutex
	ordered map[string]struct{}

	idHasher PrimaryKeyHasher
	nsHasher TableHasher
}

func NewUniqueIndexAwareHasher(orderedNamespaces []string) *UniqueIndexAwareHasher {
	hasher := &UniqueIndexAwareHasher{
		ordered: make(map[string]struct{}, len(orderedNamespaces)),
	}
	for _, ns := range orderedNamespaces {
		if ns != "" {
			hasher.ordered[ns] = struct{}{}
		}
	}
	return hasher
}

func (hasher *UniqueIndexAwareHasher) DistributeOplogByMod(log *PartialLog, mod int) uint32 {
	if mod == 1 {
		return 0
	}

	if hasher.IsOrdered(log.Namespace) {
		return hasher.nsHasher.DistributeOplogByMod(log, mod)
	}
	return hasher.idHasher.DistributeOplogByMod(log, mod)
}

// IsOrdered reports whether the namespace replays on a single worker.
func (hasher *UniqueIndexAwareHasher) IsOrdered(ns string) bool {
	hasher.mu.RLock()
	defer hasher.mu.RUnlock()

	_, ok := hasher.ordered[ns]
	return ok
}

// MarkOrdered switches a namespace to ordered replay. It returns true only for
// the call that performed the switch.
func (hasher *UniqueIndexAwareHasher) MarkOrdered(ns string) bool {
	if ns == "" {
		return false
	}

	hasher.mu.Lock()
	defer hasher.mu.Unlock()

	if _, ok := hasher.ordered[ns]; ok {
		return false
	}
	hasher.ordered[ns] = struct{}{}
	return true
}

// OrderedNamespaces returns the namespaces that replay in order, sorted.
func (hasher *UniqueIndexAwareHasher) OrderedNamespaces() []string {
	hasher.mu.RLock()
	defer hasher.mu.RUnlock()

	nsList := make([]string, 0, len(hasher.ordered))
	for ns := range hasher.ordered {
		nsList = append(nsList, ns)
	}
	sort.Strings(nsList)
	return nsList
}

// MarkOrderedByDDL switches the namespaces whose dispatch a DDL oplog makes
// unsafe and returns the namespaces that changed. It must be called where the
// DDL is turned into a barrier, so every oplog dispatched before it has been
// acknowledged before the new routing takes effect.
func (hasher *UniqueIndexAwareHasher) MarkOrderedByDDL(log *PartialLog) []string {
	if log == nil || log.Operation != "c" {
		return nil
	}

	var switched []string
	if ns, ok := EnforcesUniqueIndex(log); ok && hasher.MarkOrdered(ns) {
		switched = append(switched, ns)
	}

	if from, to, ok := RenameCollection(log); ok && hasher.IsOrdered(from) && hasher.MarkOrdered(to) {
		switched = append(switched, to)
	}

	for _, ns := range switched {
		l.Logger.Warnf("shard_key=auto: namespace[%s] switched to ordered replay by ddl ts[%v]: "+
			"it now carries a unique index besides _id and replays on a single worker", ns, log.Timestamp)
	}
	return switched
}

// EnforcesUniqueIndex reports whether a DDL oplog makes the namespace carry a
// unique index that constrains more than _id, and returns that namespace. An
// index whose leading key field is _id adds nothing to the _id dispatch.
//
// The shapes a replica set records:
//   - createIndexes with an "indexes" array, for a collection that holds data;
//   - createIndexes with the only specification inlined in o, for an index
//     built on an empty collection (the executor unwraps the same shape);
//   - startIndexBuild/commitIndexBuild, the two phase build of a non-empty
//     collection. startIndexBuild is filtered before dispatch today, so
//     commitIndexBuild is the effective trigger of the pair;
//   - collMod index.unique, which converts an existing index (MongoDB 7.0+).
func EnforcesUniqueIndex(log *PartialLog) (string, bool) {
	if log == nil || log.Operation != "c" {
		return "", false
	}

	database, _, found := strings.Cut(log.Namespace, ".")
	if !found || database == "" {
		return "", false
	}
	object := log.ObjectValue()

	for _, command := range []string{"createIndexes", "startIndexBuild", "commitIndexBuild"} {
		value, ok := LookupDocument(object, command)
		if !ok {
			continue
		}
		collection, ok := value.(string)
		if !ok || collection == "" {
			continue
		}

		indexes, ok := LookupDocument(object, "indexes")
		if !ok {
			if command == "createIndexes" && constrainsBeyondID(object) {
				return database + "." + collection, true
			}
			continue
		}
		for _, index := range documentList(indexes) {
			if constrainsBeyondID(index) {
				return database + "." + collection, true
			}
		}
	}

	// the collMod oplog carries only the index name, so the key pattern is not
	// available and any unique conversion is treated as constraining
	if value, ok := LookupDocument(object, "collMod"); ok {
		if collection, ok := value.(string); ok && collection != "" {
			if index, ok := LookupDocument(object, "index"); ok && indexIsUnique(index) {
				return database + "." + collection, true
			}
		}
	}

	return "", false
}

// constrainsBeyondID reports whether an index specification is unique and led by
// a field other than _id.
func constrainsBeyondID(spec interface{}) bool {
	if !indexIsUnique(spec) {
		return false
	}
	key, ok := LookupDocument(spec, "key")
	if !ok {
		return false
	}
	return !indexKeyLedByID(key)
}

func indexIsUnique(spec interface{}) bool {
	unique, ok := LookupDocument(spec, "unique")
	if !ok {
		return false
	}
	isUnique, ok := unique.(bool)
	return ok && isUnique
}

// RenameCollection returns the source and the target namespace of a
// renameCollection oplog.
func RenameCollection(log *PartialLog) (from string, to string, ok bool) {
	if log == nil || log.Operation != "c" {
		return "", "", false
	}

	object := log.ObjectValue()
	fromValue, found := LookupDocument(object, "renameCollection")
	if !found {
		return "", "", false
	}
	toValue, found := LookupDocument(object, "to")
	if !found {
		return "", "", false
	}

	from, fromOk := fromValue.(string)
	to, toOk := toValue.(string)
	if !fromOk || !toOk || from == "" || to == "" {
		return "", "", false
	}
	return from, to, true
}

// documentList normalizes an array of index specifications. A command oplog is
// decoded eagerly, so the input is normally bson.A of bson.D.
func documentList(value interface{}) []interface{} {
	switch list := value.(type) {
	case bson.A:
		return list
	case []bson.D:
		documents := make([]interface{}, len(list))
		for i, document := range list {
			documents[i] = document
		}
		return documents
	case []interface{}:
		return list
	}
	return nil
}

func indexKeyLedByID(key interface{}) bool {
	switch document := key.(type) {
	case bson.D:
		return len(document) > 0 && document[0].Key == PrimaryKey
	case bson.Raw:
		element, err := document.IndexErr(0)
		return err == nil && element.Key() == PrimaryKey
	}
	// bson.M is unordered and an unknown shape cannot prove _id leads.
	return false
}

/*********************************************/
func getValueFromBsonD(obj bson.D, key string) (interface{}, bool) {
	for _, ele := range obj {
		if ele.Key == key {
			return ele.Value, true
		}
	}

	return nil, false
}
func GetIdOrNSFromOplog(log *PartialLog) interface{} {
	switch log.Operation {
	case "i", "d":
		return log.ObjectKey("")
	case "u":
		if id, ok := LookupDocument(log.QueryValue(), "_id"); ok {
			return id
		} else {
			return log.ObjectKey("")
		}
	case "c":
		// we don't treat vectored insert oplog(o.applyOps:{$exists:true}) as txns and dispatch to fixed worker 0
		if log.MultiOpType != nil && *log.MultiOpType == 1 {
			// use 'ts' field to hash
			return log.Timestamp.T + log.Timestamp.I
		}
		return log.Namespace
	default:
		l.Logger.Criticalf("Unrecognized oplog object operation %s", log.Operation)
	}

	return log.Namespace
}

func stringHashValue(s string) uint32 {
	// consult from Java String.hashcode()
	var hashValue uint32
	for _, c := range s {
		hashValue = 31*hashValue + uint32(c)
	}
	if hashValue < 0 {
		return -hashValue
	}

	return hashValue
}

func Hash(hashObject interface{}) uint32 {
	switch object := hashObject.(type) {
	case primitive.ObjectID:
		return stringHashValue(object.Hex())
	case string:
		return stringHashValue(object)
	case int64:
		return uint32(object)
	case int32:
		return uint32(object)
	case int:
		return uint32(object)
	case uint:
		return uint32(object)
	case uint64:
		return uint32(object)
	case uint32:
		return object
	case nil:
		l.Logger.Warnf("Hash object is NIL. use default value %d", DefaultHashValue)
	default:
		l.Logger.Warnf("Hash object is UNKNOWN type[%T], value is [%v]. use default value %d",
			hashObject, hashObject, DefaultHashValue)
	}

	return DefaultHashValue
}
