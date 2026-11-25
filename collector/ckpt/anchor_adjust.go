package ckpt

import (
	"context"
	"fmt"
	"math"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	l "github.com/alibaba/MongoShake/v2/pkg/log"
)

// AnchorAdjustResult is the outcome of a conditional anchor adjustment. Conflict
// and WriteUncertain are NOT success: the caller must converge to the current
// durable anchor instead of streaming from the requested value (#1052 HA P1).
type AnchorAdjustResult int

const (
	AnchorAdjusted AnchorAdjustResult = iota
	AnchorConflict
	AnchorWriteUncertain
	AnchorReadFailed
	AnchorGuardFailed
)

// ConditionalAnchorStore is an optional storage capability for the pending-initial
// anchor protocol. Mongo implements it atomically (CAS + covering unique index);
// HTTP implements non-atomic single-writer semantics (quorum requires database
// storage, sanitize.go rejects quorum+API). The local manager lock does not
// replace cross-process conditional writes.
type ConditionalAnchorStore interface {
	// AdjustFrom sets the patch only if the stored ckpt still equals expected.
	// Returns the post-update storage mirror on success; adjusted=false means
	// conflict-or-absent (caller re-reads to distinguish).
	AdjustFrom(expected int64, patch bson.M) (mirror *CheckpointContext, adjusted bool, err error)
	// CreateIfAbsent inserts doc only when no record exists. created reports
	// whether THIS call inserted; mirror is the readback (caller verifies the
	// anchor still matches the request before treating it as confirmed).
	CreateIfAbsent(doc bson.M) (mirror *CheckpointContext, created bool, err error)
	// EnsureAnchorGuard verifies/creates the unique index covering every legal
	// checkpoint name write, with comparison semantics matching name queries.
	EnsureAnchorGuard() error
}

// ckptFilter matches the logical packed timestamp across every physical encoding
// decodeCheckpointTimestamp accepts (Int64 current, BSON Timestamp legacy mgo,
// Int32). A normalized-int64-only filter never matches legacy records and would
// CAS-fail forever (#1052 v3 review P1-1).
func ckptFilter(name string, expected int64) bson.M {
	forms := bson.A{
		bson.M{CheckpointName: name, "ckpt": expected},
		bson.M{CheckpointName: name, "ckpt": primitive.Timestamp{T: uint32(expected >> 32), I: uint32(expected)}},
	}
	if expected >= math.MinInt32 && expected <= math.MaxInt32 {
		forms = append(forms, bson.M{CheckpointName: name, "ckpt": int32(expected)})
	}
	return bson.M{"$or": forms}
}

func (ckpt *MongoCheckpoint) readMirror() (*CheckpointContext, error) {
	value := new(CheckpointContext)
	err := ckpt.client.Client.Database(ckpt.DB).Collection(ckpt.Table).FindOne(context.Background(),
		bson.M{CheckpointName: ckpt.Name}).Decode(value)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	if err != nil {
		ckpt.close()
		return nil, err
	}
	return value, nil
}

func (ckpt *MongoCheckpoint) AdjustFrom(expected int64, patch bson.M) (*CheckpointContext, bool, error) {
	if !ckpt.ensureNetwork() {
		return nil, false, fmt.Errorf("%s anchor adjust ensureNetwork failed", ckpt.Name)
	}
	opts := options.FindOneAndUpdate().SetReturnDocument(options.After)
	var mirror CheckpointContext
	err := ckpt.client.Client.Database(ckpt.DB).Collection(ckpt.Table).FindOneAndUpdate(context.Background(),
		ckptFilter(ckpt.Name, expected), bson.M{"$set": patch}, opts).Decode(&mirror)
	if err == mongo.ErrNoDocuments {
		return nil, false, nil
	}
	if err != nil {
		ckpt.close()
		return nil, false, err
	}
	return &mirror, true, nil
}

func (ckpt *MongoCheckpoint) CreateIfAbsent(doc bson.M) (*CheckpointContext, bool, error) {
	if !ckpt.ensureNetwork() {
		return nil, false, fmt.Errorf("%s anchor create ensureNetwork failed", ckpt.Name)
	}
	res, err := ckpt.client.Client.Database(ckpt.DB).Collection(ckpt.Table).UpdateOne(context.Background(),
		bson.M{CheckpointName: ckpt.Name}, bson.M{"$setOnInsert": doc}, options.Update().SetUpsert(true))
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			// lost the unique-key race: read the winner, never overwrite it
			mirror, rerr := ckpt.readMirror()
			return mirror, false, rerr
		}
		ckpt.close()
		return nil, false, err
	}
	created := res.UpsertedCount > 0
	mirror, rerr := ckpt.readMirror()
	return mirror, created, rerr
}

// descriptive index metadata that does not affect constraint coverage; these are
// whitelisted so normal fields (v, name, hidden, ...) are not rejected as unknown
// behavioral options (#1052 v4 review addition 3).
var indexDescriptiveFields = map[string]bool{
	"v": true, "key": true, "name": true, "ns": true,
	"background": true, "hidden": true, "unique": true,
}

// acceptableGuardIndex proves the index enforces uniqueness over EVERY legal
// checkpoint name write with the same comparison semantics as name queries.
// key+unique alone is insufficient: partial/sparse/TTL/collation can leave legal
// documents unconstrained (#1052 v3 review P1-1).
func acceptableGuardIndex(idx bson.M) bool {
	key, ok := idx["key"].(bson.M)
	if !ok || len(key) != 1 {
		return false
	}
	dir, ok := key["name"]
	if !ok || !indexDirectionOne(dir) {
		return false
	}
	if unique, _ := idx["unique"].(bool); !unique {
		return false
	}
	for field, val := range idx {
		if indexDescriptiveFields[field] {
			continue
		}
		switch field {
		case "partialFilterExpression":
			return false // checkpoint docs lack the filter field -> unconstrained
		case "sparse":
			if b, _ := val.(bool); b {
				return false
			}
		case "expireAfterSeconds":
			return false // TTL deletes anchors
		case "collation":
			if !collationSimple(val) {
				return false
			}
		default:
			return false // unknown behavioral option: conservative reject
		}
	}
	return true
}

func indexDirectionOne(v interface{}) bool {
	switch n := v.(type) {
	case int32:
		return n == 1 || n == -1
	case int64:
		return n == 1 || n == -1
	case float64:
		return n == 1 || n == -1
	}
	return false
}

func collationSimple(v interface{}) bool {
	c, ok := v.(bson.M)
	if !ok {
		return false
	}
	locale, _ := c["locale"].(string)
	return locale == "simple"
}

// collectionCollationSimple checks the collection default collation. A non-simple
// default makes name queries case-insensitive while a simple index allows "RS"/"rs"
// to coexist; fail-closed even when no name index exists yet (#1052 v4 review G2).
func (ckpt *MongoCheckpoint) collectionCollationSimple() error {
	var resp bson.M
	err := ckpt.client.Client.Database(ckpt.DB).RunCommand(context.Background(), bson.D{
		{Key: "listCollections", Value: 1},
		{Key: "filter", Value: bson.M{"name": ckpt.Table}},
	}).Decode(&resp)
	if err != nil {
		return fmt.Errorf("listCollections for collation check: %w", err)
	}
	cursor, _ := resp["cursor"].(bson.M)
	batch, _ := cursor["firstBatch"].(bson.A)
	if len(batch) == 0 {
		return nil // collection absent -> will be created with simple default
	}
	entry, _ := batch[0].(bson.M)
	opts, _ := entry["options"].(bson.M)
	collation, present := opts["collation"]
	if !present {
		return nil
	}
	if !collationSimple(collation) {
		return fmt.Errorf("collection %s default collation is not simple; anchor guard cannot prove name-query comparison semantics (fail-closed)", ckpt.Table)
	}
	return nil
}

func (ckpt *MongoCheckpoint) EnsureAnchorGuard() error {
	if !ckpt.ensureNetwork() {
		return fmt.Errorf("%s anchor guard ensureNetwork failed", ckpt.Name)
	}
	if guardPauseHook != nil {
		guardPauseHook()
	}
	if err := ckpt.collectionCollationSimple(); err != nil {
		return err
	}
	coll := ckpt.client.Client.Database(ckpt.DB).Collection(ckpt.Table)
	cursor, err := coll.Indexes().List(context.Background())
	if err != nil {
		return fmt.Errorf("listIndexes for anchor guard: %w", err)
	}
	var indexes []bson.M
	if err := cursor.All(context.Background(), &indexes); err != nil {
		return fmt.Errorf("decode indexes for anchor guard: %w", err)
	}
	for _, idx := range indexes {
		if acceptableGuardIndex(idx) {
			return nil
		}
	}
	_, err = coll.Indexes().CreateOne(context.Background(), mongo.IndexModel{
		Keys:    bson.D{{Key: CheckpointName, Value: 1}},
		Options: options.Index().SetUnique(true).SetName("name_1"),
	})
	if err != nil {
		return fmt.Errorf("anchor guard unique index creation failed (duplicate records / conflicting index options / permission / unsupported topology); resolve manually, no auto-cleanup: %w", err)
	}
	l.Logger.Infof("%s anchor guard unique index {name:1} created", ckpt.Name)
	return nil
}

// HTTP delegate: non-atomic single-writer semantics. Quorum requires database
// storage (sanitize.go), so this path never serves concurrent owners.
func (ckpt *HttpApiCheckpoint) EnsureAnchorGuard() error { return nil }

func (ckpt *HttpApiCheckpoint) AdjustFrom(expected int64, patch bson.M) (*CheckpointContext, bool, error) {
	base, exist := ckpt.Get()
	if base == nil {
		return nil, false, fmt.Errorf("http anchor read failed")
	}
	if !exist || base.Timestamp != expected {
		return base, false, nil
	}
	merged := *base
	applyAnchorPatch(&merged, patch)
	if err := ckpt.Insert(&merged); err != nil {
		return nil, false, err
	}
	mirror, exist := ckpt.Get()
	if mirror == nil || !exist {
		return nil, true, fmt.Errorf("http anchor written but confirmation readback failed")
	}
	return mirror, true, nil
}

func (ckpt *HttpApiCheckpoint) CreateIfAbsent(doc bson.M) (*CheckpointContext, bool, error) {
	base, exist := ckpt.Get()
	if base == nil {
		return nil, false, fmt.Errorf("http anchor read failed")
	}
	if exist {
		return base, false, nil
	}
	var ctx CheckpointContext
	raw, err := bson.Marshal(doc)
	if err != nil {
		return nil, false, err
	}
	if err := bson.Unmarshal(raw, &ctx); err != nil {
		return nil, false, err
	}
	if err := ckpt.Insert(&ctx); err != nil {
		return nil, false, err
	}
	mirror, _ := ckpt.Get()
	return mirror, true, nil
}

func applyAnchorPatch(ctx *CheckpointContext, patch bson.M) {
	if v, ok := patch["ckpt"].(int64); ok {
		ctx.Timestamp = v
	}
	if v, ok := patch["version"].(int); ok {
		ctx.Version = v
	}
	if v, ok := patch["fetch_method"].(string); ok {
		ctx.FetchMethod = v
	}
	if v, ok := patch["oplog_disk_queue"].(string); ok {
		ctx.OplogDiskQueue = v
	}
	if v, ok := patch["oplog_disk_queue_apply_finish_ts"].(int64); ok {
		ctx.OplogDiskQueueFinishTs = v
	}
	if v, ok := patch["full_finish_ts"].(int64); ok {
		ctx.FullFinishTs = v
	}
}

// guardPauseHook is a test seam invoked after ensureNetwork in EnsureAnchorGuard;
// nil in production. It lets a test invalidate the delegate client mid-guard to
// prove the guard is serialized with Get under the manager lock.
var guardPauseHook func()
