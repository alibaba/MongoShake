package ckpt

import (
	"errors"
	"fmt"
	"sync"

	"go.mongodb.org/mongo-driver/bson"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
)

const (
	CheckpointName = "name"
)

type CheckpointManager struct {
	Type string

	ctx        *CheckpointContext
	ctxRecLock sync.Mutex
	ctxRec     *CheckpointContext // only used to store temporary value that will be lazy load
	finishSet  bool
	queueSet   bool
	fetchSet   bool
	fullFinSet bool
	delegate   CheckpointOperation

	// anchor guard: only success is cached; failure stays un-ready and is
	// re-verified on every call so an operator index fix self-heals (#1052 v3
	// review P2-2). Online dropIndex/dropCollection requires a restart re-check.
	guardLock  sync.Mutex
	guardReady bool
}

func NewCheckpointManager(name string, startPosition int64) *CheckpointManager {
	newManager := &CheckpointManager{}

	switch conf.Options.CheckpointStorage {
	case utils.VarCheckpointStorageApi:
		newManager.delegate = &HttpApiCheckpoint{
			CheckpointContext: CheckpointContext{
				Name:                   name,
				Timestamp:              startPosition,
				Version:                utils.FcvCheckpoint.CurrentVersion,
				OplogDiskQueue:         "",
				OplogDiskQueueFinishTs: InitCheckpoint,
			},
			URL: conf.Options.CheckpointStorageCollection,
		}
	case utils.VarCheckpointStorageDatabase:
		newManager.delegate = &MongoCheckpoint{
			CheckpointContext: CheckpointContext{
				Name:                   name,
				Timestamp:              startPosition,
				Version:                utils.FcvCheckpoint.CurrentVersion,
				OplogDiskQueue:         "",
				OplogDiskQueueFinishTs: InitCheckpoint,
			},
			DB:    conf.Options.CheckpointStorageDb,
			URL:   conf.Options.CheckpointStorageUrl,
			Table: conf.Options.CheckpointStorageCollection,
		}
	default:
		return nil
	}
	return newManager
}

// Get return persistent checkpoint
func (manager *CheckpointManager) Get() (*CheckpointContext, bool, error) {
	// Serialize with Update: the reader's anchor-persist hook writes from the
	// fetcher goroutine (#1052), so an unlocked reload could restore a stale
	// future timestamp over a just-persisted adjusted anchor.
	manager.ctxRecLock.Lock()
	defer manager.ctxRecLock.Unlock()
	var exist bool
	manager.ctx, exist = manager.delegate.Get()
	if manager.ctx == nil {
		return nil, exist, fmt.Errorf("get by checkpoint info from db or api failed, please see err log")
	}

	// check fcv
	if exist && utils.FcvCheckpoint.IsCompatible(manager.ctx.Version) == false {
		return nil, exist, fmt.Errorf("current required checkpoint version[%v] > input[%v],"+
			" please upgrade MongoShake to version >= %v",
			utils.FcvCheckpoint.CurrentVersion, manager.ctx.Version,
			utils.LowestCheckpointVersion[utils.FcvCheckpoint.CurrentVersion])
	}

	return manager.ctx, exist, nil
}

// GetInMemory return in-memory checkpoint
func (manager *CheckpointManager) GetInMemory() *CheckpointContext {
	return manager.ctx
}

// Update checkpoint update memory & persistence(db or file)
// The lock serializes concurrent checkpoint writers (the checkpoint routine
// and the parallel-mode full-finish backfill, #1047) and the reload in Get.
// It does not make the unlocked GetInMemory readers race-free; those are
// sequenced by the startup/handover protocol instead.
func (manager *CheckpointManager) Update(ts int64) error {
	manager.ctxRecLock.Lock()
	defer manager.ctxRecLock.Unlock()
	return manager.updateLocked(ts)
}

// updateLocked is Update's body; callers must hold ctxRecLock.
func (manager *CheckpointManager) updateLocked(ts int64) error {
	if manager.ctx == nil || len(manager.ctx.Name) == 0 {
		// must run Get() first
		return errors.New("current ckpt context is empty")
	}

	candidate := *manager.ctx
	candidate.Timestamp = ts
	candidate.Version = utils.FcvCheckpoint.CurrentVersion

	// update OplogDiskQueueFinishTs if set
	if manager.ctxRec != nil {
		if manager.finishSet {
			candidate.OplogDiskQueueFinishTs = manager.ctxRec.OplogDiskQueueFinishTs
		}
		if manager.queueSet {
			candidate.OplogDiskQueue = manager.ctxRec.OplogDiskQueue
		}
		if manager.fetchSet {
			candidate.FetchMethod = manager.ctxRec.FetchMethod
		}
		if manager.fullFinSet {
			candidate.FullFinishTs = manager.ctxRec.FullFinishTs
		}
	}

	if err := manager.delegate.Insert(&candidate); err != nil {
		return err
	}
	manager.ctx = &candidate
	manager.ctxRec = nil
	manager.finishSet = false
	manager.queueSet = false
	manager.fetchSet = false
	manager.fullFinSet = false
	return nil
}

// FlushStaged persists staged fields without advancing the checkpoint past
// what is already durable. Needed when no oplog traffic moves the checkpoint
// forward (e.g. full sync just finished on an idle source) but the staged
// value must survive a restart. Another writer (docsyncer.Checkpoint at the
// full-sync handover) may have advanced the durable document beyond this
// manager's in-memory view, so refresh from storage first and never write
// backwards (#1050 review B2). The whole read-refresh-write runs under the
// lock so a concurrent Update cannot interleave.
func (manager *CheckpointManager) FlushStaged() error {
	manager.ctxRecLock.Lock()
	defer manager.ctxRecLock.Unlock()

	fresh, exist := manager.delegate.Get()
	if fresh == nil {
		// A nil context means the READ failed (both Mongo and HTTP delegates
		// return nil,false on error, but a non-nil regenerated context with
		// exist=false when no document exists). Writing the stale in-memory
		// ts here would silently reintroduce the backwards write (#1050 R3).
		return errors.New("checkpoint refresh read failed")
	}
	if exist && (manager.ctx == nil || fresh.Timestamp > manager.ctx.Timestamp) {
		manager.ctx = fresh
	}
	if manager.ctx == nil {
		return errors.New("current ckpt context is empty")
	}
	return manager.updateLocked(manager.ctx.Timestamp)
}

// SetOplogDiskFinishTs
// OplogDiskQueueFinishTs and OplogDiskQueue won't take effect immediately, will be inserted in the next Update call.
func (manager *CheckpointManager) SetOplogDiskFinishTs(ts int64) {
	manager.ctxRecLock.Lock()
	defer manager.ctxRecLock.Unlock()
	if manager.ctxRec == nil {
		manager.ctxRec = new(CheckpointContext)
	}
	manager.ctxRec.OplogDiskQueueFinishTs = ts
	manager.finishSet = true
}

func (manager *CheckpointManager) SetOplogDiskQueueName(name string) {
	manager.ctxRecLock.Lock()
	defer manager.ctxRecLock.Unlock()
	if manager.ctxRec == nil {
		manager.ctxRec = new(CheckpointContext)
	}
	manager.ctxRec.OplogDiskQueue = name
	manager.queueSet = true
}

func (manager *CheckpointManager) SetFetchMethod(method string) {
	manager.ctxRecLock.Lock()
	defer manager.ctxRecLock.Unlock()
	if manager.ctxRec == nil {
		manager.ctxRec = new(CheckpointContext)
	}
	manager.ctxRec.FetchMethod = method
	manager.fetchSet = true
}

// SetFullFinishTs stages the full-sync finish timestamp (#1047); it is written
// on the next Update call, like the other staged fields.
func (manager *CheckpointManager) SetFullFinishTs(ts int64) {
	manager.ctxRecLock.Lock()
	defer manager.ctxRecLock.Unlock()
	if manager.ctxRec == nil {
		manager.ctxRec = new(CheckpointContext)
	}
	manager.ctxRec.FullFinishTs = ts
	manager.fullFinSet = true
}

// Delegate returns the storage delegate (fault-injection tests).
func (manager *CheckpointManager) Delegate() CheckpointOperation {
	return manager.delegate
}

// SetDelegate replaces the storage delegate (fault-injection tests only).
func (manager *CheckpointManager) SetDelegate(op CheckpointOperation) {
	manager.delegate = op
}

// PrecheckAnchorGuard verifies the covering unique index at HA startup so a
// misconfigured store fails fast (exit-and-restart) instead of blocking the
// first read. The lazy non-HA path uses ensureAnchorGuard directly and self-heals.
// It takes ctxRecLock so the guard's delegate access shares the same client
// lifecycle as Get/Update/FlushStaged (lock order: ctxRecLock -> guardLock).
func (manager *CheckpointManager) PrecheckAnchorGuard() error {
	store, ok := manager.delegate.(ConditionalAnchorStore)
	if !ok {
		return fmt.Errorf("checkpoint delegate lacks conditional anchor capability")
	}
	manager.ctxRecLock.Lock()
	defer manager.ctxRecLock.Unlock()
	return manager.ensureAnchorGuard(store)
}

// ensureAnchorGuard is retryable: only a proven guard is cached as ready; a
// failure leaves it un-ready so the next call re-verifies (operator fix self-heals).
// HA startup preflight may still exit-and-restart; the lazy non-HA path honors
// self-heal. Online index/collection drop requires a restart re-check.
// Callers MUST hold ctxRecLock: the guard dereferences the delegate client, which
// Get's error path closes/nils; without the manager lock a concurrent Get could
// nil-deref the guard (#1052 review P1). Lock order is ctxRecLock -> guardLock.
func (manager *CheckpointManager) ensureAnchorGuard(store ConditionalAnchorStore) error {
	manager.guardLock.Lock()
	defer manager.guardLock.Unlock()
	if manager.guardReady {
		return nil
	}
	if err := store.EnsureAnchorGuard(); err != nil {
		return err
	}
	manager.guardReady = true
	return nil
}

// AdjustAnchor conditionally moves the durable anchor from expected to clamped
// for the pending-initial protocol (#1052 HA P1). It runs under ctxRecLock and
// talks to the delegate directly (never the public Get/Update, which re-lock).
// The guard check is inside the same lock so the whole EnsureAnchorGuard -> Get
// -> CAS/Create -> mirror-commit lifecycle shares one client lifecycle.
// The confirmed in-memory ctx is the post-storage mirror, not the stale base, so
// a later normal full-field Update cannot roll back another writer's metadata.
// Conflict / WriteUncertain / ReadFailed never commit nor clear staged flags.
func (manager *CheckpointManager) AdjustAnchor(expected, clamped int64) (AnchorAdjustResult, error) {
	store, ok := manager.delegate.(ConditionalAnchorStore)
	if !ok {
		return AnchorGuardFailed, fmt.Errorf("checkpoint delegate lacks conditional anchor capability")
	}
	manager.ctxRecLock.Lock()
	defer manager.ctxRecLock.Unlock()
	if err := manager.ensureAnchorGuard(store); err != nil {
		return AnchorGuardFailed, err
	}

	base, exist := manager.delegate.Get()
	if base == nil {
		return AnchorReadFailed, fmt.Errorf("checkpoint read failed before anchor write (no write issued)")
	}
	staged := manager.stagedPatchLocked()
	patch := bson.M{"ckpt": clamped, "version": utils.FcvCheckpoint.CurrentVersion}
	for k, v := range staged {
		patch[k] = v
	}

	if exist {
		mirror, adjusted, err := store.AdjustFrom(expected, patch)
		if err != nil {
			return AnchorWriteUncertain, err
		}
		if adjusted {
			if mirror == nil {
				return AnchorReadFailed, fmt.Errorf("anchor adjusted but the confirmation mirror readback failed (no confirmed anchor)")
			}
			manager.commitAnchorMirrorLocked(mirror)
			return AnchorAdjusted, nil
		}
		// not adjusted: re-read to tell a moved anchor from an absent record
		base, exist = manager.delegate.Get()
		if base == nil {
			return AnchorReadFailed, fmt.Errorf("checkpoint re-read failed after anchor conflict (no write issued)")
		}
		if exist {
			return AnchorConflict, nil
		}
		// absent -> fall through to conditional creation
	}

	doc := anchorCandidateDoc(base, clamped, staged)
	mirror, created, err := store.CreateIfAbsent(doc)
	if err != nil {
		return AnchorWriteUncertain, err
	}
	if mirror == nil {
		return AnchorReadFailed, fmt.Errorf("anchor readback returned no record after creation (no confirmed anchor)")
	}
	if !created {
		return AnchorConflict, nil
	}
	// UpsertedCount>0 only proves THIS insert; the readback may already reflect a
	// successor advancing the anchor. Verify it still matches the request before
	// confirming, else converge to the readback value (#1052 v4 review addition 1).
	if mirror.Timestamp != clamped {
		return AnchorConflict, nil
	}
	manager.commitAnchorMirrorLocked(mirror)
	return AnchorAdjusted, nil
}

func (manager *CheckpointManager) stagedPatchLocked() bson.M {
	p := bson.M{}
	if manager.ctxRec != nil {
		if manager.finishSet {
			p["oplog_disk_queue_apply_finish_ts"] = manager.ctxRec.OplogDiskQueueFinishTs
		}
		if manager.queueSet {
			p["oplog_disk_queue"] = manager.ctxRec.OplogDiskQueue
		}
		if manager.fetchSet {
			p["fetch_method"] = manager.ctxRec.FetchMethod
		}
		if manager.fullFinSet {
			p["full_finish_ts"] = manager.ctxRec.FullFinishTs
		}
	}
	return p
}

// commitAnchorMirrorLocked publishes the confirmed post-storage mirror and clears
// the staged flags it already carried. It does NOT overlay old staged onto the
// mirror: the readback is the confirmation source (#1052 v4 review addition 1).
func (manager *CheckpointManager) commitAnchorMirrorLocked(mirror *CheckpointContext) {
	manager.ctx = mirror
	manager.ctxRec = nil
	manager.finishSet = false
	manager.queueSet = false
	manager.fetchSet = false
	manager.fullFinSet = false
}

func anchorCandidateDoc(base *CheckpointContext, clamped int64, staged bson.M) bson.M {
	doc := bson.M{
		CheckpointName:                     base.Name,
		"ckpt":                             clamped,
		"version":                          utils.FcvCheckpoint.CurrentVersion,
		"fetch_method":                     base.FetchMethod,
		"oplog_disk_queue":                 base.OplogDiskQueue,
		"oplog_disk_queue_apply_finish_ts": base.OplogDiskQueueFinishTs,
		"full_finish_ts":                   base.FullFinishTs,
	}
	for k, v := range staged {
		doc[k] = v
	}
	return doc
}
