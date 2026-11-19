package executor

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
	l "github.com/alibaba/MongoShake/v2/pkg/log"
)

// BulkWriter use general bulk interface such like Insert/Update/Delete to execute command
type BulkWriter struct {
	// mongo connection
	conn *utils.MongoCommunityConn
	// init sync finish timestamp
	fullFinishTs int64
}

func (bw *BulkWriter) doInsert(database, collection string, metadata bson.E, oplogs []*OplogRecord, dupUpdate bool) (insertErr error) {

	var models []mongo.WriteModel
	var modelOplogs []*OplogRecord
	for _, log := range oplogs {
		if log.original.partialLog.Operation == "i" &&
			strings.HasSuffix(log.original.partialLog.Namespace, utils.VarSystemViewsCollection) {
			// zhongli: use applyOps directly, since we couldn't set
			// {EnforceConstraints:false} outside the mongo server
			l.Logger.Warnf("use 'applyOps' to handle insert op to ns[%v]", log.original.partialLog.Namespace)
			result := bw.conn.Client.Database("admin").RunCommand(
				nil, bson.D{{Key: "applyOps", Value: []bson.D{applyOpsDoc(log.original.partialLog)}}})
			if result.Err() != nil {
				return result.Err()
			}
		} else {
			models = append(models, mongo.NewInsertOneModel().SetDocument(log.original.partialLog.ObjectValue()))
			modelOplogs = append(modelOplogs, log)
			l.Logger.Debugf("bulk_writer: insert org_oplog:%v insert_doc:%v",
				log.original.partialLog, log.original.partialLog.ObjectValue())
		}
	}

	if len(models) == 0 { // nothing to insert for bulkWriter
		return nil
	}

	opts := options.BulkWrite().SetOrdered(false)
	if bypassDocumentValidationFor(collection) {
		opts = opts.SetBypassDocumentValidation(true)
	}
	res, err := bw.conn.Client.Database(database).Collection(collection).BulkWrite(nil, models, opts)

	if err != nil {
		var bulkErr mongo.BulkWriteException
		ok := errors.As(err, &bulkErr)
		// Aggregate pure duplicates after the selected conversion/ignore/skip
		// path succeeds. Skip still validates every error code and index below;
		// rejected batches retain their full warning detail.
		handledByConfig := dupUpdate ||
			overlapWindowSkipEnabled(bw.fullFinishTs, parseLastTimestamp(oplogs)) ||
			conf.Options.IncrSyncExecutorDupKeyStrategy == "" ||
			conf.Options.IncrSyncExecutorDupKeyStrategy == utils.VarIncrSyncExecutorDupKeyStrategyIgnore ||
			conf.Options.IncrSyncExecutorDupKeyStrategy == utils.VarIncrSyncExecutorDupKeyStrategySkip
		if !ok {
			l.Logger.Warnf("insert docs with length[%v] into ns[%v] of dest mongo failed[type:%T err:%v] res[%v]",
				len(models), database+"."+collection, err, err, res)
		} else if handledByConfig && utils.AllWriteErrorsDupKey(bulkErr) {
			l.Logger.Debugf("insert docs with length[%v] into ns[%s.%s] of dest mongo handling dup[%v] res[%v]",
				len(models), database, collection, bulkErr, res)
			// The conversion can still fail; record handled batches only
			// after the selected duplicate-key strategy returns success.
			defer func() {
				if insertErr == nil {
					utils.GlobalDupKeyLog.ReportHandled(database+"."+collection, len(bulkErr.WriteErrors), err)
				} else {
					l.Logger.Warnf("insert docs with length[%v] into ns[%s.%s] of dest mongo failed[%v] res[%v]",
						len(models), database, collection, bulkErr, res)
				}
			}()
		} else {
			l.Logger.Warnf("insert docs with length[%v] into ns[%v] of dest mongo failed[%v] res[%v]",
				len(models), database+"."+collection, bulkErr, res)
		}

		if utils.DuplicateKey(err) {
			// DuplicateKey only reports that SOME write error is a duplicate key,
			// so a batch mixing E11000 with e.g. a document validation failure
			// would be converted or ignored as a whole. The rejected documents
			// would then be acknowledged without ever being written, and the
			// checkpoint would move past them. Only handle batches whose write
			// errors are all duplicates.
			if ok && !utils.AllWriteErrorsDupKey(bulkErr) {
				l.Logger.Warnf("insert docs with length[%v] into ns[%s.%s] of dest mongo mixed "+
					"duplicate and non-duplicate errors, refusing to handle the batch: %v",
					len(models), database, collection, bulkErr)
				return err
			}
			// Inside the full/incr overlap window an _id duplicate is expected:
			// the full scan already carried the document at a version >= this
			// insert oplog, so acknowledging it converges (#1047). The unordered
			// bulk already applied every non-conflicting document; no conversion
			// or per-document retry is needed. Secondary-index duplicates are
			// real conflicts and stay on the configured strategy path below.
			if overlapWindowSkipEnabled(bw.fullFinishTs, parseLastTimestamp(oplogs)) && allDupKeyErrorsOnIDIndex(bulkErr) {
				// audit only the actual duplicates: the unordered bulk already
				// inserted the rest, dumping them would pollute the conflict
				// collection and amplify writes (#1050 review B3)
				RecordDuplicatedOplog(bw.conn, collection, failedInsertSubset(modelOplogs, bulkErr))
				return nil
			}
			// update on duplicated key occur
			if dupUpdate {
				// audit only the failed subset here too: with the shortcut
				// guarded off (default DDL config) this is the main path, and
				// dumping successfully inserted documents would pollute the
				// conflict collection (#1050 review M3)
				RecordDuplicatedOplog(bw.conn, collection, failedInsertSubset(modelOplogs, bulkErr))
				l.Logger.Debugf("Duplicated document found. reinsert or update to [%s] [%s]", database, collection)
				// Only the failed documents need conversion; the unordered bulk
				// already inserted the rest, and rewriting them would double-write
				// every document in the batch (#1047).
				return bw.doUpdateOnInsert(database, collection, metadata, failedInsertSubset(modelOplogs, bulkErr),
					conf.Options.IncrSyncExecutorUpsert)
			}
			switch conf.Options.IncrSyncExecutorDupKeyStrategy {
			case "", utils.VarIncrSyncExecutorDupKeyStrategyIgnore:
				// The bulk error is already retained at Debug and successful
				// pure duplicates are summarized by the defer above. Keep the
				// shared single/command-writer helper's per-op logging separate.
				RecordDuplicatedOplog(bw.conn, collection, oplogs)
				return nil
			case utils.VarIncrSyncExecutorDupKeyStrategySkip:
				return bw.handleBulkInsertDupKeySkip(database, collection, modelOplogs, err)
			default:
				return err
			}
		}
		return err
	}
	return nil
}

// overlapWindowSkipEnabled reports whether the benign-dup shortcut may apply
// for a batch whose last oplog timestamp is lastTs (#1047). Filtering out
// delete ops breaks the convergence premise: with filter.op_types=d the
// sequence insert(v1)/delete/insert(v2) of one _id collapses into a single
// insert batch, and skipping the duplicate second insert would strand the
// target at v1 while the source holds v2.
func overlapWindowSkipEnabled(fullFinishTs, lastTs int64) bool {
	if fullFinishTs <= 0 || lastTs > fullFinishTs {
		return false
	}
	// MaxInt64 means "no finite overlap bound" (serverless / mongos-only).
	// IgnoreError keeps its always-in-window u/ui semantics there, but the
	// insert shortcut is new behavior and its convergence premise (empty or
	// dedicated target) is not guaranteed for a plain mongos-only config, so
	// it stays off and inserts keep the pre-#1047 conversion path (#1050 F2).
	if fullFinishTs == math.MaxInt64 {
		return false
	}
	for _, op := range conf.Options.FilterOpTypes {
		if op == "d" {
			return false
		}
	}
	// DDL filtering breaks the same premise isomorphically: with
	// filter.ddl_enable=false (the factory default; sharded+oplog sources
	// cannot enable it — sanitize rejects the combination with an error) a
	// drop+recreate inside the window is dropped, the target keeps the OLD
	// incarnation, and the new incarnation's insert hits an _id dup that must
	// NOT be acknowledged as "scan carried a newer version" (#1050 review F4).
	// With DDL replayed the drop clears the target and the insert never dups,
	// so the shortcut is safe there.
	if !conf.Options.FilterDDLEnable {
		return false
	}
	// dup_key_strategy=error is an explicit user intent to stop on ANY
	// duplicate; the shortcut must not silently override it (#1050 review M1,
	// author-decided option 2). Such users keep the pre-#1047 behavior.
	if conf.Options.IncrSyncExecutorDupKeyStrategy == utils.VarIncrSyncExecutorDupKeyStrategyError {
		return false
	}
	return true
}

// allDupKeyErrorsOnIDIndex reports whether every write error is an E11000 on
// the _id index. Only those are the benign overlap-window duplicates (#1047);
// a secondary unique-index conflict (different _id) must keep the configured
// strategy. E11000 may name a secondary index even when _id also conflicts,
// so such batches conservatively stay on the existing path.
func allDupKeyErrorsOnIDIndex(bulkErr mongo.BulkWriteException) bool {
	if len(bulkErr.WriteErrors) == 0 {
		return false
	}
	for _, writeErr := range bulkErr.WriteErrors {
		if !writeErr.HasErrorCode(11000) {
			return false
		}
		if parseDupKeyIndexName(fmt.Errorf("%s", writeErr.Message)) != "_id_" {
			return false
		}
	}
	return true
}

// failedInsertSubset extracts the documents rejected by the unordered bulk,
// ordered by their model index so the relative application order is preserved
// even when the server reports write errors out of order (e.g. via mongos).
// Indices map onto the models actually sent (modelOplogs), not the raw oplogs:
// system.views inserts are handled via applyOps and never enter the bulk. An
// out-of-range index falls back to the whole batch, the pre-#1047 behavior.
func failedInsertSubset(modelOplogs []*OplogRecord, bulkErr mongo.BulkWriteException) []*OplogRecord {
	indexes := make([]int, 0, len(bulkErr.WriteErrors))
	for _, writeErr := range bulkErr.WriteErrors {
		if writeErr.Index < 0 || writeErr.Index >= len(modelOplogs) {
			return modelOplogs
		}
		indexes = append(indexes, writeErr.Index)
	}
	if len(indexes) == 0 {
		return modelOplogs
	}
	sort.Ints(indexes)
	subset := make([]*OplogRecord, 0, len(indexes))
	for _, idx := range indexes {
		subset = append(subset, modelOplogs[idx])
	}
	return subset
}

func (bw *BulkWriter) handleBulkInsertDupKeySkip(database, collection string, oplogs []*OplogRecord, err error) error {
	var bulkErr mongo.BulkWriteException
	if !errors.As(err, &bulkErr) {
		return err
	}
	if bulkErr.WriteConcernError != nil {
		return err
	}
	for _, writeErr := range bulkErr.WriteErrors {
		if !writeErr.HasErrorCode(11000) {
			return err
		}
		if writeErr.Index < 0 || writeErr.Index >= len(oplogs) {
			return err
		}
		indexName := parseDupKeyIndexName(fmt.Errorf("%s", writeErr.Message))
		if indexName != "_id_" && !shouldSkipDupKeyIndex(database, collection, indexName) {
			return err
		}
	}
	for _, writeErr := range bulkErr.WriteErrors {
		RecordDuplicatedOplog(bw.conn, collection, []*OplogRecord{oplogs[writeErr.Index]})
	}
	l.Logger.Debugf("bulk_writer::doInsert duplicated oplogs skipped, ns[%s.%s], err[%v]",
		database, collection, err)
	return nil
}

func (bw *BulkWriter) doUpdateOnInsert(database, collection string, metadata bson.E, oplogs []*OplogRecord, upsert bool) error {
	var models []mongo.WriteModel
	var modelOplogs []*OplogRecord

	for _, log := range oplogs {
		newObject := log.original.partialLog.ObjectValue()
		if upsert && len(log.original.partialLog.DocumentKey) > 0 {

			models = append(models, mongo.NewUpdateOneModel().
				SetFilter(log.original.partialLog.DocumentKey).
				SetUpdate(bson.D{{"$set", newObject}}).SetUpsert(true))
			modelOplogs = append(modelOplogs, log)
		} else {
			// if upsert {
			//	l.Logger.Warnf("doUpdateOnInsert runs upsert but lack documentKey: %v", log.original.partialLog)
			// }
			// insert must have _id
			if id := log.original.partialLog.ObjectKey(""); id != nil {

				model := mongo.NewUpdateOneModel().
					SetFilter(bson.D{{"_id", id}}).
					SetUpdate(bson.D{{"$set", newObject}})
				if upsert {
					model.SetUpsert(true)
				}
				models = append(models, model)
				modelOplogs = append(modelOplogs, log)
			} else {
				l.Logger.Warnf("Insert on duplicated update _id look up failed. %v", log)
			}
		}

		l.Logger.Debugf("bulk_writer: updateOnInsert %v", log.original.partialLog)
	}

	if len(models) == 0 {
		return nil
	}

	opts := options.BulkWrite()
	if bypassDocumentValidationFor(collection) {
		opts = opts.SetBypassDocumentValidation(true)
	}
	res, err := bw.conn.Client.Database(database).Collection(collection).BulkWrite(nil, models, opts)

	if err != nil {
		// parse error
		index, errMsg, dup := utils.FindFirstErrorIndexAndMessageN(err)
		l.Logger.Errorf("detail error info with index[%v] msg[%v] dup[%v] res[%v]", index, errMsg, dup, res)

		if utils.DuplicateKey(err) {
			// create single writer to write one by one
			sw := NewDbWriter(bw.conn, bson.E{}, false, bw.fullFinishTs)
			if index < 0 || index >= len(modelOplogs) {
				return err
			}
			return sw.doUpdateOnInsert(database, collection, metadata, modelOplogs[index:], upsert)
		}

		// error can be ignored
		if IgnoreError(err, "u", parseLastTimestamp(oplogs) <= bw.fullFinishTs) {
			var oplogRecord *OplogRecord
			if index != -1 {
				oplogRecord = oplogs[index]
			}
			l.Logger.Warnf("ignore error[%v] when run operation[%v], initialSync[%v], oplog[%v]",
				err, "u", parseLastTimestamp(oplogs) <= bw.fullFinishTs, oplogRecord)
			return nil
		}

		// immutable shard key fallback: if update fails because the
		// existing document has a different shard key value, delete
		// the old document and re-insert with the new value.
		// This fallback is only enabled when
		// incr_sync.executor.immutable_shard_key_fallback = true because
		// delete+insert is not atomic — if the target has concurrent writes
		// on the same _id, data loss can occur between the delete and insert.
		if conf.Options.IncrSyncExecutorImmutableShardKeyFallback && utils.IsImmutableShardKeyError(err) {
			l.Logger.Warnf("doUpdateOnInsert hit immutable shard key error on ns[%v.%v], falling back to delete+insert: %v", database, collection, err)

			// Maps _id values already seen in this fallback to avoid
			// re-insert collision when multiple oplogs share the same _id.
			seenIDs := make(map[interface{}]bool)
			var deleteModels []mongo.WriteModel
			var reInsertModels []mongo.WriteModel
			if bulkErr, ok := err.(mongo.BulkWriteException); ok {
				for _, wError := range bulkErr.WriteErrors {
					if wError.Index < 0 || wError.Index >= len(modelOplogs) {
						continue
					}
					doc := modelOplogs[wError.Index].original.partialLog.ObjectValue()
					id := modelOplogs[wError.Index].original.partialLog.ObjectKey("")
					if id != nil && !seenIDs[id] {
						seenIDs[id] = true
						deleteModels = append(deleteModels, mongo.NewDeleteOneModel().SetFilter(bson.D{{"_id", id}}))
						reInsertModels = append(reInsertModels, mongo.NewInsertOneModel().SetDocument(doc))
					}
				}
			} else {
				// bulk error type not available (e.g. wrapped by mongos),
				// fall back to single-document delete+insert for safety.
				// Dedup by _id to avoid re-insert collision.
				for _, log := range modelOplogs {
					doc := log.original.partialLog.ObjectValue()
					id := log.original.partialLog.ObjectKey("")
					if id != nil && !seenIDs[id] {
						seenIDs[id] = true
						deleteModels = append(deleteModels, mongo.NewDeleteOneModel().SetFilter(bson.D{{"_id", id}}))
						reInsertModels = append(reInsertModels, mongo.NewInsertOneModel().SetDocument(doc))
					}
				}
			}
			if len(deleteModels) > 0 {
				delOpts := options.BulkWrite().SetOrdered(false)
				_, delErr := bw.conn.Client.Database(database).Collection(collection).BulkWrite(nil, deleteModels, delOpts)
				if delErr != nil {
					return fmt.Errorf("delete+insert fallback: delete failed on ns[%v.%v]: %w", database, collection, delErr)
				}
				insOpts := options.BulkWrite().SetOrdered(false)
				_, insErr := bw.conn.Client.Database(database).Collection(collection).BulkWrite(nil, reInsertModels, insOpts)
				if insErr != nil {
					return fmt.Errorf("delete+insert fallback: re-insert failed on ns[%v.%v]: %w", database, collection, insErr)
				}
				l.Logger.Infof("delete+insert fallback succeeded for %d docs on ns[%v.%v]", len(deleteModels), database, collection)
			}

			// The bulk is ordered (no SetOrdered(false)), so the server
			// stopped at the first error. ops after index were never
			// executed — re-run them via single writer.
			if index >= 0 && index+1 < len(modelOplogs) {
				sw := NewDbWriter(bw.conn, bson.E{}, false, bw.fullFinishTs)
				return sw.doUpdateOnInsert(database, collection, metadata, modelOplogs[index+1:], upsert)
			}
			return nil
		}

		l.Logger.Errorf("doUpdateOnInsert run upsert/update[%v] failed[%v]", upsert, err)
		return err
	}
	return nil
}

/*
1. update oplog:

	{
	    "ts": Timestamp(1582533077,
	    2),
	    "t": NumberLong(1),
	    "h": NumberLong(0),
	    "v": 2,
	    "op": "u",
	    "ns": "zz.test",
	    "ui": UUID("ee9b60d8-845f-42ff-989d-09018a730d60"),
	    "o2": {
	        "_id": ObjectId("5e5384f97dc0f30426f01b79")
	    },
	    "wall": ISODate("2020-02-24T08:31:17.681Z"),
	    "o": {
	        "$v": 1,
	        "$unset": {
	            "ok": true
	        },
	        "$set": {
	            "plus_field": 2
	        }
	    }
	}

2. replacement oplog:

	{
	    "ts": {
	        "T": 1664192510,
	        "I": 1
	    },
	    "t": 1,
	    "h": null,
	    "v": 2,
	    "op": "u",
	    "ns": "test.car",
	    "o": [
	        {
	            "Key": "_id",
	            "Value": "63318f67024749a30fc12af6"
	        },
	        {
	            "Key": "b",
	            "Value": 3
	        }
	    ],
	    "o2": [
	        {
	            "Key": "_id",
	            "Value": "63318f67024749a30fc12af6"
	        }
	    ],
	    "PrevOpTime": null,
	    "ui": {
	        "Subtype": 4,
	        "Data": "3p7boGbmTvqYSWp42PaZnw=="
	    }
	}

3. chunkSplit oplog: (NOTE:there's an 'b' field inside 'o.applyOps')

	{
	    "op": "u",
	    "b": true,
	    "ns": "config.chunks",
	    "o": {
	        "_id": {
	            "$oid": "6581397503de9a2282ff6cc9"
	        },
	        "lastmod": {
	            "$timestamp": {
	                "t": 1,
	                "i": 1
	            }
	        },
	        "lastmodEpoch": {
	            "$oid": "6581397526a5b24c88d7a6d4"
	        },
	        "ns": "ycsb.test6",
	        "min": {
	            "_id": {
	                "$minKey": 1
	            }
	        },
	        "max": {
	            "_id": {
	                "$oid": "65813993ed6de3163ad08fe8"
	            }
	        },
	        "shard": "d-bp1be4d809f7b554",
	        "history": [
	            {
	                "validAfter": {
	                    "$timestamp": {
	                        "t": 1702967669,
	                        "i": 1
	                    }
	                },
	                "shard": "d-bp1be4d809f7b554"
	            }
	        ]
	    },
	    "o2": {
	        "_id": {
	            "$oid": "6581397503de9a2282ff6cc9"
	        }
	    },
	    "ui": {
	        "$binary": {
	            "base64": "qo7OUh4jREO5XpfQ2m2XdQ==",
	            "subType": "04"
	        }
	    }
	}
*/
func (bw *BulkWriter) doUpdate(database, collection string, metadata bson.E, oplogs []*OplogRecord, upsert bool) error {

	var models []mongo.WriteModel
	for _, log := range oplogs {
		var newObject interface{}

		updateCmd := "update"
		l.Logger.Debugf("bulk_writer doUpdate: org_doc:%v", log.original.partialLog)
		if log.original.partialLog.ObjectHasPrefix("$") {
			var oplogErr error
			if newObject, oplogErr = log.original.partialLog.UpdateValue(); oplogErr != nil {
				// Column-store diffs in time-series buckets need the original applyOps.
				if strings.HasPrefix(collection, utils.VarSystemBucketsPrefix) {
					// oplogErr embeds the raw column-store binary diff, so it is left out of the log.
					l.Logger.Infof("bulk_writer fall back to applyOps for time-series bucket update on %s.%s", database, collection)
					if applyErr := replayUpdateViaApplyOps(bw.conn.Client, log.original.partialLog); applyErr != nil {
						return applyErr
					}
					continue
				}
				l.Logger.Errorf("doUpdate run failed err[%v] org_doc[%v]", oplogErr, log.original.partialLog)
				return oplogErr
			}

			if upsert && len(log.original.partialLog.DocumentKey) > 0 {
				models = append(models, mongo.NewUpdateOneModel().
					SetFilter(log.original.partialLog.DocumentKey).
					SetUpdate(newObject).SetUpsert(true))
			} else {
				// if upsert {
				//	l.Logger.Warnf("doUpdate runs upsert but lack documentKey: %v", log.original.partialLog)
				// }

				model := mongo.NewUpdateOneModel().
					SetFilter(log.original.partialLog.QueryValue()).
					SetUpdate(newObject)
				if upsert {
					model.SetUpsert(true)
				}
				models = append(models, model)
			}
		} else {
			newObject = log.original.partialLog.ObjectValue()

			if upsert && len(log.original.partialLog.DocumentKey) > 0 {
				models = append(models, mongo.NewReplaceOneModel().
					SetFilter(log.original.partialLog.DocumentKey).
					SetReplacement(log.original.partialLog.ObjectValue()).
					SetUpsert(true))
			} else {
				model := mongo.NewReplaceOneModel().
					SetFilter(log.original.partialLog.QueryValue()).
					SetReplacement(log.original.partialLog.ObjectValue())
				if upsert || log.original.partialLog.Upsert {
					model.SetUpsert(true)
				}
				models = append(models, model)
			}
			updateCmd = "replace"
		}

		l.Logger.Debugf("bulk_writer: %s %v after_modify_doc:%v", updateCmd, newObject, log.original.partialLog)
	}

	l.Logger.Debugf("bulk_writer: update models len %v", len(models))
	if len(models) == 0 {
		// Every record may already have been replayed via the applyOps fallback.
		return nil
	}

	opts := options.BulkWrite()
	if bypassDocumentValidationFor(collection) {
		opts = opts.SetBypassDocumentValidation(true)
	}
	res, err := bw.conn.Client.Database(database).Collection(collection).BulkWrite(
		context.Background(), models, opts)

	if err != nil {
		// parse error
		index, errMsg, dup := utils.FindFirstErrorIndexAndMessageN(err)
		var oplogRecord *OplogRecord
		if index != -1 {
			oplogRecord = oplogs[index]
			l.Logger.Warnf("detail error info with index[%v] msg[%v] dup[%v], isFullSyncStage[%v], oplog[%v] res[%v]",
				index, errMsg, dup, parseLastTimestamp(oplogs) <= bw.fullFinishTs,
				*oplogRecord.original.partialLog, res)
		}

		if utils.DuplicateKey(err) {
			RecordDuplicatedOplog(bw.conn, collection, oplogs)
			// create single writer to write one by one
			sw := NewDbWriter(bw.conn, bson.E{}, false, bw.fullFinishTs)
			return sw.doUpdate(database, collection, metadata, oplogs[index:], upsert)
		}

		// error can be ignored
		if IgnoreError(err, "u", parseLastTimestamp(oplogs) <= bw.fullFinishTs) {
			l.Logger.Warnf("ignore error[%v] when run operation[%v], initialSync[%v]", err, "u",
				parseLastTimestamp(oplogs) <= bw.fullFinishTs)

			// re-run (index, len(oplogs) - 1]
			sw := NewDbWriter(bw.conn, bson.E{}, false, bw.fullFinishTs)
			return sw.doUpdate(database, collection, metadata, oplogs[index+1:], upsert)
		}
		if strings.Contains(err.Error(), shardKeyUpdateErr) {
			l.Logger.Errorf("multiUpdateShardKey err_string:%s, index:%d, redo update shardKey singly",
				err.Error(), index)

			sw := NewDbWriter(bw.conn, bson.E{}, false, bw.fullFinishTs)
			return sw.doUpdate(database, collection, metadata, oplogs[index:], upsert)
		}

		l.Logger.Errorf("doUpdate run upsert/update[%v] failed[%v]", upsert, err)
		return err
	}
	return nil
}

func (bw *BulkWriter) doDelete(database, collection string, metadata bson.E, oplogs []*OplogRecord) error {
	var models []mongo.WriteModel
	for _, log := range oplogs {
		models = append(models, mongo.NewDeleteOneModel().SetFilter(log.original.partialLog.ObjectValue()))

		l.Logger.Debugf("bulk_writer: delete %v", log.original.partialLog)
	}

	opts := options.BulkWrite().SetOrdered(false)
	res, err := bw.conn.Client.Database(database).Collection(collection).BulkWrite(context.Background(), models, opts)
	if err != nil {
		// error can be ignored
		if IgnoreError(err, "d", parseLastTimestamp(oplogs) <= bw.fullFinishTs) {
			l.Logger.Warnf("ignore error[%v] when run operation[%v], initialSync[%v]",
				err, "d", parseLastTimestamp(oplogs) <= bw.fullFinishTs)
			return nil
		}

		l.Logger.Errorf("doDelete run delete[%v] failed[%v] res[%v]", models, err, res)
		return err
	}
	return nil
}

func (bw *BulkWriter) doCommand(database string, metadata bson.E, oplogs []*OplogRecord) error {
	var err error
	for _, log := range oplogs {
		newObject := log.original.partialLog.ObjectValue()
		operation, found := oplog.ExtraCommandName(newObject)
		if conf.Options.FilterDDLEnable || (found && oplog.IsSyncDataCommand(operation)) {
			// execute one by one with sequence order
			if err = RunCommand(database, operation, log.original.partialLog, bw.conn.Client); err == nil {
				l.Logger.Infof("execute command(op=c) oplog, operation[%s]", operation)
			} else if err.Error() == "ns not found" {
				l.Logger.Infof("execute command(op=c) oplog, operation[%s], ignore error[ns not found]", operation)
			} else if IgnoreError(err, "c", parseLastTimestamp(oplogs) <= bw.fullFinishTs) {
				l.Logger.Infof("ignore error[%v] db[%s] oplog[%v], inFullSync[%v]",
					err, database, log.original.partialLog, parseLastTimestamp(oplogs) <= bw.fullFinishTs)
				return nil
			} else {
				return err
			}
		} else {
			// exec.batchExecutor.ReplMetric.AddFilter(1)
		}

		l.Logger.Debugf("bulk_writer: command %v", log.original.partialLog)
	}
	return nil
}
