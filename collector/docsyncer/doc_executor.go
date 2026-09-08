package docsyncer

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	l "github.com/alibaba/MongoShake/v2/pkg/log"
)

var (
	GlobalCollExecutorId int32 = -1
	GlobalDocExecutorId  int32 = -1
)

type CollectionExecutor struct {
	// multi executor
	executors []*DocExecutor
	// worker id
	id int
	// mongo url
	mongoUrl    string
	sslRootFile string

	ns utils.NS

	wg sync.WaitGroup
	// batchCount int64

	conn *utils.MongoCommunityConn

	docBatch chan []*bson.Raw

	// not own
	syncer *DBSyncer
}

func GenerateCollExecutorId() int {
	return int(atomic.AddInt32(&GlobalCollExecutorId, 1))
}

func NewCollectionExecutor(id int, mongoUrl string, ns utils.NS, syncer *DBSyncer, sslRootFile string) *CollectionExecutor {
	return &CollectionExecutor{
		id:          id,
		mongoUrl:    mongoUrl,
		sslRootFile: sslRootFile,
		ns:          ns,
		syncer:      syncer,
		// batchCount: 0,
	}
}

func (colExecutor *CollectionExecutor) Start() error {
	var err error
	if !conf.Options.FullSyncExecutorDebug {
		writeConcern := utils.ReadWriteConcernDefault
		if conf.Options.FullSyncExecutorMajorityEnable {
			writeConcern = utils.ReadWriteConcernMajority
		}
		if colExecutor.conn, err = utils.NewMongoCommunityConn(colExecutor.mongoUrl,
			utils.VarMongoConnectModePrimary, true,
			utils.ReadWriteConcernDefault, writeConcern,
			colExecutor.sslRootFile); err != nil {
			return err
		}
	}

	parallel := conf.Options.FullSyncReaderWriteDocumentParallel
	colExecutor.docBatch = make(chan []*bson.Raw, parallel)

	executors := make([]*DocExecutor, parallel)
	for i := 0; i != len(executors); i++ {
		// Client is a handle representing a pool of connections, can be use by multi routines
		// You Can get one idle connection, if all is idle, then always get the same one
		// connections pool default parameter(min_conn:0 max_conn:100 create_conn_once:2)
		executors[i] = NewDocExecutor(GenerateDocExecutorId(), colExecutor, colExecutor.conn, colExecutor.syncer)
		go executors[i].start()
	}
	colExecutor.executors = executors
	return nil
}

func (colExecutor *CollectionExecutor) Sync(docs []*bson.Raw) {
	count := uint64(len(docs))
	if count == 0 {
		return
	}

	/*
	 * TODO, waitGroup.Add may overflow, so use atomic to replace waitGroup
	 * // colExecutor.wg.Add(1)
	 */
	colExecutor.wg.Add(1)
	// atomic.AddInt64(&colExecutor.batchCount, 1)
	colExecutor.docBatch <- docs
}

func (colExecutor *CollectionExecutor) Wait() error {
	colExecutor.wg.Wait()
	/*for v := atomic.LoadInt64(&colExecutor.batchCount); v != 0; {
		utils.YieldInMs(1000)
		l.Logger.Infof("CollectionExecutor[%v %v] wait batchCount[%v] == 0", colExecutor.ns, colExecutor.id, v)
	}*/

	close(colExecutor.docBatch)
	if !conf.Options.FullSyncExecutorDebug {
		colExecutor.conn.Close()
	}

	for _, exec := range colExecutor.executors {
		if exec.error != nil {
			return errors.New(fmt.Sprintf("sync ns %v failed. %v", colExecutor.ns, exec.error))
		}
	}
	return nil
}

type DocExecutor struct {
	// sequence index id in each replayer
	id int
	// colExecutor, not owned
	colExecutor *CollectionExecutor

	conn *utils.MongoCommunityConn

	error error

	// not own
	syncer *DBSyncer
}

func GenerateDocExecutorId() int {
	return int(atomic.AddInt32(&GlobalDocExecutorId, 1))
}

func NewDocExecutor(id int, colExecutor *CollectionExecutor, conn *utils.MongoCommunityConn,
	syncer *DBSyncer) *DocExecutor {
	return &DocExecutor{
		id:          id,
		colExecutor: colExecutor,
		conn:        conn,
		syncer:      syncer,
	}
}

func (exec *DocExecutor) String() string {
	return fmt.Sprintf("DocExecutor[%v] collectionExecutor[%v]", exec.id, exec.colExecutor.ns)
}

func (exec *DocExecutor) start() {
	// NOTE: do NOT close exec.conn here — all DocExecutors within a
	// CollectionExecutor share the same connection.  Closing it from one
	// goroutine would tear down the connection for all others, causing
	// "use of closed network connection" panics.
	// CollectionExecutor.Wait() is responsible for closing the shared
	// connection after all workers have finished.

	for {
		docs, ok := <-exec.colExecutor.docBatch
		if !ok {
			break
		}

		if exec.error == nil {
			if err := exec.doSync(docs); err != nil {
				exec.error = err
				// since v2.4.11: panic directly if meets error
				l.Logger.Panicf("%s sync failed: %v", exec, err)
			}
		}

		exec.colExecutor.wg.Done()
		// atomic.AddInt64(&exec.colExecutor.batchCount, -1)
	}
}

// use by full sync
func (exec *DocExecutor) doSync(docs []*bson.Raw) error {
	if len(docs) == 0 || conf.Options.FullSyncExecutorDebug {
		return nil
	}

	ns := exec.colExecutor.ns

	var models []mongo.WriteModel
	// modelDocs[i] corresponds to models[i] — 1:1 mapping, needed because
	// orphan filter may skip documents from docs[], breaking the index
	// alignment between models and docs.
	var modelDocs []*bson.Raw
	for _, doc := range docs {

		if conf.Options.FullSyncExecutorFilterOrphanDocument && exec.syncer.orphanFilter != nil {
			var docData bson.D
			if err := bson.Unmarshal(*doc, &docData); err != nil {
				l.Logger.Errorf("doSync skip orphan check, bson unmarshal failed: %v", err)
				// intentional fall-through to BulkWrite
			} else if exec.syncer.orphanFilter.Filter(docData, ns.Database+"."+ns.Collection) {
				l.Logger.Infof("orphan document [%v] filter", doc)
				continue
			}
		}

		models = append(models, mongo.NewInsertOneModel().SetDocument(doc))
		modelDocs = append(modelDocs, doc)
	}

	// qps limit if enable
	if exec.syncer.qos.Limit > 0 {
		exec.syncer.qos.FetchBucket()
	}

	if conf.Options.LogLevel == utils.VarLogLevelDebug {
		var docBeg, docEnd bson.M
		errUnmarshalBeg := bson.Unmarshal(*docs[0], &docBeg)
		errUnmarshalEnd := bson.Unmarshal(*docs[len(docs)-1], &docEnd)
		if errUnmarshalBeg == nil && errUnmarshalEnd == nil {
			l.Logger.Debugf("DBSyncer id[%v] doSync BulkWrite with table[%v] batch _id interval [%v, %v]",
				exec.syncer.id, ns, docBeg, docEnd)
		} else {
			l.Logger.Errorf("unmarshal doc failed, begin:[%v], end:[%v]", errUnmarshalBeg, errUnmarshalEnd)
		}
	}

	opts := options.BulkWrite().SetOrdered(false)
	res, err := exec.conn.Client.Database(ns.Database).Collection(ns.Collection).BulkWrite(nil, models, opts)

	if err != nil {
		bulkErr, ok := err.(mongo.BulkWriteException)
		if !ok {
			l.Logger.Warnf("insert docs with length[%v] into ns[%v] of dest mongo failed[type:%T err:%v] res[%v]",
				len(models), ns, err, err, res)
		} else {
			l.Logger.Warnf("insert docs with length[%v] into ns[%v] of dest mongo failed[%v] res[%v]",
				len(models), ns, bulkErr, res)
		}

		var updateModels []mongo.WriteModel
		// updateDocs[i] and updateFilters[i] correspond to updateModels[i].
		var updateDocs []*bson.Raw
		var updateFilters []bson.D
		for _, wError := range bulkErr.WriteErrors {
			if utils.DuplicateKey(wError) {
				if !conf.Options.FullSyncExecutorInsertOnDupUpdate {
					return fmt.Errorf("duplicate key error[%v], you can clean the document on the target mongodb, "+
						"or enable %v to solve, but full-sync stage needs restart",
						wError, "full_sync.executor.insert_on_dup_update")
				}

				dupDocument := *modelDocs[wError.Index]
				var updateFilter bson.D
				updateFilterBool := false
				var docData bson.D
				if err := bson.Unmarshal(dupDocument, &docData); err == nil {
					for _, bsonE := range docData {
						if bsonE.Key == "_id" {
							updateFilter = bson.D{bsonE}
							updateFilterBool = true
						}
					}
				}
				if updateFilterBool == false {
					return fmt.Errorf("duplicate key error[%v], can't get _id from document", wError)
				}
				updateModels = append(updateModels, mongo.NewUpdateOneModel().
					SetFilter(updateFilter).SetUpdate(bson.D{{"$set", dupDocument}}))
				updateDocs = append(updateDocs, modelDocs[wError.Index])
				updateFilters = append(updateFilters, updateFilter)
			} else {
				return fmt.Errorf("bulk run failed[%v]", wError)
			}
		}

		if len(updateModels) != 0 {
			updOpts := options.BulkWrite().SetOrdered(false)
			_, upErr := exec.conn.Client.Database(ns.Database).Collection(ns.Collection).BulkWrite(nil, updateModels, updOpts)
			if upErr != nil {
				// Check if the update failed due to immutable shard key field.
				// On sharded collections, $set on an immutable shard key field is
				// rejected. Fall back to delete + insert for only the truly
				// failed documents (from the update's own error, not the insert's).
				// This fallback is only enabled when
				// full_sync.executor.immutable_shard_key_fallback = true because
				// delete+insert is not atomic — if the target has concurrent writes
				// on the same _id, data loss can occur between the delete and insert.
				if conf.Options.FullSyncExecutorImmutableShardKeyFallback && utils.IsImmutableShardKeyError(upErr) {
					l.Logger.Warnf("updateForInsert hit immutable shard key error on ns[%v], falling back to delete+insert: %v", ns, upErr)

					// Only delete+re-insert the documents whose updates actually
					// failed (from the update's BulkWriteException), not all
					// dup-key documents from the insert.
					var deleteModels []mongo.WriteModel
					var reInsertModels []mongo.WriteModel
					if updBulkErr, ok := upErr.(mongo.BulkWriteException); ok {
						for _, wError := range updBulkErr.WriteErrors {
							if wError.Index < 0 || wError.Index >= len(updateDocs) {
								continue
							}
							dupDocument := *updateDocs[wError.Index]
							deleteModels = append(deleteModels, mongo.NewDeleteOneModel().SetFilter(updateFilters[wError.Index]))
							reInsertModels = append(reInsertModels, mongo.NewInsertOneModel().SetDocument(dupDocument))
						}
					} else {
						// Update error is not a BulkWriteException (e.g. mongos wrapped).
						// Fall back to delete+insert for all updateModels (conservative).
						for i := range updateModels {
							deleteModels = append(deleteModels, mongo.NewDeleteOneModel().SetFilter(updateFilters[i]))
							reInsertModels = append(reInsertModels, mongo.NewInsertOneModel().SetDocument(*updateDocs[i]))
						}
					}

					if len(deleteModels) > 0 {
						// Phase 1: Delete old documents by _id
						delOpts := options.BulkWrite().SetOrdered(false)
						_, delErr := exec.conn.Client.Database(ns.Database).Collection(ns.Collection).BulkWrite(nil, deleteModels, delOpts)
						if delErr != nil {
							return fmt.Errorf("delete+insert fallback: delete failed on ns[%v]: %v", ns, delErr)
						}

						// Phase 2: Re-insert with new shard key values
						insOpts := options.BulkWrite().SetOrdered(false)
						_, insErr := exec.conn.Client.Database(ns.Database).Collection(ns.Collection).BulkWrite(nil, reInsertModels, insOpts)
						if insErr != nil {
							return fmt.Errorf("delete+insert fallback: re-insert failed on ns[%v]: %v", ns, insErr)
						}
						l.Logger.Infof("delete+insert fallback succeeded for %d docs on ns[%v]", len(deleteModels), ns)
						return nil
					}
				}
				return fmt.Errorf("bulk run updateForInsert failed[%v]", upErr)
			}
			l.Logger.Debugf("updateForInsert succeed, updateModels.len:%d updateModules[0]:%v",
				len(updateModels), updateModels[0])
		} else {
			return fmt.Errorf("bulk run failed[%v]", err)
		}
	}

	return nil
}
