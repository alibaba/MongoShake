package collector

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alibaba/MongoShake/v2/collector/ckpt"
	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	"github.com/alibaba/MongoShake/v2/collector/spool"
	utils "github.com/alibaba/MongoShake/v2/common"
)

func mockCheckpointSyncer(workerNum int) *OplogSyncer {
	workers := make([]*Worker, workerNum)
	for i := 0; i < workerNum; i++ {
		workers[i] = new(Worker)
	}
	return &OplogSyncer{
		batcher: &Batcher{
			workerGroup: workers,
		},
	}
}

func TestCalculateWorkerLowestCheckpoint(t *testing.T) {
	// test calculateWorkerLowestCheckpoint

	var (
		nr         int
		checkpoint int64
		err        error
	)

	// do nothing, return 0
	{
		fmt.Printf("TestCalculateWorkerLowestCheckpoint case %d.\n", nr)
		nr++

		syncer := mockCheckpointSyncer(8)
		checkpoint, err = syncer.calculateWorkerLowestCheckpoint()
		assert.Equal(t, "no candidates ack values found", err.Error(), "should be equal")
		assert.Equal(t, int64(0), checkpoint, "should be equal")
	}

	// one of the workers return ack
	{
		fmt.Printf("TestCalculateWorkerLowestCheckpoint case %d.\n", nr)
		nr++

		syncer := mockCheckpointSyncer(8)
		worker3 := syncer.batcher.workerGroup[3]
		worker3.ack = 10
		worker3.unack = 10
		checkpoint, err = syncer.calculateWorkerLowestCheckpoint()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, int64(10), checkpoint, "should be equal")
	}

	// not all ack, return the smallest candidate
	{
		fmt.Printf("TestCalculateWorkerLowestCheckpoint case %d.\n", nr)
		nr++

		syncer := mockCheckpointSyncer(8)
		worker3 := syncer.batcher.workerGroup[3]
		worker3.ack = 10
		worker3.unack = 10
		worker4 := syncer.batcher.workerGroup[4]
		worker4.ack = 20
		worker4.unack = 30
		checkpoint, err = syncer.calculateWorkerLowestCheckpoint()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, int64(20), checkpoint, "should be equal")
	}

	// not all ack, return the smallest candidate
	{
		fmt.Printf("TestCalculateWorkerLowestCheckpoint case %d.\n", nr)
		nr++

		syncer := mockCheckpointSyncer(8)
		worker3 := syncer.batcher.workerGroup[3]
		worker3.ack = 0
		worker3.unack = 10
		checkpoint, err = syncer.calculateWorkerLowestCheckpoint()
		assert.Equal(t, "smallest candidates is zero", err.Error(), "should be equal")
		assert.Equal(t, int64(0), checkpoint, "should be equal")
	}

	// not all ack, return the smallest candidate
	{
		fmt.Printf("TestCalculateWorkerLowestCheckpoint case %d.\n", nr)
		nr++

		syncer := mockCheckpointSyncer(8)
		worker3 := syncer.batcher.workerGroup[3]
		worker3.ack = 5
		worker3.unack = 10
		checkpoint, err = syncer.calculateWorkerLowestCheckpoint()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, int64(5), checkpoint, "should be equal")
	}

	// not all ack again, unack candidate is smaller than ack, return the smallest candidate
	{
		fmt.Printf("TestCalculateWorkerLowestCheckpoint case %d.\n", nr)
		nr++

		syncer := mockCheckpointSyncer(8)
		worker3 := syncer.batcher.workerGroup[3]
		worker3.ack = 10
		worker3.unack = 10
		worker4 := syncer.batcher.workerGroup[4]
		worker4.ack = 20
		worker4.unack = 30
		worker5 := syncer.batcher.workerGroup[5]
		worker5.ack = 40
		worker5.unack = 40
		checkpoint, err = syncer.calculateWorkerLowestCheckpoint()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, int64(20), checkpoint, "should be equal")
	}

	// all ack
	{
		fmt.Printf("TestCalculateWorkerLowestCheckpoint case %d.\n", nr)
		nr++

		syncer := mockCheckpointSyncer(4)
		worker3 := syncer.batcher.workerGroup[3]
		worker3.ack = 10
		worker3.unack = 10
		worker2 := syncer.batcher.workerGroup[2]
		worker2.ack = 20
		worker2.unack = 20
		worker1 := syncer.batcher.workerGroup[1]
		worker1.ack = 40
		worker1.unack = 40
		checkpoint, err = syncer.calculateWorkerLowestCheckpoint()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, int64(40), checkpoint, "should be equal")
	}

	// ack less than unack, unack != 0
	{
		fmt.Printf("TestCalculateWorkerLowestCheckpoint case %d.\n", nr)
		nr++

		syncer := mockCheckpointSyncer(4)
		worker3 := syncer.batcher.workerGroup[3]
		worker3.ack = 10
		worker3.unack = 10
		worker2 := syncer.batcher.workerGroup[2]
		worker2.ack = 20
		worker2.unack = 20
		worker1 := syncer.batcher.workerGroup[1]
		worker1.ack = 40
		worker1.unack = 30
		checkpoint, err = syncer.calculateWorkerLowestCheckpoint()
		assert.Equal(t, true, err != nil, "should be equal")
		assert.Equal(t, int64(0), checkpoint, "should be equal")
	}

	// ack less than unack, unack == 0, has candidate
	{
		fmt.Printf("TestCalculateWorkerLowestCheckpoint case %d.\n", nr)
		nr++

		syncer := mockCheckpointSyncer(4)
		worker3 := syncer.batcher.workerGroup[3]
		worker3.ack = 10
		worker3.unack = 10
		worker2 := syncer.batcher.workerGroup[2]
		worker2.ack = 20
		worker2.unack = 30
		worker1 := syncer.batcher.workerGroup[1]
		worker1.ack = 40
		worker1.unack = 0
		checkpoint, err = syncer.calculateWorkerLowestCheckpoint()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, int64(20), checkpoint, "should be equal")
	}

	// ack less than unack, unack == 0, no candidate
	{
		fmt.Printf("TestCalculateWorkerLowestCheckpoint case %d.\n", nr)
		nr++

		syncer := mockCheckpointSyncer(4)
		worker3 := syncer.batcher.workerGroup[3]
		worker3.ack = 10
		worker3.unack = 10
		worker2 := syncer.batcher.workerGroup[2]
		worker2.ack = 20
		worker2.unack = 20
		worker1 := syncer.batcher.workerGroup[1]
		worker1.ack = 40
		worker1.unack = 0
		checkpoint, err = syncer.calculateWorkerLowestCheckpoint()
		assert.Equal(t, "no candidates ack values found", err.Error(), "should be equal")
		assert.Equal(t, int64(0), checkpoint, "should be equal")
	}
}

func TestCheckpointSetsOplogDiskFinishTsAfterWorkerCatchesUp(t *testing.T) {
	oldCheckpointStorage := conf.Options.CheckpointStorage
	oldCheckpointURL := conf.Options.CheckpointStorageCollection
	oldOplogStoreDisk := conf.Options.FullSyncReaderOplogStoreDisk
	oldCheckpointInterval := conf.Options.CheckpointInterval
	defer func() {
		conf.Options.CheckpointStorage = oldCheckpointStorage
		conf.Options.CheckpointStorageCollection = oldCheckpointURL
		conf.Options.FullSyncReaderOplogStoreDisk = oldOplogStoreDisk
		conf.Options.CheckpointInterval = oldCheckpointInterval
	}()

	var posted []ckpt.CheckpointContext
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(&ckpt.CheckpointContext{
				Name:                   "rs-checkpoint-spool",
				Timestamp:              100,
				Version:                utils.FcvCheckpoint.CurrentVersion,
				OplogDiskQueue:         "oplog-spool-rs-checkpoint",
				OplogDiskQueueFinishTs: ckpt.InitCheckpoint,
			})
		case http.MethodPost:
			var value ckpt.CheckpointContext
			_ = json.NewDecoder(r.Body).Decode(&value)
			posted = append(posted, value)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	conf.Options.CheckpointStorage = utils.VarCheckpointStorageApi
	conf.Options.CheckpointStorageCollection = server.URL
	conf.Options.FullSyncReaderOplogStoreDisk = true
	conf.Options.CheckpointInterval = 0

	manager := ckpt.NewCheckpointManager("rs-checkpoint-spool", 100)
	_, _, err := manager.Get()
	assert.Equal(t, nil, err, "should be equal")

	metric := utils.NewMetric("rs-checkpoint-spool", utils.TypeIncr, 0)
	defer metric.Close()
	fakeSpool := &fakeOplogSpool{name: "oplog-spool-rs-checkpoint"}
	syncer := &OplogSyncer{
		ckptManager: manager,
		persister: &Persister{
			diskQueueLastTs: 200,
			DiskQueue:       fakeSpool,
		},
		replMetric: metric,
		ckptTime:   time.Now().Add(-time.Hour),
		startTime:  time.Now().Add(-time.Hour),
	}

	updated := syncer.checkpoint(true, 200)
	assert.Equal(t, true, updated, "should be equal")
	require.Len(t, posted, 2)
	assert.Equal(t, int64(200), posted[0].Timestamp, "should be equal")
	assert.Equal(t, int64(200), posted[0].OplogDiskQueueFinishTs, "should be equal")
	assert.Equal(t, "oplog-spool-rs-checkpoint", posted[0].OplogDiskQueue, "should be equal")
	assert.Equal(t, int64(200), posted[1].OplogDiskQueueFinishTs, "should be equal")
	assert.Equal(t, "", posted[1].OplogDiskQueue, "should be equal")
	assert.True(t, fakeSpool.deleted, "should be equal")
	assert.Nil(t, syncer.persister.DiskQueue, "should be equal")
	assert.Equal(t, int64(-2), syncer.persister.diskQueueLastTs, "should be equal")
}

func TestCheckpointRetriesClearingQueueNameAfterPhaseBFailure(t *testing.T) {
	origin := conf.Options
	defer func() {
		conf.Options = origin
	}()

	var posted []ckpt.CheckpointContext
	postCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(&ckpt.CheckpointContext{
				Name:                   "rs-phase-b-retry",
				Timestamp:              100,
				Version:                utils.FcvCheckpoint.CurrentVersion,
				OplogDiskQueue:         "oplog-spool-rs-phase-b-retry",
				OplogDiskQueueFinishTs: ckpt.InitCheckpoint,
			})
		case http.MethodPost:
			var value ckpt.CheckpointContext
			_ = json.NewDecoder(r.Body).Decode(&value)
			posted = append(posted, value)
			postCount++
			if postCount == 2 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	conf.Options.CheckpointStorage = utils.VarCheckpointStorageApi
	conf.Options.CheckpointStorageCollection = server.URL
	conf.Options.FullSyncReaderOplogStoreDisk = true
	conf.Options.CheckpointInterval = 0

	manager := ckpt.NewCheckpointManager("rs-phase-b-retry", 100)
	_, _, err := manager.Get()
	require.NoError(t, err)

	fakeSpool := &fakeOplogSpool{name: "oplog-spool-rs-phase-b-retry"}
	syncer := &OplogSyncer{
		ckptManager: manager,
		persister: &Persister{
			diskQueueLastTs: 200,
			DiskQueue:       fakeSpool,
		},
		batcher:   &Batcher{workerGroup: []*Worker{}},
		ckptTime:  time.Now().Add(-time.Hour),
		startTime: time.Now().Add(-time.Hour),
	}

	assert.False(t, syncer.checkpoint(true, 200), "should be equal")
	require.Len(t, posted, 2)
	assert.Equal(t, "oplog-spool-rs-phase-b-retry", manager.GetInMemory().OplogDiskQueue, "should be equal")
	assert.Equal(t, int64(200), manager.GetInMemory().OplogDiskQueueFinishTs, "should be equal")
	assert.True(t, fakeSpool.deleted, "should be equal")
	assert.Nil(t, syncer.persister.DiskQueue, "should be equal")
	assert.Equal(t, int64(200), syncer.persister.diskQueueLastTs, "should be equal")

	assert.True(t, syncer.checkpoint(true, 0), "should be equal")
	require.Len(t, posted, 3)
	assert.Empty(t, posted[2].OplogDiskQueue, "should be empty")
	assert.Equal(t, int64(-2), syncer.persister.diskQueueLastTs, "should be equal")
}

func TestLoadCheckpointReopensUnfinishedInitCheckpointSpool(t *testing.T) {
	origin := conf.Options
	defer func() {
		conf.Options = origin
	}()

	logDir := t.TempDir()
	replset := "rs-restart-init-spool"
	queueName := "oplog-spool-rs-restart-init-spool"
	created, err := spool.Open(spool.OpenOptions{
		Name:            queueName,
		LogDir:          logDir,
		CreateIfMissing: true,
		MetricName:      replset,
		MetricStage:     utils.TypeIncr,
		MaxBytesMB:      256000,
	})
	require.NoError(t, err)
	require.NoError(t, created.Put(mockOplogsBinaryWithTs(t, 201)))
	require.NoError(t, created.Close())

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(&ckpt.CheckpointContext{
				Name:                   replset,
				Timestamp:              200,
				Version:                utils.FcvCheckpoint.CurrentVersion,
				OplogDiskQueue:         queueName,
				OplogDiskQueueFinishTs: ckpt.InitCheckpoint,
				FetchMethod:            utils.VarIncrSyncMongoFetchMethodOplog,
			})
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	conf.Options.CheckpointStorage = utils.VarCheckpointStorageApi
	conf.Options.CheckpointStorageCollection = server.URL
	conf.Options.FullSyncReaderOplogStoreDisk = true
	conf.Options.LogDirectory = logDir
	conf.Options.SyncMode = utils.VarSyncModeAll
	conf.Options.IncrSyncFetcherBufferCapacity = 1
	conf.Options.IncrSyncMongoFetchMethod = utils.VarIncrSyncMongoFetchMethodOplog
	conf.Options.FullSyncReaderOplogStoreDiskMaxSize = 256000

	manager := ckpt.NewCheckpointManager(replset, 200)
	reader := &checkpointTestReader{}
	syncer := &OplogSyncer{
		Replset:     replset,
		ckptManager: manager,
		reader:      reader,
	}
	syncer.persister = &Persister{
		replset:         replset,
		sync:            syncer,
		Buffer:          make([][]byte, 0, conf.Options.IncrSyncFetcherBufferCapacity),
		diskQueueLastTs: -1,
	}

	err = syncer.loadCheckpoint()
	require.NoError(t, err)
	assert.Equal(t, utils.FetchStageStoreDiskNoApply, syncer.persister.GetFetchStage(), "should be equal")
	require.NotNil(t, syncer.persister.DiskQueue, "should be equal")
	stats := syncer.persister.DiskQueue.Stats()
	assert.Equal(t, uint64(1), stats.Depth, "should be equal")
	assert.Equal(t, utils.TimeStampToInt64(utils.TimeToTimestamp(201)), reader.updatedTs, "should be equal")
	require.NoError(t, syncer.persister.DiskQueue.Delete())
}

func TestLoadCheckpointPersistsNewSpoolMetadataBeforeFetch(t *testing.T) {
	origin := conf.Options
	defer func() {
		conf.Options = origin
	}()

	var posted []ckpt.CheckpointContext
	replset := "rs-new-spool-metadata"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(&ckpt.CheckpointContext{})
		case http.MethodPost:
			var value ckpt.CheckpointContext
			_ = json.NewDecoder(r.Body).Decode(&value)
			posted = append(posted, value)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	conf.Options.CheckpointStorage = utils.VarCheckpointStorageApi
	conf.Options.CheckpointStorageCollection = server.URL
	conf.Options.FullSyncReaderOplogStoreDisk = true
	conf.Options.LogDirectory = t.TempDir()
	conf.Options.SyncMode = utils.VarSyncModeAll
	conf.Options.IncrSyncFetcherBufferCapacity = 10
	conf.Options.IncrSyncMongoFetchMethod = utils.VarIncrSyncMongoFetchMethodOplog
	conf.Options.FullSyncReaderOplogStoreDiskMaxSize = 256000

	manager := ckpt.NewCheckpointManager(replset, 100)
	syncer := &OplogSyncer{
		Replset:     replset,
		ckptManager: manager,
		reader:      &checkpointTestReader{},
	}
	syncer.persister = NewPersister(replset, syncer)

	err := syncer.loadCheckpoint()
	require.NoError(t, err)
	require.Len(t, posted, 1)
	assert.Equal(t, int64(100), posted[0].Timestamp, "should be equal")
	assert.Equal(t, ckpt.InitCheckpoint, posted[0].OplogDiskQueueFinishTs, "should be equal")
	assert.NotEmpty(t, posted[0].OplogDiskQueue, "should not be empty")
	assert.Equal(t, utils.VarIncrSyncMongoFetchMethodOplog, posted[0].FetchMethod, "should be equal")
	require.NotNil(t, syncer.persister.DiskQueue, "should not be nil")
	assert.Equal(t, posted[0].OplogDiskQueue, syncer.persister.DiskQueue.Stats().Name, "should be equal")
	require.NoError(t, syncer.persister.DiskQueue.Delete())
}

func TestLoadCheckpointCompletesPendingSpoolCleanup(t *testing.T) {
	origin := conf.Options
	defer func() {
		conf.Options = origin
	}()

	logDir := t.TempDir()
	replset := "rs-pending-spool-cleanup"
	queueName := "oplog-spool-rs-pending-spool-cleanup"
	created, err := spool.Open(spool.OpenOptions{
		Name:            queueName,
		LogDir:          logDir,
		CreateIfMissing: true,
		MetricName:      replset,
		MetricStage:     utils.TypeIncr,
	})
	require.NoError(t, err)
	require.NoError(t, created.Put(mockOplogsBinaryWithTs(t, 200)))
	require.NoError(t, created.Close())

	var posted []ckpt.CheckpointContext
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(&ckpt.CheckpointContext{
				Name:                   replset,
				Timestamp:              300,
				Version:                utils.FcvCheckpoint.CurrentVersion,
				OplogDiskQueue:         queueName,
				OplogDiskQueueFinishTs: 200,
				FetchMethod:            utils.VarIncrSyncMongoFetchMethodOplog,
			})
		case http.MethodPost:
			var value ckpt.CheckpointContext
			_ = json.NewDecoder(r.Body).Decode(&value)
			posted = append(posted, value)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	conf.Options.CheckpointStorage = utils.VarCheckpointStorageApi
	conf.Options.CheckpointStorageCollection = server.URL
	conf.Options.FullSyncReaderOplogStoreDisk = true
	conf.Options.LogDirectory = logDir
	conf.Options.SyncMode = utils.VarSyncModeAll
	conf.Options.IncrSyncFetcherBufferCapacity = 10
	conf.Options.IncrSyncMongoFetchMethod = utils.VarIncrSyncMongoFetchMethodOplog

	manager := ckpt.NewCheckpointManager(replset, 100)
	syncer := &OplogSyncer{
		Replset:     replset,
		ckptManager: manager,
		reader:      &checkpointTestReader{},
	}
	syncer.persister = NewPersister(replset, syncer)

	err = syncer.loadCheckpoint()
	require.NoError(t, err)
	require.Len(t, posted, 1)
	assert.Empty(t, posted[0].OplogDiskQueue, "should be empty")
	assert.Equal(t, int64(200), posted[0].OplogDiskQueueFinishTs, "should be equal")
	assert.Equal(t, utils.VarIncrSyncMongoFetchMethodOplog, posted[0].FetchMethod, "should be equal")
	assert.Equal(t, utils.FetchStageStoreMemoryApply, syncer.persister.GetFetchStage(), "should be equal")
	_, statErr := os.Stat(spool.Path(logDir, queueName))
	assert.True(t, os.IsNotExist(statErr), "should be equal")
}

type checkpointTestReader struct {
	updatedTs int64
}

func (r *checkpointTestReader) Name() string {
	return "checkpoint-test-reader"
}

func (r *checkpointTestReader) StartFetcher() {}

func (r *checkpointTestReader) SetQueryTimestampOnEmpty(interface{}) {}

func (r *checkpointTestReader) UpdateQueryTimestamp(ts int64) {
	r.updatedTs = ts
}

func (r *checkpointTestReader) Next() ([]byte, error) {
	return nil, nil
}

func (r *checkpointTestReader) EnsureNetwork() error {
	return nil
}

func (r *checkpointTestReader) FetchNewestTimestamp() (interface{}, error) {
	return nil, nil
}

func TestLoadCheckpointTreatsEmptyZeroDiskSpoolAsCompleted(t *testing.T) {
	origin := conf.Options
	defer func() {
		conf.Options = origin
	}()

	replset := "rs-old-empty-spool-fields"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(&ckpt.CheckpointContext{
				Name:                   replset,
				Timestamp:              200,
				Version:                utils.FcvCheckpoint.CurrentVersion,
				OplogDiskQueue:         "",
				OplogDiskQueueFinishTs: 0,
				FetchMethod:            utils.VarIncrSyncMongoFetchMethodOplog,
			})
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	conf.Options.CheckpointStorage = utils.VarCheckpointStorageApi
	conf.Options.CheckpointStorageCollection = server.URL
	conf.Options.FullSyncReaderOplogStoreDisk = true
	conf.Options.LogDirectory = t.TempDir()
	conf.Options.SyncMode = utils.VarSyncModeAll
	conf.Options.IncrSyncFetcherBufferCapacity = 1
	conf.Options.IncrSyncMongoFetchMethod = utils.VarIncrSyncMongoFetchMethodOplog
	conf.Options.FullSyncReaderOplogStoreDiskMaxSize = 256000

	manager := ckpt.NewCheckpointManager(replset, 200)
	syncer := &OplogSyncer{
		Replset:     replset,
		ckptManager: manager,
	}
	syncer.persister = &Persister{
		replset:         replset,
		sync:            syncer,
		Buffer:          make([][]byte, 0, conf.Options.IncrSyncFetcherBufferCapacity),
		diskQueueLastTs: -1,
	}

	err := syncer.loadCheckpoint()
	require.NoError(t, err)
	assert.Equal(t, utils.FetchStageStoreMemoryApply, syncer.persister.GetFetchStage(), "should be equal")
	assert.Nil(t, syncer.persister.DiskQueue, "should be equal")
}

func TestCheckpointPersistsEmptyDiskSpoolCompletion(t *testing.T) {
	origin := conf.Options
	defer func() {
		conf.Options = origin
	}()

	var posted ckpt.CheckpointContext
	replset := "rs-empty-spool-finish"
	queueName := "oplog-spool-rs-empty-spool-finish"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(&ckpt.CheckpointContext{
				Name:                   replset,
				Timestamp:              100,
				Version:                utils.FcvCheckpoint.CurrentVersion,
				OplogDiskQueue:         queueName,
				OplogDiskQueueFinishTs: ckpt.InitCheckpoint,
				FetchMethod:            utils.VarIncrSyncMongoFetchMethodOplog,
			})
		case http.MethodPost:
			_ = json.NewDecoder(r.Body).Decode(&posted)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	conf.Options.CheckpointStorage = utils.VarCheckpointStorageApi
	conf.Options.CheckpointStorageCollection = server.URL
	conf.Options.FullSyncReaderOplogStoreDisk = true
	conf.Options.CheckpointInterval = 0

	manager := ckpt.NewCheckpointManager(replset, 100)
	_, _, err := manager.Get()
	require.NoError(t, err)

	metric := utils.NewMetric(replset, utils.TypeIncr, 0)
	defer metric.Close()
	fakeSpool := &fakeOplogSpool{name: queueName}
	syncer := &OplogSyncer{
		ckptManager: manager,
		persister: &Persister{
			diskQueueLastTs: ckpt.InitCheckpoint,
			DiskQueue:       fakeSpool,
		},
		replMetric: metric,
		batcher: &Batcher{
			workerGroup: []*Worker{},
		},
		ckptTime:  time.Now().Add(-time.Hour),
		startTime: time.Now().Add(-time.Hour),
	}

	updated := syncer.checkpoint(true, 0)
	assert.Equal(t, true, updated, "should be equal")
	assert.Equal(t, int64(100), posted.Timestamp, "should be equal")
	assert.Equal(t, int64(100), posted.OplogDiskQueueFinishTs, "should be equal")
	assert.Equal(t, "", posted.OplogDiskQueue, "should be equal")
	assert.True(t, fakeSpool.deleted, "should be equal")
	assert.Nil(t, syncer.persister.DiskQueue, "should be equal")
	assert.Equal(t, int64(-2), syncer.persister.diskQueueLastTs, "should be equal")
}
