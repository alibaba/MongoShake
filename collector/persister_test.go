package collector

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/alibaba/MongoShake/v2/collector/ckpt"
	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	"github.com/alibaba/MongoShake/v2/collector/spool"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
)

func mockOplogsBinary() []byte {
	oplog := oplog.ParsedLog{
		Namespace: "a.b",
	}

	ret, err := bson.Marshal(&oplog)
	if err != nil {
		panic(err)
	}
	return ret
}

func mockOplogsBinaryWithTs(t *testing.T, ts int64) []byte {
	t.Helper()

	log := oplog.ParsedLog{
		Timestamp: utils.TimeToTimestamp(ts),
		Operation: "i",
		Namespace: "a.b",
		Object:    bson.D{{Key: "_id", Value: ts}},
	}

	ret, err := bson.Marshal(&log)
	require.NoError(t, err)
	return ret
}

func mockRawOplogBinaryWithTs(t *testing.T, ts primitive.Timestamp) []byte {
	t.Helper()

	ret, err := bson.Marshal(bson.D{
		{Key: "ts", Value: ts},
		{Key: "op", Value: "n"},
		{Key: "ns", Value: ""},
		{Key: "o", Value: bson.D{}},
	})
	require.NoError(t, err)
	return ret
}

func TestPersisterGetQueryTsFromDiskQueueReadsStoredTimestamp(t *testing.T) {
	oldFetchMethod := conf.Options.IncrSyncMongoFetchMethod
	conf.Options.IncrSyncMongoFetchMethod = utils.VarIncrSyncMongoFetchMethodOplog
	defer func() {
		conf.Options.IncrSyncMongoFetchMethod = oldFetchMethod
	}()

	want := primitive.Timestamp{T: 1234, I: 56}
	persister := &Persister{
		replset: "rs-raw-ts",
		DiskQueue: &fakeOplogSpool{
			name: "fake-raw-ts-spool",
			data: [][]byte{mockRawOplogBinaryWithTs(t, want)},
		},
	}

	assert.Equal(t, want, persister.GetQueryTsFromDiskQueue(), "should be equal")
}

func TestPersisterInjectWritesDiskBatches(t *testing.T) {
	oldCapacity := conf.Options.IncrSyncFetcherBufferCapacity
	oldThreshold := conf.Options.IncrSyncFetcherBufferSizeThresholdInKB
	conf.Options.IncrSyncFetcherBufferCapacity = 3
	conf.Options.IncrSyncFetcherBufferSizeThresholdInKB = 0
	defer func() {
		conf.Options.IncrSyncFetcherBufferCapacity = oldCapacity
		conf.Options.IncrSyncFetcherBufferSizeThresholdInKB = oldThreshold
	}()

	fakeSpool := &fakeOplogSpool{name: "fake-batch-spool"}
	persister := &Persister{
		replset:           "rs-batch",
		enableDiskPersist: true,
		fetchStage:        utils.FetchStageStoreDiskNoApply,
		DiskQueue:         fakeSpool,
	}

	persister.Inject(mockOplogsBinaryWithTs(t, 1))
	persister.Inject(mockOplogsBinaryWithTs(t, 2))
	assert.Empty(t, fakeSpool.data, "should be empty")
	assert.Equal(t, 0, fakeSpool.putBatchCalls, "should be equal")

	persister.Inject(mockOplogsBinaryWithTs(t, 3))
	assert.Len(t, fakeSpool.data, 3)
	assert.Equal(t, 1, fakeSpool.putBatchCalls, "should be equal")
	assert.Equal(t, uint64(3), persister.diskWriteCount, "should be equal")

	persister.Inject(mockOplogsBinaryWithTs(t, 4))
	persister.Inject(nil)
	assert.Len(t, fakeSpool.data, 4)
	assert.Equal(t, 2, fakeSpool.putBatchCalls, "should be equal")
	assert.Equal(t, uint64(4), persister.diskWriteCount, "should be equal")
}

func TestInject(t *testing.T) {
	// test Inject

	var nr int
	// normal
	{
		fmt.Printf("TestInject case %d.\n", nr)
		nr++

		conf.Options.IncrSyncFetcherBufferCapacity = 5
		conf.Options.IncrSyncFetcherBufferSizeThresholdInKB = 10240
		conf.Options.FullSyncReaderOplogStoreDisk = false

		syncer := mockSyncer()
		syncer.startDeserializer()
		persister := NewPersister("test-replica", syncer)

		persister.Inject(mockOplogsBinary())
		persister.Inject(mockOplogsBinary())
		persister.Inject(mockOplogsBinary())
		persister.Inject(mockOplogsBinary())
		persister.Inject(mockOplogsBinary())
		persister.Inject(mockOplogsBinary())
		persister.Inject(mockOplogsBinary())
		persister.Inject(nil)
		persister.Inject(mockOplogsBinary())
		persister.Inject(nil)
		persister.Inject(nil)
		persister.Inject(nil)
		persister.Inject(mockOplogsBinary())
		persister.Inject(nil)

		mergeBatch := <-syncer.logsQueue[0]
		assert.Equal(t, 5, len(mergeBatch), "should be equal")
		mergeBatch = <-syncer.logsQueue[1]
		assert.Equal(t, 2, len(mergeBatch), "should be equal")
		mergeBatch = <-syncer.logsQueue[2]
		assert.Equal(t, 1, len(mergeBatch), "should be equal")
		mergeBatch = <-syncer.logsQueue[3]
		assert.Equal(t, 1, len(mergeBatch), "should be equal")
	}
}

func TestPersisterRetrieveMemoryApplyShortcut(t *testing.T) {
	oldWaitInterval := persisterRetrieveWaitInterval
	persisterRetrieveWaitInterval = time.Millisecond
	defer func() {
		persisterRetrieveWaitInterval = oldWaitInterval
	}()

	persister := &Persister{
		replset:           "rs-memory-shortcut",
		fetchStage:        utils.FetchStageStoreMemoryApply,
		diskQueueLastTs:   -1,
		enableDiskPersist: true,
	}

	done := make(chan struct{})
	go func() {
		persister.retrieve()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("retrieve did not return on MemoryApply shortcut")
	}
	assert.Nil(t, persister.DiskQueue, "should be equal")
	assert.Equal(t, int64(-1), persister.diskQueueLastTs, "should be equal")
}

func TestPersisterRetrieveDiskApplyE2E(t *testing.T) {
	oldWaitInterval := persisterRetrieveWaitInterval
	oldReadInterval := persisterRetrieveReadInterval
	persisterRetrieveWaitInterval = time.Millisecond
	persisterRetrieveReadInterval = time.Millisecond
	defer func() {
		persisterRetrieveWaitInterval = oldWaitInterval
		persisterRetrieveReadInterval = oldReadInterval
	}()

	oldCapacity := conf.Options.IncrSyncFetcherBufferCapacity
	oldThreshold := conf.Options.IncrSyncFetcherBufferSizeThresholdInKB
	oldMethod := conf.Options.IncrSyncMongoFetchMethod
	conf.Options.IncrSyncFetcherBufferCapacity = 1
	conf.Options.IncrSyncFetcherBufferSizeThresholdInKB = 0
	conf.Options.IncrSyncMongoFetchMethod = utils.VarIncrSyncMongoFetchMethodOplog
	defer func() {
		conf.Options.IncrSyncFetcherBufferCapacity = oldCapacity
		conf.Options.IncrSyncFetcherBufferSizeThresholdInKB = oldThreshold
		conf.Options.IncrSyncMongoFetchMethod = oldMethod
	}()

	syncer := &OplogSyncer{
		Replset:      "rs-disk-e2e",
		PendingQueue: []chan [][]byte{make(chan [][]byte, 4)},
	}
	fakeSpool := &fakeOplogSpool{
		name: "fake-spool",
		data: [][]byte{
			mockOplogsBinaryWithTs(t, 101),
			mockOplogsBinaryWithTs(t, 102),
		},
	}
	persister := &Persister{
		replset:           syncer.Replset,
		sync:              syncer,
		Buffer:            make([][]byte, 0, conf.Options.IncrSyncFetcherBufferCapacity),
		enableDiskPersist: true,
		fetchStage:        utils.FetchStageStoreDiskApply,
		diskQueueLastTs:   -1,
		DiskQueue:         fakeSpool,
	}

	done := make(chan struct{})
	go func() {
		persister.retrieve()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("retrieve did not finish disk apply")
	}

	first := <-syncer.PendingQueue[0]
	second := <-syncer.PendingQueue[0]
	assert.Equal(t, mockOplogsBinaryWithTs(t, 101), first[0], "should be equal")
	assert.Equal(t, mockOplogsBinaryWithTs(t, 102), second[0], "should be equal")
	assert.Equal(t, utils.FetchStageStoreMemoryApply, persister.GetFetchStage(), "should be equal")
	assert.Equal(t, uint64(2), persister.diskReadCount, "should be equal")
	assert.Equal(t, utils.TimeStampToInt64(utils.TimeToTimestamp(102)), persister.diskQueueLastTs, "should be equal")
	assert.False(t, fakeSpool.deleted, "should be equal")
	assert.Same(t, fakeSpool, persister.DiskQueue, "should be equal")
}

func TestPersisterRetrieveEmptyDiskApplyMarksFinishSentinel(t *testing.T) {
	oldWaitInterval := persisterRetrieveWaitInterval
	oldReadInterval := persisterRetrieveReadInterval
	persisterRetrieveWaitInterval = time.Millisecond
	persisterRetrieveReadInterval = time.Millisecond
	defer func() {
		persisterRetrieveWaitInterval = oldWaitInterval
		persisterRetrieveReadInterval = oldReadInterval
	}()

	oldCapacity := conf.Options.IncrSyncFetcherBufferCapacity
	conf.Options.IncrSyncFetcherBufferCapacity = 1
	defer func() {
		conf.Options.IncrSyncFetcherBufferCapacity = oldCapacity
	}()

	syncer := &OplogSyncer{
		Replset:      "rs-empty-disk-e2e",
		PendingQueue: []chan [][]byte{make(chan [][]byte, 1)},
	}
	fakeSpool := &fakeOplogSpool{name: "fake-empty-spool"}
	persister := &Persister{
		replset:           syncer.Replset,
		sync:              syncer,
		Buffer:            make([][]byte, 0, conf.Options.IncrSyncFetcherBufferCapacity),
		enableDiskPersist: true,
		fetchStage:        utils.FetchStageStoreDiskApply,
		diskQueueLastTs:   -1,
		DiskQueue:         fakeSpool,
	}

	done := make(chan struct{})
	go func() {
		persister.retrieve()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("retrieve did not finish empty disk apply")
	}

	assert.Equal(t, utils.FetchStageStoreMemoryApply, persister.GetFetchStage(), "should be equal")
	assert.Equal(t, uint64(0), persister.diskReadCount, "should be equal")
	assert.Equal(t, ckpt.InitCheckpoint, persister.diskQueueLastTs, "should be equal")
	assert.False(t, fakeSpool.deleted, "should be equal")
	assert.Same(t, fakeSpool, persister.DiskQueue, "should be equal")
}

type fakeOplogSpool struct {
	name          string
	data          [][]byte
	readSeq       uint64
	putBatchCalls int
	deleted       bool
}

func (s *fakeOplogSpool) Put(data []byte) error {
	return s.PutBatch([][]byte{data})
}

func (s *fakeOplogSpool) PutBatch(data [][]byte) error {
	s.putBatchCalls++
	s.data = append(s.data, cloneBytesSlice(data)...)
	return nil
}

func (s *fakeOplogSpool) ReadBatch(max int) ([][]byte, error) {
	if max <= 0 {
		return nil, nil
	}
	start := int(s.currentReadSeq() - 1)
	if start >= len(s.data) {
		return nil, nil
	}
	end := start + max
	if end > len(s.data) {
		end = len(s.data)
	}
	return cloneBytesSlice(s.data[start:end]), nil
}

func (s *fakeOplogSpool) Advance(n int) error {
	s.readSeq = s.currentReadSeq() + uint64(n)
	return nil
}

func (s *fakeOplogSpool) ReadAll() ([][]byte, error) {
	start := int(s.currentReadSeq() - 1)
	if start >= len(s.data) {
		return nil, nil
	}
	return cloneBytesSlice(s.data[start:]), nil
}

func (s *fakeOplogSpool) LastWriteTimestamp() (int64, error) {
	if len(s.data) == 0 {
		return 0, nil
	}
	ts, err := oplog.ExtractRawTimestamp(s.data[len(s.data)-1], "ts")
	if err != nil {
		return 0, err
	}
	return utils.TimeStampToInt64(ts), nil
}

func (s *fakeOplogSpool) Depth() (uint64, error) {
	stats := s.Stats()
	return stats.Depth, nil
}

func (s *fakeOplogSpool) Stats() spool.Stats {
	readSeq := s.currentReadSeq()
	writeSeq := uint64(len(s.data))
	var depth uint64
	if writeSeq >= readSeq {
		depth = writeSeq - readSeq + 1
	}
	return spool.Stats{
		Name:     s.name,
		ReadSeq:  readSeq,
		WriteSeq: writeSeq,
		Depth:    depth,
	}
}

func (s *fakeOplogSpool) Close() error {
	return nil
}

func (s *fakeOplogSpool) Delete() error {
	s.deleted = true
	return nil
}

func (s *fakeOplogSpool) currentReadSeq() uint64 {
	if s.readSeq == 0 {
		return 1
	}
	return s.readSeq
}

func cloneBytesSlice(in [][]byte) [][]byte {
	out := make([][]byte, 0, len(in))
	for _, item := range in {
		out = append(out, append([]byte(nil), item...))
	}
	return out
}
