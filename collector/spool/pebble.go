package spool

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"

	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
)

const (
	// Version 1 was used by an earlier revision of this PR. Version 2 removes
	// durable read_seq and always replays from sequence 1 after reopen.
	schemaVersion            uint32 = 2
	pebbleWriteOverheadBytes        = 64 * 1024
)

var (
	metaVersionKey     = []byte("m/version")
	metaWriteSeqKey    = []byte("m/write_seq")
	metaLastWriteTsKey = []byte("m/last_write_ts")
	dataUpperBound     = []byte{'e'}
)

type PebbleSpool struct {
	mu sync.Mutex

	db       *pebble.DB
	name     string
	path     string
	metric   string
	stage    string
	maxBytes uint64
	fs       vfs.FS

	readSeq     uint64
	writeSeq    uint64
	lastWriteTs int64
	closed      bool
	deleted     bool
}

func Open(opts OpenOptions) (*PebbleSpool, error) {
	if err := validateName(opts.Name); err != nil {
		return nil, err
	}
	if opts.LogDir == "" {
		return nil, errors.New("oplog spool log directory is empty")
	}
	if opts.MetricStage == "" {
		opts.MetricStage = utils.TypeIncr
	}
	if opts.MetricName == "" {
		opts.MetricName = opts.Name
	}
	if opts.FS == nil {
		opts.FS = vfs.Default
	}
	var maxBytes uint64
	if opts.MaxBytesMB > 0 {
		if uint64(opts.MaxBytesMB) > ^uint64(0)/utils.MB {
			return nil, fmt.Errorf("oplog spool max size %d MiB overflows bytes", opts.MaxBytesMB)
		}
		maxBytes = uint64(opts.MaxBytesMB) * utils.MB
	}

	path := opts.FS.PathJoin(opts.LogDir, "spool", opts.Name)
	if err := opts.FS.MkdirAll(opts.FS.PathDir(path), 0o755); err != nil {
		observeError(opts.MetricName, opts.MetricStage, "open")
		return nil, err
	}
	if err := syncDirectory(opts.FS, opts.LogDir); err != nil {
		observeError(opts.MetricName, opts.MetricStage, "open")
		return nil, err
	}
	if !opts.CreateIfMissing {
		if _, err := opts.FS.Stat(path); os.IsNotExist(err) && hasLegacyDiskQueueFiles(opts.FS, opts.LogDir, opts.Name) {
			observeError(opts.MetricName, opts.MetricStage, "open")
			return nil, legacyOpenError(opts.Name, path, pebble.ErrDBDoesNotExist)
		}
	}

	dbOpts := &pebble.Options{FS: opts.FS}
	if opts.CreateIfMissing {
		dbOpts.ErrorIfExists = true
	} else {
		dbOpts.ErrorIfNotExists = true
	}

	db, err := pebble.Open(path, dbOpts)
	if err != nil {
		observeError(opts.MetricName, opts.MetricStage, "open")
		return nil, formatOpenError(opts.FS, opts.Name, path, err)
	}

	ps := &PebbleSpool{
		db:       db,
		name:     opts.Name,
		path:     path,
		metric:   opts.MetricName,
		stage:    opts.MetricStage,
		maxBytes: maxBytes,
		fs:       opts.FS,
	}

	if opts.CreateIfMissing {
		if err := ps.init(); err != nil {
			_ = db.Close()
			_ = opts.FS.RemoveAll(path)
			observeError(opts.MetricName, opts.MetricStage, "init")
			return nil, err
		}
		if err := syncDirectory(opts.FS, opts.FS.PathDir(path)); err != nil {
			_ = db.Close()
			_ = opts.FS.RemoveAll(path)
			observeError(opts.MetricName, opts.MetricStage, "init")
			return nil, err
		}
	} else if err := ps.load(); err != nil {
		_ = db.Close()
		observeError(opts.MetricName, opts.MetricStage, "open")
		return nil, err
	}

	ps.observeSeq()
	return ps, nil
}

func Path(logDir, name string) string {
	return filepath.Join(logDir, "spool", name)
}

func DeletePath(logDir, name string) error {
	if err := validateName(name); err != nil {
		return err
	}
	return os.RemoveAll(Path(logDir, name))
}

func validateName(name string) error {
	if name == "" || name == "." || name == ".." || filepath.Base(name) != name || strings.ContainsAny(name, `/\`) {
		return fmt.Errorf("invalid oplog spool name %q", name)
	}
	return nil
}

func (ps *PebbleSpool) init() error {
	batch := ps.db.NewBatch()
	defer batch.Close()

	if err := batch.Set(metaVersionKey, encodeUint32(schemaVersion), pebble.NoSync); err != nil {
		return err
	}
	if err := batch.Set(metaWriteSeqKey, encodeUint64(0), pebble.NoSync); err != nil {
		return err
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}

	ps.readSeq = 1
	ps.writeSeq = 0
	ps.lastWriteTs = 0
	return nil
}

func (ps *PebbleSpool) load() error {
	version, err := ps.getUint32(metaVersionKey)
	if err != nil {
		return fmt.Errorf("oplog spool %q path %q is missing schema version: %w", ps.name, ps.path, err)
	}
	if version != schemaVersion {
		return fmt.Errorf("oplog spool %q path %q schema version mismatch: got %d want %d",
			ps.name, ps.path, version, schemaVersion)
	}

	writeSeq, err := ps.getUint64(metaWriteSeqKey)
	if err != nil {
		return fmt.Errorf("oplog spool %q path %q is missing write_seq: %w", ps.name, ps.path, err)
	}
	var lastWriteTs int64
	if writeSeq > 0 {
		lastWriteTs, err = ps.getInt64(metaLastWriteTsKey)
		if err != nil {
			return fmt.Errorf("oplog spool %q path %q has data but missing last_write_ts: %w", ps.name, ps.path, err)
		}
	}

	ps.readSeq = 1
	ps.writeSeq = writeSeq
	ps.lastWriteTs = lastWriteTs
	return nil
}

func (ps *PebbleSpool) Put(data []byte) error {
	return ps.PutBatch([][]byte{data})
}

func (ps *PebbleSpool) PutBatch(data [][]byte) error {
	if len(data) == 0 {
		return nil
	}

	timestamps := make([]int64, len(data))
	var incomingBytes uint64
	for i, item := range data {
		if len(item) == 0 {
			observeError(ps.metric, ps.stage, "put")
			return errors.New("oplog spool rejects empty data")
		}
		ts, err := parseOplogTimestamp(item)
		if err != nil {
			observeError(ps.metric, ps.stage, "put")
			return err
		}
		timestamps[i] = ts
		if uint64(len(item)) > ^uint64(0)-incomingBytes {
			incomingBytes = ^uint64(0)
		} else {
			incomingBytes += uint64(len(item))
		}
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()
	if err := ps.ensureOpenLocked(); err != nil {
		observeError(ps.metric, ps.stage, "put")
		return err
	}
	if err := ps.ensureUnderMaxBytesLocked(incomingBytes); err != nil {
		observeError(ps.metric, ps.stage, "put")
		return err
	}

	batch := ps.db.NewBatch()
	defer batch.Close()

	if uint64(len(data)) > ^uint64(0)-ps.writeSeq {
		observeError(ps.metric, ps.stage, "put")
		return fmt.Errorf("oplog spool %q path %q write sequence overflow", ps.name, ps.path)
	}
	nextSeq := ps.writeSeq
	for _, item := range data {
		nextSeq++
		if err := batch.Set(dataKey(nextSeq), item, pebble.NoSync); err != nil {
			observeError(ps.metric, ps.stage, "put")
			return err
		}
	}
	if err := batch.Set(metaWriteSeqKey, encodeUint64(nextSeq), pebble.NoSync); err != nil {
		observeError(ps.metric, ps.stage, "put")
		return err
	}
	if err := batch.Set(metaLastWriteTsKey, encodeInt64(timestamps[len(timestamps)-1]), pebble.NoSync); err != nil {
		observeError(ps.metric, ps.stage, "put")
		return err
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		observeError(ps.metric, ps.stage, "put")
		return err
	}

	ps.writeSeq = nextSeq
	ps.lastWriteTs = timestamps[len(timestamps)-1]
	utils.SpoolWriteTotalProm.WithLabelValues(ps.metric, ps.stage).Add(float64(len(data)))
	ps.observeSeq()
	return nil
}

func (ps *PebbleSpool) ReadBatch(max int) ([][]byte, error) {
	if max <= 0 {
		return nil, nil
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()
	if err := ps.ensureOpenLocked(); err != nil {
		observeError(ps.metric, ps.stage, "read")
		return nil, err
	}
	rows, err := ps.readRangeLocked(ps.readSeq, ps.writeSeq, max)
	if err != nil {
		observeError(ps.metric, ps.stage, "read")
		return nil, err
	}
	if len(rows) > 0 {
		utils.SpoolReadTotalProm.WithLabelValues(ps.metric, ps.stage).Add(float64(len(rows)))
	}
	return rows, nil
}

func (ps *PebbleSpool) ReadAll() ([][]byte, error) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if err := ps.ensureOpenLocked(); err != nil {
		observeError(ps.metric, ps.stage, "read")
		return nil, err
	}
	rows, err := ps.readRangeLocked(ps.readSeq, ps.writeSeq, 0)
	if err != nil {
		observeError(ps.metric, ps.stage, "read")
		return nil, err
	}
	if len(rows) > 0 {
		utils.SpoolReadTotalProm.WithLabelValues(ps.metric, ps.stage).Add(float64(len(rows)))
	}
	return rows, nil
}

func (ps *PebbleSpool) readRangeLocked(start, end uint64, max int) ([][]byte, error) {
	if start > end {
		return nil, nil
	}

	iter, err := ps.db.NewIter(&pebble.IterOptions{
		LowerBound: dataKey(start),
		UpperBound: dataUpperBound,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	rows := make([][]byte, 0)
	seq := start
	for valid := iter.First(); valid && seq <= end; valid = iter.Next() {
		if max > 0 && len(rows) >= max {
			break
		}
		expected := dataKey(seq)
		if !bytes.Equal(iter.Key(), expected) {
			return nil, fmt.Errorf("oplog spool %q path %q missing data key for seq %d", ps.name, ps.path, seq)
		}
		rows = append(rows, append([]byte(nil), iter.Value()...))
		seq++
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	if seq <= end && (max <= 0 || len(rows) < max) {
		return nil, fmt.Errorf("oplog spool %q path %q missing data key for seq %d", ps.name, ps.path, seq)
	}
	return rows, nil
}

func (ps *PebbleSpool) Advance(n int) error {
	if n < 0 {
		observeError(ps.metric, ps.stage, "advance")
		return fmt.Errorf("oplog spool advance rejects negative count %d", n)
	}
	if n == 0 {
		return nil
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()
	if err := ps.ensureOpenLocked(); err != nil {
		observeError(ps.metric, ps.stage, "advance")
		return err
	}

	nextReadSeq := ps.readSeq + uint64(n)
	if nextReadSeq > ps.writeSeq+1 {
		observeError(ps.metric, ps.stage, "advance")
		return fmt.Errorf("oplog spool %q path %q advance beyond write_seq: read_seq=%d n=%d write_seq=%d",
			ps.name, ps.path, ps.readSeq, n, ps.writeSeq)
	}
	ps.readSeq = nextReadSeq
	ps.observeSeq()
	return nil
}

func (ps *PebbleSpool) LastWriteTimestamp() (int64, error) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if err := ps.ensureOpenLocked(); err != nil {
		observeError(ps.metric, ps.stage, "last_write")
		return 0, err
	}
	return ps.lastWriteTs, nil
}

func (ps *PebbleSpool) Depth() (uint64, error) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if err := ps.ensureOpenLocked(); err != nil {
		observeError(ps.metric, ps.stage, "depth")
		return 0, err
	}
	return ps.depthLocked(), nil
}

func (ps *PebbleSpool) Stats() Stats {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return Stats{
		Name:     ps.name,
		Path:     ps.path,
		ReadSeq:  ps.readSeq,
		WriteSeq: ps.writeSeq,
		Depth:    ps.depthLocked(),
		Bytes:    ps.diskUsageLocked(),
	}
}

func (ps *PebbleSpool) Close() error {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if ps.closed || ps.db == nil {
		ps.closed = true
		return nil
	}
	if err := ps.db.Flush(); err != nil {
		observeError(ps.metric, ps.stage, "close")
		return err
	}
	if err := ps.db.Close(); err != nil {
		observeError(ps.metric, ps.stage, "close")
		return err
	}
	ps.closed = true
	ps.db = nil
	return nil
}

func (ps *PebbleSpool) Delete() error {
	if err := ps.Close(); err != nil {
		observeError(ps.metric, ps.stage, "delete")
		return err
	}
	if err := ps.fs.RemoveAll(ps.path); err != nil {
		observeError(ps.metric, ps.stage, "delete")
		return err
	}

	ps.mu.Lock()
	ps.deleted = true
	ps.readSeq = 1
	ps.writeSeq = 0
	ps.lastWriteTs = 0
	ps.mu.Unlock()
	utils.SpoolDepthProm.WithLabelValues(ps.metric, ps.stage).Set(0)
	utils.SpoolReadSeqProm.WithLabelValues(ps.metric, ps.stage).Set(0)
	utils.SpoolWriteSeqProm.WithLabelValues(ps.metric, ps.stage).Set(0)
	return nil
}

func (ps *PebbleSpool) getUint32(key []byte) (uint32, error) {
	data, err := ps.getBytes(key)
	if err != nil {
		return 0, err
	}
	if len(data) != 4 {
		return 0, fmt.Errorf("meta key %q has invalid uint32 length %d", key, len(data))
	}
	return binary.BigEndian.Uint32(data), nil
}

func (ps *PebbleSpool) getUint64(key []byte) (uint64, error) {
	data, err := ps.getBytes(key)
	if err != nil {
		return 0, err
	}
	if len(data) != 8 {
		return 0, fmt.Errorf("meta key %q has invalid uint64 length %d", key, len(data))
	}
	return binary.BigEndian.Uint64(data), nil
}

func (ps *PebbleSpool) getInt64(key []byte) (int64, error) {
	data, err := ps.getUint64(key)
	if err != nil {
		return 0, err
	}
	return int64(data), nil
}

func (ps *PebbleSpool) getBytes(key []byte) ([]byte, error) {
	data, closer, err := ps.db.Get(key)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	return append([]byte(nil), data...), nil
}

func (ps *PebbleSpool) ensureOpenLocked() error {
	if ps.closed || ps.db == nil {
		if ps.deleted {
			return fmt.Errorf("oplog spool %q path %q is deleted", ps.name, ps.path)
		}
		return fmt.Errorf("oplog spool %q path %q is closed", ps.name, ps.path)
	}
	return nil
}

func (ps *PebbleSpool) ensureUnderMaxBytesLocked(incomingDataBytes uint64) error {
	if ps.maxBytes == 0 {
		return nil
	}
	current := ps.diskUsageLocked()
	incoming := estimateWriteBytes(incomingDataBytes)
	if current >= ps.maxBytes || incoming > ps.maxBytes-current {
		return fmt.Errorf("oplog spool %q path %q exceeds max size: current=%d incoming=%d max=%d",
			ps.name, ps.path, current, incoming, ps.maxBytes)
	}
	return nil
}

func (ps *PebbleSpool) diskUsageLocked() uint64 {
	if ps.db == nil {
		return 0
	}
	return ps.db.Metrics().DiskSpaceUsage()
}

func (ps *PebbleSpool) depthLocked() uint64 {
	if ps.writeSeq < ps.readSeq {
		return 0
	}
	return ps.writeSeq - ps.readSeq + 1
}

func (ps *PebbleSpool) observeSeq() {
	utils.SpoolReadSeqProm.WithLabelValues(ps.metric, ps.stage).Set(float64(ps.readSeq))
	utils.SpoolWriteSeqProm.WithLabelValues(ps.metric, ps.stage).Set(float64(ps.writeSeq))
	utils.SpoolDepthProm.WithLabelValues(ps.metric, ps.stage).Set(float64(ps.depthLocked()))
}

func observeError(name, stage, op string) {
	if stage == "" {
		stage = utils.TypeIncr
	}
	utils.SpoolErrorsTotalProm.WithLabelValues(name, stage, op).Inc()
}

func parseOplogTimestamp(data []byte) (int64, error) {
	ts, err := oplog.ExtractRawTimestamp(data, "ts")
	if err != nil {
		return 0, fmt.Errorf("oplog spool data: %w", err)
	}
	return utils.TimeStampToInt64(ts), nil
}

func dataKey(seq uint64) []byte {
	key := make([]byte, 9)
	key[0] = 'd'
	binary.BigEndian.PutUint64(key[1:], seq)
	return key
}

func encodeUint32(v uint32) []byte {
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, v)
	return data
}

func encodeUint64(v uint64) []byte {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, v)
	return data
}

func encodeInt64(v int64) []byte {
	return encodeUint64(uint64(v))
}

func estimateWriteBytes(dataBytes uint64) uint64 {
	if dataBytes > ^uint64(0)-pebbleWriteOverheadBytes {
		return ^uint64(0)
	}
	return dataBytes + pebbleWriteOverheadBytes
}

func formatOpenError(fs vfs.FS, name, path string, err error) error {
	if errors.Is(err, pebble.ErrDBDoesNotExist) {
		if entries, readErr := fs.List(path); readErr == nil && len(entries) > 0 {
			return legacyOpenError(name, path, err)
		}
		return fmt.Errorf("oplog spool %q path %q does not exist: %w", name, path, err)
	}
	return fmt.Errorf("open pebble oplog spool %q path %q failed: %w", name, path, err)
}

func hasLegacyDiskQueueFiles(fs vfs.FS, logDir, name string) bool {
	entries, err := fs.List(logDir)
	if err != nil {
		return false
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry, name) {
			return true
		}
	}
	return false
}

func syncDirectory(fs vfs.FS, path string) error {
	dir, err := fs.OpenDir(path)
	if err != nil {
		return err
	}
	if err := dir.Sync(); err != nil {
		_ = dir.Close()
		return err
	}
	return dir.Close()
}

func legacyOpenError(name, path string, err error) error {
	return fmt.Errorf("oplog spool %q path %q is not a Pebble spool. Legacy go-diskqueue files are not supported: %w",
		name, path, err)
}
