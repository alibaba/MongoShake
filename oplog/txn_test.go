package oplog

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

const (
	OplogEntriesFile = "testdata/oplog_entries.json"
)

var testCases = []*TestData{
	{name: "not transaction", entryCount: 1, notTxn: true},
	{name: "applyops not transaction", entryCount: 1, notTxn: true},
	{name: "small, unprepared", entryCount: 1, innerOpCount: 3, commits: true},
	{name: "large, unprepared", entryCount: 3, innerOpCount: 6, commits: true},
	{name: "small, prepared, committed", entryCount: 2, innerOpCount: 4, commits: true},
	{name: "small, prepared, aborted", entryCount: 2, innerOpCount: 5, aborts: true},
	{name: "large, prepared, committed", entryCount: 4, innerOpCount: 10, commits: true},
	{name: "large, prepared, aborted", entryCount: 4, innerOpCount: 9, aborts: true},
	{name: "not transaction with lsid", entryCount: 1, notTxn: true},
	{name: "not transaction with lsid and txnNumber", entryCount: 1, notTxn: true},
	{name: "not transaction with lsid and txnNumber and command", entryCount: 1, notTxn: true},
	{name: "not transaction with multiOpType", entryCount: 1, notTxn: true},
}

type TestData struct {
	name         string
	entryCount   int
	innerOpCount int
	notTxn       bool
	commits      bool
	aborts       bool
	ops          []ParsedLog
}

func readTestData() (bson.Raw, error) {
	b, err := os.ReadFile(OplogEntriesFile)
	if err != nil {
		return nil, fmt.Errorf("couldn't load %s: %v", OplogEntriesFile, err)
	}
	var data bson.Raw
	err = bson.UnmarshalExtJSON(b, false, &data)
	if err != nil {
		return nil, fmt.Errorf("couldn't decode JSON: %v", err)
	}
	return data, nil
}
func getOpsForCase(name string, data bson.Raw) ([]ParsedLog, error) {
	rawArray, err := data.LookupErr(name, "ops")
	if err != nil {
		return nil, fmt.Errorf("couldn't find ops for case %s: %v", name, err)
	}
	rawOps, err := rawArray.Array().Elements()
	if err != nil {
		return nil, fmt.Errorf("couldn't extract array elements for case %s: %v", name, err)
	}
	ops := make([]ParsedLog, len(rawOps))
	for i, e := range rawOps {
		err := e.Value().Unmarshal(&ops[i])
		if err != nil {
			return nil, fmt.Errorf("couldn't unmarshal op %d for case %s: %v", i, name, err)
		}
	}
	return ops, nil
}
func mapTestTxnByID() (map[TxnID]*TestData, error) {
	m := make(map[TxnID]*TestData)
	for _, c := range testCases {
		meta, err := NewTxnMeta(c.ops[0])
		if err != nil {
			return nil, err
		}
		if meta.IsTxn() {
			m[meta.id] = c
		}
	}
	return m, nil
}
func TestMain(m *testing.M) {
	flag.Parse()
	data, err := readTestData()
	if err != nil {
		panic(err)
	}
	for i, c := range testCases {
		ops, err := getOpsForCase(c.name, data)
		if err != nil {
			panic(err)
		}
		// c is a copy and we want to change the original
		testCases[i].ops = ops
	}
	os.Exit(m.Run())
}

// ------------- fot txn_meta.go ----------------
func TestTxnMeta(t *testing.T) {
	for _, c := range testCases {
		t.Run(c.name, func(*testing.T) {
			if c.notTxn {
				runNonTxnMetaCase(t, c)
			} else {
				runTxnMetaCase(t, c)
			}
		})
	}
}
func runNonTxnMetaCase(t *testing.T, c *TestData) {
	meta, err := NewTxnMeta(c.ops[0])
	if err != nil {
		t.Fatalf("case %s: failed to parse op: %v", c.name, err)
	}
	if meta.IsTxn() {
		t.Errorf("case %s: non-txn meta looks like transaction", c.name)
	}
	return
}
func runTxnMetaCase(t *testing.T, c *TestData) {
	ops := c.ops
	// Double check that we get all the ops we expected.
	if len(ops) != c.entryCount {
		t.Errorf("case %s: expected %d ops, but got %d", c.name, c.entryCount, len(ops))
	}
	// Test properties of each op.
	for i, o := range ops {
		meta, err := NewTxnMeta(o)
		if err != nil {
			t.Fatalf("case %s [%d]: failed to parse op: %v", c.name, i, err)
		}
		if (meta.id == TxnID{}) {
			t.Errorf("case %s [%d]: Id was zero value", c.name, i)
		}
		isMultiOp := c.entryCount > 1
		if meta.IsMultiOp() != isMultiOp {
			t.Errorf(
				"case %s [%d]: expected IsMultiOp %v, but got %v",
				c.name,
				i,
				meta.IsMultiOp(),
				isMultiOp,
			)
		}
		if i == 0 {
			if !meta.IsData() {
				t.Errorf("case %s [%d]: op should have parsed as data, but it wasn't", c.name, i)
			}
		}
		if i != len(ops)-1 {
			if meta.IsFinal() {
				t.Errorf("case %s [%d]: op parsed as final, but it wasn't", c.name, i)
			}
		}
	}
	// Test properties of the last op.
	lastOp := ops[len(ops)-1]
	meta, _ := NewTxnMeta(lastOp)
	if !meta.IsFinal() {
		t.Errorf("case %s: last oplog entry not marked final", c.name)
	}
	if c.commits && !meta.IsCommit() {
		t.Errorf("case %s: expected last oplog entry to be a commit but it wasn't", c.name)
	}
	if c.aborts && !meta.IsAbort() {
		t.Errorf("case %s: expected last oplog entry to be a abort but it wasn't", c.name)
	}
}

// ------------- fot txn_buffer.go ----------------
// test each type of transaction individually and serially.
func TestSingleTxnBuffer(t *testing.T) {
	buffer := NewBuffer()
	txnByID, err := mapTestTxnByID()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			testBufferOps(t, buffer, c.ops, txnByID)
		})
	}
}
func TestMixedTxnBuffer(t *testing.T) {
	buffer := NewBuffer()
	txnByID, err := mapTestTxnByID()
	if err != nil {
		t.Fatal(err)
	}
	streams := make([][]ParsedLog, len(testCases))
	for i, c := range testCases {
		streams[i] = c.ops
	}
	ops := mergeOplogStreams(streams)
	testBufferOps(t, buffer, ops, txnByID)
}
func testBufferOps(t *testing.T, buffer *TxnBuffer, ops []ParsedLog, txnByID map[TxnID]*TestData) {
	innerOpCounter := make(map[TxnID]int)
	for _, op := range ops {
		meta, _ := NewTxnMeta(op)
		if !meta.IsTxn() {
			return
		}
		err := buffer.AddOp(meta, &GenericOplog{
			Parsed: &PartialLog{
				ParsedLog: op,
			},
		})
		if err != nil {
			t.Fatalf("AddOp failed: %v", err)
		}
		if meta.IsAbort() {
			err := buffer.PurgeTxn(meta)
			if err != nil {
				t.Fatalf("PurgeTxn (abort) failed: %v", err)
			}
			assertNoStateForID(t, meta, buffer)
			continue
		}
		if !meta.IsCommit() {
			continue
		}
		// From here, we're simulating "applying" transaction entries
		ops, errs := buffer.GetTxnStream(meta)
	LOOP:
		for {
			select {
			case _, ok := <-ops:
				if !ok {
					break LOOP
				}
				innerOpCounter[meta.id]++
			case err := <-errs:
				if err != nil {
					t.Fatalf("GetTxnStream streaming failed: %v", err)
				}
				break LOOP
			}
		}
		expectedCnt := txnByID[meta.id].innerOpCount
		if innerOpCounter[meta.id] != expectedCnt {
			t.Errorf(
				"incorrect streamed op count; got %d, expected %d",
				innerOpCounter[meta.id],
				expectedCnt,
			)
		}
		err = buffer.PurgeTxn(meta)
		if err != nil {
			t.Fatalf("PurgeTxn (commit) failed: %v", err)
		}
		assertNoStateForID(t, meta, buffer)
	}
}

// MergeOplogStreams combines oplog arrays such that the order of entries is
// random, but order-preserving with respect to each initial stream.
func mergeOplogStreams(input [][]ParsedLog) []ParsedLog {
	// Copy input op arrays so we can destructively shuffle them together
	streams := make([][]ParsedLog, len(input))
	opCount := 0
	for i, v := range input {
		streams[i] = make([]ParsedLog, len(v))
		copy(streams[i], v)
		opCount += len(v)
	}
	ops := make([]ParsedLog, 0, opCount)
	for len(streams) != 0 {
		// randomly pick a stream to add an op
		rand.Shuffle(len(streams), func(i, j int) {
			streams[i], streams[j] = streams[j], streams[i]
		})
		ops = append(ops, streams[0][0])
		// remove the op and its stream if empty
		streams[0] = streams[0][1:]
		if len(streams[0]) == 0 {
			streams = streams[1:]
		}
	}
	return ops
}
func assertNoStateForID(t *testing.T, meta TxnMeta, buffer *TxnBuffer) {
	_, ok := buffer.txns[meta.id]
	if ok {
		t.Errorf("state not cleared for %v", meta.id)
	}
}
func TestOldestTimestamp(t *testing.T) {
	buffer := NewBuffer()
	// With no transactions, oldest active is zero value
	oldest := buffer.OldestOpTime()
	zeroTimestamp := primitive.Timestamp{}
	if oldest.Timestamp != zeroTimestamp {
		t.Errorf("expected zero timestamp, but got %v", oldest.Timestamp)
	}
	// Constructing manually requires pointers to int64, so they can't be constants.
	txnN := []int64{0, 1}
	ops := []ParsedLog{
		{
			Timestamp: primitive.Timestamp{T: 1234, I: 1},
			LSID:      bson.Raw{0, 0, 0, 0, 1},
			TxnNumber: &txnN[0],
			Operation: "c",
			Namespace: "admin.$cmd",
			Object: bson.D{
				{"applyOps", bson.A{bson.D{{"op", "n"}}}},
				{"partialTxn", true},
			},
		},
		{
			Timestamp: primitive.Timestamp{T: 1235, I: 1},
			LSID:      bson.Raw{0, 0, 0, 0, 2},
			TxnNumber: &txnN[1],
			Operation: "c",
			Namespace: "admin.$cmd",
			Object: bson.D{
				{"applyOps", bson.A{bson.D{{"op", "n"}}}},
				{"partialTxn", true},
			},
		},
		{
			Timestamp: primitive.Timestamp{T: 1236, I: 1},
			LSID:      bson.Raw{0, 0, 0, 0, 1},
			TxnNumber: &txnN[0],
			Operation: "c",
			Namespace: "admin.$cmd",
			Object: bson.D{
				{"applyOps", bson.A{bson.D{{"op", "n"}}}},
				{"partialTxn", true},
			},
		},
	}
	for _, v := range ops {
		meta, err := NewTxnMeta(v)
		if err != nil {
			t.Fatal(err)
		}
		err = buffer.AddOp(meta, &GenericOplog{
			Parsed: &PartialLog{
				ParsedLog: v,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	// With uncommitted transactions, we should see the oldest among them.
	oldest = buffer.OldestOpTime()
	expect := primitive.Timestamp{T: 1234, I: 1}
	if oldest.Timestamp != expect {
		t.Fatalf("expected timestamp %v, but got %v", expect, oldest)
	}
}
func TestExtractInnerOps(t *testing.T) {
	// Constructing manually requires pointers to int64, so they can't be constants.
	txnN := []int64{0}
	term := []int64{1}
	hash := []int64{2}
	timestamp := primitive.Timestamp{T: 1234, I: 1}
	Convey(
		"extracted oplogs from transaction oplog should have the same timestamp, term and hash",
		t,
		func() {
			op := ParsedLog{
				Timestamp: primitive.Timestamp{T: 1234, I: 1},
				Term:      &term[0],
				Hash:      &hash[0],
				LSID:      bson.Raw{0, 0, 0, 0, 1},
				TxnNumber: &txnN[0],
				Operation: "c",
				Namespace: "admin.$cmd",
				Object: bson.D{
					{"applyOps", bson.A{bson.D{{"op", "n"}}}},
					{"partialTxn", true},
				},
			}
			innerOps, err := ExtractInnerOps(&op)
			if err != nil {
				t.Fatalf("PurgeTxn (abort) failed: %v", err)
			}
			for _, innerOp := range innerOps {
				So(innerOp.Timestamp, ShouldEqual, timestamp)
				So(*innerOp.Term, ShouldEqual, term[0])
				So(*innerOp.Hash, ShouldEqual, hash[0])
			}
		},
	)
}
