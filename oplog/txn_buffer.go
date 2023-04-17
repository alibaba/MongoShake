// Copyright (C) MongoDB, Inc. 2019-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

// Package txn implements functions for examining and processing transaction
// oplog entries.
package oplog

import (
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
)

var ErrBufferClosed = errors.New("transaction buffer already closed")
var ErrTxnAborted = errors.New("transaction aborted")
var ErrNotTransaction = errors.New("oplog entry is not a transaction")

type txnTask struct {
	meta TxnMeta
	op   ParsedLog
}

// txnState tracks an individual transaction, including storage of related ops
// and communication channels.  It includes a WaitGroup for waiting on
// transaction-related goroutines.
type txnState struct {
	buffer     []ParsedLog
	ingestChan chan txnTask
	ingestDone chan struct{}
	ingestErr  error
	stopChan   chan struct{}
	startTime  TxnOpTime
	wg         sync.WaitGroup
}

func newTxnState(op ParsedLog) *txnState {
	return &txnState{
		ingestChan: make(chan txnTask),
		ingestDone: make(chan struct{}),
		stopChan:   make(chan struct{}),
		buffer:     make([]ParsedLog, 0),
		startTime:  GetTxnOpTimeFromOplogEntry(&op),
	}
}

// Because state is currently kept in memory, purge merely drops the reference
// so the GC will eventually clean up.  Eventually, this might clean up a file
// on disk.
func (ts *txnState) purge() error {
	ts.buffer = nil
	return nil
}

// TxnBuffer stores transaction oplog entries until they are needed
// to commit them to a desination.  It includes a WaitGroup for tracking
// all goroutines across all transactions for use in global shutdown.
type TxnBuffer struct {
	sync.Mutex
	stopped bool
	txns    map[TxnID]*txnState
	wg      sync.WaitGroup
}

// NewBuffer initializes a transaction oplog buffer.
func NewBuffer() *TxnBuffer {
	return &TxnBuffer{
		txns: make(map[TxnID]*txnState),
	}
}

func (b *TxnBuffer) Size() int {
	return len(b.txns)
}

// Concurrency notes:
//
// We require that AddOp, GetTxnStream and PurgeTxn be called serially as part
// of orchestrating replay of oplog entries.  The only method that could run
// concurrently is Stop. If Stop is called, we're in some sort of global
// shutdown, so we don't care how other methods and goroutines resolve, only
// that they do so without panicking.
//

// AddOp sends a transaction oplog entry to a background goroutine (starting
// one for a new transaction TxnID) for asynchronous pre-processing and storage.
// If the oplog entry is not a transaction, an error will be returned.  Any
// errors during processing can be discovered later via the error channel from
// `GetTxnStream`.
//
// Must not be called concurrently with other transaction-related operations.
// Must not be called for a given transaction after starting to stream that
// transaction.
func (b *TxnBuffer) AddOp(m TxnMeta, op ParsedLog) error {
	b.Lock()
	defer b.Unlock()

	if b.stopped {
		return ErrBufferClosed
	}

	if !m.IsTxn() {
		return ErrNotTransaction
	}

	// Get or initialize transaction state
	state, ok := b.txns[m.id]
	if !ok {
		state = newTxnState(op)
		b.txns[m.id] = state
		b.wg.Add(1)
		state.wg.Add(1)
		go b.ingester(state)
	}

	// Send unless the ingester has shut down, e.g. on error
	select {
	case <-state.ingestDone:
	case state.ingestChan <- txnTask{meta: m, op: op}:
	}

	return nil
}

func (b *TxnBuffer) ingester(state *txnState) {
LOOP:
	for {
		select {
		case t := <-state.ingestChan:
			if t.meta.IsData() {
				// process it
				innerOps, err := ExtractInnerOps(&t.op)
				if err != nil {
					state.ingestErr = err
					break LOOP
				}
				// store it
				for _, op := range innerOps {
					state.buffer = append(state.buffer, op)
				}
			}
			if t.meta.IsFinal() {
				break LOOP
			}
		case <-state.stopChan:
			break LOOP
		}
	}
	close(state.ingestDone)
	state.wg.Done()
	b.wg.Done()
}

// GetTxnStream returns a channel of Oplog entries in a transaction and a
// channel for errors.  If the buffer has been stopped, the returned op channel
// will be closed and the error channel will have an error on it.
//
// Must not be called concurrently with other transaction-related operations.
// For a given transaction, it must not be called until after a final oplog
// entry has been passed to AddOp and it must not be called more than once.
func (b *TxnBuffer) GetTxnStream(m TxnMeta) (<-chan ParsedLog, <-chan error) {
	b.Lock()
	defer b.Unlock()

	opChan := make(chan ParsedLog)
	errChan := make(chan error, 1)

	if b.stopped {
		return sendErrAndClose(opChan, errChan, ErrBufferClosed)
	}

	if !m.IsTxn() {
		return sendErrAndClose(opChan, errChan, ErrNotTransaction)
	}

	state := b.txns[m.id]
	if state == nil {
		return sendErrAndClose(opChan, errChan, fmt.Errorf("GetTxnStream found no state for %v", m.id))
	}

	// The final oplog entry must have been passed to AddOp before calling this
	// method, so we know this will be able to make progress.
	<-state.ingestDone

	if state.ingestErr != nil {
		return sendErrAndClose(opChan, errChan, state.ingestErr)
	}

	// Launch streaming goroutine
	b.wg.Add(1)
	state.wg.Add(1)
	go b.streamer(state, opChan, errChan)

	return opChan, errChan
}

func (b *TxnBuffer) streamer(state *txnState, opChan chan<- ParsedLog, errChan chan<- error) {
LOOP:
	for _, op := range state.buffer {
		select {
		case opChan <- op:
		case <-state.stopChan:
			errChan <- ErrTxnAborted
			break LOOP
		}
	}
	close(opChan)
	close(errChan)
	state.wg.Done()
	b.wg.Done()
}

// OldestOpTime returns the optime of the oldest buffered transaction, or
// an empty optime if no transactions are buffered. This will include
// committed transactions until they are purged.
func (b *TxnBuffer) OldestOpTime() TxnOpTime {
	b.Lock()
	defer b.Unlock()
	oldest := TxnOpTime{}
	for _, v := range b.txns {
		if TxnOpTimeIsEmpty(oldest) || TxnOpTimeLessThan(v.startTime, oldest) {
			oldest = v.startTime
		}
	}
	return oldest
}

// PurgeTxn closes any transaction streams in progress and deletes all oplog
// entries associated with a transaction.
//
// Must not be called concurrently with other transaction-related operations.
// For a given transaction, it must not be called until after a final oplog
// entry has been passed to AddOp and it must not be called more than once.
func (b *TxnBuffer) PurgeTxn(m TxnMeta) error {
	b.Lock()
	defer b.Unlock()
	if b.stopped {
		return ErrBufferClosed
	}
	state := b.txns[m.id]
	if state == nil {
		return fmt.Errorf("PurgeTxn found no state for %v", m.id)
	}

	// When the lock is dropped, we don't want Stop to find this transaction and
	// double-close it.
	delete(b.txns, m.id)
	close(state.stopChan)

	// Wait for goroutines to terminate, then clean up.
	state.wg.Wait()
	state.purge()

	return nil
}

// Stop shuts down processing and cleans up.  Subsequent calls to Stop() will return nil.
// All other methods error after this is called.
func (b *TxnBuffer) Stop() error {
	b.Lock()
	if b.stopped {
		b.Unlock()
		return nil
	}

	b.stopped = true
	for _, state := range b.txns {
		close(state.stopChan)
	}

	b.Unlock()

	// At this point we know any subsequent public method will see the buffer
	// is stopped, no new goroutines will be launched, and existing goroutines
	// have been signaled to close.  Next, wait for goroutines to stop, then
	// clean up.

	b.wg.Wait()
	var firstErr error
	for _, state := range b.txns {
		err := state.purge()
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// sendErrAndClose is a utility for putting an error on a channel before closing.
func sendErrAndClose(o chan ParsedLog, e chan error, err error) (chan ParsedLog, chan error) {
	e <- err
	close(o)
	close(e)
	return o, e
}

func findValueByKey(keyName string, document *bson.D) (interface{}, error) {
	for _, key := range *document {
		if key.Key == keyName {
			return key.Value, nil
		}
	}
	return nil, errors.New("no such field")
}

const extractErrorFmt = "error extracting transaction ops: %s: %v"

// ExtractInnerOps
// doc.applyOps[i].ts（Let ckpt use the last ts to judge complete）
//     applyOps[0 - n-1].ts = doc.ts - 1
//     applyOps[n-1].ts = doc.ts
func ExtractInnerOps(tranOp *ParsedLog) ([]ParsedLog, error) {
	doc := tranOp.Object
	rawAO, err := findValueByKey("applyOps", &doc)
	if err != nil {
		return nil, fmt.Errorf(extractErrorFmt, "applyOps field", err)
	}

	ao, ok := rawAO.(bson.A)
	if !ok {
		return nil, fmt.Errorf(extractErrorFmt, "applyOps field", "not a BSON array")
	}

	tmpTimestamp := tranOp.Timestamp
	tmpTimestamp.I = tmpTimestamp.I - 1
	ops := make([]ParsedLog, len(ao))
	for i, v := range ao {
		opDoc, ok := v.(bson.D)
		if !ok {
			return nil, fmt.Errorf(extractErrorFmt, "applyOps op", "not a BSON document")
		}
		op, err := bsonDocToOplog(opDoc)
		if err != nil {
			return nil, fmt.Errorf(extractErrorFmt, "applyOps op", err)
		}

		// The inner ops doesn't have these fields and they are required by lastAppliedTime.Latest in Mongomirror,
		// so we are assigning them from the parent transaction op
		op.Timestamp = tmpTimestamp
		op.Term = tranOp.Term
		op.Hash = tranOp.Hash

		ops[i] = *op
	}
	if len(ops) > 0 {
		// applyOps maybe empty : https://jira.mongodb.org/browse/SERVER-50769
		ops[len(ops)-1].Timestamp = tranOp.Timestamp
	}

	return ops, nil
}

const opConvertErrorFmt = "error converting bson.D to op: %s: %v"

func bsonDocToOplog(doc bson.D) (*ParsedLog, error) {
	op := ParsedLog{}

	for _, v := range doc {
		switch v.Key {
		case "op":
			s, ok := v.Value.(string)
			if !ok {
				return nil, fmt.Errorf(opConvertErrorFmt, "op field", "not a string")
			}
			op.Operation = s
		case "ns":
			s, ok := v.Value.(string)
			if !ok {
				return nil, fmt.Errorf(opConvertErrorFmt, "ns field", "not a string")
			}
			op.Namespace = s
		case "o":
			d, ok := v.Value.(bson.D)
			if !ok {
				return nil, fmt.Errorf(opConvertErrorFmt, "o field", "not a BSON Document")
			}
			op.Object = d
		case "o2":
			d, ok := v.Value.(bson.D)
			if !ok {
				return nil, fmt.Errorf(opConvertErrorFmt, "o2 field", "not a BSON Document")
			}
			op.Query = d
		case "ui":
			u, ok := v.Value.(primitive.Binary)
			if !ok {
				return nil, fmt.Errorf(opConvertErrorFmt, "ui field", "not binary data")
			}
			op.UI = &u
		}
	}

	return &op, nil
}
