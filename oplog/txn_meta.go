// Copyright (C) MongoDB, Inc. 2019-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package oplog

import (
	"encoding/base64"
	"fmt"
)

// "empty" prevOpTime is {ts: Timestamp(0, 0), t: NumberLong(-1)} as BSON.
var emptyPrev = string([]byte{
	28, 0, 0, 0, 17, 116, 115, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	18, 116, 0, 255, 255, 255, 255, 255, 255, 255, 255, 0,
})

// TxnID wraps fields needed to uniquely identify a transaction for use as a map
// key.  The 'lsid' is a string rather than bson.Raw or []byte so that this
// type is a valid map key.
type TxnID struct {
	lsid      string
	txnNumber int64
}

func (id TxnID) String() string {
	return fmt.Sprintf("%s-%d", base64.RawStdEncoding.EncodeToString([]byte(id.lsid)), id.txnNumber)
}

// TxnMeta holds information extracted from an oplog entry for later routing
// logic.  Zero value means 'not a transaction'.  We store 'prevOpTime' as
// string so the struct is comparable.
type TxnMeta struct {
	id         TxnID
	commit     bool
	abort      bool
	partial    bool
	prepare    bool
	prevOpTime string
}

// NewTxnMeta extracts transaction metadata from an oplog entry.  A
// non-transaction will return a zero-value TxnMeta struct, not an error.
//
// Currently there is no way for this to error, but that may change in the
// future if we change the db.Oplog.Object to bson.Raw, so the API is designed
// with failure as a possibility.
func NewTxnMeta(op ParsedLog) (TxnMeta, error) {
	if op.LSID == nil || op.TxnNumber == nil || op.Operation != "c" {
		return TxnMeta{}, nil
	}

	// Default prevOpTime to empty to "upgrade" 4.0 transactions without it.
	m := TxnMeta{
		id:         TxnID{lsid: string(op.LSID), txnNumber: *op.TxnNumber},
		prevOpTime: emptyPrev,
	}

	if op.PrevOpTime != nil {
		m.prevOpTime = string(op.PrevOpTime)
	}

	// Inspect command to confirm a transaction command and identify parameters.
	var isRealTxn bool
	for _, e := range op.Object {
		switch e.Key {
		case "applyOps":
			isRealTxn = true
		case "commitTransaction":
			isRealTxn = true
			m.commit = true
		case "abortTransaction":
			isRealTxn = true
			m.abort = true
		case "partialTxn":
			m.partial = true
		case "prepare":
			m.prepare = true
		}
	}

	// Defensive, in case some other op command ever includes lsid+txnNumber
	if !isRealTxn {
		return TxnMeta{}, nil
	}

	return m, nil
}

// IsAbort is true if the oplog entry had the abort command.
func (m TxnMeta) IsAbort() bool {
	return m.abort
}

// IsData is true if the oplog entry contains transaction data
func (m TxnMeta) IsData() bool {
	return !m.commit && !m.abort
}

// IsCommit is true if the oplog entry was an abort command or was the
// final entry of an unprepared transaction.
func (m TxnMeta) IsCommit() bool {
	return m.commit || (m.IsTxn() && !m.prepare && !m.partial)
}

// IsFinal is true if the oplog entry is the closing entry of a transaction,
// i.e. if IsAbort or IsCommit is true.
func (m TxnMeta) IsFinal() bool {
	return m.IsCommit() || m.IsAbort()
}

// IsCommitOp is commitTransaction oplog
func (m TxnMeta) IsCommitOp() bool {
	return m.commit
}

// IsMultiOp is true if the oplog entry is part of a prepared and/or large
// transaction.
func (m TxnMeta) IsMultiOp() bool {
	return m.partial || m.prepare || (m.IsTxn() && m.prevOpTime != emptyPrev)
}

// IsTxn is true if the oplog entry is part of any transaction, i.e. the lsid field
// exists.
func (m TxnMeta) IsTxn() bool {
	return m != TxnMeta{}
}

func (m TxnMeta) String() string {
	return fmt.Sprintf("id:{%v, %v}, commit:%v, abort:%v, partial:%v, prepare:%v, prevOpTime:%v",
		m.id.lsid, m.id.txnNumber, m.commit, m.abort, m.partial, m.prepare, m.prevOpTime)
}
