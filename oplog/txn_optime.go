package oplog

import (
	"fmt"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// TimestampGreaterThan returns true if lhs comes after rhs, false otherwise.
func TimestampGreaterThan(lhs, rhs primitive.Timestamp) bool {
	return lhs.T > rhs.T || lhs.T == rhs.T && lhs.I > rhs.I
}

// TimestampLessThan returns true if lhs comes before rhs, false otherwise.
func TimestampLessThan(lhs, rhs primitive.Timestamp) bool {
	return lhs.T < rhs.T || lhs.T == rhs.T && lhs.I < rhs.I
}

// TxnOpTime represents the values to uniquely identify an oplog entry.
// An TxnOpTime must always have a timestamp, but may or may not have a term.
// The hash is set uniquely up until (and including) version 4.0, but is set
// to zero in version 4.2+ with plans to remove it soon (see SERVER-36334).
type TxnOpTime struct {
	Timestamp primitive.Timestamp `json:"timestamp"`
	Term      *int64              `json:"term"`
	Hash      *int64              `json:"hash"`
}

// GetTxnOpTimeFromOplogEntry returns an TxnOpTime struct from the relevant fields in an ParsedLog struct.
func GetTxnOpTimeFromOplogEntry(oplogEntry *ParsedLog) TxnOpTime {
	return TxnOpTime{
		Timestamp: oplogEntry.Timestamp,
		Term:      oplogEntry.Term,
		Hash:      oplogEntry.Hash,
	}
}

// TxnOpTimeIsEmpty returns true if opTime is uninitialized, false otherwise.
func TxnOpTimeIsEmpty(opTime TxnOpTime) bool {
	return opTime == TxnOpTime{}
}

// TxnOpTimeEquals returns true if lhs equals rhs, false otherwise.
// We first check for nil / not nil mismatches between the terms and
// between the hashes. Then we check for equality between the terms and
// between the hashes (if they exist) before checking the timestamps.
func TxnOpTimeEquals(lhs TxnOpTime, rhs TxnOpTime) bool {
	if (lhs.Term == nil && rhs.Term != nil) || (lhs.Term != nil && rhs.Term == nil) ||
		(lhs.Hash == nil && rhs.Hash != nil) || (lhs.Hash != nil && rhs.Hash == nil) {
		return false
	}

	termsBothNilOrEqual := true
	if lhs.Term != nil && rhs.Term != nil {
		termsBothNilOrEqual = *lhs.Term == *rhs.Term
	}

	hashesBothNilOrEqual := true
	if lhs.Hash != nil && rhs.Hash != nil {
		hashesBothNilOrEqual = *lhs.Hash == *rhs.Hash
	}

	return lhs.Timestamp.Equal(rhs.Timestamp) && termsBothNilOrEqual && hashesBothNilOrEqual
}

// TxnOpTimeLessThan returns true if lhs comes before rhs, false otherwise.
// We first check if both the terms exist. If they don't or they're equal,
// we compare just the timestamps.
func TxnOpTimeLessThan(lhs TxnOpTime, rhs TxnOpTime) bool {
	if lhs.Term != nil && rhs.Term != nil {
		if *lhs.Term == *rhs.Term {
			return TimestampLessThan(lhs.Timestamp, rhs.Timestamp)
		}
		return *lhs.Term < *rhs.Term
	}

	return TimestampLessThan(lhs.Timestamp, rhs.Timestamp)
}

// TxnOpTimeGreaterThan returns true if lhs comes after rhs, false otherwise.
// We first check if both the terms exist. If they don't or they're equal,
// we compare just the timestamps.
func TxnOpTimeGreaterThan(lhs TxnOpTime, rhs TxnOpTime) bool {
	if lhs.Term != nil && rhs.Term != nil {
		if *lhs.Term == *rhs.Term {
			return TimestampGreaterThan(lhs.Timestamp, rhs.Timestamp)
		}
		return *lhs.Term > *rhs.Term
	}

	return TimestampGreaterThan(lhs.Timestamp, rhs.Timestamp)
}

func (ot TxnOpTime) String() string {
	if ot.Term != nil && ot.Hash != nil {
		return fmt.Sprintf("{Timestamp: %v, Term: %v, Hash: %v}", ot.Timestamp, *ot.Term, *ot.Hash)
	} else if ot.Term == nil && ot.Hash != nil {
		return fmt.Sprintf("{Timestamp: %v, Term: %v, Hash: %v}", ot.Timestamp, nil, *ot.Hash)
	} else if ot.Term != nil && ot.Hash == nil {
		return fmt.Sprintf("{Timestamp: %v, Term: %v, Hash: %v}", ot.Timestamp, *ot.Term, nil)
	} else {
		return fmt.Sprintf("{Timestamp: %v, Term: %v, Hash: %v}", ot.Timestamp, nil, nil)
	}
}
