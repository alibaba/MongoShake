package sourceReader

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/oplog"
	l "github.com/alibaba/MongoShake/v2/pkg/log"
)

const (
	QueryTs    = "ts"
	QueryOpGT  = "$gt"
	QueryOpGTE = "$gte"

	localDB = "local"
)

const (
	// MaxCappedRetry is the maximum number of automatic cursor reset retries
	// when CappedPositionLost error occurs before treating it as fatal.
	MaxCappedRetry = utils.VarOplogReaderMaxCappedRetry
)

// TimeoutError defines mongodb query executed timeout
var TimeoutError = errors.New("read next log timeout, It shouldn't be happen")
var CollectionCappedError = errors.New("collection capped error")
var CollectionCappedFatalError = errors.New("collection capped error persists after max retries, fatal")

// OplogReader represents stream reader from mongodb that specified
// by an url. And with query options. user can iterate oplogs.
type OplogReader struct {
	// source mongo address url
	src     string
	replset string

	// mongo oplog reader
	conn         *utils.MongoCommunityConn
	oplogsCursor *mongo.Cursor

	// query statement and current max cursor
	query bson.M

	// oplog channel
	oplogChan    chan *retOplog
	fetcherExist bool
	fetcherLock  sync.Mutex

	firstRead bool

	// cappedErrorCount tracks consecutive CappedPositionLost errors
	cappedErrorCount int

	// anchorPersist durably records an adjusted starting point before the
	// clamped stream flows. Without it a restart would forget the adjustment
	// and skip the gap (#1052 review P1), so no hook means no clamp.
	anchorPersist func(int64) error

	// probe overrides for tests; nil means the real probes. The serving !=
	// primary semantics cannot be exercised on single-node fixtures otherwise.
	servingProbe func() int64
	primaryProbe func() int64
}

// NewOplogReader creates reader with mongodb url
func NewOplogReader(src string, replset string) *OplogReader {
	return &OplogReader{
		src:       src,
		replset:   replset,
		query:     bson.M{},
		oplogChan: make(chan *retOplog, 10*conf.Options.IncrSyncReaderFetchBatchSize), // ten times of batchSize
		firstRead: true,
	}
}

func (or *OplogReader) String() string {
	return fmt.Sprintf("oplogReader[src:%s replset:%s]", utils.BlockMongoUrlPassword(or.src, "***"),
		or.replset)
}

func (or *OplogReader) Name() string {
	return utils.VarIncrSyncMongoFetchMethodOplog
}

// SetQueryTimestampOnEmpty set internal timestamp if
// not exist in this or. initial stage most of the time
func (or *OplogReader) SetQueryTimestampOnEmpty(ts interface{}) {
	tsB := ts.(int64)
	if _, exist := or.query[QueryTs]; !exist {
		l.Logger.Infof("set query timestamp: %v", utils.ExtractTimestampForLog(tsB))
		or.UpdateQueryTimestamp(tsB)
	}
}

func (or *OplogReader) UpdateQueryTimestamp(ts int64) {
	or.query[QueryTs] = bson.M{QueryOpGT: utils.Int64ToTimestamp(ts)}
	// per-batch query anchor advance: noisy in steady state, keep at debug;
	// the initial anchor is reported once via SetQueryTimestampOnEmpty.
	l.Logger.Debugf("update or.query to %v", or.query)
}

// SetAnchorPersistHook installs the durable writer for an adjusted starting
// point. The syncer wires it to the checkpoint manager; the hook must persist
// synchronously and return an error when persistence fails.
func (or *OplogReader) SetAnchorPersistHook(hook func(int64) error) {
	or.anchorPersist = hook
}

func (or *OplogReader) getQueryTimestamp() int64 {
	return utils.TimeStampToInt64(or.query[QueryTs].(bson.M)[QueryOpGT].(primitive.Timestamp))
}

// Next returns an oplog by raw bytes which is []byte
func (or *OplogReader) Next() ([]byte, error) {
	return or.get()
}

// NextOplog returns an oplog by oplog.GenericOplog struct
func (or *OplogReader) NextOplog() (log *oplog.GenericOplog, err error) {
	var raw []byte
	if raw, err = or.Next(); err != nil {
		return nil, err
	}

	log = &oplog.GenericOplog{Raw: raw, Parsed: new(oplog.PartialLog)}
	if err = bson.Unmarshal(raw, log.Parsed); err != nil {
		return nil, err
	}
	return log, nil
}

// internal get next oplog. Used in Next() and NextOplog(). The channel and current function may both return
// timeout which is acceptable.
func (or *OplogReader) get() (log []byte, err error) {
	select {
	case ret := <-or.oplogChan:
		return ret.log, ret.err
	case <-time.After(time.Second * time.Duration(conf.Options.IncrSyncReaderBufferTime)):
		return nil, TimeoutError
	}
}

// StartFetcher start fetcher if not exist
func (or *OplogReader) StartFetcher() {
	if or.fetcherExist == true {
		return
	}

	or.fetcherLock.Lock()
	if or.fetcherExist == false { // double check
		or.fetcherExist = true
		go or.fetcher()
	}
	or.fetcherLock.Unlock()
}

// fetch oplog tp store disk queue or memory
func (or *OplogReader) fetcher() {
	l.Logger.Infof("start %s fetcher with src[%v] replica-name[%v] query-ts[%v]",
		or.String(), utils.BlockMongoUrlPassword(or.src, "***"), or.replset,
		or.query[QueryTs].(bson.M)[QueryOpGT].(primitive.Timestamp))
	for {
		if err := or.EnsureNetwork(); err != nil {
			// EnsureNetwork returns CollectionCappedError when queryTs falls behind oldest oplog.
			// Count consecutive capped errors and escalate to fatal after MaxCappedRetry.
			if errors.Is(err, CollectionCappedError) {
				or.cappedErrorCount++
				l.Logger.Errorf("oplog collection capped error from EnsureNetwork [%d/%d]",
					or.cappedErrorCount, MaxCappedRetry)
				if or.cappedErrorCount > MaxCappedRetry {
					or.oplogChan <- &retOplog{nil, CollectionCappedFatalError}
				} else {
					or.oplogChan <- &retOplog{nil, err}
				}
			} else {
				or.oplogChan <- &retOplog{nil, err}
			}
			time.Sleep(1 * time.Second)
			continue
		}

		if !or.oplogsCursor.TryNext(context.Background()) {
			if err := or.oplogsCursor.Err(); err != nil {
				// some internal error. need rebuild the oplogsCursor
				or.releaseCursor()
				if utils.IsCollectionCappedError(err) {
					or.cappedErrorCount++
					l.Logger.Errorf("oplog collection capped may happen [%d/%d]: %v",
						or.cappedErrorCount, MaxCappedRetry, err)
					if or.cappedErrorCount > MaxCappedRetry {
						l.Logger.Errorf("oplog collection capped error persists after %d retries, treating as fatal",
							MaxCappedRetry)
						or.oplogChan <- &retOplog{nil, CollectionCappedFatalError}
					} else {
						or.oplogChan <- &retOplog{nil, CollectionCappedError}
					}
				} else {
					e := fmt.Errorf("get next oplog failed, release oplogsIterator, %s", err.Error())
					or.oplogChan <- &retOplog{nil, e}
				}
				// wait a moment
				time.Sleep(1 * time.Second)
			} else {
				// query timeout
				or.oplogChan <- &retOplog{nil, TimeoutError}
			}
			continue
		}

		// successfully read data, reset capped error counter
		or.cappedErrorCount = 0
		or.oplogChan <- &retOplog{cloneOplogRaw(or.oplogsCursor.Current), nil}
	}
}

func cloneOplogRaw(raw bson.Raw) bson.Raw {
	if raw == nil {
		return nil
	}
	return append(bson.Raw(nil), raw...)
}

// EnsureNetwork establish the mongodb connection at first
// if current connection is not ready or disconnected
func (or *OplogReader) EnsureNetwork() (err error) {
	if or.oplogsCursor != nil {
		return nil
	}
	l.Logger.Infof("%s ensure network", or.String())

	if or.conn == nil || (or.conn != nil && !or.conn.IsGood()) {
		if or.conn != nil {
			or.conn.Close()
		}
		// reconnect
		if or.conn, err = utils.NewMongoCommunityConn(or.src, conf.Options.MongoConnectMode, true,
			utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault,
			conf.Options.MongoSslRootCaFile); or.conn == nil || err != nil {
			err = fmt.Errorf("oplog_reader reconnect mongo instance [%s] error. %s", or.src, err.Error())
			return err
		}
	}

	findOptions := options.Find().SetBatchSize(int32(conf.Options.IncrSyncReaderFetchBatchSize)).
		SetNoCursorTimeout(true).
		SetCursorType(options.TailableAwait).
		SetMaxAwaitTime(time.Millisecond * 1000).
		SetOplogReplay(true)

	var queryTs int64
	// the given oplog timestamp shouldn't bigger than the newest
	if or.firstRead == true {
		if err := or.adjustFutureStartPosition(); err != nil {
			return err
		}
	}

	/*
	 * the given oplog timestamp shouldn't smaller than the oldest.
	 * this may happen when collection capped.
	 */
	oldestTs := or.getOldestTimestamp()
	queryTs = or.getQueryTimestamp()
	if oldestTs > queryTs {
		// bugfix v2.8.10: a lost real position must fail on the first read too,
		// not only on later ones. Only the "start from earliest" sentinel (no
		// usable checkpoint) may continue here; a real position that fell out of
		// this member's window would silently skip the changes in between and
		// poison every later replay (#1046).
		if !or.firstRead || queryTs > (1<<32) {
			l.Logger.Errorf("oplog_reader queryTs[%v] is behind oldest oplog[%v], oplog may have been overwritten (CappedPositionLost)",
				utils.ExtractTimestampForLog(queryTs), utils.ExtractTimestampForLog(oldestTs))
			return CollectionCappedError
		}
		l.Logger.Warnf("oplog_reader current starting point[%v] is smaller than the oldest timestamp[%v]!",
			utils.ExtractTimestampForLog(queryTs), utils.ExtractTimestampForLog(oldestTs))
	}
	or.firstRead = false

	or.oplogsCursor, err = or.conn.Client.Database(localDB).Collection(utils.OplogNS).Find(context.Background(),
		or.query, findOptions)
	if or.oplogsCursor == nil || err != nil {
		err = fmt.Errorf("oplog_reader Find mongo instance [%s] error. %s", or.src, err.Error())
		l.Logger.Warnf("oplog_reader failed err[%v] or.query[%v]", err, or.query)
		return err
	}

	l.Logger.Infof("%s generates new cursor query[%v]", or.String(), or.query)

	return
}

// clampFuturePosition returns the serving member's newest as the adjusted
// anchor only when the start is genuinely future: ahead of both the serving
// member and the primary. A start ahead of a lagging serving member but not of
// the primary is valid checkpoint progress and must not be rewritten. Either
// probe returning <= 0 means it failed; never rewrite on a failed probe.
func clampFuturePosition(queryTs, servingNewest, primaryNewest int64) int64 {
	if servingNewest > 0 && primaryNewest > 0 && queryTs > servingNewest && queryTs > primaryNewest {
		return servingNewest
	}
	return queryTs
}

// adjustFutureStartPosition runs on the first read only. When the starting
// point is genuinely future, it persists the adjusted anchor BEFORE any oplog
// flows: a volatile-only clamp is forgotten on restart, which then clamps to a
// later newest and skips everything in between (#1049, #1052 review P1).
func (or *OplogReader) adjustFutureStartPosition() error {
	newestTs := or.getNewestTimestamp()
	queryTs := or.getQueryTimestamp()
	if newestTs <= 0 {
		l.Logger.Warnf("%s newest oplog probe failed on first read, a future starting point would not be clamped", or.String())
		return nil
	}
	if queryTs <= newestTs {
		return nil
	}
	// probe the primary only for a candidate ahead of the serving member
	primaryTs, probeErr := or.getPrimaryNewestTimestamp()
	if probeErr == nil && primaryTs <= 0 {
		probeErr = fmt.Errorf("probe returned invalid timestamp %d", primaryTs)
	}
	if probeErr != nil {
		// Already known: queryTs > servingNewest. Letting this pass would latch
		// firstRead=false and wait at the future point forever — the original
		// #1049 silent skip. Block and retry like a persist failure instead.
		// The address and cause are included so operators can tell election,
		// DNS, auth and TLS failures apart (review M2).
		return fmt.Errorf("oplog_reader primary newest probe failed, starting point[%v] ahead of serving newest[%v]: %w — retry",
			utils.ExtractTimestampForLog(queryTs), utils.ExtractTimestampForLog(newestTs), probeErr)
	}
	return or.applyFuturePositionDecision(queryTs, newestTs, primaryTs)
}

// applyFuturePositionDecision is the probe-free core of the clamp so the
// serving != primary semantics can be tested without a lagging secondary.
func (or *OplogReader) applyFuturePositionDecision(queryTs, servingNewest, primaryNewest int64) error {
	if servingNewest <= 0 {
		l.Logger.Warnf("%s newest oplog probe failed on first read, a future starting point would not be clamped", or.String())
		return nil
	}
	if queryTs <= servingNewest {
		return nil
	}
	if primaryNewest <= 0 {
		// Already known: queryTs > servingNewest. Letting this pass would latch
		// firstRead=false and wait at the future point forever — the original
		// #1049 silent skip. Block and retry like a persist failure instead.
		return fmt.Errorf("oplog_reader primary newest probe failed, starting point[%v] ahead of serving newest[%v]: retry",
			utils.ExtractTimestampForLog(queryTs), utils.ExtractTimestampForLog(servingNewest))
	}
	clamped := clampFuturePosition(queryTs, servingNewest, primaryNewest)
	if clamped == queryTs {
		l.Logger.Infof("%s starting point[%v] is ahead of the serving member newest[%v] but not of the primary newest[%v], waiting for the member to catch up (a delayed/hidden member waits out its full delay)",
			or.String(), utils.ExtractTimestampForLog(queryTs), utils.ExtractTimestampForLog(servingNewest), utils.ExtractTimestampForLog(primaryNewest))
		return nil
	}
	if or.anchorPersist == nil {
		l.Logger.Warnf("%s no anchor persist hook, starting point[%v] ahead of the newest[%v] is left unchanged",
			or.String(), utils.ExtractTimestampForLog(queryTs), utils.ExtractTimestampForLog(servingNewest))
		return nil
	}
	if err := or.anchorPersist(clamped); err != nil {
		return fmt.Errorf("oplog_reader persist adjusted starting point[%v] failed: %w",
			utils.ExtractTimestampForLog(clamped), err)
	}
	l.Logger.Warnf("oplog_reader current starting point[%v] is bigger than the newest timestamp[%v], adjust the starting point to the newest and persist it",
		utils.ExtractTimestampForLog(queryTs), utils.ExtractTimestampForLog(servingNewest))
	// write back so the Find below and any later cursor rebuild use the
	// adjusted anchor; waiting at the future point would silently skip
	// every change between newest and it (#1049).
	or.UpdateQueryTimestamp(clamped)
	return nil
}

// get newest oplog
func (or *OplogReader) getNewestTimestamp() int64 {
	if or.servingProbe != nil {
		return or.servingProbe()
	}
	ts, _ := utils.GetNewestTimestampByConn(or.conn)
	return ts
}

// getPrimaryNewestTimestamp probes the actual primary of the serving member's
// replica set, to tell a genuinely future checkpoint from a valid one ahead of
// a lagging secondary. Read preference alone is not enough: a directConnection
// URI pins the driver to one node (Single topology ignores readpref), so the
// primary address is taken from the serving member's isMaster response and
// probed directly. isMaster (not hello) keeps 4.x sources working. Errors carry
// the address and cause; the caller blocks and retries (no silent fail-open).
// Note: the discovered primary address must be reachable from the collector;
// single-node tunnel deployments must forward it too (review M2, documented).
func (or *OplogReader) getPrimaryNewestTimestamp() (int64, error) {
	if or.primaryProbe != nil {
		return or.primaryProbe(), nil
	}
	var hello bson.M
	if err := or.conn.Client.Database("admin").RunCommand(context.Background(), bson.D{{Key: "isMaster", Value: 1}}).Decode(&hello); err != nil {
		return 0, fmt.Errorf("isMaster on serving member: %w", err)
	}
	addr, _ := hello["primary"].(string)
	if addr == "" {
		return 0, errors.New("no primary in isMaster response (election in progress, or a loadBalanced/standalone source)")
	}
	ts, err := utils.GetNewestTimestampByUrl(primaryProbeURL(or.src, addr), false,
		utils.VarMongoConnectModeStandalone, conf.Options.MongoSslRootCaFile)
	if err != nil {
		return 0, fmt.Errorf("probe primary %s: %w", addr, err)
	}
	return ts, nil
}

// primaryProbeURL builds a direct connection to the discovered primary.
// The userinfo is carried over RAW: NewMongoCommunityConn runs EncodeMongoURI
// on the probe URL exactly like the main connection, so pre-encoding here
// would double-escape special-character passwords (review B1). The source
// path (path-derived authSource) and connection options (authSource,
// authMechanism, tls, ...) are inherited; only the topology pinning is
// replaced by directConnection to the primary.
func primaryProbeURL(src, addr string) string {
	rest, _ := strings.CutPrefix(src, "mongodb://")
	userInfo := ""
	if at := strings.LastIndex(rest, "@"); at != -1 {
		userInfo = rest[:at] + "@"
		rest = rest[at+1:]
	}
	hostPart := rest
	path := "/"
	if h, _, ok := strings.Cut(rest, "?"); ok {
		hostPart = h
	}
	if slash := strings.Index(hostPart, "/"); slash != -1 {
		path = hostPart[slash:]
	}
	query := url.Values{}
	rawFallback := ""
	if _, rawQuery, ok := strings.Cut(rest, "?"); ok {
		// the driver splits options on both '&' and ';'; mirror that before
		// parsing, and never silently drop options the main path would keep
		if parsed, err := url.ParseQuery(strings.ReplaceAll(rawQuery, ";", "&")); err == nil {
			query = parsed
		} else {
			rawFallback = rawQuery
		}
	}
	query.Del("directConnection")
	query.Del("replicaSet")
	query.Del("connect")
	query.Del("loadBalanced")
	query.Set("directConnection", "true")
	suffix := "?" + query.Encode()
	if rawFallback != "" {
		suffix = "?" + rawFallback
	}
	return "mongodb://" + userInfo + addr + path + suffix
}

// get oldest oplog
func (or *OplogReader) getOldestTimestamp() int64 {
	ts, _ := utils.GetOldestTimestampByConn(or.conn)
	return ts
}

func (or *OplogReader) releaseCursor() {
	if or.oplogsCursor != nil {
		_ = or.oplogsCursor.Close(context.Background())
	}
	or.oplogsCursor = nil
}

func (or *OplogReader) FetchNewestTimestamp() (interface{}, error) {
	return nil, fmt.Errorf("interface not implement")
}
