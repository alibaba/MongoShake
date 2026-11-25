package quorum

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	utils "github.com/alibaba/MongoShake/v2/common"
	"github.com/alibaba/MongoShake/v2/unit_test_common"
)

func isolateQuorumGlobals(t *testing.T) {
	t.Helper()
	oldId, oldMaster := electionObjectId, master
	UseElectionObjectId(primitive.NewObjectID())
	t.Cleanup(func() {
		UseElectionObjectId(oldId)
		master = oldMaster
	})
}

// O1=a contract: a pre-cancelled ctx bounds connection init and returns promptly.
func TestBecomeMasterWithContextPreCancelled(t *testing.T) {
	isolateQuorumGlobals(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() { done <- BecomeMasterWithContext(ctx, unit_test_common.TestUrl, "ut_quorum_precancel") }()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(15 * time.Second):
		t.Fatal("pre-cancelled ctx did not bound connection init")
	}
}

// O1=a contract: cancel during the heartbeat wait returns, the function is joined
// before globals are restored, and no election write happens after cancellation.
func TestBecomeMasterWithContextCancelDuringWait(t *testing.T) {
	isolateQuorumGlobals(t)
	url := unit_test_common.TestUrl
	db := fmt.Sprintf("ut_quorum_wait_%d", time.Now().UnixNano())
	conn, err := utils.NewMongoCommunityConn(url, utils.VarMongoConnectModePrimary, true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Client.Database(db).Drop(context.Background())
		conn.Close()
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- BecomeMasterWithContext(ctx, url, db) }()
	// let the loop elect itself and enter the heartbeat wait
	time.Sleep(500 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(20 * time.Second):
		t.Fatal("BecomeMasterWithContext did not return after cancel during wait")
	}
	// joined: no further election writes after cancellation
	coll := conn.Client.Database(db).Collection(QUORUM_COLLECTION)
	n1, err := coll.CountDocuments(context.Background(), bson.M{})
	require.NoError(t, err)
	time.Sleep(HeartBeatPeriod + 500*time.Millisecond)
	n2, err := coll.CountDocuments(context.Background(), bson.M{})
	require.NoError(t, err)
	require.Equal(t, n1, n2, "election loop kept writing after cancellation")
}

// Regression for the SESSION_CLOSE refactor: a session-close must be a retry
// signal (errSessionClose), NEVER a nil success that terminates the election
// loop and strands the master flag (#1052 HA review blocking 1).
func TestKeepQuorumSessionCloseIsRetrySignal(t *testing.T) {
	isolateQuorumGlobals(t)
	conn, err := utils.NewMongoCommunityConn(unit_test_common.TestUrl, utils.VarMongoConnectModePrimary, true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
	require.NoError(t, err)
	conn.Close() // force the election FindOne to fail -> STATUS_SESSION_CLOSE
	err = keepQuorum(context.Background(), conn, fmt.Sprintf("ut_quorum_sc_%d", time.Now().UnixNano()))
	require.ErrorIs(t, err, errSessionClose, "session close must signal retry, not return nil success")
}

// Cancel during active election I/O (master heartbeat updates), join, then prove
// the heartbeat content freezes — counting documents alone cannot detect an
// update to the same election doc (#1052 review P2).
func TestBecomeMasterWithContextCancelDuringElectionIO(t *testing.T) {
	isolateQuorumGlobals(t)
	url := unit_test_common.TestUrl
	db := fmt.Sprintf("ut_quorum_io_%d", time.Now().UnixNano())
	conn, err := utils.NewMongoCommunityConn(url, utils.VarMongoConnectModePrimary, true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Client.Database(db).Drop(context.Background())
		conn.Close()
	})
	coll := conn.Client.Database(db).Collection(QUORUM_COLLECTION)
	heartbeat := func() int64 {
		var doc bson.M
		if err := coll.FindOne(context.Background(), bson.M{"_id": electionObjectId}).Decode(&doc); err != nil {
			return 0
		}
		hb, _ := doc["heartbeat"].(int64)
		return hb
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- BecomeMasterWithContext(ctx, url, db) }()
	// wait until the heartbeat actually advances (election I/O is active)
	var firstHB int64
	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		hb := heartbeat()
		if hb != 0 {
			if firstHB == 0 {
				firstHB = hb
			} else if hb != firstHB {
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	require.NotZero(t, firstHB, "election never reached active heartbeat I/O")
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(20 * time.Second):
		t.Fatal("did not return after cancel during election I/O")
	}
	// joined: heartbeat content must freeze (no further election writes)
	hb1 := heartbeat()
	time.Sleep(HeartBeatPeriod + 500*time.Millisecond)
	hb2 := heartbeat()
	require.Equal(t, hb1, hb2, "heartbeat kept updating after cancellation")
}

// Cancel while a real election Mongo operation is blocked in flight (private view
// with a slow JS projection; currentOp is the barrier). Passing Background instead
// of ctx to the Mongo ops would ignore cancellation and fail this test (#1052 P2).
func TestBecomeMasterWithContextCancelDuringBlockedIO(t *testing.T) {
	isolateQuorumGlobals(t)
	dbName := fmt.Sprintf("ut_quorum_blocked_%d", time.Now().UnixNano())
	conn, err := utils.NewMongoCommunityConn(unit_test_common.TestUrl, utils.VarMongoConnectModePrimary, true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, "")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Client.Database(dbName).Drop(context.Background())
		conn.Close()
	})
	_, err = conn.Client.Database(dbName).Collection("backing").InsertOne(context.Background(),
		bson.M{"_id": electionObjectId, "pid": int64(-1), "host": "blocked_fixture", "heartbeat": time.Now().Unix() + 600})
	require.NoError(t, err)
	err = conn.Client.Database(dbName).RunCommand(context.Background(), bson.D{
		{Key: "create", Value: QUORUM_COLLECTION}, {Key: "viewOn", Value: "backing"},
		{Key: "pipeline", Value: bson.A{bson.M{"$addFields": bson.M{"delay": bson.M{"$function": bson.M{
			"body": "function() { sleep(6000); return 1; }", "args": bson.A{}, "lang": "js"}}}}}},
	}).Err()
	require.NoError(t, err)
	u, err := url.Parse(unit_test_common.TestUrl)
	require.NoError(t, err)
	if u.Path == "" {
		u.Path = "/"
	}
	app := fmt.Sprintf("ut1052io%d", time.Now().UnixNano())
	q := u.Query()
	q.Set("appName", app)
	u.RawQuery = q.Encode()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var electionErr error
	go func() { electionErr = BecomeMasterWithContext(ctx, u.String(), dbName); close(done) }()
	var opID interface{}
	t.Cleanup(func() {
		cancel()
		if opID != nil {
			_ = conn.Client.Database("admin").RunCommand(context.Background(),
				bson.D{{Key: "killOp", Value: 1}, {Key: "op", Value: opID}}).Err()
		}
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Error("election goroutine was not joined")
		}
	})
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var resp bson.M
		err = conn.Client.Database("admin").RunCommand(context.Background(), bson.D{
			{Key: "currentOp", Value: 1}, {Key: "active", Value: true},
			{Key: "clientMetadata.application.name", Value: app}, {Key: "command.$db", Value: dbName},
		}).Decode(&resp)
		require.NoError(t, err)
		if ops, ok := resp["inprog"].(bson.A); ok && len(ops) > 0 {
			if op, ok := ops[0].(bson.M); ok {
				opID = op["opid"]
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.NotNil(t, opID, "election Mongo operation never became active")
	cancel()
	select {
	case <-done:
		require.ErrorIs(t, electionErr, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("blocked Mongo operation ignored cancellation (ctx not threaded to Mongo ops)")
	}
}
