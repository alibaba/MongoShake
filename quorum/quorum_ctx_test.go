package quorum

import (
	"context"
	"fmt"
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
