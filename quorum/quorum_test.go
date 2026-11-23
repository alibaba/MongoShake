package quorum

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func testURI(t *testing.T) string {
	t.Helper()
	uri := os.Getenv("MONGOSHAKE_TEST_URL")
	if uri == "" {
		t.Skip("MONGOSHAKE_TEST_URL not set; election needs a real MongoDB")
	}
	return uri
}

func TestMasterStatusAndNotifier(t *testing.T) {
	savedMaster, savedID := master, electionObjectId
	t.Cleanup(func() { master, electionObjectId = savedMaster, savedID })

	// drain any pending notification so the promote path is observable
	select {
	case <-MasterPromotionNotifier:
	default:
	}

	masterChanged(DescendMaster)
	assert.False(t, IsMaster())

	masterChanged(PromoteMaster)
	assert.True(t, IsMaster())
	select {
	case v := <-MasterPromotionNotifier:
		assert.True(t, v)
	case <-time.After(time.Second):
		t.Fatal("promote must notify the waiter")
	}

	// a second promote must not block or double-notify (channel len guard)
	masterChanged(PromoteMaster)
	assert.True(t, IsMaster())

	AlwaysMaster()
	assert.True(t, IsMaster())
	masterChanged(DescendMaster)
	assert.False(t, IsMaster())

	id := primitive.NewObjectID()
	UseElectionObjectId(id)
	assert.Equal(t, id, electionObjectId)
}

func TestPromotionAndNetAddr(t *testing.T) {
	savedID := electionObjectId
	t.Cleanup(func() { electionObjectId = savedID })
	id := primitive.NewObjectID()
	UseElectionObjectId(id)

	p := promotion()
	m := bson.M{}
	for _, e := range p {
		m[e.Key] = e.Value
	}
	assert.Equal(t, id, m["_id"])
	assert.Equal(t, os.Getpid(), m["pid"])
	assert.Equal(t, getNetAddr(), m["host"])
	hb, ok := m["heartbeat"].(int64)
	require.True(t, ok)
	assert.InDelta(t, time.Now().Unix(), hb, 5)

	addr := getNetAddr()
	assert.NotEqual(t, "error", addr, "host has a non-loopback IPv4")
	assert.NotContains(t, addr, "127.0.0.1")
}

func TestMakeSessionBadURIFailsFast(t *testing.T) {
	_, err := makeSession("mongodb://127.0.0.1:1/?connectTimeoutMS=300&serverSelectionTimeoutMS=300")
	assert.Error(t, err)
}

func TestCompeteMasterAgainstRealMongo(t *testing.T) {
	uri := testURI(t)
	conn, err := makeSession(uri)
	require.NoError(t, err)
	defer conn.Close()

	coll := conn.Client.Database("quorum_it").Collection("election_it")
	require.NoError(t, coll.Drop(context.Background()))
	t.Cleanup(func() { _ = coll.Drop(context.Background()) })

	savedID := electionObjectId
	t.Cleanup(func() { electionObjectId = savedID })
	UseElectionObjectId(primitive.NewObjectID())

	// first insert wins the election
	assert.True(t, competeMaster(coll))
	// same _id again is a duplicate key -> lose, not crash
	assert.False(t, competeMaster(coll))

	var entry ElectionEntry
	require.NoError(t, coll.FindOne(context.Background(), bson.M{"_id": electionObjectId}).Decode(&entry))
	assert.Equal(t, os.Getpid(), entry.PID)
	assert.Equal(t, getNetAddr(), entry.Host)
}

func TestMakeSessionGoodURI(t *testing.T) {
	uri := testURI(t)
	conn, err := makeSession(uri)
	require.NoError(t, err)
	require.NotNil(t, conn.Client)
	conn.Close()
}
