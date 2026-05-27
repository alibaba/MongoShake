package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson/primitive"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
)

func TestInitStartupHTTPApisStartPrometheusBeforeLeaderElection(t *testing.T) {
	origin := conf.Options
	originPrometheusInit := prometheusInitHttpApiFunc
	originStartPrometheus := startPrometheusHttpApiFunc
	originSelectLeader := selectLeaderFunc
	originFullSyncInit := fullSyncInitHttpApiFunc
	originIncrSyncInit := incrSyncInitHttpApiFunc
	originRegisterConf := registerConfHttpApiFunc
	defer func() {
		conf.Options = origin
		prometheusInitHttpApiFunc = originPrometheusInit
		startPrometheusHttpApiFunc = originStartPrometheus
		selectLeaderFunc = originSelectLeader
		fullSyncInitHttpApiFunc = originFullSyncInit
		incrSyncInitHttpApiFunc = originIncrSyncInit
		registerConfHttpApiFunc = originRegisterConf
	}()

	conf.Options = conf.Configuration{
		Id:                     "rs-startup",
		FullSyncHTTPListenPort: 9101,
		IncrSyncHTTPListenPort: 9100,
		PromHTTPListenPort:     9102,
	}

	order := make(chan string, 8)
	leaderEntered := make(chan struct{})
	releaseLeader := make(chan struct{})
	done := make(chan struct{})

	prometheusInitHttpApiFunc = func(port int, name string) {
		assert.Equal(t, 9102, port, "should be equal")
		assert.Equal(t, "rs-startup", name, "should be equal")
		order <- "prometheus-init"
	}
	startPrometheusHttpApiFunc = func() {
		order <- "prometheus-listen"
	}
	selectLeaderFunc = func() {
		order <- "select-leader"
		close(leaderEntered)
		<-releaseLeader
	}
	fullSyncInitHttpApiFunc = func(int) {
		order <- "full-sync-init"
	}
	incrSyncInitHttpApiFunc = func(int) {
		order <- "incr-sync-init"
	}
	registerConfHttpApiFunc = func() {
		order <- "register-conf"
	}

	go func() {
		initStartupHTTPApis()
		close(done)
	}()

	waitClosed(t, leaderEntered, "leader election was not reached")
	assert.Equal(t, "prometheus-init", readOrder(t, order), "should be equal")
	assert.Equal(t, "prometheus-listen", readOrder(t, order), "should be equal")
	assert.Equal(t, "select-leader", readOrder(t, order), "should be equal")

	select {
	case item := <-order:
		t.Fatalf("unexpected startup step before leader election released: %s", item)
	default:
	}

	close(releaseLeader)
	waitClosed(t, done, "startup HTTP initialization did not finish")
	assert.Equal(t, "full-sync-init", readOrder(t, order), "should be equal")
	assert.Equal(t, "incr-sync-init", readOrder(t, order), "should be equal")
	assert.Equal(t, "register-conf", readOrder(t, order), "should be equal")
}

func TestSelectLeaderUseConfiguredElectionID(t *testing.T) {
	origin := conf.Options
	originUseElectionObjectId := useElectionObjectIdFunc
	originBecomeMaster := becomeMasterFunc
	originWaitMasterPromotion := waitMasterPromotionFunc
	defer func() {
		conf.Options = origin
		useElectionObjectIdFunc = originUseElectionObjectId
		becomeMasterFunc = originBecomeMaster
		waitMasterPromotionFunc = originWaitMasterPromotion
	}()

	expected, err := primitive.ObjectIDFromHex("66e3ffdd9df1af8e2308fb53")
	assert.NoError(t, err, "should be equal")

	var actual primitive.ObjectID
	becomeMasterCall := make(chan struct {
		uri string
		db  string
	}, 1)
	useElectionObjectIdFunc = func(objectID primitive.ObjectID) {
		actual = objectID
	}
	becomeMasterFunc = func(uri string, db string) error {
		becomeMasterCall <- struct {
			uri string
			db  string
		}{uri: uri, db: db}
		return nil
	}
	waitMasterPromotionFunc = func() {}

	conf.Options = conf.Configuration{
		MasterQuorum:           true,
		MasterQuorumElectionID: expected.Hex(),
		CheckpointStorage:      utils.VarCheckpointStorageDatabase,
		CheckpointStorageUrl:   "mongodb://checkpoint",
	}

	selectLeader()

	assert.Equal(t, expected, actual, "should be equal")
	call := readBecomeMasterCall(t, becomeMasterCall)
	assert.Equal(t, "mongodb://checkpoint", call.uri, "should be equal")
	assert.Equal(t, utils.VarCheckpointStorageDbReplicaDefault, call.db, "should be equal")
}

func waitClosed(t *testing.T, ch <-chan struct{}, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal(msg)
	}
}

func readOrder(t *testing.T, order <-chan string) string {
	t.Helper()
	select {
	case item := <-order:
		return item
	case <-time.After(time.Second):
		t.Fatal("startup step was not recorded")
	}
	return ""
}

func readBecomeMasterCall(t *testing.T, calls <-chan struct {
	uri string
	db  string
}) struct {
	uri string
	db  string
} {
	t.Helper()
	select {
	case call := <-calls:
		return call
	case <-time.After(time.Second):
		t.Fatal("BecomeMaster was not called")
	}
	return struct {
		uri string
		db  string
	}{}
}
