//go:build integration

// Regression for #1043: the startup oplog-window probe must consult the node
// class that mongo_connect_mode selects.
//
// GetAllTimestamp used to hardcode secondaryPreferred, so the "can I still run
// incr sync from this checkpoint?" decision read a SECONDARY's oplog window even
// when mongo_connect_mode=primary. A secondary whose oplog starts later than the
// checkpoint (rebuilt / resynced member, smaller oplog) then made the collector
// refuse to start with "start time illegal, can't run incr sync", while the
// primary could have served that checkpoint fine.
//
// The fixture builds a real 3-node replica set and resyncs both secondaries one
// at a time (removing two members at once would cost the primary its majority),
// so the primary's oplog still covers the checkpoint and neither secondary's
// does. Both directions are asserted: primary must see the old window,
// secondaryPreferred must see the new one. The second assertion is the fixture
// self-check -- without it a failed resync would make the first one pass vacuously.
//
// Two layers are guarded: the helper's mode compliance (GetAllTimestamp directly)
// and the caller wiring (a real collector run per mode, asserting it proceeds to
// incr sync under primary and refuses under secondaryPreferred). The helper-only
// layer would stay green if a coordinator call site went back to a hardcoded
// constant, which is the shape the original defect had.
//
// Run with:
//
//	go test -tags integration ./integration -run TestConnectModeWindowProbe -v -timeout 10m
//
// Requires a working local docker; skips otherwise. Containers are named
// mshake-cm-n1..n3 on ports 27081-27083 and removed on cleanup.
package integration

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"testing"
	"time"

	utils "github.com/alibaba/MongoShake/v2/common"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	cmwReplSet   = "cmwin"
	cmwDB        = "cmwin_it"
	cmwColl      = "c"
	cmwImage     = "mongo:7.0"
	cmwSetupTO   = 120 * time.Second
	cmwDecideTO  = 180 * time.Second
	cmwMarkerID  = "checkpoint-marker"
	cmwLogFile   = "collector.log"
	cmwIncrLog   = "sync mode run incr"
	cmwRejectMsg = "start time illegal"
)

var cmwPorts = []int{27081, 27082, 27083}

func cmwName(i int) string { return fmt.Sprintf("mshake-cm-n%d", i+1) }

func cmwDirectURL(i int) string {
	return fmt.Sprintf("mongodb://127.0.0.1:%d/?directConnection=true", cmwPorts[i])
}

func cmwSeedURL() string {
	return fmt.Sprintf("mongodb://127.0.0.1:%d,127.0.0.1:%d,127.0.0.1:%d/?replicaSet=%s",
		cmwPorts[0], cmwPorts[1], cmwPorts[2], cmwReplSet)
}

func cmwDocker(t *testing.T, args ...string) {
	t.Helper()
	cmd := exec.Command("docker", args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("docker %v failed: %v\n%s", args, err, out)
	}
}

func cmwDial(t *testing.T, url string) *mongo.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(url))
	if err != nil {
		t.Fatalf("dial %s: %v", url, err)
	}
	return client
}

func cmwStartNode(t *testing.T, i int) {
	t.Helper()
	_ = exec.Command("docker", "rm", "-fv", cmwName(i)).Run()
	cmwDocker(t, "run", "-d", "--name", cmwName(i), "--network", "host", cmwImage,
		"--replSet", cmwReplSet, "--bind_ip", "127.0.0.1", "--port", fmt.Sprint(cmwPorts[i]))
}

// cmwWaitReachable blocks until the member accepts a direct connection, so
// replSetInitiate does not race the container's startup.
func cmwWaitReachable(t *testing.T, i int) {
	t.Helper()
	deadline := time.Now().Add(cmwSetupTO)
	var last error
	for time.Now().Before(deadline) {
		client, err := mongo.Connect(context.Background(),
			options.Client().ApplyURI(cmwDirectURL(i)).SetDirect(true))
		if err == nil {
			err = client.Ping(context.Background(), nil)
			_ = client.Disconnect(context.Background())
			if err == nil {
				return
			}
		}
		last = err
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("node %s never became reachable: %v", cmwName(i), last)
}

// cmwWaitRole polls a node directly until it reports the wanted role.
func cmwWaitRole(t *testing.T, i int, wantPrimary bool) {
	t.Helper()
	deadline := time.Now().Add(cmwSetupTO)
	var last string
	for time.Now().Before(deadline) {
		client, err := mongo.Connect(context.Background(),
			options.Client().ApplyURI(cmwDirectURL(i)).SetDirect(true))
		if err == nil {
			var hello bson.M
			err = client.Database("admin").RunCommand(context.Background(),
				bson.D{{"hello", 1}}).Decode(&hello)
			_ = client.Disconnect(context.Background())
			if err == nil {
				isPrimary, _ := hello["isWritablePrimary"].(bool)
				secondary, _ := hello["secondary"].(bool)
				last = fmt.Sprintf("isWritablePrimary=%v secondary=%v", isPrimary, secondary)
				if isPrimary == wantPrimary && secondary == !wantPrimary {
					return
				}
			} else {
				last = err.Error()
			}
		} else {
			last = err.Error()
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("node %s never reached primary=%v, last: %s", cmwName(i), wantPrimary, last)
}

// cmwOldest reads the first oplog entry of one specific member, bypassing the
// code under test so the fixture cannot be validated by its own output.
func cmwOldest(t *testing.T, i int) int64 {
	t.Helper()
	client := cmwDial(t, cmwDirectURL(i))
	defer func() { _ = client.Disconnect(context.Background()) }()
	var doc struct {
		Ts primitive.Timestamp `bson:"ts"`
	}
	opts := options.FindOne().SetSort(bson.D{{"$natural", 1}})
	err := client.Database("local").Collection("oplog.rs").
		FindOne(context.Background(), bson.M{}, opts).Decode(&doc)
	if err != nil {
		t.Fatalf("read oldest oplog of %s: %v", cmwName(i), err)
	}
	return utils.TimeStampToInt64(doc.Ts)
}

func TestConnectModeWindowProbe(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available")
	}
	if out, err := exec.Command("docker", "info").CombinedOutput(); err != nil {
		t.Skipf("docker daemon not usable: %v\n%s", err, out)
	}

	for i := range cmwPorts {
		i := i
		t.Cleanup(func() { _ = exec.Command("docker", "rm", "-fv", cmwName(i)).Run() })
		cmwStartNode(t, i)
	}
	for i := range cmwPorts {
		cmwWaitReachable(t, i)
	}

	primary := cmwDial(t, cmwDirectURL(0))
	defer func() { _ = primary.Disconnect(context.Background()) }()
	members := make([]bson.M, 0, len(cmwPorts))
	for i, p := range cmwPorts {
		// Pin the primary to n1: with equal priority any member can win the
		// first election, and the fixture reads/writes n1 directly.
		priority := 1
		if i == 0 {
			priority = 10
		}
		members = append(members, bson.M{
			"_id": i, "host": fmt.Sprintf("127.0.0.1:%d", p), "priority": priority,
		})
	}
	err := primary.Database("admin").RunCommand(context.Background(), bson.D{
		{"replSetInitiate", bson.M{"_id": cmwReplSet, "members": members}},
	}).Err()
	if err != nil {
		t.Fatalf("replSetInitiate: %v", err)
	}
	cmwWaitRole(t, 0, true)

	coll := primary.Database(cmwDB).Collection(cmwColl)
	if _, err := coll.InsertOne(context.Background(), bson.M{"_id": cmwMarkerID}); err != nil {
		t.Fatalf("insert marker: %v", err)
	}
	for i := 0; i < 300; i++ {
		if _, err := coll.InsertOne(context.Background(), bson.M{"_id": i, "pad": "x"}); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	var markerDoc struct {
		Ts primitive.Timestamp `bson:"ts"`
	}
	err = primary.Database("local").Collection("oplog.rs").FindOne(context.Background(),
		bson.M{"ns": cmwDB + "." + cmwColl, "o._id": cmwMarkerID}).Decode(&markerDoc)
	if err != nil {
		t.Fatalf("locate marker in primary oplog: %v", err)
	}
	markerTs := utils.TimeStampToInt64(markerDoc.Ts)
	primaryOldest := cmwOldest(t, 0)
	if primaryOldest >= markerTs {
		t.Fatalf("fixture invalid: primary oldest %v already past marker %v", primaryOldest, markerTs)
	}

	// Resync one secondary at a time: dropping two of three members at once
	// would leave the primary without a majority and it would step down.
	for _, i := range []int{2, 1} {
		cmwStartNode(t, i)
		cmwWaitRole(t, i, false)
		deadline := time.Now().Add(cmwSetupTO)
		for {
			if oldest := cmwOldest(t, i); oldest > markerTs {
				break
			} else if time.Now().After(deadline) {
				t.Fatalf("resynced %s still covers the marker: oldest %v <= marker %v",
					cmwName(i), oldest, markerTs)
			}
			time.Sleep(1 * time.Second)
		}
	}
	cmwWaitRole(t, 0, true)

	sources := []*utils.MongoSource{{URL: cmwSeedURL(), ReplicaName: cmwReplSet}}

	// primary must be judged against the primary's window, which covers the marker.
	tsMapPrimary, _, _, _, _, err := utils.GetAllTimestamp(sources, utils.VarMongoConnectModePrimary, "")
	if err != nil {
		t.Fatalf("GetAllTimestamp(primary): %v", err)
	}
	gotPrimary := tsMapPrimary[cmwReplSet].Oldest
	if gotPrimary != primaryOldest {
		t.Fatalf("primary mode read the wrong node: oldest %v, want the primary's %v",
			gotPrimary, primaryOldest)
	}
	if gotPrimary >= markerTs {
		t.Fatalf("primary mode lost the checkpoint window: oldest %v >= marker %v", gotPrimary, markerTs)
	}

	// Fixture self-check: secondaryPreferred really does land on a resynced member.
	tsMapSecondary, _, _, _, _, err := utils.GetAllTimestamp(sources,
		utils.VarMongoConnectModeSecondaryPreferred, "")
	if err != nil {
		t.Fatalf("GetAllTimestamp(secondaryPreferred): %v", err)
	}
	if got := tsMapSecondary[cmwReplSet].Oldest; got <= markerTs {
		t.Fatalf("fixture invalid: secondaryPreferred oldest %v still covers marker %v", got, markerTs)
	}

	// The assertions above only guard the helper. These two guard the caller
	// wiring (coordinator -> conf.Options.MongoConnectMode) through the real
	// collector, which is the shape the original defect had: a call site passing
	// a hardcoded constant compiles fine and leaves the helper test green.
	if got := cmwCollectorDecision(t, utils.VarMongoConnectModePrimary, markerTs, 19700); got != cmwIncrLog {
		t.Fatalf("primary mode was rejected at startup: got %q, want %q", got, cmwIncrLog)
	}
	if got := cmwCollectorDecision(t, utils.VarMongoConnectModeSecondaryPreferred, markerTs, 19710); got != cmwRejectMsg {
		t.Fatalf("secondaryPreferred should have been rejected by the short secondary window: got %q, want %q",
			got, cmwRejectMsg)
	}
}

// cmwCollectorDecision runs the collector from the marker checkpoint and reports
// which way the startup window check decided: cmwIncrLog when it proceeds to
// incremental sync, cmwRejectMsg when it refuses.
//
// The rejection is printed before the file logger exists, so stdout/stderr must
// be captured -- reading only collector.log would show an empty log and look
// like a collector crash.
func cmwCollectorDecision(t *testing.T, mode string, markerTs int64, portBase int) string {
	t.Helper()
	logDir := t.TempDir()
	confPath := cmwWriteConf(t, mode, markerTs, portBase, logDir)

	root := csRepoRoot(t)
	var cmd *exec.Cmd
	if bin := os.Getenv("MSHAKE_COLLECTOR_BIN"); bin != "" {
		cmd = exec.Command(bin, "-conf="+confPath)
	} else {
		cmd = exec.Command("go", "run", "./cmd/collector", "-conf="+confPath)
	}
	cmd.Dir = root
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	outFile, err := os.Create(filepath.Join(logDir, "stdout.log"))
	if err != nil {
		t.Fatalf("create stdout capture: %v", err)
	}
	defer func() { _ = outFile.Close() }()
	cmd.Stdout = outFile
	cmd.Stderr = outFile
	if err := cmd.Start(); err != nil {
		t.Fatalf("start collector (%s): %v", mode, err)
	}
	defer func() {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
	}()

	deadline := time.Now().Add(cmwDecideTO)
	for time.Now().Before(deadline) {
		if cmwFileHas(filepath.Join(logDir, "stdout.log"), cmwRejectMsg) {
			t.Logf("collector(%s) decision: %s", mode, cmwRejectMsg)
			return cmwRejectMsg
		}
		if cmwFileHas(filepath.Join(logDir, cmwLogFile), cmwIncrLog) {
			t.Logf("collector(%s) decision: %s", mode, cmwIncrLog)
			return cmwIncrLog
		}
		time.Sleep(500 * time.Millisecond)
	}
	stdout, _ := os.ReadFile(filepath.Join(logDir, "stdout.log"))
	clog, _ := os.ReadFile(filepath.Join(logDir, cmwLogFile))
	t.Fatalf("collector (%s) never decided; stdout:\n%s\nlog:\n%s", mode, stdout, clog)
	return ""
}

func cmwFileHas(path, substr string) bool {
	data, err := os.ReadFile(path)
	return err == nil && strings.Contains(string(data), substr)
}

func cmwWriteConf(t *testing.T, mode string, markerTs int64, portBase int, logDir string) string {
	t.Helper()
	tpl, err := os.ReadFile(filepath.Join(csRepoRoot(t), "conf", "collector.conf"))
	if err != nil {
		t.Fatalf("read conf template: %v", err)
	}
	conf := string(tpl)
	set := func(key, value string) {
		re := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(key) + `\s*=.*$`)
		if re.MatchString(conf) {
			conf = re.ReplaceAllString(conf, key+" = "+value)
		} else {
			conf += "\n" + key + " = " + value + "\n"
		}
	}
	start := time.Unix(markerTs>>32, 0).UTC().Format("2006-01-02T15:04:05Z")
	set("id", "cmw_"+mode)
	set("sync_mode", "incr")
	set("mongo_urls", cmwSeedURL())
	set("mongo_connect_mode", mode)
	set("incr_sync.mongo_fetch_method", "oplog")
	set("tunnel", "direct")
	set("tunnel.address", cmwEnvURL("MSHAKE_TGT_URL", "mongodb://127.0.0.1:27019"))
	set("checkpoint.storage.url", cmwDirectURL(0))
	set("checkpoint.storage.collection", "ckpt_cmw_"+mode)
	set("checkpoint.start_position", start)
	set("full_sync.http_port", fmt.Sprint(portBase))
	set("incr_sync.http_port", fmt.Sprint(portBase+1))
	set("prom.http_port", fmt.Sprint(portBase+2))
	set("system_profile_port", fmt.Sprint(portBase+3))
	set("log.dir", logDir)
	set("log.file", cmwLogFile)
	set("log.flush", "true")

	path := filepath.Join(logDir, "collector.conf")
	if err := os.WriteFile(path, []byte(conf), 0o644); err != nil {
		t.Fatalf("write conf: %v", err)
	}
	return path
}

func cmwEnvURL(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
