package quorum

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	l "github.com/alibaba/MongoShake/v2/pkg/log"
)

const (
	HeartBeatPeriodInSeconds  = 5
	HeartBeatTimeoutInSeconds = HeartBeatPeriodInSeconds * 3
	HeartBeatPeriod           = time.Second * HeartBeatPeriodInSeconds
)

const (
	PromoteMaster = 0
	DescendMaster = 1
)

// become or lost master status notifier
var MasterPromotionNotifier chan bool

var electionObjectId primitive.ObjectID
var master bool

func init() {
	MasterPromotionNotifier = make(chan bool, 1)
}

func masterChanged(status int) {
	if status == PromoteMaster {
		if len(MasterPromotionNotifier) == 0 {
			MasterPromotionNotifier <- true
			l.Logger.Infof("become the master and notify waiter")
		}
		master = true
	} else {
		master = false
	}
}

func IsMaster() bool {
	return master
}

func AlwaysMaster() {
	master = true
}

func UseElectionObjectId(electionId primitive.ObjectID) {
	electionObjectId = electionId
}

type ElectionEntry struct {
	ObjectId  primitive.ObjectID `bson:"_id"`
	PID       int                `bson:"pid"`
	Host      string             `bson:"host"`
	Heartbeat int64              `bson:"heartbeat"`
}

const (
	QUORUM_COLLECTION string = "election"
)

const (
	STATUS_LOOKASIDE int = iota
	STATUS_COMPETE_MASTER
	STATUS_MASTER
	STATUS_FOLLOW
	STATUS_SESSION_CLOSE
)

func BecomeMaster(uri string, db string) error {
	return BecomeMasterWithContext(context.Background(), uri, db)
}

// BecomeMasterWithContext is the cancellable election loop. ctx covers the
// waits, retries, Mongo reads/writes and connection init; on cancellation it
// closes the connection it owns and returns without further role callbacks or
// election writes. The election policy is identical to the legacy entry point
// (#1052 HA P1, O1=a test isolation). Tests must join this function's return
// before restoring global master/election state.
func BecomeMasterWithContext(ctx context.Context, uri string, db string) error {
	retry := 30
	for retry != 0 {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		conn, err := makeSessionWithContext(ctx, uri)
		if err == nil {
			// keepQuorum runs until cancellation or a session-close retry signal;
			// a transient read failure must rebuild the session (retry--), NOT
			// terminate the election loop (baseline break Keep semantics).
			_ = keepQuorum(ctx, conn, db)
			conn.Close() // close the connection this entry owns
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		retry--
	}
	return fmt.Errorf("unreachable master election mongo %s", uri)
}

// errSessionClose signals the outer loop to rebuild the session and retry,
// mirroring the legacy `break Keep` path. It is never a success exit.
var errSessionClose = fmt.Errorf("quorum session close, retry with a fresh session")

// keepQuorum runs the election state machine on an established session. It
// returns nil on a clean session-close retry signal, or ctx.Err() on cancel.
func keepQuorum(ctx context.Context, conn *utils.MongoCommunityConn, db string) error {
	masterCollection := conn.Client.Database(db).Collection(QUORUM_COLLECTION)
	status := STATUS_LOOKASIDE
	entry := new(ElectionEntry)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if status == STATUS_LOOKASIDE || status == STATUS_MASTER {
			if !waitCtx(ctx, HeartBeatPeriod) {
				return ctx.Err()
			}
		}
		switch status {
		case STATUS_LOOKASIDE:
			err := masterCollection.FindOne(ctx, bson.M{"_id": electionObjectId}).Decode(entry)
			switch err {
			case nil:
				if entry.Host == getNetAddr() && entry.PID == os.Getpid() {
					status = STATUS_MASTER
				} else {
					status = STATUS_FOLLOW
				}
			case mongo.ErrNoDocuments:
				l.Logger.Debugf("No master node found. we elect myself")
				status = STATUS_COMPETE_MASTER
			default:
				l.Logger.Warnf("Fetch master election info %s failed. %v", electionObjectId, err)
				status = STATUS_SESSION_CLOSE
			}
		case STATUS_MASTER:
			if _, err := masterCollection.UpdateOne(ctx,
				bson.D{{"_id", electionObjectId}, {"pid", os.Getpid()}},
				bson.M{"$set": promotion()}); err == nil {
				if ctx.Err() == nil {
					masterChanged(PromoteMaster)
				}
			} else {
				l.Logger.Warnf("Update master election info failed. %v", err)
				status = STATUS_LOOKASIDE
			}
		case STATUS_COMPETE_MASTER:
			competeMasterCtx(ctx, masterCollection)
			status = STATUS_LOOKASIDE
		case STATUS_FOLLOW:
			if master && ctx.Err() == nil {
				masterChanged(DescendMaster)
			}
			heartbeat := entry.Heartbeat
			if time.Now().Unix()-heartbeat >= int64(HeartBeatTimeoutInSeconds) {
				masterCollection.UpdateOne(ctx,
					bson.D{{"_id", electionObjectId}}, bson.M{"$set": promotion()})
				l.Logger.Infof("Expired master found. compete to become master")
				if !waitCtx(ctx, time.Millisecond*time.Duration(rand.Uint32()%2500+1)) {
					return ctx.Err()
				}
			} else {
				l.Logger.Infof("Follow current master %v", entry)
			}
			status = STATUS_LOOKASIDE
		case STATUS_SESSION_CLOSE:
			return errSessionClose // outer loop rebuilds the session and retries
		}
	}
}

func waitCtx(ctx context.Context, duration time.Duration) bool {
	t := time.NewTimer(duration)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func competeMasterCtx(ctx context.Context, coll *mongo.Collection) bool {
	if _, err := coll.InsertOne(ctx, promotion()); err == nil {
		l.Logger.Infof("This node become master with election info %v", master)
		return true
	} else if utils.DuplicateKey(err) {
		l.Logger.Warnf("Another node is compete to master. we hold on a second")
	}
	return false
}

// makeSessionWithContext builds a primary session whose connect and ping honor
// ctx, so cancellation during connection init is bounded (#1052 O1=a contract 1).
func makeSessionWithContext(ctx context.Context, uri string) (*utils.MongoCommunityConn, error) {
	encoded, err := utils.EncodeMongoURI(uri)
	if err != nil {
		return nil, err
	}
	opts := options.Client().ApplyURI(encoded).SetReadPreference(readpref.Primary()).
		SetConnectTimeout(20 * time.Minute)
	if conf.Options.MongoSslRootCaFile != "" {
		tlsCfg, err := utils.NewChainVerifyingTLSConfig(conf.Options.MongoSslRootCaFile)
		if err != nil {
			return nil, err
		}
		opts.SetTLSConfig(tlsCfg)
	}
	client, err := mongo.NewClient(opts)
	if err != nil {
		return nil, err
	}
	if err := client.Connect(ctx); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, err
	}
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, err
	}
	return &utils.MongoCommunityConn{Client: client, URL: uri}, nil
}

func promotion() bson.D {
	return bson.D{
		{"_id", electionObjectId},
		{"pid", os.Getpid()},
		{"host", getNetAddr()},
		{"heartbeat", time.Now().Unix()},
	}
}

func getNetAddr() string {
	addressArray, err := net.InterfaceAddrs()
	if err != nil {
		l.Logger.Criticalf("Get network interface address failed. %v", err)
		return "error"
	}

	for _, ip := range addressArray {
		if ipnet, ok := ip.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}

	return "error"
}
