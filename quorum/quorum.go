package quorum

import (
	"context"
	"fmt"
	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"math/rand"
	"net"
	"os"
	"time"

	utils "github.com/alibaba/MongoShake/v2/common"

	LOG "github.com/vinllen/log4go"
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
			LOG.Info("become the master and notify waiter")
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

	retry := 30
	for retry != 0 {
		if conn, err := makeSession(uri); err == nil {
			masterCollection := conn.Client.Database(db).Collection(QUORUM_COLLECTION)

			status := STATUS_LOOKASIDE
			entry := new(ElectionEntry)

			// keep quorum
		Keep:
			for {
				if status == STATUS_LOOKASIDE || status == STATUS_MASTER {
					wait(HeartBeatPeriod)
				}

				switch status {
				case STATUS_LOOKASIDE:
					// take from database firstly
					err = masterCollection.FindOne(context.Background(),
						bson.M{"_id": electionObjectId}).Decode(entry)

					switch err {
					case nil:
						if entry.Host == getNetAddr() && entry.PID == os.Getpid() {
							// master is me. just update heartbeat
							status = STATUS_MASTER
						} else {
							status = STATUS_FOLLOW
						}
					case mongo.ErrNoDocuments:
						LOG.Debug("No master node found. we elect myself")
						status = STATUS_COMPETE_MASTER
					default:
						LOG.Warn("Fetch master election info %s failed. %v", electionObjectId, err)
						status = STATUS_SESSION_CLOSE
					}

				case STATUS_MASTER:
					if _, err := masterCollection.UpdateOne(context.Background(),
						bson.D{{"_id", electionObjectId}, {"pid", os.Getpid()}},
						bson.M{"$set": promotion()}); err == nil {
						masterChanged(PromoteMaster)
					} else {
						LOG.Warn("Update master election info failed. %v", err)
						status = STATUS_LOOKASIDE
					}

				case STATUS_COMPETE_MASTER:
					// there is no one master
					competeMaster(masterCollection)
					status = STATUS_LOOKASIDE

				case STATUS_FOLLOW:
					if master {
						masterChanged(DescendMaster)
					}

					// there has been already another master. check its heartbeat
					heartbeat := entry.Heartbeat
					if time.Now().Unix()-heartbeat >= int64(HeartBeatTimeoutInSeconds) {
						// I wanna be the master. DON'T care about the success of update
						masterCollection.UpdateOne(context.Background(),
							bson.D{{"_id", electionObjectId}},bson.M{"$set": promotion()})
						LOG.Info("Expired master found. compete to become master")
						// wait random time. just disrupt others compete
						wait(time.Millisecond * time.Duration(rand.Uint32()%2500+1))
					} else {
						// follow current master
						LOG.Info("Follow current master %v", entry)
					}
					status = STATUS_LOOKASIDE

				case STATUS_SESSION_CLOSE:
					conn.Close()
					break Keep
				}
			}
		}

		retry--
	}

	return fmt.Errorf("unreachable master election mongo %s", uri)
}

func competeMaster(coll *mongo.Collection) bool {
	if _, err := coll.InsertOne(context.Background(), promotion()); err == nil {
		LOG.Info("This node become master with election info %v", master)
		return true
	} else if utils.DuplicateKey(err) {
		LOG.Warn("Another node is compete to master. we hold on a second")
	}
	return false
}

func makeSession(uri string) (*utils.MongoCommunityConn, error) {
	if conn, err := utils.NewMongoCommunityConn(uri, utils.VarMongoConnectModePrimary, true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault, conf.Options.MongoSslRootCaFile); err == nil {
		return conn, nil
	} else {
		return nil, err
	}
}

func promotion() bson.D {
	return bson.D{
		{"_id", electionObjectId},
		{"pid", os.Getpid()},
		{"host", getNetAddr()},
		{"heartbeat", time.Now().Unix()},
	}
}

func wait(duration time.Duration) {
	time.Sleep(duration)
}

func getNetAddr() string {
	addressArray, err := net.InterfaceAddrs()
	if err != nil {
		LOG.Critical("Get network interface address failed. %v", err)
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
