package utils

import (
	"fmt"
	"math/rand"
	_ "net/http/pprof" // for profiling
	"os"
	"path/filepath"
	"strconv"
	"time"

	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo/bson"
	"reflect"
	"path"
)

func YieldInMs(n int64) {
	time.Sleep(time.Millisecond * time.Duration(n))
}

type ElapsedTask struct {
	// timer trigger
	TimeLimit int64
	// batch trigger
	BatchLimit int64

	stone        int64
	triggerTimes int64
}

func NewThresholder(timeLimit, batchLimit int64) *ElapsedTask {
	return &ElapsedTask{TimeLimit: timeLimit, BatchLimit: batchLimit, stone: time.Now().Unix(), triggerTimes: 0}
}

func (thresholder *ElapsedTask) Reset() {
	thresholder.stone = time.Now().Unix()
	thresholder.triggerTimes = 0
}

func (thresholder *ElapsedTask) Triiger() bool {
	thresholder.triggerTimes++
	current := time.Now().Unix()

	if current > (thresholder.stone + thresholder.TimeLimit) {
		return true
	}

	if thresholder.triggerTimes >= thresholder.BatchLimit {
		return true
	}

	return false
}

type Int64Slice []int64

func (p Int64Slice) Len() int {
	return len(p)
}
func (p Int64Slice) Less(i, j int) bool {
	return p[i] < p[j]
}
func (p Int64Slice) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func TimestampToInt64(ts bson.MongoTimestamp) int64 {
	return int64(ts)
}

func TimestampToString(ts int64) string {
	return time.Unix(ts, 0).Format(TimeFormat)
}

func ExtractMongoTimestamp(ts interface{}) int64 {
	switch src := ts.(type) {
	case bson.MongoTimestamp:
		return int64(src) >> 32
	case int64:
		return src >> 32
	}

	return 0
}

func ExtractMongoTimestampCounter(ts interface{}) int64 {
	switch src := ts.(type) {
	case bson.MongoTimestamp:
		return int64(src) & Int32max
	case int64:
		return src & Int32max
	}

	return 0
}

func ExtractTimestampForLog(ts interface{}) string {
	return fmt.Sprintf("%v[%v, %v]", ts, ExtractMongoTimestamp(ts), ExtractMongoTimestampCounter(ts))
}

func Int64ToString(v int64) string {
	return strconv.FormatInt(v, 10)
}

func ParseIntFromInterface(input interface{}) (int64, error) {
	switch src := input.(type) {
	case int:
		return int64(src), nil
	case int8:
		return int64(src), nil
	case int16:
		return int64(src), nil
	case int32:
		return int64(src), nil
	case int64:
		return src, nil
	case uint:
		return int64(src), nil
	case uint8:
		return int64(src), nil
	case uint16:
		return int64(src), nil
	case uint32:
		return int64(src), nil
	case uint64:
		return int64(src), nil
	case string:
		v, err := strconv.Atoi(src)
		return int64(v), err
	default:
		return 0, fmt.Errorf("unknown type[%v] with input[%v]", reflect.TypeOf(src), src)
	}

	panic("can't see me!")
}

// one writer and multi readers
type OpsCounter struct {
	counter [OpsMax + 1]uint64
}

const (
	OpsMax = 'z' - 'A'
)

func (opsCounter *OpsCounter) Add(char byte, v uint64) {
	if 0 <= char-'A' && char-'A' <= OpsMax {
		opsCounter.counter[char-'A'] += v
	}
}

func (opsCounter *OpsCounter) Map() map[string]uint64 {
	toMap := make(map[string]uint64)
	for index, v := range opsCounter.counter {
		if v != 0 {
			toMap[string('A'+index)] = v
		}
	}
	return toMap
}

func HasDuplicated(slice []string) bool {
	unique := map[string]int{}
	for _, s := range slice {
		currentSize := len(unique)
		unique[s] = 0
		if currentSize+1 != len(unique) {
			return true
		}
	}
	return false
}

func MayBeRandom(port int) int {
	// random a port number
	if port == 0 {
		// non-negative
		nr := rand.Intn(10000)
		if nr <= 1024 {
			nr += 1024
		}
		return nr
	}
	return port
}

func Mkdirs(dirs ...string) error {
	for _, dir := range dirs {
		if _, err := os.Stat(dir); err != nil && os.IsNotExist(err) {
			if err = os.Mkdir(dir, 0777); err != nil {
				return err
			}
		}
	}
	return nil
}

func WritePidById(dir, id string) bool {
	if len(dir) == 0 {
		dir, _ = os.Getwd()
	} else if dir[0] != '/' {
		// relative path
		baseDir, _ := os.Getwd()
		dir = path.Join(baseDir, dir)
	}

	pidfile := filepath.Join(dir, id) + ".pid"
	if err := WritePid(pidfile); err != nil {
		LOG.Critical("Process write pid and lock file failed : %v", err)
		return false
	}
	return true
}

func Welcome() {
	welcome :=
		`______________________________
\                             \           _         ______ |
 \                             \        /   \___-=O'/|O'/__|
  \  MongoShake, Here we go !!  \_______\          / | /    )
  /                             /        '/-==__ _/__|/__=-|  -GM
 /        Alibaba Cloud        /         *             \ | |
/                             /                        (o)
------------------------------
`
	startMsg := "if you have any problem, please visit https://github.com/alibaba/MongoShake/wiki/FAQ"
	LOG.Warn(fmt.Sprintf("\n%s\n%s\n", welcome, startMsg))
}

func Goodbye() {
	goodbye := `
                ##### | #####
Oh we finish ? # _ _ #|# _ _ #
               #      |      #
         |       ############
                     # #
  |                  # #
                    #   #
         |     |    #   #      |        |
  |  |             #     #               |
         | |   |   # .-. #         |
                   #( O )#    |    |     |
  |  ################. .###############  |
   ##  _ _|____|     ###     |_ __| _  ##
  #  |                                |  #
  #  |    |    |    |   |    |    |   |  #
   ######################################
                   #     #
                    #####
`

	LOG.Warn(goodbye)
}
