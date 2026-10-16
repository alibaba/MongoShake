package collector

import (
	"fmt"
	"testing"
	"time"

	"github.com/alibaba/MongoShake/v2/collector/ckpt"
	utils "github.com/alibaba/MongoShake/v2/common"

	"github.com/stretchr/testify/assert"
)

// When the persisted checkpoint is pinned below an in-flight transaction's
// start ts, the barrier wait must still complete once all workers have acked
// newestTs. Otherwise the pin and the barrier deadlock each other.
func TestCheckCheckpointUpdateUsesWorkerAckWhenCheckpointPinned(t *testing.T) {
	utils.InitialLogger("", "", "info", true, 1)

	origGetFunc := barrierCkptGetFunc
	origFlushFunc := barrierCkptFlushFunc
	origInterval := DDLCheckpointInterval
	defer func() {
		barrierCkptGetFunc = origGetFunc
		barrierCkptFlushFunc = origFlushFunc
		DDLCheckpointInterval = origInterval
	}()
	DDLCheckpointInterval = 1

	pinnedTs := utils.TimeStampToInt64(utils.TimeToTimestamp(99))
	newestTs := utils.TimeStampToInt64(utils.TimeToTimestamp(101))

	// remote checkpoint is pinned at 99 (< newestTs=101) and will never move
	barrierCkptGetFunc = func(sync *OplogSyncer) (*ckpt.CheckpointContext, error) {
		return &ckpt.CheckpointContext{Timestamp: pinnedTs}, nil
	}
	barrierCkptFlushFunc = func(sync *OplogSyncer) bool { return false }

	// but every worker has acked newestTs already
	worker := new(Worker)
	worker.ack = newestTs
	worker.unack = newestTs
	syncer := &OplogSyncer{
		Replset: "test-pinned-barrier",
		batcher: &Batcher{workerGroup: []*Worker{worker}},
	}

	done := make(chan bool, 1)
	go func() {
		done <- syncer.checkCheckpointUpdate(true, newestTs)
	}()

	select {
	case result := <-done:
		assert.Equal(t, true, result,
			"barrier must pass on worker-ack progress even if persisted checkpoint is pinned lower")
	case <-time.After(3 * time.Second):
		fmt.Println("checkCheckpointUpdate deadlocked with pinned checkpoint")
		t.Fatal("checkCheckpointUpdate deadlocked: it only polls the persisted checkpoint, " +
			"which is pinned below newestTs by an in-flight transaction")
	}
}
