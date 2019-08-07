package collector

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func mockCheckpointSyncer(workerNum int) *OplogSyncer {
	workers := make([]*Worker, workerNum)
	for i := 0; i < workerNum; i++ {
		workers[i] = new(Worker)
	}
	return &OplogSyncer{
		batcher: &Batcher{
			workerGroup: workers,
		},
	}
}

func TestCalculateWorkerLowestCheckpoint(t *testing.T) {
	// test calculateWorkerLowestCheckpoint

	var (
		nr         int
		checkpoint int64
		err        error
	)

	// do nothing, return 0
	{
		fmt.Printf("TestCalculateWorkerLowestCheckpoint case %d.\n", nr)
		nr++

		syncer := mockCheckpointSyncer(8)
		checkpoint, err = syncer.calculateWorkerLowestCheckpoint()
		assert.Equal(t, "no candidates ack values found", err.Error(), "should be equal")
		assert.Equal(t, int64(0), checkpoint, "should be equal")
	}

	// one of the workers return ack
	{
		fmt.Printf("TestCalculateWorkerLowestCheckpoint case %d.\n", nr)
		nr++

		syncer := mockCheckpointSyncer(8)
		worker3 := syncer.batcher.workerGroup[3]
		worker3.ack = 10
		worker3.unack = 10
		checkpoint, err = syncer.calculateWorkerLowestCheckpoint()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, int64(10), checkpoint, "should be equal")
	}

	// not all ack, return the smallest candidate
	{
		fmt.Printf("TestCalculateWorkerLowestCheckpoint case %d.\n", nr)
		nr++

		syncer := mockCheckpointSyncer(8)
		worker3 := syncer.batcher.workerGroup[3]
		worker3.ack = 10
		worker3.unack = 10
		worker4 := syncer.batcher.workerGroup[4]
		worker4.ack = 20
		worker4.unack = 30
		checkpoint, err = syncer.calculateWorkerLowestCheckpoint()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, int64(20), checkpoint, "should be equal")
	}

	// not all ack, return the smallest candidate
	{
		fmt.Printf("TestCalculateWorkerLowestCheckpoint case %d.\n", nr)
		nr++

		syncer := mockCheckpointSyncer(8)
		worker3 := syncer.batcher.workerGroup[3]
		worker3.ack = 0
		worker3.unack = 10
		checkpoint, err = syncer.calculateWorkerLowestCheckpoint()
		assert.Equal(t, "smallest candidates is zero", err.Error(), "should be equal")
		assert.Equal(t, int64(0), checkpoint, "should be equal")
	}

	// not all ack, return the smallest candidate
	{
		fmt.Printf("TestCalculateWorkerLowestCheckpoint case %d.\n", nr)
		nr++

		syncer := mockCheckpointSyncer(8)
		worker3 := syncer.batcher.workerGroup[3]
		worker3.ack = 5
		worker3.unack = 10
		checkpoint, err = syncer.calculateWorkerLowestCheckpoint()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, int64(5), checkpoint, "should be equal")
	}

	// not all ack again, unack candidate is smaller than ack, return the smallest candidate
	{
		fmt.Printf("TestCalculateWorkerLowestCheckpoint case %d.\n", nr)
		nr++

		syncer := mockCheckpointSyncer(8)
		worker3 := syncer.batcher.workerGroup[3]
		worker3.ack = 10
		worker3.unack = 10
		worker4 := syncer.batcher.workerGroup[4]
		worker4.ack = 20
		worker4.unack = 30
		worker5 := syncer.batcher.workerGroup[5]
		worker5.ack = 40
		worker5.unack = 40
		checkpoint, err = syncer.calculateWorkerLowestCheckpoint()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, int64(20), checkpoint, "should be equal")
	}

	// all ack
	{
		fmt.Printf("TestCalculateWorkerLowestCheckpoint case %d.\n", nr)
		nr++

		syncer := mockCheckpointSyncer(4)
		worker3 := syncer.batcher.workerGroup[3]
		worker3.ack = 10
		worker3.unack = 10
		worker2 := syncer.batcher.workerGroup[2]
		worker2.ack = 20
		worker2.unack = 20
		worker1 := syncer.batcher.workerGroup[1]
		worker1.ack = 40
		worker1.unack = 40
		checkpoint, err = syncer.calculateWorkerLowestCheckpoint()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, int64(40), checkpoint, "should be equal")
	}

	// ack less than unack, unack != 0
	{
		fmt.Printf("TestCalculateWorkerLowestCheckpoint case %d.\n", nr)
		nr++

		syncer := mockCheckpointSyncer(4)
		worker3 := syncer.batcher.workerGroup[3]
		worker3.ack = 10
		worker3.unack = 10
		worker2 := syncer.batcher.workerGroup[2]
		worker2.ack = 20
		worker2.unack = 20
		worker1 := syncer.batcher.workerGroup[1]
		worker1.ack = 40
		worker1.unack = 30
		checkpoint, err = syncer.calculateWorkerLowestCheckpoint()
		assert.Equal(t, true, err != nil, "should be equal")
		assert.Equal(t, int64(0), checkpoint, "should be equal")
	}

	// ack less than unack, unack == 0, has candidate
	{
		fmt.Printf("TestCalculateWorkerLowestCheckpoint case %d.\n", nr)
		nr++

		syncer := mockCheckpointSyncer(4)
		worker3 := syncer.batcher.workerGroup[3]
		worker3.ack = 10
		worker3.unack = 10
		worker2 := syncer.batcher.workerGroup[2]
		worker2.ack = 20
		worker2.unack = 30
		worker1 := syncer.batcher.workerGroup[1]
		worker1.ack = 40
		worker1.unack = 0
		checkpoint, err = syncer.calculateWorkerLowestCheckpoint()
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, int64(20), checkpoint, "should be equal")
	}

	// ack less than unack, unack == 0, no candidate
	{
		fmt.Printf("TestCalculateWorkerLowestCheckpoint case %d.\n", nr)
		nr++

		syncer := mockCheckpointSyncer(4)
		worker3 := syncer.batcher.workerGroup[3]
		worker3.ack = 10
		worker3.unack = 10
		worker2 := syncer.batcher.workerGroup[2]
		worker2.ack = 20
		worker2.unack = 20
		worker1 := syncer.batcher.workerGroup[1]
		worker1.ack = 40
		worker1.unack = 0
		checkpoint, err = syncer.calculateWorkerLowestCheckpoint()
		assert.Equal(t, "no candidates ack values found", err.Error(), "should be equal")
		assert.Equal(t, int64(0), checkpoint, "should be equal")
	}
}
