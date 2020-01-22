package utils

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestDiskQueue(t *testing.T) {
	// Test DiskQueue
	dq := NewDiskQueue("diskqueue", "",
		10, 5,
		1<<30, 0, 1<<26,
		1000, 2*time.Second)
	for i := 0; i < 13; i++ {
		ib := byte(i)
		if err := dq.Put([]byte{ib, ib, ib, ib, ib, ib}); err != nil {
			return
		}
	}
	outputData := <-dq.ReadChan()
	dq.Next()
	fmt.Println(outputData)
	assert.Equal(t, len(outputData), 5)
	assert.Equal(t, dq.Depth(), int64(8))
	outputData = <-dq.ReadChan()
	dq.Next()
	fmt.Println(outputData)
	assert.Equal(t, len(outputData), 5)
	assert.Equal(t, dq.Depth(), int64(3))
	outputData = dq.ReadAll()
	fmt.Println(outputData)
	assert.Equal(t, len(outputData), 3)
	assert.Equal(t, dq.Depth(), int64(3))
	dq.Close()

	tdq := NewDiskQueue("diskqueue", "",
		10, 5,
		1<<30, 0, 1<<26,
		1000, 2*time.Second)
	data := tdq.GetLastWriteData()
	fmt.Println(data)
	outputData = tdq.ReadAll()
	fmt.Println(outputData)
	assert.Equal(t, len(outputData), 3)
	assert.Equal(t, tdq.Depth(), int64(3))
	tdq.Next()
	assert.Equal(t, tdq.Depth(), int64(0))
	tdq.Delete()
}
