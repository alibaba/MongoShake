package utils

import (
	"testing"
	"fmt"

	"github.com/stretchr/testify/assert"
)

func TestWritePidById(t *testing.T) {
	var nr int
	{
		fmt.Printf("TestWritePidById case %d.\n", nr)
		nr++

		ok := WritePidById("", "123")
		assert.Equal(t, true, ok, "should be equal")

		ok = WritePidById("", "123")
		assert.Equal(t, true, ok, "should be equal")
	}

	{
		fmt.Printf("TestWritePidById case %d.\n", nr)
		nr++

		// file not exists
		ok := WritePidById("xx", "123")
		assert.Equal(t, false, ok, "should be equal")

		ok = WritePidById("/tmp", "123")
		assert.Equal(t, true, ok, "should be equal")
	}
}