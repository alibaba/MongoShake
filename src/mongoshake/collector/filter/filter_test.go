package filter

import (
	"testing"
	"fmt"
	"mongoshake/oplog"
	"github.com/stretchr/testify/assert"
)

func TestNamespaceFilter(t *testing.T) {
	// test NamespaceFilter

	var nr int
	{
		fmt.Printf("TestNamespaceFilter case %d.\n", nr)
		nr++

		filter := NewNamespaceFilter([]string{"gogo.test1", "gogo.test2"}, nil)
		log := &oplog.PartialLog{
			Namespace: "gogo.$cmd",
		}
		assert.Equal(t, false, filter.Filter(log), "should be equal")
	}
}