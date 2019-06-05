package transform

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTransform(t *testing.T) {
	var nr int
	{
		fmt.Printf("TestTransform case %d.\n", nr)
		nr++

		transRule := []string{"fromDB1:toDB1", "fromDB2.fromCol2:toDB2.toCol2"}
		transform := NewNamespaceTransform(transRule)
		assert.Equal(t,"toDB1.fromCol1", transform.Transform("fromDB1.fromCol1"), "should be equal")
		assert.Equal(t,"toDB1", transform.Transform("fromDB1"), "should be equal")
		assert.Equal(t,"fromDB2", transform.Transform("fromDB2"), "should be equal")
		assert.Equal(t,"toDB2.toCol2", transform.Transform("fromDB2.fromCol2"), "should be equal")
	}
}