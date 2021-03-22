package transform

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTransform(t *testing.T) {
	var nr int
	{
		fmt.Printf("TestTransform case %d.\n", nr)
		nr++
		transRule := []string{"fromDB1:toDB1", "fromDB2.fromCol2:toDB2.toCol2"}
		trans := NewNamespaceTransform(transRule)
		assert.Equal(t, "toDB1.fromCol1", trans.Transform("fromDB1.fromCol1"), "should be equal")
		assert.Equal(t, "toDB1", trans.Transform("fromDB1"), "should be equal")
		assert.Equal(t, "fromDB2", trans.Transform("fromDB2"), "should be equal")
		assert.Equal(t, "toDB2.toCol2", trans.Transform("fromDB2.fromCol2"), "should be equal")
	}
	{
		fmt.Printf("TestTransform case %d.\n", nr)
		nr++
		transRule := []string{"fromDB1.fromCol2:toDB2.toCol2", "fromDB1:toDB1"}
		trans := NewNamespaceTransform(transRule)
		assert.Equal(t, "toDB1.fromCol1", trans.Transform("fromDB1.fromCol1"), "should be equal")
		assert.Equal(t, "toDB2.toCol2", trans.Transform("fromDB1.fromCol2"), "should be equal")
	}
	{
		fmt.Printf("TestTransform case %d.\n", nr)
		nr++
		transRule := []string{"fromDB1:toDB1", "fromDB2.fromCol2:toDB2.toCol2"}
		trans := NewDBTransform(transRule)
		assert.Equal(t, []string{"toDB1"}, trans.Transform("fromDB1"), "should be equal")
		assert.Equal(t, []string{"toDB2"}, trans.Transform("fromDB2"), "should be equal")
		assert.Equal(t, []string{"fromDB3"}, trans.Transform("fromDB3"), "should be equal")
	}
	{
		fmt.Printf("TestTransform case %d.\n", nr)
		nr++
		transRule := []string{"fromDB1:toDB1", "fromDB1.fromCol1:toDB2.toCol2"}
		trans := NewDBTransform(transRule)
		assert.Equal(t, []string{"toDB1", "toDB2"}, trans.Transform("fromDB1"), "should be equal")
		assert.Equal(t, []string{"fromDB2"}, trans.Transform("fromDB2"), "should be equal")
	}
}
