package conf

import (
	"testing"
	"fmt"
	"os"

	"github.com/stretchr/testify/assert"
)

const testFile = "ut_test.conf"

func TestCheckFcv(t *testing.T) {
	// only test CheckFcv

	var nr int

	{
		fmt.Printf("TestCheckFcv case %d.\n", nr)
		nr++

		// write file
		f, err := os.Create(testFile)
		assert.Equal(t, nil, err, "should be equal")

		f.WriteString("version = 10\n")
		f.WriteString("# conf.version = 11\n")
		f.WriteString("conf.version =    12\n")
		f.Close()

		// do comparison
		v, err := CheckFcv(testFile, 1)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 12, v, "should be equal")

		// remove
		err = os.Remove(testFile)
		assert.Equal(t, nil, err, "should be equal")
	}

	{
		fmt.Printf("TestCheckFcv case %d.\n", nr)
		nr++

		// write file
		f, err := os.Create(testFile)
		assert.Equal(t, nil, err, "should be equal")

		f.WriteString("version = 10\n")
		f.WriteString("# conf.version = 11\n")
		f.WriteString("conf.version =12\n")
		f.Close()

		// do comparison
		v, err := CheckFcv(testFile, 13)
		assert.NotEqual(t, nil, err, "should be equal")
		assert.Equal(t, 12, v, "should be equal")

		// remove
		err = os.Remove(testFile)
		assert.Equal(t, nil, err, "should be equal")
	}

	{
		fmt.Printf("TestCheckFcv case %d.\n", nr)
		nr++

		// write file
		f, err := os.Create(testFile)
		assert.Equal(t, nil, err, "should be equal")

		f.WriteString("version = 10\n")
		f.WriteString("# conf.version = 11\n")
		// f.WriteString("conf.version =12\n")
		f.Close()

		// do comparison
		v, err := CheckFcv(testFile, 0)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 0, v, "should be equal")

		// remove
		err = os.Remove(testFile)
		assert.Equal(t, nil, err, "should be equal")
	}

	{
		fmt.Printf("TestCheckFcv case %d.\n", nr)
		nr++

		// write file
		f, err := os.Create(testFile)
		assert.Equal(t, nil, err, "should be equal")

		f.WriteString("version = 10\n")
		f.WriteString("# conf.version = 11\n")
		// f.WriteString("conf.version =12\n")
		f.Close()

		// do comparison
		v, err := CheckFcv(testFile, 1)
		assert.NotEqual(t, nil, err, "should be equal")
		assert.Equal(t, 0, v, "should be equal")
		fmt.Println(err)

		// remove
		err = os.Remove(testFile)
		assert.Equal(t, nil, err, "should be equal")
	}
}