package docsyncer

import (
	"testing"
	"fmt"

	"mongoshake/common"
	"mongoshake/collector/configure"

	"github.com/stretchr/testify/assert"
	"github.com/vinllen/mgo/bson"
)

func TestGtDbNamespace(t *testing.T) {
	// test GetDbNamespace

	var nr int

	// test drop
	{
		fmt.Printf("TestGtDbNamespace case %d.\n", nr)
		nr++

		conn, err := utils.NewMongoConn(testMongoAddress, utils.VarMongoConnectModePrimary, true,
			utils.ReadWriteConcernLocal, utils.ReadWriteConcernDefault)
		assert.Equal(t, nil, err, "should be equal")

		err = conn.Session.DB("db1").DropDatabase()
		assert.Equal(t, nil, err, "should be equal")

		err = conn.Session.DB("db2").DropDatabase()
		assert.Equal(t, nil, err, "should be equal")

		conn.Session.DB("db1").C("c1").Insert(bson.M{"x": 1})
		conn.Session.DB("db1").C("c2").Insert(bson.M{"x": 1})
		conn.Session.DB("db1").C("c3").Insert(bson.M{"x": 1})
		conn.Session.DB("db2").C("c1").Insert(bson.M{"x": 1})
		conn.Session.DB("db2").C("c4").Insert(bson.M{"x": 1})

		// set whitelist
		conf.Options.FilterNamespaceWhite = []string{"db1", "db2"}

		nsList, nsMap, err := GetDbNamespace(testMongoAddress)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 5, len(nsList), "should be equal")
		assert.Equal(t, 2, len(nsMap), "should be equal")
		assert.Equal(t, 3, len(nsMap["db1"]), "should be equal")
		assert.Equal(t, 2, len(nsMap["db2"]), "should be equal")
	}
}