package filter

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/vinllen/mgo/bson"
	utils "mongoshake/common"
	"mongoshake/oplog"
	"testing"
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

func TestOrphanFilter(t *testing.T) {
	// test TestOrphanFilter

	chunkMap := make(utils.DBChunkMap)
	chunkMap["tbl"] = &utils.ShardCollection{Key: "a", ShardType: utils.RangedShard}
	chunkMap["tbl"].Chunks = append(chunkMap["tbl"].Chunks, &utils.ChunkRange{Min: bson.MinKey, Max: 0})
	chunkMap["tbl"].Chunks = append(chunkMap["tbl"].Chunks, &utils.ChunkRange{Min: 28, Max: 40})
	chunkMap["tbl"].Chunks = append(chunkMap["tbl"].Chunks, &utils.ChunkRange{Min: 54, Max: 66})

	chunkMap["col"] = &utils.ShardCollection{Key: "b", ShardType: utils.HashedShard}
	chunkMap["col"].Chunks = append(chunkMap["col"].Chunks, &utils.ChunkRange{Min: -3074457345618258602, Max: 0})
	chunkMap["col"].Chunks = append(chunkMap["col"].Chunks, &utils.ChunkRange{Min: 0, Max: 3074457345618258602})
	chunkMap["col"].Chunks = append(chunkMap["col"].Chunks, &utils.ChunkRange{Min: 6148914691236517204, Max: bson.MaxKey})

	var nr int
	{
		fmt.Printf("TestOrphanFilter case %d.\n", nr)
		nr++

		filter := NewOrphanFilter("db-1", chunkMap)
		var doc bson.Raw
		doc.Data, _ = bson.Marshal(bson.D{{"a", 1}, {"b", "b"}})
		assert.Equal(t, true, filter.Filter(&doc, "tbl"), "should be equal")
		doc.Data, _ = bson.Marshal(bson.D{{"a", 1.5}, {"b", "b"}})
		assert.Equal(t, true, filter.Filter(&doc, "tbl"), "should be equal")
		doc.Data, _ = bson.Marshal(bson.D{{"a", 0}, {"b", "b"}})
		assert.Equal(t, true, filter.Filter(&doc, "tbl"), "should be equal")
		doc.Data, _ = bson.Marshal(bson.D{{"a", 27.5}, {"b", "b"}})
		assert.Equal(t, true, filter.Filter(&doc, "tbl"), "should be equal")
		doc.Data, _ = bson.Marshal(bson.D{{"a", 28}, {"b", "b"}})
		assert.Equal(t, false, filter.Filter(&doc, "tbl"), "should be equal")
		doc.Data, _ = bson.Marshal(bson.D{{"a", 28.5}, {"b", "b"}})
		assert.Equal(t, false, filter.Filter(&doc, "tbl"), "should be equal")
		doc.Data, _ = bson.Marshal(bson.D{{"a", "aaa"}, {"b", "b"}})
		assert.Equal(t, true, filter.Filter(&doc, "tbl"), "should be equal")
		doc.Data, _ = bson.Marshal(bson.D{{"a", bson.ObjectIdHex("5d5ceef31a3088623ce706ad")}, {"b", "b"}})
		assert.Equal(t, true, filter.Filter(&doc, "tbl"), "should be equal")
	}

	{
		fmt.Printf("TestOrphanFilter case %d.\n", nr)
		nr++

		filter := NewOrphanFilter("db-2", chunkMap)
		var doc bson.Raw
		doc.Data, _ = bson.Marshal(bson.D{{"b", "b"}, {"c", "c"}})
		assert.Equal(t, true, filter.Filter(&doc, "col"), "should be equal")
		doc.Data, _ = bson.Marshal(bson.D{{"b", "c"}, {"c", "c"}})
		assert.Equal(t, true, filter.Filter(&doc, "col"), "should be equal")
		doc.Data, _ = bson.Marshal(bson.D{{"b", 12}, {"c", "c"}})
		assert.Equal(t, true, filter.Filter(&doc, "col"), "should be equal")
		doc.Data, _ = bson.Marshal(bson.D{{"b", "a"}, {"c", "c"}})
		assert.Equal(t, false, filter.Filter(&doc, "col"), "should be equal")
		doc.Data, _ = bson.Marshal(bson.D{{"b", 10}, {"c", "c"}})
		assert.Equal(t, false, filter.Filter(&doc, "col"), "should be equal")
		doc.Data, _ = bson.Marshal(bson.D{{"b", 10.5}, {"c", "c"}})
		assert.Equal(t, false, filter.Filter(&doc, "col"), "should be equal")
		doc.Data, _ = bson.Marshal(bson.D{{"b", bson.ObjectIdHex("5d5ceef31a3088623ce706ad")}, {"c", "c"}})
		assert.Equal(t, true, filter.Filter(&doc, "col"), "should be equal")
	}
}
