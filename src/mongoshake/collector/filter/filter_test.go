package filter

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/vinllen/mgo/bson"
	conf "mongoshake/collector/configure"
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
	chunkMap["tbl"] = &utils.ShardCollection{Keys: []string{"a"}, ShardType: utils.RangedShard}
	chunkMap["tbl"].Chunks = append(chunkMap["tbl"].Chunks, &utils.ChunkRange{Mins: []interface{}{bson.MinKey}, Maxs: []interface{}{0}})
	chunkMap["tbl"].Chunks = append(chunkMap["tbl"].Chunks, &utils.ChunkRange{Mins: []interface{}{28}, Maxs: []interface{}{40}})
	chunkMap["tbl"].Chunks = append(chunkMap["tbl"].Chunks, &utils.ChunkRange{Mins: []interface{}{54}, Maxs: []interface{}{66}})

	chunkMap["col"] = &utils.ShardCollection{Keys: []string{"b"}, ShardType: utils.HashedShard}
	chunkMap["col"].Chunks = append(chunkMap["col"].Chunks, &utils.ChunkRange{Mins: []interface{}{-3074457345618258602}, Maxs: []interface{}{0}})
	chunkMap["col"].Chunks = append(chunkMap["col"].Chunks, &utils.ChunkRange{Mins: []interface{}{0}, Maxs: []interface{}{3074457345618258602}})
	chunkMap["col"].Chunks = append(chunkMap["col"].Chunks, &utils.ChunkRange{Mins: []interface{}{6148914691236517204}, Maxs: []interface{}{bson.MaxKey}})

	chunkMap["test"] = &utils.ShardCollection{Keys: []string{"a", "b"}, ShardType: utils.RangedShard}
	chunkMap["test"].Chunks = append(chunkMap["test"].Chunks, &utils.ChunkRange{Mins: []interface{}{bson.MinKey, bson.MinKey}, Maxs: []interface{}{0, 1}})
	chunkMap["test"].Chunks = append(chunkMap["test"].Chunks, &utils.ChunkRange{Mins: []interface{}{28, 29}, Maxs: []interface{}{40, 41}})
	chunkMap["test"].Chunks = append(chunkMap["test"].Chunks, &utils.ChunkRange{Mins: []interface{}{54, 55}, Maxs: []interface{}{66, 67}})

	var nr int
	{
		fmt.Printf("TestOrphanFilter case %d.\n", nr)
		nr++

		filter := NewOrphanFilter("db", chunkMap)
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

		filter := NewOrphanFilter("db", chunkMap)
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

	{
		fmt.Printf("TestOrphanFilter case %d.\n", nr)
		nr++

		filter := NewOrphanFilter("db", chunkMap)
		var doc bson.Raw
		doc.Data, _ = bson.Marshal(bson.D{{"a", -1}, {"b", 10}})
		assert.Equal(t, false, filter.Filter(&doc, "test"), "should be equal")
		doc.Data, _ = bson.Marshal(bson.D{{"a", 0}, {"b", 0.9}})
		assert.Equal(t, false, filter.Filter(&doc, "test"), "should be equal")
		doc.Data, _ = bson.Marshal(bson.D{{"a", 0}, {"b", 1}})
		assert.Equal(t, true, filter.Filter(&doc, "test"), "should be equal")
		doc.Data, _ = bson.Marshal(bson.D{{"a", 0}, {"b", 1.1}})
		assert.Equal(t, true, filter.Filter(&doc, "test"), "should be equal")
		doc.Data, _ = bson.Marshal(bson.D{{"a", bson.MinKey}, {"b", 1}})
		assert.Equal(t, false, filter.Filter(&doc, "test"), "should be equal")
		doc.Data, _ = bson.Marshal(bson.D{{"a", 28}, {"b", 29}})
		assert.Equal(t, false, filter.Filter(&doc, "test"), "should be equal")
		doc.Data, _ = bson.Marshal(bson.D{{"a", 28.1}, {"b", 29.1}})
		assert.Equal(t, false, filter.Filter(&doc, "test"), "should be equal")
		doc.Data, _ = bson.Marshal(bson.D{{"a", "aaa"}, {"b", "b"}})
		assert.Equal(t, true, filter.Filter(&doc, "test"), "should be equal")
	}
}

func TestGidFilter(t *testing.T) {
	// test GidFilter

	var nr int
	{
		fmt.Printf("TestGidFilter case %d.\n", nr)
		nr++

		filter := NewGidFilter([]string{})
		log := &oplog.PartialLog{
			Gid: "1",
		}
		assert.Equal(t, false, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{}
		assert.Equal(t, false, filter.Filter(log), "should be equal")
	}

	{
		fmt.Printf("TestGidFilter case %d.\n", nr)
		nr++

		filter := NewGidFilter([]string{"5", "6", "7"})
		log := &oplog.PartialLog{
			Gid: "1",
		}
		assert.Equal(t, true, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{}
		assert.Equal(t, true, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			Gid: "5",
		}
		assert.Equal(t, false, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			Gid: "8",
		}
		assert.Equal(t, true, filter.Filter(log), "should be equal")
	}
}

func TestAutologousFilter(t *testing.T) {
	// test AutologousFilter

	var nr int
	{
		fmt.Printf("TestAutologousFilter case %d.\n", nr)
		nr++

		conf.Options.FilterPassSpecialDb = []string{}
		conf.Options.ContextStorageDB = "mongoshake"

		filter := NewAutologousFilter()
		log := &oplog.PartialLog{
			Namespace: "a.b",
		}
		assert.Equal(t, false, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{}
		assert.Equal(t, false, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			Namespace: "mongoshake.x",
		}
		assert.Equal(t, true, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			Namespace: "local.x.z.y",
		}
		assert.Equal(t, true, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			Namespace: "a.system.views",
		}
		assert.Equal(t, true, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			Namespace: "a.system.view",
		}
		assert.Equal(t, false, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			Namespace: "admin.x",
		}
		assert.Equal(t, true, filter.Filter(log), "should be equal")
	}

	{
		fmt.Printf("TestAutologousFilter case %d.\n", nr)
		nr++

		conf.Options.FilterPassSpecialDb = []string{"admin", "system.views"}
		conf.Options.ContextStorageDB = "mongoshake"

		filter := NewAutologousFilter()
		log := &oplog.PartialLog{
			Namespace: "a.b",
		}
		assert.Equal(t, false, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{}
		assert.Equal(t, false, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			Namespace: "mongoshake.x",
		}
		assert.Equal(t, true, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			Namespace: "local.x.z.y",
		}
		assert.Equal(t, true, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			Namespace: "a.system.views",
		}
		assert.Equal(t, false, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			Namespace: "a.system.view",
		}
		assert.Equal(t, false, filter.Filter(log), "should be equal")

		log = &oplog.PartialLog{
			Namespace: "admin.x",
		}
		assert.Equal(t, false, filter.Filter(log), "should be equal")
	}
}
