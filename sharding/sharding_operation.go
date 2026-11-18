package sharding

import (
	"context"
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	conf "github.com/alibaba/MongoShake/v2/collector/configure"
	utils "github.com/alibaba/MongoShake/v2/common"
	l "github.com/alibaba/MongoShake/v2/pkg/log"
)

const (
	ConfigDB = "config"

	SettingsCol   = "settings"
	ShardCol      = "shards"
	ChunkCol      = "chunks"
	CollectionCol = "collections"

	HashedShard = "hashed"
	RangedShard = "ranged"
)

// get balancer status from config server
func GetBalancerStatusByUrl(csUrl string) (bool, error) {
	var conn *utils.MongoCommunityConn
	var err error
	if conn, err = utils.NewMongoCommunityConn(csUrl, utils.VarMongoConnectModePrimary, true,
		utils.ReadWriteConcernMajority, utils.ReadWriteConcernDefault,
		conf.Options.MongoSslRootCaFile); conn == nil || err != nil {
		return true, err
	}
	defer conn.Close()

	var result bson.M
	err = conn.Client.Database(ConfigDB).Collection(SettingsCol).FindOne(nil,
		bson.M{"_id": "balancer"}, nil).Decode(&result)
	if err != nil && err != mongo.ErrNoDocuments {
		return true, err
	}
	if stopped, ok := result["stopped"].(bool); ok {
		return !stopped, nil
	} else {
		return true, nil
	}
}

type ChunkRange struct {
	// the minimum/maximum of the chunk range of multiple columns shard key has multiple values
	Mins []interface{}
	Maxs []interface{}
}

type ShardCollection struct {
	Chunks []*ChunkRange
	// Shard key columns and the corresponding per-column shard type.
	// ShardTypes[i] is the type of Keys[i] and is either HashedShard or
	// RangedShard. Compound hashed keys (MongoDB 4.4+) such as
	// {a: 1, b: "hashed"} are represented with a mix of values; consumers
	// must check ShardTypes[i] per key instead of treating the collection
	// as uniformly hashed or ranged.
	Keys       []string
	ShardTypes []string
}

// {replset: {namespace: []ChunkRange} }
type ShardingChunkMap map[string]map[string]*ShardCollection

type DBChunkMap map[string]*ShardCollection

// ChunkDoc is one config.chunks document.
//
// MongoDB 5.0 replaced the "ns" field with the collection "uuid". Measured
// against real servers: 4.4.30 writes ns and no uuid, while 5.0.33 / 6.0.28 /
// 7.0.37 / 8.0.4 / 8.2.12 all write uuid and no ns. Both are decoded and the
// namespace is recovered through collectionIndex.resolve, so a single code
// path serves every version without probing the server.
type ChunkDoc struct {
	Ns    string           `bson:"ns"`
	UUID  primitive.Binary `bson:"uuid"`
	Min   *bson.Raw        `bson:"min"`
	Max   *bson.Raw        `bson:"max"`
	Shard string           `bson:"shard"`
}

// CollectionDoc is one config.collections document. _id is the namespace
// string on every version from 4.4 through 8.2, which is what makes the
// uuid -> namespace reverse lookup possible.
type CollectionDoc struct {
	Ns   string           `bson:"_id"`
	UUID primitive.Binary `bson:"uuid"`
	Key  bson.D           `bson:"key"`
}

// collectionMeta is a config.collections entry reduced to what chunk
// interpretation needs: the namespace plus its shard key columns and the
// per-column shard type.
type collectionMeta struct {
	ns         string
	keys       []string
	shardTypes []string
}

// collectionIndex resolves a chunk to its collection. byNS serves pre-5.0
// chunks (which name the namespace themselves), byUUID serves 5.0+ chunks
// (which only carry the uuid).
type collectionIndex struct {
	byNS   map[string]*collectionMeta
	byUUID map[string]*collectionMeta
}

// uuidKey stringifies a Binary so it can be used as a map key —
// primitive.Binary holds a slice and is therefore not comparable.
func uuidKey(u primitive.Binary) string {
	return hex.EncodeToString(u.Data)
}

// buildCollectionIndex parses config.collections in one pass. Besides making
// the uuid -> namespace lookup possible, this removes the per-chunk FindOne
// the previous implementation issued: a cluster with N chunks used to cost N
// round trips to the config server.
//
// An entry without a usable shard key is skipped with a warning rather than
// failing the build — the previous code only errored once a chunk actually
// referenced that namespace, and keeping such an entry would leave the chunk
// min/max validation indexing an empty key list. Chunks of a skipped entry
// then fail to resolve and are dropped individually by buildChunkMap.
func buildCollectionIndex(docs []CollectionDoc) (*collectionIndex, error) {
	idx := &collectionIndex{
		byNS:   make(map[string]*collectionMeta, len(docs)),
		byUUID: make(map[string]*collectionMeta, len(docs)),
	}
	for _, doc := range docs {
		if len(doc.Key) == 0 {
			l.Logger.Warnf("buildCollectionIndex skip collection[%v] with no shard key", doc.Ns)
			continue
		}
		keys, shardTypes, err := parseShardKey(doc.Key)
		if err != nil {
			return nil, fmt.Errorf("config.collections entry[%v]: %w", doc.Ns, err)
		}
		meta := &collectionMeta{ns: doc.Ns, keys: keys, shardTypes: shardTypes}
		if doc.Ns != "" {
			idx.byNS[doc.Ns] = meta
		}
		if len(doc.UUID.Data) > 0 {
			idx.byUUID[uuidKey(doc.UUID)] = meta
		}
	}
	return idx, nil
}

// resolve recovers the collection a chunk belongs to. The chunk's own ns wins
// when present; otherwise the uuid is looked up. An ns that is not in the
// index still falls through to the uuid, so a chunk carrying a stale ns is
// resolved rather than dropped.
func (idx *collectionIndex) resolve(chunkNs string, chunkUUID primitive.Binary) (*collectionMeta, error) {
	if chunkNs != "" {
		if meta, ok := idx.byNS[chunkNs]; ok {
			return meta, nil
		}
	}
	if len(chunkUUID.Data) > 0 {
		if meta, ok := idx.byUUID[uuidKey(chunkUUID)]; ok {
			return meta, nil
		}
		return nil, fmt.Errorf("no config.collections entry for chunk uuid[%x]", chunkUUID.Data)
	}
	if chunkNs != "" {
		return nil, fmt.Errorf("no config.collections entry for namespace[%v]", chunkNs)
	}
	return nil, fmt.Errorf("chunk carries neither ns nor uuid")
}

func GetChunkMapByUrl(csUrl string) (ShardingChunkMap, error) {
	var conn *utils.MongoCommunityConn
	var err error
	if conn, err = utils.NewMongoCommunityConn(csUrl, utils.VarMongoConnectModePrimary, true,
		utils.ReadWriteConcernMajority, utils.ReadWriteConcernDefault, conf.Options.MongoSslRootCaFile); conn == nil || err != nil {
		return nil, err
	}
	defer conn.Close()

	type ShardDoc struct {
		Tag  string `bson:"_id"`
		Host string `bson:"host"`
	}
	// map: config.shards _id -> replset name
	shardMap := make(map[string]string)
	shardCursor, err := conn.Client.Database(ConfigDB).Collection(ShardCol).Find(context.Background(), bson.M{})
	if err != nil {
		return nil, err
	}
	for shardCursor.Next(context.Background()) {
		var shardDoc ShardDoc
		if err = shardCursor.Decode(&shardDoc); err != nil {
			l.Logger.Warnf("GetChunkMapByUrl decode config.shards failed, err[%v]", err)
			continue
		}
		shardMap[shardDoc.Tag] = strings.Split(shardDoc.Host, "/")[0]
	}

	// config.collections is read once, up front: it supplies both the shard key
	// of each namespace and the uuid -> namespace mapping that 5.0+ chunks need.
	var collectionDocs []CollectionDoc
	colCursor, err := conn.Client.Database(ConfigDB).Collection(CollectionCol).Find(context.Background(), bson.M{})
	if err != nil {
		return nil, err
	}
	for colCursor.Next(context.Background()) {
		var colDoc CollectionDoc
		if err = colCursor.Decode(&colDoc); err != nil {
			l.Logger.Warnf("GetChunkMapByUrl decode config.collections failed, err[%v]", err)
			continue
		}
		collectionDocs = append(collectionDocs, colDoc)
	}
	idx, err := buildCollectionIndex(collectionDocs)
	if err != nil {
		return nil, err
	}

	// only sharded collections exist on "config.chunks"
	var chunks []ChunkDoc
	chunkCursor, err := conn.Client.Database(ConfigDB).Collection(ChunkCol).Find(context.Background(), bson.M{})
	if err != nil {
		return nil, err
	}
	for chunkCursor.Next(context.Background()) {
		var chunkDoc ChunkDoc
		if err = chunkCursor.Decode(&chunkDoc); err != nil {
			l.Logger.Warnf("GetChunkMapByUrl decode config.chunks failed, err[%v]", err)
			continue
		}
		chunks = append(chunks, chunkDoc)
	}

	return buildChunkMap(shardMap, idx, chunks)
}

// buildChunkMap assembles {replset: {namespace: *ShardCollection}} from the
// shard list and the decoded chunks. It is pure so the pre-5.0 and 5.0+ chunk
// shapes can be exercised without a config server.
func buildChunkMap(shardMap map[string]string, idx *collectionIndex, chunks []ChunkDoc) (ShardingChunkMap, error) {
	chunkMap := make(ShardingChunkMap, len(shardMap))
	for _, replset := range shardMap {
		chunkMap[replset] = make(DBChunkMap)
	}

	unresolved := 0
	for i := range chunks {
		chunk := &chunks[i]
		meta, err := idx.resolve(chunk.Ns, chunk.UUID)
		if err != nil {
			// A collection dropped between the config.collections read and the
			// config.chunks read lands here. Skip the chunk instead of aborting:
			// the remaining ones are still usable.
			unresolved++
			l.Logger.Warnf("buildChunkMap skip chunk[%v] on shard[%v]: %v", i, chunk.Shard, err)
			continue
		}

		replset, ok := shardMap[chunk.Shard]
		if !ok {
			return nil, fmt.Errorf("buildChunkMap chunk[%v] of %v refers to shard[%v] which is "+
				"absent from config.shards", i, meta.ns, chunk.Shard)
		}

		// the namespace is sharded, so every replset needs an entry for it —
		// including the ones holding none of its chunks. OrphanFilter reads
		// presence to tell "sharded collection" apart from "not sharded".
		for _, dbChunkMap := range chunkMap {
			if _, exists := dbChunkMap[meta.ns]; !exists {
				dbChunkMap[meta.ns] = &ShardCollection{Keys: meta.keys, ShardTypes: meta.shardTypes}
			}
		}

		if chunk.Min == nil || chunk.Max == nil {
			return nil, fmt.Errorf("buildChunkMap chunk[%v] of %v has nil min/max (min nil[%v] max nil[%v])",
				i, meta.ns, chunk.Min == nil, chunk.Max == nil)
		}
		var minD, maxD bson.D
		errMin := bson.Unmarshal(*chunk.Min, &minD)
		errMax := bson.Unmarshal(*chunk.Max, &maxD)
		if errMin != nil || errMax != nil {
			return nil, fmt.Errorf("buildChunkMap chunk[%v] of %v has illegal min/max. errMin[%v] errMax[%v]",
				i, meta.ns, errMin, errMax)
		}
		// widths must agree with each other and with the shard key, otherwise
		// the key lookups below would run off the end of meta.keys
		if len(minD) != len(maxD) || len(minD) != len(meta.keys) {
			return nil, fmt.Errorf("buildChunkMap chunk[%v] of %v width mismatch: min[%v] max[%v] shardKey[%v]",
				i, meta.ns, minD, maxD, meta.keys)
		}

		shardCol := chunkMap[replset][meta.ns]
		mins := make([]interface{}, 0, len(minD))
		maxs := make([]interface{}, 0, len(maxD))
		for j, item := range minD {
			if item.Key != meta.keys[j] {
				return nil, fmt.Errorf("buildChunkMap chunk[%v] of %v min keys[%v] != shard keys[%v]",
					i, meta.ns, minD, meta.keys)
			}
			mins = append(mins, item.Value)
		}
		for j, item := range maxD {
			if item.Key != meta.keys[j] {
				return nil, fmt.Errorf("buildChunkMap chunk[%v] of %v max keys[%v] != shard keys[%v]",
					i, meta.ns, maxD, meta.keys)
			}
			maxs = append(maxs, item.Value)
		}
		shardCol.Chunks = append(shardCol.Chunks, &ChunkRange{Mins: mins, Maxs: maxs})
	}

	// Nothing resolved means the chunk map would come back empty and
	// OrphanFilter would silently stop filtering — the failure class #978 was
	// about. Fail loudly instead of handing back a map that looks valid.
	if unresolved > 0 && unresolved == len(chunks) {
		return nil, fmt.Errorf("buildChunkMap resolved none of the %v config.chunks documents against "+
			"config.collections, so the orphan filter would silently do nothing; check that mongo_cs_url "+
			"points at the config server of the source cluster", len(chunks))
	}

	return chunkMap, nil
}

// parseShardKey turns a config.collections "key" subdocument into the per-
// column (name, type) pair. It is the pure data path of buildCollectionIndex
// and is split out so we can unit-test compound hashed shard keys without a
// mongos. Each item's value is either a string ("hashed") or a numeric
// direction (1 / -1, including int32/int64/float64 as decoded by the BSON
// driver). The two slices returned are 1:1 with keyDoc and are intentionally
// not coalesced — callers like OrphanFilter.Filter rely on the per-column
// type rather than a collection-wide flag.
func parseShardKey(keyDoc bson.D) ([]string, []string, error) {
	keys := make([]string, 0, len(keyDoc))
	shardTypes := make([]string, 0, len(keyDoc))
	for _, item := range keyDoc {
		switch v := item.Value.(type) {
		case string:
			shardTypes = append(shardTypes, HashedShard)
		case int, int32, int64, float64:
			shardTypes = append(shardTypes, RangedShard)
		default:
			return nil, nil, fmt.Errorf("shard key field[%v] has unsupported value type[%v]",
				item.Key, reflect.TypeOf(v))
		}
		keys = append(keys, item.Key)
	}
	return keys, shardTypes, nil
}

type ShardCollectionSpec struct {
	Ns     string
	Key    bson.D
	Unique bool
}
