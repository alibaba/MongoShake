package utils

var (
	FcvCheckpoint = Checkpoint{
		CurrentVersion:           2,
		FeatureCompatibleVersion: 1,
	}
	FcvConfiguration = Configuration{
		CurrentVersion:           10,
		FeatureCompatibleVersion: 10,
	}

	LowestCheckpointVersion = map[int]string{
		0: "1.0.0",
		1: "2.4.0",
		2: "2.4.6", // change sharding checkpoint position from cs to mongos
	}
	LowestConfigurationVersion = map[int]string{
		0:  "1.0.0",
		1:  "2.4.0",
		2:  "2.4.1",
		3:  "2.4.3",
		4:  "2.4.6",  // add incr_sync.target_delay
		5:  "2.4.7",  // add full_sync.reader.read_document_count
		6:  "2.4.12", // add incr_sync.shard_by_object_id_whitelist
		7:  "2.4.17", // add filter.oplog.gids
		8:  "2.4.20", // add special.source.db.flag
		9:  "2.4.21", // remove incr_sync.worker.oplog_compressor; add incr_sync.tunnel.write_thread, tunnel.kafka.partition_number
		10: "2.6.4",  // remove full_sync.reader.read_document_count; add full_sync.reader.parallel_thread
	}
)

type Fcv interface {
	IsCompatible(int) bool
}

// for checkpoint
type Checkpoint struct {
	/*
	 * version: 0(or set not), MongoShake < 2.4, fcv == 0
	 * version: 1, MongoShake == 2.4, 0 < fcv <= 1
	 */
	CurrentVersion           int
	FeatureCompatibleVersion int
}

func (c Checkpoint) IsCompatible(v int) bool {
	return v >= c.FeatureCompatibleVersion && v <= c.CurrentVersion
}

// for configuration
type Configuration struct {
	/*
	 * version: 0(or set not), MongoShake < 2.4.0, fcv == 0
	 * version: 1, MongoShake == 2.4.0, 0 <= fcv <= 1
	 */
	CurrentVersion           int
	FeatureCompatibleVersion int
}

func (c Configuration) IsCompatible(v int) bool {
	return v >= c.FeatureCompatibleVersion && v <= c.CurrentVersion
}
