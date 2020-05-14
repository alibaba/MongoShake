package utils

var (
	FcvCheckpoint = Checkpoint{
		CurrentVersion:           1,
		FeatureCompatibleVersion: 1,
	}
	FcvConfiguration = Configuration{
		CurrentVersion:           3,
		FeatureCompatibleVersion: 3,
	}

	LowestCheckpointVersion = map[int]string {
		0: "1.0.0",
		1: "2.4.0",
	}
	LowestConfigurationVersion = map[int]string {
		0: "1.0.0",
		1: "2.4.0",
		2: "2.4.1",
		3: "2.4.3",
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
