// MongoDB JavaScript脚本 - 模拟preSharingForRange和moveChunkIfNeeded函数功能
// 该脚本应在MongoDB源端实例上执行

// 配置参数（需要根据实际情况修改）
var dbName = "ycsb";           // 数据库名
var collName = "test1";        // 集合名
var sampleRate = 0.01;          // 采样率
var dryRun = true;             // dry run模式，只输出命令不执行

// 常量定义
var Version500 = [5, 0, 0];
var Version600 = [6, 0, 0];
var Version603 = [6, 0, 3];
var SampleThreshold = 5000;
var ShardKeyTypeRange = "range";
var ShardKeyTypeHashed = "hashed";

// 获取数据库版本
function getDbVersion() {
    var buildInfo = db.adminCommand("buildInfo");
    var versionStr = buildInfo.version;
    var versionParts = versionStr.split(".");
    return versionParts.map(function(part) { return parseInt(part); });
}

// 版本比较函数
function versionCompare(v1, v2) {
    for (var i = 0; i < Math.min(v1.length, v2.length); i++) {
        if (v1[i] < v2[i]) return -1;
        if (v1[i] > v2[i]) return 1;
    }
    return 0;
}

function versionGTE(v1, v2) {
    return versionCompare(v1, v2) >= 0;
}

function versionLT(v1, v2) {
    return versionCompare(v1, v2) < 0;
}

// 获取分片键和UUID
function getShardKeyAndUuid(dbName, collName) {
    var ns = dbName + "." + collName;
    var collectionDoc = db.getSiblingDB("config").collections.findOne({"_id": ns});

    if (!collectionDoc) {
        print("Namespace " + ns + " is not sharded or does not exist");
        return null;
    }

    print("Found namespace " + ns + " in config.collections");

    var shardKeyType = ShardKeyTypeRange;

    // 检查是否为hashed分片键
    for (var key in collectionDoc.key) {
        if (collectionDoc.key[key] === "hashed") {
            shardKeyType = ShardKeyTypeHashed;
            break;
        }
    }

    return {
        shardKey: collectionDoc.key,
        shardType: shardKeyType,
        uuid: collectionDoc.uuid,
        ns: ns
    };
}

// 获取所有分片列表
function getShardsList() {
    var shards = db.getSiblingDB("config").shards.find({});
    var shardNames = [];
    shards.forEach(function(shard) {
        shardNames.push(shard._id);
    });
    return shardNames;
}

// 获取数据库的主分片
function getPrimaryShard(dbName) {
    var dbDoc = db.getSiblingDB("config").databases.findOne({"_id": dbName});
    if (!dbDoc) {
        print("Database " + dbName + " not found in config.databases");
        return null;
    }
    return dbDoc.primary;
}

// 计算chunk数量
function countChunks(dbName, collName, uuid) {
    var filter;
    var dbVersion = getDbVersion();

    if (versionGTE(dbVersion, Version500)) {
        if (!uuid) {
            print("Namespace [" + dbName + "." + collName + "] uuid not found");
            return 0;
        }
        filter = {"uuid": uuid};
    } else {
        filter = {"ns": dbName + "." + collName};
    }

    return db.getSiblingDB("config").chunks.count(filter);
}

// 检查边界是否包含minKey或maxKey
function haveMinMaxKey(min, max) {
    var haveMin = false;
    var haveMax = false;

    for (var field in min) {
        if (min[field].$minKey !== undefined) {
            haveMin = true;
            break;
        }
    }

    for (var field in max) {
        if (max[field].$maxKey !== undefined) {
            haveMax = true;
            break;
        }
    }

    return {haveMin: haveMin, haveMax: haveMax};
}

// moveChunkIfNeeded函数的模拟实现
function moveChunkIfNeeded(dbName, collName, hasHashed) {
    // 获取新UUID
    var shardInfo = getShardKeyAndUuid(dbName, collName);
    if (!shardInfo) {
        return;
    }

    if (hasHashed && shardInfo.shardType !== ShardKeyTypeHashed) {
        print("Unexpected shard key:", shardInfo.shardKey);
        return;
    }

    // 计算chunk数量
    var chunkNum = countChunks(dbName, collName, shardInfo.uuid);
    print("Destination chunk num:", chunkNum);

    // 获取所有分片列表
    var shardNames = getShardsList();
    if (shardNames.length === 0) {
        print("No shards found");
        return;
    }

    // 获取主分片
    var primaryShard = getPrimaryShard(dbName);
    if (!primaryShard) {
        print("Failed to get primary shard for database " + dbName);
        return;
    }

    // 遍历所有chunks并均匀地移动到各分片
    var filter = {"uuid": shardInfo.uuid};
    var chunks = db.getSiblingDB("config").chunks.find(filter);

    var i = 0;
    chunks.forEach(function(chunk) {
        i++;
        if (chunk.shard !== primaryShard) {
            print("Unexpected chunk info:", JSON.stringify(chunk), ", not in primary shard:", primaryShard);
            return;
        }

        var targetShard = shardNames[i % shardNames.length];
        if (targetShard === primaryShard) {
            return;
        }

        var haveMinMax = haveMinMaxKey(chunk.min, chunk.max);
        if (haveMinMax.haveMin) {
            return;
        }

        var moveChunkCmd;
        if (!hasHashed) {
            // 使用'find'选项的moveChunk命令
            moveChunkCmd = {
                "moveChunk": dbName + "." + collName,
                "find": chunk.min,
                "to": targetShard
            };
        } else {
            // 使用'bounds'选项的moveChunk命令
            moveChunkCmd = {
                "moveChunk": dbName + "." + collName,
                "bounds": [chunk.min, chunk.max],
                "to": targetShard
            };
        }

        if (dryRun) {
            print("[DRY_RUN] db.adminCommand(", JSON.stringify(moveChunkCmd),")");
        } else {
            var result = db.adminCommand(moveChunkCmd);
            if (result.ok !== 1) {
                print("moveChunk failed:", result, ", cmd:", JSON.stringify(moveChunkCmd));
            }
        }
    });
}

// 主要的预分片处理函数（针对range分片）
function preSharingForRange(dbName, collName) {
    var dbVersion = getDbVersion();

    // 获取分片键和UUID
    var shardInfo = getShardKeyAndUuid(dbName, collName);
    if (!shardInfo) {
        return;
    }

    // 启用分片
    if (versionGTE(dbVersion, Version600)) {
        var enableShardingCmd = {"enableSharding": dbName};
        if (dryRun) {
            print("[DRY_RUN] db.adminCommand(:", JSON.stringify(enableShardingCmd), ")");
        } else {
            var result = db.adminCommand(enableShardingCmd);
            if (result.ok !== 1) {
                print("Error enabling sharding for " + dbName + ": ", result);
                return;
            }
        }
    }

    // 对集合进行分片
    var shardCollectionCmd = {
        "shardCollection": dbName + "." + collName,
        "key": shardInfo.shardKey
    };

    if (dryRun) {
        print("[DRY_RUN] db.adminCommand(", JSON.stringify(shardCollectionCmd), ")");
    } else {
        var result = db.adminCommand(shardCollectionCmd);
        if (result.ok !== 1) {
            print("Error sharding collection " + collName + ": ", result);
            return;
        } else {
            print("Run shardCollection for " + dbName + "." + collName + " succeed");
        }
    }

    // 计算chunk数量
    var chunkNum = countChunks(dbName, collName, shardInfo.uuid);
    print("Chunk num:", chunkNum);

    // 确定采样参数
    if (chunkNum <= SampleThreshold) {
        sampleRate = 1.0;
    }

    var expectedNum = Math.floor(sampleRate * chunkNum);
    var step = Math.max(1, Math.floor(chunkNum / expectedNum));
    print("Sample rate:", sampleRate, ", expected chunk num:", expectedNum, ", step:", step);

    // 构建查询条件
    var filter;
    if (versionGTE(dbVersion, Version500)) {
        filter = {"uuid": shardInfo.uuid};
    } else {
        filter = {"ns": shardInfo.ns};
    }

    // 获取chunks并执行split操作
    var chunks = db.getSiblingDB("config").chunks.find(filter).sort({min: 1});
    var i = 0;

    chunks.forEach(function(chunk) {
        i++;
        if (i % step !== 0) {
            return;
        }

        print("Processing chunk:", JSON.stringify(chunk));

        if (!chunk.min || !chunk.max) {
            print("Unexpected chunk doc:", JSON.stringify(chunk));
            return;
        }

        // 检查是否包含minKey
        var haveMinKey = false;
        for (var field in chunk.min) {
            if (chunk.min[field].$minKey !== undefined) {
                haveMinKey = true;
                break;
            }
        }

        if (haveMinKey) {
            print("Skipping chunk with minKey:", JSON.stringify(chunk));
            return;
        }

        var splitCmd = {
            "split": dbName + "." + collName,
            "middle": chunk.min
        };

        if (dryRun) {
            print("[DRY_RUN] db.adminCommand(:", JSON.stringify(splitCmd),")");
        } else {
            var result = db.adminCommand(splitCmd);
            if (result.ok !== 1) {
                print("Error splitting chunk:", result, ", cmd:", JSON.stringify(splitCmd));
            }
        }
    });

    // TODO:also support dryRun for moveChunk cmds, but need connectivity to the target instance.

}

// 执行主函数
preSharingForRange(dbName, collName);