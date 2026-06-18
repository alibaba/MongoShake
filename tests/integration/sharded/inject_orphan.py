#!/usr/bin/env python3
"""
Construct an orphan-document scenario on the sharded cluster brought up by
docker-compose.yml + setup_cluster.sh, then run a sanity check.

Layout produced (range shard on {a:1, b:1}):

    chunk on rs1: { (MinKey, MinKey) <= (a,b) < (5, 0) }
    chunk on rs2: { (5, 0)           <= (a,b) < (MaxKey, MaxKey) }

Documents:
    1. {_id: "owned-by-rs1", a: 1, b: 100}        ← inserted via mongos, lives on rs1
    2. {_id: "owned-by-rs2", a: 7, b: 100}        ← inserted via mongos, lives on rs2
    3. {_id: "owned-by-rs1", a: 1, b: 100}        ← injected DIRECTLY on rs2 (orphan)

After full sync with full_sync.executor.filter.orphan_document=true (and
mongod-direct source), the destination must contain exactly 2 docs (#1 and
#2). Doc #3 must be filtered out as orphan.

Without the filter (or in mongos-mode source) the destination either fails
on dup-key (#1 vs #3) or contains all three docs.

Requires: pymongo (`pip install pymongo`).
"""
from __future__ import annotations

import sys

try:
    from pymongo import MongoClient
except ImportError:
    sys.exit("pymongo missing; pip install pymongo")

DB, COLL = "shaketest", "orphan_demo"
NS = f"{DB}.{COLL}"


def main() -> int:
    mongos = MongoClient("mongodb://127.0.0.1:27017", directConnection=False)
    rs1 = MongoClient("mongodb://127.0.0.1:27018", directConnection=True)
    rs2 = MongoClient("mongodb://127.0.0.1:27028", directConnection=True)

    # Reset namespace
    mongos.drop_database(DB)

    # Enable sharding and set the chunk split.
    admin = mongos["admin"]
    admin.command("enableSharding", DB)
    admin.command("shardCollection", NS, key={"a": 1, "b": 1})
    admin.command("split", NS, middle={"a": 5, "b": 0})

    # Move chunks so rs1 owns [MinKey, (5,0)) and rs2 owns [(5,0), MaxKey).
    # After split, both chunks may live on the same shard. Move as needed.
    admin.command("moveChunk", NS, find={"a": 1, "b": 0}, to="rs1")
    admin.command("moveChunk", NS, find={"a": 5, "b": 0}, to="rs2")

    # Normal inserts via mongos.
    mongos[DB][COLL].insert_many([
        {"_id": "owned-by-rs1", "a": 1, "b": 100, "tag": "normal"},
        {"_id": "owned-by-rs2", "a": 7, "b": 100, "tag": "normal"},
    ])

    # Orphan: inject {_id: "owned-by-rs1", a: 1, b: 100} directly into rs2.
    # rs2 only owns chunks with a >= 5, so this doc does not belong here.
    #
    # Direct shardsvr writes to a sharded collection normally fail with
    # StaleConfig because rs2's local chunk metadata says it owns nothing
    # in the (a<5) range. The compose file enables enableTestCommands=1
    # on each shard so this insert bypasses the sharded-write version
    # check. If you ever see "StaleConfig" or similar here, double-check
    # that the shards were brought up with that setParameter.
    try:
        rs2[DB][COLL].insert_one({"_id": "owned-by-rs1", "a": 1, "b": 100, "tag": "orphan"})
    except Exception as e:  # broad: pymongo raises various OperationFailures
        sys.exit(
            f"direct write to rs2 failed ({e!r}). The orphan-injection step needs "
            "shardsvr in test mode (enableTestCommands=1, set in docker-compose.yml). "
            "If you are running against a non-test cluster, this scenario can't be "
            "reproduced via direct insert."
        )

    # Sanity: rs1 has 1, rs2 has 2 (one legit + one orphan).
    rs1_count = rs1[DB][COLL].count_documents({})
    rs2_count = rs2[DB][COLL].count_documents({})
    print(f"rs1 holds {rs1_count} doc(s); rs2 holds {rs2_count} doc(s)")
    assert rs1_count == 1, f"expected 1 on rs1, got {rs1_count}"
    assert rs2_count == 2, f"expected 2 on rs2 (legit + orphan), got {rs2_count}"

    # mongos sees what it routes — should be 2 (orphan filtered by routing).
    via_mongos = mongos[DB][COLL].count_documents({})
    print(f"mongos sees {via_mongos} doc(s) — orphan {'NOT visible' if via_mongos == 2 else 'VISIBLE'} via routing")

    print("orphan scenario ready.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
