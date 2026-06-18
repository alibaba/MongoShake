#!/usr/bin/env python3
"""
Run MongoShake against the orphan scenario with three configs and assert
the destination state matches the expected behaviour for each.

Prereqs:
    1. ./setup_cluster.sh (cluster up)
    2. ./inject_orphan.py (orphan scenario in place)
    3. MongoShake collector binary at ../../../bin/collector.darwin (or .linux)

Run:
    ./verify_orphan_filter.py /path/to/collector.linux

Cases:
    A. mongo_urls only + filter.orphan_document=true
       → dest should have exactly 2 docs (orphan filtered)
    B. mongo_urls + mongo_s_url + filter.orphan_document=true (issue #978 repro)
       → dest should have exactly 2 docs (mongos routes past orphan) with rc=0,
         AND the collector log must contain the new WARN message
    C. mongo_urls only + filter.orphan_document=false
       → dest should fail with dup-key (baseline)
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import time

from pymongo import MongoClient

DB, COLL = "shaketest", "orphan_demo"
NS = f"{DB}.{COLL}"

CONF_TEMPLATE = """\
id = shake-test
conf.version = 10
sync_mode = full
log.level = info
log.dir = {log_dir}
log.file = collector.log
mongo_urls = mongodb://127.0.0.1:27018/?directConnection=true;mongodb://127.0.0.1:27028/?directConnection=true
mongo_cs_url = mongodb://127.0.0.1:27019/?directConnection=true
mongo_connect_mode = standalone
{mongo_s_line}
tunnel.address = mongodb://127.0.0.1:27117/?directConnection=true
checkpoint.storage.url = mongodb://127.0.0.1:27117/?directConnection=true
full_sync.executor.filter.orphan_document = {orphan_filter}
full_sync.executor.insert_on_dup_update = false
full_sync.create_index = none
"""


def write_conf(path: str, log_dir: str, with_mongos: bool, orphan_filter: bool) -> None:
    mongo_s_line = "mongo_s_url = mongodb://127.0.0.1:27017" if with_mongos else "# mongo_s_url ="
    with open(path, "w") as f:
        f.write(CONF_TEMPLATE.format(
            log_dir=log_dir,
            mongo_s_line=mongo_s_line,
            orphan_filter="true" if orphan_filter else "false",
        ))


def reset_dest() -> None:
    MongoClient("mongodb://127.0.0.1:27117", directConnection=True).drop_database(DB)


def run_collector(binary: str, conf_path: str, timeout: int = 60) -> tuple[int, str]:
    proc = subprocess.run(
        [binary, "-conf", conf_path, "-verbose", "0"],
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    return proc.returncode, proc.stdout + "\n" + proc.stderr


def dest_count() -> int:
    return MongoClient("mongodb://127.0.0.1:27117", directConnection=True)[DB][COLL].count_documents({})


def case(name: str, ok: bool, detail: str) -> None:
    flag = "PASS" if ok else "FAIL"
    print(f"  [{flag}] {name}: {detail}")


def main(binary: str) -> int:
    workdir = tempfile.mkdtemp(prefix="shake-it-")
    print(f"workdir: {workdir}")
    failures = 0

    # Case A: mongod-direct + filter on → expect 2 docs in dest.
    print("\nCase A: mongod-direct + orphan_document=true")
    reset_dest()
    conf = os.path.join(workdir, "A.conf")
    log_dir = os.path.join(workdir, "A_log"); os.makedirs(log_dir, exist_ok=True)
    write_conf(conf, log_dir, with_mongos=False, orphan_filter=True)
    rc, out = run_collector(binary, conf)
    cnt = dest_count()
    ok = (rc == 0 and cnt == 2)
    case("filter takes effect", ok, f"rc={rc} dest_count={cnt} (expect 2)")
    if not ok:
        failures += 1

    # Case B: mongod-direct + mongos + filter on → WARN expected, filter
    # silently disabled. With mongo_s_url configured, full sync reads via
    # mongos which never exposes orphan docs (they're invisible to the
    # router). So the sync succeeds normally with 2 docs — same as Case A
    # in terms of dest state, but the mechanism is different: mongos routing
    # hides the orphan rather than the filter removing it.
    # Assert:
    #   1) the new WARN line is present (proves the fix logged the heads-up)
    #   2) rc=0 and dest_count=2 (mongos-routed sync sees only owned docs)
    print("\nCase B: mongos + orphan_document=true (#978 repro)")
    reset_dest()
    conf = os.path.join(workdir, "B.conf")
    log_dir = os.path.join(workdir, "B_log"); os.makedirs(log_dir, exist_ok=True)
    write_conf(conf, log_dir, with_mongos=True, orphan_filter=True)
    rc, out = run_collector(binary, conf)
    cnt = dest_count()
    log_blob = ""
    log_path = os.path.join(log_dir, "collector.log")
    if os.path.exists(log_path):
        with open(log_path) as f:
            log_blob = f.read()
    has_warn = "orphan filter will NOT take effect" in log_blob
    case("WARN present in log", has_warn, "looking for 'orphan filter will NOT take effect'")
    if not has_warn:
        failures += 1
    # mongos routes reads to the owning shard only, so orphan is invisible
    # and sync completes cleanly with all 2 legitimate docs.
    behaved_as_mongos_routed = (rc == 0 and cnt == 2)
    case("mongos-routed sync succeeds normally", behaved_as_mongos_routed,
         f"rc={rc} dest_count={cnt} (expect rc=0, dest=2)")
    if not behaved_as_mongos_routed:
        failures += 1

    # Case C: filter off → baseline failure mode.
    print("\nCase C: orphan_document=false (baseline)")
    reset_dest()
    conf = os.path.join(workdir, "C.conf")
    log_dir = os.path.join(workdir, "C_log"); os.makedirs(log_dir, exist_ok=True)
    write_conf(conf, log_dir, with_mongos=False, orphan_filter=False)
    rc, _ = run_collector(binary, conf)
    cnt = dest_count()
    # Expect either a non-zero rc (panic on dup-key) or fewer than 2 docs from
    # interrupted sync. Accepting either captures the baseline failure mode.
    ok = (rc != 0 or cnt < 2)
    case("baseline fails or partial", ok, f"rc={rc} dest_count={cnt}")
    if not ok:
        failures += 1

    shutil.rmtree(workdir, ignore_errors=True)
    if failures:
        print(f"\n{failures} case(s) failed")
        return 1
    print("\nall cases passed")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("usage: ./verify_orphan_filter.py /path/to/collector")
    sys.exit(main(sys.argv[1]))
