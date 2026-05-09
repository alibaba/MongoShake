#!/usr/bin/env python

# -*- coding:utf-8 -*-  

import pymongo
import time
import sys
import getopt


def _safe_close_mongo_cluster(cluster):
    if cluster is None:
        return
    conn = getattr(cluster, "conn", None)
    if conn is None:
        return
    try:
        cluster.close()
    except Exception:
        pass

# constant
COMPARISON_COUNT = "comparison_count"
COMPARISON_MODE = "comparisonMode"
EXCLUDE_DBS = "excludeDbs"
EXCLUDE_COLLS = "excludeColls"
SAMPLE = "sample"
# we don't check collections and index here because sharding's collection(`db.stats`) is splitted.
CheckList = {"objects": 1, "numExtents": 1, "ok": 1}
configure = {}

def log_info(message):
    print("INFO  [%s] %s " % (time.strftime('%Y-%m-%d %H:%M:%S'), message))

def log_error(message):
    print("ERROR [%s] %s " % (time.strftime('%Y-%m-%d %H:%M:%S'), message))

class MongoCluster:

    # pymongo connection
    conn = None

    # connection string
    url = ""

    def __init__(self, url):
        self.url = url

    def connect(self):
        self.conn = pymongo.MongoClient(
            self.url,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
            socketTimeoutMS=30000,
            maxPoolSize=10,
            maxIdleTimeMS=45000
        )

    def close(self):
        self.conn.close()


def filter_check(m):
    new_m = {}
    for k in CheckList:
        new_m[k] = m[k]
    return new_m

"""
    check meta data. include db.collection names and stats()
"""
def check(src, dst):

    #
    # check metadata 
    #
    srcDbNames = src.conn.list_database_names()
    dstDbNames = dst.conn.list_database_names()
    srcDbNames = [db for db in srcDbNames if db not in configure[EXCLUDE_DBS]]
    dstDbNames = [db for db in dstDbNames if db not in configure[EXCLUDE_DBS]]
    if len(srcDbNames) != len(dstDbNames):
        log_error("DIFF => database count not equals src[%s] != dst[%s].\nsrc: %s\ndst: %s" % (len(srcDbNames),
                                                                                              len(dstDbNames),
                                                                                              srcDbNames,
                                                                                              dstDbNames))
        return False
    else:
        log_info("EQUL => database count equals")

    # check database names and collections
    for db in srcDbNames:
        if db in configure[EXCLUDE_DBS]:
            log_info("IGNR => ignore database [%s]" % db)
            continue

        if dstDbNames.count(db) == 0:
            log_error("DIFF => database [%s] only in srcDb" % (db))
            return False

        # db.stats() comparison
        srcDb = src.conn[db] 
        dstDb = dst.conn[db] 
        # srcStats = srcDb.command("dbstats")
        # dstStats = dstDb.command("dbstats")
        #
        # srcStats = filter_check(srcStats)
        # dstStats = filter_check(dstStats)
        #
        # if srcStats != dstStats:
        #     log_error("DIFF => database [%s] stats not equals src[%s], dst[%s]" % (db, srcStats, dstStats))
        #     return False
        # else:
        #     log_info("EQUL => database [%s] stats equals" % db)

        # for collections in db
        srcColls = srcDb.list_collection_names()
        dstColls = dstDb.list_collection_names()
        srcColls = [coll for coll in srcColls if coll not in configure[EXCLUDE_COLLS] and srcColls.count(coll) > 0]
        dstColls = [coll for coll in dstColls if coll not in configure[EXCLUDE_COLLS] and dstColls.count(coll) > 0]
        if len(srcColls) != len(dstColls):
            log_error("DIFF => database [%s] collections count not equals, src[%s], dst[%s]" % (db, srcColls, dstColls))
            return False
        else:
            log_info("EQUL => database [%s] collections count equals" % (db))

        for coll in srcColls:
            if coll in configure[EXCLUDE_COLLS]:
                log_info("IGNR => ignore collection [%s]" % coll)
                continue

            if dstColls.count(coll) == 0:
                log_error("DIFF => collection only in source [%s]" % (coll))
                return False

            srcColl = srcDb[coll]
            dstColl = dstDb[coll]
            # comparison collection records number
            src_count = srcColl.estimated_document_count()
            dst_count = dstColl.estimated_document_count()
            if src_count != dst_count:
                log_error("DIFF => collection [%s] record count not equals: src[%d], dst[%d]" % (coll, src_count, dst_count))
                return False
            else:
                log_info("EQUL => collection [%s] record count equals: %d" % (coll, src_count))

            # comparison collection index number
            src_index_length = len(srcColl.index_information())
            dst_index_length = len(dstColl.index_information())
            if src_index_length != dst_index_length:
                log_error("DIFF => collection [%s] index number not equals: src[%r], dst[%r]" % (coll, src_index_length, dst_index_length))
                return False
            else:
                log_info("EQUL => collection [%s] index number equals" % (coll))

            # check sample data
            if not data_comparison(srcColl, dstColl, configure[COMPARISON_MODE]):
                log_error("DIFF => collection [%s] data comparison not equals" % (coll))
                return False
            else:
                log_info("EQUL => collection [%s] data comparison exactly equals" % (coll))

    return True


"""
    check sample data. comparison every entry
"""
def data_comparison(srcColl, dstColl, mode):
    if mode == "no":
        return True

    src_total_count = srcColl.estimated_document_count()

    if mode == "sample":
        # srcColl.count() must equals to dstColl.count()
        count = min(configure[COMPARISON_COUNT], src_total_count)
    else: # all
        count = src_total_count

    if count == 0:
        return True

    rec_count = count
    batch = 100  # Increased batch size for better performance
    show_progress = (batch * 10)  # Adjust progress frequency
    total = 0

    # Pre-compile the aggregation pipeline
    while count > 0:
        current_batch = min(batch, count)
        # Sample a batch of docs
        docs = list(srcColl.aggregate([{"$sample": {"size": current_batch}}]))

        # Collect all IDs for batch lookup
        doc_ids = [doc["_id"] for doc in docs]

        # Batch find the migrated docs
        migrated_docs = {doc["_id"]: doc for doc in dstColl.find({"_id": {"$in": doc_ids}})}

        # Compare each doc
        for doc in docs:
            migrated = migrated_docs.get(doc["_id"])
            if doc != migrated:
                log_error("DIFF => src_record[%s], dst_record[%s]" % (doc, migrated))
                return False

        total += current_batch
        count -= current_batch

        if total % show_progress == 0 or count == 0:
            log_info("  ... processed %d docs, %.2f%% complete" % (total, total * 100.0 / rec_count))
            

    return True


def usage():
    print('|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|')
    print("| Usage: ./comparison.py --src=localhost:27017/db? --dest=localhost:27018/db? --count=10000 (the sample number) --excludeDbs=admin,local --excludeCollections=system.profile --comparisonMode=sample/all/no (sample: comparison sample number, default; all: comparison all data; no: only comparison outline without data)  |")
    print('|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|')
    print('| Like : ./comparison.py --src="localhost:3001" --dest=localhost:3100  --count=1000  --excludeDbs=admin,local,mongoshake --excludeCollections=system.profile --comparisonMode=sample  |')
    print('|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|')
    exit(0)

if __name__ == "__main__":
    opts, args = getopt.getopt(sys.argv[1:], "hs:d:n:e:x:", ["help", "src=", "dest=", "count=", "excludeDbs=", "excludeCollections=", "comparisonMode="])

    configure[SAMPLE] = True
    configure[EXCLUDE_DBS] = []
    configure[EXCLUDE_COLLS] = []
    srcUrl, dstUrl = "", ""

    for key, value in opts:
        if key in ("-h", "--help"):
            usage()
        if key in ("-s", "--src"):
            srcUrl = value
        if key in ("-d", "--dest"):
            dstUrl = value
        if key in ("-n", "--count"):
            configure[COMPARISON_COUNT] = int(value)
        if key in ("-e", "--excludeDbs"):
            configure[EXCLUDE_DBS] = value.split(",")
        if key in ("-x", "--excludeCollections"):
            configure[EXCLUDE_COLLS] = value.split(",")
        if key in ("--comparisonMode"):
            print(value)
            if value != "all" and value != "no" and value != "sample":
                log_info("comparisonMode[%r] illegal" % (value))
                exit(1)
            configure[COMPARISON_MODE] = value
    if COMPARISON_MODE not in configure:
        configure[COMPARISON_MODE] = "sample"

    # params verify
    if len(srcUrl) == 0 or len(dstUrl) == 0:
        usage()

    # default count is 10000
    if configure.get(COMPARISON_COUNT) is None or configure.get(COMPARISON_COUNT) <= 0:
        configure[COMPARISON_COUNT] = 10000

    # ignore databases
    configure[EXCLUDE_DBS] += ["admin", "local"]
    configure[EXCLUDE_COLLS] += ["system.profile"]

    # dump configuration
    log_info("Configuration [sample=%s, count=%d, excludeDbs=%s, excludeColls=%s]" % (configure[SAMPLE], configure[COMPARISON_COUNT], configure[EXCLUDE_DBS], configure[EXCLUDE_COLLS]))

    src, dst = None, None
    try :
        src, dst = MongoCluster(srcUrl), MongoCluster(dstUrl)
        print("[src = %s]" % srcUrl)
        print("[dst = %s]" % dstUrl)
        src.connect()
        dst.connect()
    except Exception as e:
        print(e)
        log_error("create mongo connection failed %s|%s" % (srcUrl, dstUrl))
        _safe_close_mongo_cluster(src)
        _safe_close_mongo_cluster(dst)
        sys.exit(1)

    try:
        if check(src, dst):
            print("SUCCESS")
            sys.exit(0)
        else:
            print("FAIL")
            sys.exit(-1)
    finally:
        _safe_close_mongo_cluster(src)
        _safe_close_mongo_cluster(dst)
