#!/usr/bin/env python

# -*- coding:utf-8 -*-  

import pymongo
import time
import random
import sys
import getopt

# constant
COMPARISION_COUNT = "comparison_count"
COMPARISION_MODE = "comparisonMode"
EXCLUDE_DBS = "excludeDbs"
EXCLUDE_COLLS = "excludeColls"
SAMPLE = "sample"
# we don't check collections and index here because sharding's collection(`db.stats`) is splitted.
CheckList = {"objects": 1, "numExtents": 1, "ok": 1}
configure = {}

def log_info(message):
    print "INFO  [%s] %s " % (time.strftime('%Y-%m-%d %H:%M:%S'), message)

def log_error(message):
    print "ERROR [%s] %s " % (time.strftime('%Y-%m-%d %H:%M:%S'), message)

class MongoCluster:

    # pymongo connection
    conn = None

    # connection string
    url = ""

    def __init__(self, url):
        self.url = url

    def connect(self):
        self.conn = pymongo.MongoClient(self.url)

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
    srcDbNames = src.conn.database_names()
    dstDbNames = dst.conn.database_names()
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
        srcColls = srcDb.collection_names()
        dstColls = dstDb.collection_names()
        srcColls = [coll for coll in srcColls if coll not in configure[EXCLUDE_COLLS]]
        dstColls = [coll for coll in dstColls if coll not in configure[EXCLUDE_COLLS]]
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
            if srcColl.count() != dstColl.count():
                log_error("DIFF => collection [%s] record count not equals" % (coll))
                return False
            else:
                log_info("EQUL => collection [%s] record count equals" % (coll))

            # comparison collection index number
            src_index_length = len(srcColl.index_information())
            dst_index_length = len(dstColl.index_information())
            if src_index_length != dst_index_length:
                log_error("DIFF => collection [%s] index number not equals: src[%r], dst[%r]" % (coll, src_index_length, dst_index_length))
                return False
            else:
                log_info("EQUL => collection [%s] index number equals" % (coll))

            # check sample data
            if not data_comparison(srcColl, dstColl, configure[COMPARISION_MODE]):
                log_error("DIFF => collection [%s] data comparison not equals" % (coll))
                return False
            else:
                log_info("EQUL => collection [%s] data data comparison exactly eauals" % (coll))

    return True


"""
    check sample data. comparison every entry
"""
def data_comparison(srcColl, dstColl, mode):
    if mode == "no":
        return True
    elif mode == "sample":
        # srcColl.count() mus::t equals to dstColl.count()
        count = configure[COMPARISION_COUNT] if configure[COMPARISION_COUNT] <= srcColl.count() else srcColl.count()
    else: # all
        count = srcColl.count()

    if count == 0:
        return True

    rec_count = count
    batch = 16
    show_progress = (batch * 64)
    total = 0
    while count > 0:
        # sample a bounch of docs

        docs = srcColl.aggregate([{"$sample": {"size":batch}}])
        while docs.alive:
            doc = docs.next()
            migrated = dstColl.find_one(doc["_id"])
            # both origin and migrated bson is Map . so use ==
            if doc != migrated:
                log_error("DIFF => src_record[%s], dst_record[%s]" % (doc, migrated))
                return False

        total += batch
        count -= batch

        if total % show_progress == 0:
            log_info("  ... process %d docs, %.2f %% !" % (total, total * 100.0 / rec_count))
            

    return True


def usage():
    print '|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|'
    print "| Usage: ./comparison.py --src=localhost:27017/db? --dest=localhost:27018/db? --count=10000 (the sample number) --excludeDbs=admin,local --excludeCollections=system.profile --comparisonMode=sample/all/no (sample: comparison sample number, default; all: comparison all data; no: only comparison outline without data)  |"
    print '|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|'
    print '| Like : ./comparison.py --src="localhost:3001" --dest=localhost:3100  --count=1000  --excludeDbs=admin,local,mongoshake --excludeCollections=system.profile --comparisonMode=sample  |'
    print '|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|'
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
            configure[COMPARISION_COUNT] = int(value)
        if key in ("-e", "--excludeDbs"):
            configure[EXCLUDE_DBS] = value.split(",")
        if key in ("-x", "--excludeCollections"):
            configure[EXCLUDE_COLLS] = value.split(",")
        if key in ("--comparisonMode"):
            print value
            if value != "all" and value != "no" and value != "sample":
                log_info("comparisonMode[%r] illegal" % (value))
                exit(1)
            configure[COMPARISION_MODE] = value
    if COMPARISION_MODE not in configure:
        configure[COMPARISION_MODE] = "sample"

    # params verify
    if len(srcUrl) == 0 or len(dstUrl) == 0:
        usage()

    # default count is 10000
    if configure.get(COMPARISION_COUNT) is None or configure.get(COMPARISION_COUNT) <= 0:
        configure[COMPARISION_COUNT] = 10000

    # ignore databases
    configure[EXCLUDE_DBS] += ["admin", "local"]
    configure[EXCLUDE_COLLS] += ["system.profile"]

    # dump configuration
    log_info("Configuration [sample=%s, count=%d, excludeDbs=%s, excludeColls=%s]" % (configure[SAMPLE], configure[COMPARISION_COUNT], configure[EXCLUDE_DBS], configure[EXCLUDE_COLLS]))

    try :
        src, dst = MongoCluster(srcUrl), MongoCluster(dstUrl)
        print "[src = %s]" % srcUrl
        print "[dst = %s]" % dstUrl
        src.connect()
        dst.connect()
    except Exception, e:
        print e
        log_error("create mongo connection failed %s|%s" % (srcUrl, dstUrl))
        exit()

    if check(src, dst):
        print "SUCCESS"
        exit(0)
    else:
        print "FAIL"
        exit(-1)

    src.close()
    dst.close()


