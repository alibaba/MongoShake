#!/usr/bin/env python

# -*- coding:utf-8 -*-  

import getopt
import string

import sys
import pymongo
import time

# constant
EXCLUDE_DBS = "excludeDbs"
EXCLUDE_COLLS = "excludeColls"

AUTO_IGNR_DATABASES = ["admin", "local"]
AUTO_IGNR_TABLES = ["system.profile"]

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


def removeUncheck(m):
    # del m["collections"]
    # del m["indexes"]
    del m["avgObjSize"]
    del m["storageSize"]
    del m["indexSize"]
    del m["objects"]
    del m["dataSize"]
    if m.get('''$gleStats''') is not None:
        del m['''$gleStats''']


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
    srcDbNames.sort()
    dstDbNames.sort()
    if len(srcDbNames) != len(dstDbNames) or len(set(srcDbNames).difference(set(dstDbNames))):
        log_error("DIFF => database count not equals. \nsrc[%s], \ndst[%s]" % (srcDbNames, dstDbNames))
        return False
    log_info("EQUL => database set equals. db_list : [%s]" % string.join(srcDbNames))

    # check database names and collections
    for db in srcDbNames:
        # if db in configure[EXCLUDE_DBS]:
        #     log_info("IGNR => ignore database [%s]" % db)
        #     continue
        #
        # if dstDbNames.count(db) == 0:
        #     log_error("DIFF => database [%s] only in srcDb" % (db))
        #     return False

        # db.stats() comparision
        srcDb = src.conn[db]
        dstDb = dst.conn[db]
        srcStats = srcDb.command("dbstats")
        dstStats = dstDb.command("dbstats")

        removeUncheck(srcStats)
        removeUncheck(dstStats)

        if srcStats != dstStats:
            log_error("DIFF => database [%s] stats not equals \nsrc[%s], \ndst[%s]" % (db, srcStats, dstStats))
            return False
        log_info("EQUL => database [%s] stats() equals" % db)

        # for collections in db
        srcColls = srcDb.collection_names()
        dstColls = dstDb.collection_names()
        srcColls = [coll for coll in srcColls if coll not in configure[EXCLUDE_COLLS]]
        dstColls = [coll for coll in dstColls if coll not in configure[EXCLUDE_COLLS]]
        srcColls.sort()
        dstColls.sort()
        if len(srcColls) != len(dstColls) or len(set(srcColls).difference(set(dstColls))):
            log_error("DIFF => database [%s] collections count not equals, \nsrc[%s], \ndst[%s]" % (db, srcColls, dstColls))
            return False
        log_info("EQUL => database [%s] collections set equals, coll_list : [%s]" % (db, string.join(srcColls)))

        for coll in srcColls:
            srcColl = srcDb[coll]
            dstColl = dstDb[coll]
            srcIndexes, dstIndexes = [], []
            # compare collection indexes
            for index in srcColl.list_indexes():
                srcIndexes.append(index["name"])
            for index in dstColl.list_indexes():
                dstIndexes.append(index["name"])
            if len(set(srcIndexes).difference(set(dstIndexes))):
                log_error("DIFF => collection [%s] has diffrence indexes. \nsrc[%s], \ndst[%s]" % (coll, srcIndexes, dstIndexes))
                return False
            log_info("EQUL => collection [%s] indexes equals. indexes[%s]" % (coll, srcIndexes))

    return True


def usage():
    print "Usage: %s --src=mongodb://localhost:8001/? --dest=mongodb://localhost:8001/? " \
          "--excludeDbs=admin,local --excludeCollections=system.profile" % \
          sys.argv[0]
    exit(0)


if __name__ == "__main__":
    opts, args = getopt.getopt(sys.argv[1:], "hs:n:e:", ["help", "src=", "dest=", "excludeDbs=", "excludeCollections="])

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
        if key in ("-e", "--excludeDbs"):
            configure[EXCLUDE_DBS] = value.split(",")
        if key in ("-x", "--excludeCollections"):
            configure[EXCLUDE_COLLS] = value.split(",")

    # params verify
    if len(srcUrl) == 0 or len(dstUrl) == 0:
        usage()

    # ignore databases
    configure[EXCLUDE_DBS] += AUTO_IGNR_DATABASES
    configure[EXCLUDE_COLLS] += AUTO_IGNR_TABLES

    # dump configuration
    log_info(
        "configuration : excludeDbs=%s, excludeColls=%s" % (configure[EXCLUDE_DBS], configure[EXCLUDE_COLLS]))

    try:
        src, dst = MongoCluster(srcUrl), MongoCluster(dstUrl)
        src.connect()
        dst.connect()
    except Exception, e:
        print e
        log_error("create mongo connection failed src[%s], dest[%s]" % (srcUrl, dstUrl))
        exit()

    if check(src, dst):
        exit(0)
    else:
        exit(-1)

    src.close()
    dst.close()
