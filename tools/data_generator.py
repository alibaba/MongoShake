#!/usr/bin/env python

# -*- coding:utf-8 -*-

import traceback
import threading
import random
import pymongo
import time
import os
from random import Random

mongo_url = "mongodb://127.0.0.1:8001"
mongo_database = "generator"

globalLock = threading.Lock()
mark = 0

init_schema_only = False

def log_info(message):
    print "[%s] %s " % (time.strftime('%Y-%m-%d %H:%M:%S'), message)


def random_string(randomlength=8):
    str = ''
    chars = 'AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz0123456789'
    length = len(chars) - 1
    random = Random()
    for i in range(randomlength):
        str += chars[random.randint(0, length)]
    return str


def worker(collection, index):
    global mark
    global init_schema_only

    # schema :
    #   userId, phone, firstName, lastName, info{col1,col2}
    #

    # create a number of unique index
    collection.create_index([('userId', pymongo.ASCENDING)], unique=True)
    collection.create_index([('firstName', pymongo.ASCENDING), ('lastName', pymongo.DESCENDING)], unique=True)
    collection.create_index([('phone', pymongo.ASCENDING)])
    collection.create_index([('info.col2', pymongo.ASCENDING)], unique=True)

    if init_schema_only:
        log_info("worker-%d only create collection's schema. no datas populate")
    else:
        log_info("worker-%d start to fill up data ")

        # insert lots of records
        success = []
        batch = []
        times = 5000
        for i in range(0, times):
            try:
                userId = random_string()
                batch.append({'userId': userId, 'firstName': "Kobe_%i" % i, 'lastName': "Brant_%d" % (i * 17),
                              'phone': 18911100000 + i, "info": {"col1": i, "col2": i}})
                if len(batch) >= 100:
                    collection.insert_many(batch)
                    batch = []
                success.append(userId)
            except Exception, e:
                log_info("duplicated records insert %d.... just skip" % i)
                print e

        log_info("worker-%d finish filling phase")

        # random update 5k times
        for i in range(0, times * 10):
            try:
                n = random.randint(0, len(success))
                collection.update({'userId': success[n]}, {'userId': success[n], 'firstName': "Michael_%i" % i, 'lastName': "Jorndan_%d" % (i * 13),
                                                           'phone': 17711100000 + i, "info": {"col1": times + n, "col2": times + i * 3}})
            except Exception, e:
                log_info("duplicated records update %d.... just skip" % i)
                print e

        log_info("worker-%d finish random updates")
        log_info("worker-%d complete job")

    globalLock.acquire(1)
    mark = mark - index
    globalLock.release()


if __name__ == "__main__":
    conn = pymongo.MongoClient(mongo_url)
    if len(conn.database_names()) == 0:
        log_info("connect to MongoDB %s failed" % (mongo_url))
        os._exit(0)

    try:
        # purge all data at first !
        log_info("1). drop database")
        conn.drop_database(mongo_database)
        database = conn[mongo_database]
        colls = []
        log_info("2). start worker")
        nWorker = 16
        for i in range(0, nWorker):
            mark = mark + i
        for i in range(0, nWorker):
            # new real worker
            collection = database["test%d" % i]
            threading._start_new_thread(worker, (collection, i))
            colls.append(collection)
        log_info("3). wait complete")
        # wait workers
        time.sleep(5)
        while True:
            globalLock.acquire(1)
            quit = (mark == 0)
            globalLock.release()
            if quit:
                break
    except Exception, e:
        traceback.print_exc()
