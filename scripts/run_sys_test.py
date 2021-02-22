#!/usr/bin/env python

# -*- coding:utf-8 -*-

from gevent import monkey

monkey.patch_all()
import os
import gevent
import pymongo
import platform
import requests
import subprocess
from datetime import datetime
import time

cur_dir = os.path.abspath(os.path.curdir)
test_dir = os.path.join(cur_dir, "run_sys_test_dir")
test_db = "shake_sys_test_db"
test_coll = "shake_sys_test_coll"
test_checkpoint_db = "shake_sys_test_checkpoint"
test_inject_doc_nr = 500
src_oplog_test_url = [
    'mongodb://100.81.164.186:31771,100.81.164.186:31772,100.81.164.186:31773',  # 4.2 replica
]
src_changestream_test_url = [
    # 4.2 replica
    'mongodb://100.81.164.186:31771,100.81.164.186:31772,100.81.164.186:31773',
    # 4.4 sharding: mongos#shards#cs
    'mongodb://100.81.164.181:35309#mongodb://100.81.164.181:35301;mongodb://100.81.164.181:35302#mongodb://100.81.164.181:35306',
    # 4.4 sharding serverless
    'serverless^mongodb://100.81.164.181:36106',
]
dst_test_url = 'mongodb://100.81.164.186:31881,100.81.164.186:31882,100.81.164.186:31883'


# write conf file
def generate_conf(tp, all, id, src_url, dst_url, dir_name, ckpt_time):
    content = {
        "conf.version": "7",
        "id": id,
        "log.dir": test_dir,
        "log.file": id + ".log",
        "log.flush": "true",
        "sync_mode": "all" if all else "incr",  # we only test sync_mode=all
        "tunnel": "direct",
        "tunnel.address": dst_url,
        "filter.namespace.white": '%s.%s' % (test_db, test_coll),
        "mongo_connect_mode": "secondaryPreferred",
        "checkpoint.storage.db": test_checkpoint_db,
        "checkpoint.start_position": "1970-01-01T00:00:00Z" if all else ckpt_time,
        "incr_sync.mongo_fetch_method": tp,
    }
    if "#" in src_url:
        # sharding
        role_lists = src_url.split("#")
        print(role_lists)
        content["mongo_urls"] = role_lists[1]
        content["mongo_cs_url"] = role_lists[2]
        content["mongo_s_url"] = role_lists[0]
    elif "^" in src_url:
        content["special.source.db.flag"] = "aliyun_serverless"
        addr = src_url.split("^")
        print(addr)
        content["mongo_urls"] = addr[1]
    else:
        content["mongo_urls"] = src_url

    file_name = os.path.join(dir_name, id + ".conf")
    f = open(file_name, "w")
    for key, val in content.items():
        f.write(' = '.join([key, val]) + '\n')
    f.close()

    print('generate conf: %r with details:\n%r' % (file_name, content))
    return file_name


def _get_mongodb_addr(src_url):
    if "#" in src_url:
        # sharding
        role_lists = src_url.split("#")
        return role_lists[0]
    elif "^" in src_url:
        return src_url.split("^")[1]
    return src_url


"""
simple test:
1. start routine 1 to inject data into $test_db.$test_coll: 1~2000
2. start routine 2 to run shake
"""


def run_full_sync(conf_name, src_url, dst_url):
    # remove test namespace and checkpoint in mongodb
    src_client = pymongo.MongoClient(_get_mongodb_addr(src_url))
    src_client.get_database(test_db).drop_collection(test_coll)
    src_client.drop_database(test_checkpoint_db)
    dst_client = pymongo.MongoClient(dst_url)
    dst_client.get_database(test_db).drop_collection(test_coll)

    gevent.joinall([
        gevent.spawn(_full_sync_inject_src_and_check, src_url, dst_url, src_client),
        gevent.spawn(_run_shake, conf_name),
    ])
    return None


def _full_sync_inject_src_and_check(src_url, dst_url, client):
    # inject data into source mongodb
    for i in range(test_inject_doc_nr):
        if i % 20 == 0:
            print("injected %r docs" % i)
        client.get_database(test_db).get_collection(test_coll).insert({"x": i})

    # check full sync finish
    while True:
        ret = requests.get("http://127.0.0.1:9101/progress")
        if ret.status_code != 200:
            print(ret.status_code)
            break

        response = ret.json()
        if response["progress"] == "100.00%":
            break

    # wait all sync
    gevent.sleep(3)

    # check target doc number equal
    for i in range(10):
        client = pymongo.MongoClient(dst_url)
        nr = client.get_database(test_db).get_collection(test_coll).count()
        if nr == test_inject_doc_nr:
            print("check %r time(s) target doc number[%r] == source doc number[%r]" % (i + 1, nr, test_inject_doc_nr))
            proc.terminate()
            return None
        print("check %r time(s) target doc number[%r] != source doc number[%r]" % (i + 1, nr, test_inject_doc_nr))
        gevent.sleep(3)

    print("target doc number[%r] != source doc number[%r]" % (nr, test_inject_doc_nr))
    exit_process(1)


def _run_shake(conf_name):
    platform_name = platform.system().lower()
    shake_path = os.path.join(cur_dir, "../bin/", "collector." + platform_name)
    global proc
    proc = subprocess.Popen([shake_path, '-conf=%s' % conf_name])
    if proc.returncode and proc.returncode != 0:
        print("start shake failed: %r" % proc.returncode)
        exit_process(proc.returncode)


"""
simple test:
1. insert checkpoint with current start timestamp, inject data into $test_db.$test_coll: 1~2000
2. start routine 2 to run shake
"""


def run_incr_sync(conf_name, src_url, dst_url):
    src_client = pymongo.MongoClient(_get_mongodb_addr(src_url))
    src_client.get_database(test_db).drop_collection(test_coll)
    src_client.drop_database(test_checkpoint_db)
    dst_client = pymongo.MongoClient(dst_url)
    dst_client.get_database(test_db).drop_collection(test_coll)
    gevent.joinall([
        gevent.spawn(_incr_sync_inject_src_and_check, src_url, dst_url, src_client),
        gevent.spawn(_run_shake, conf_name),
    ])
    return None


def _incr_sync_inject_src_and_check(src_url, dst_url, client):
    # inject data into source mongodb
    for i in range(test_inject_doc_nr):
        x = i + test_inject_doc_nr * 10
        if i % 20 == 0:
            print("injected %r docs" % i)
        client.get_database(test_db).get_collection(test_coll).insert({"x": x})

    # check target doc number equal
    for i in range(10):
        client = pymongo.MongoClient(dst_url)
        nr = client.get_database(test_db).get_collection(test_coll).count()
        if nr == test_inject_doc_nr:
            print("check %r time(s) target doc number[%r] == source doc number[%r]" % (i + 1, nr, test_inject_doc_nr))
            proc.terminate()
            return None
        print("check %r time(s) target doc number[%r] != source doc number[%r]" % (i + 1, nr, test_inject_doc_nr))
        gevent.sleep(3)

    print("target doc number[%r] != source doc number[%r]" % (nr, test_inject_doc_nr))
    exit_process(1)


def exit_process(code):
    if proc:
        proc.terminate()
    exit(code)


def test_full_sync():
    print("test with oplog full_sync")
    for i in range(len(src_oplog_test_url)):
        id = 'oplog_%s' % i
        src_url = src_oplog_test_url[i]
        print("start run oplog full_sync test %s with url[%s]" % (id, src_url))
        # generate conf
        conf = generate_conf('oplog', True, id, src_url, dst_test_url, test_dir, "")
        # run
        run_full_sync(conf, src_url, dst_test_url)

        print("finish run oplog full_sync test %s with url[%s]" % (id, src_url))

    print("test with change_stream full_sync")
    for i in range(len(src_changestream_test_url)):
        id = 'change_stream_%s' % i
        src_url = src_changestream_test_url[i]
        print("start run change_stream full_sync test %s with url[%s]" % (id, src_url))
        # generate conf
        conf = generate_conf('change_stream', True, id, src_url, dst_test_url, test_dir, "")
        # run
        run_full_sync(conf, src_url, dst_test_url)

        print("finish run change_stream full_sync test %s with url[%s]" % (id, src_url))


def test_incr_sync():
    print("test with oplog incr_sync")
    for i in range(len(src_oplog_test_url)):
        time.sleep(10)
        ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        time.sleep(3)

        id = 'oplog_%s' % i
        src_url = src_oplog_test_url[i]
        print("start run oplog incr_sync test %s with url[%s]" % (id, src_url))
        # generate conf
        conf = generate_conf('oplog', False, id, src_url, dst_test_url, test_dir, ts)
        # run
        run_incr_sync(conf, src_url, dst_test_url)

        print("finish run oplog incr_sync test %s with url[%s]" % (id, src_url))

    print("test with change_stream incr_sync")
    for i in range(len(src_changestream_test_url)):
        time.sleep(10)
        ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        time.sleep(3)

        id = 'change_stream_%s' % i
        src_url = src_changestream_test_url[i]
        if "^" in src_url:
            # no need to test serverless
            continue
        print("start run change_stream incr_sync test %s with url[%s]" % (id, src_url))
        # generate conf
        conf = generate_conf('change_stream', False, id, src_url, dst_test_url, test_dir, ts)
        # run
        run_incr_sync(conf, src_url, dst_test_url)

        print("finish run change_stream incr_sync test %s with url[%s]" % (id, src_url))

    return None


if __name__ == "__main__":
    # create test directory if not exists
    if not os.path.isdir(test_dir):
        os.mkdir(test_dir)

    test_full_sync()
    print("---------finish full_sync test---------")

    test_incr_sync()
    print("---------finish incr_sync test---------")
    
    print("sys test: all is well ^_^")
