#!/usr/bin/env python

# -*- coding:utf-8 -*-  

import os
from os import listdir
from os.path import isfile, join, isdir
import subprocess

go_path="."
"""
    run unit test in recursive
"""
def run_ut(cur_path):
    print os.path.abspath(os.path.curdir), cur_path

    only_files = [f for f in listdir(".") if isfile(join(".", f))]
    only_dirs = [f for f in listdir(".") if isdir(join(".", f))]
    ut_files = [f for f in only_files if "_test.go" in f]
    print only_files, only_dirs, ut_files

    if len(ut_files) != 0:
        # with ut file, need run ut test
        print "----------- run ut test on dir[%s] -----------" % os.path.abspath(os.path.curdir)
        ret = subprocess.call(["go", "test"])
        # subprocess.check_output(["/bin/sh", "-c", "go", "test"])

        print "********************************** %s *************************************" % ("OK" if ret == 0 else "FAIL")
        if ret != 0:
            print "run failed"
            exit(ret)

    for dir in only_dirs:
        print "cd dir[%s]" % dir

        # dfs
        os.chdir(dir)
        run_ut(dir)

        # backtracking
        os.chdir("..")

if __name__ == "__main__":
    root_path = os.path.join("..", "src/mongoshake")
    os.chdir(root_path)
    go_path=os.path.abspath("../..")
    print "GOPATH=%s" % go_path
    #subprocess.call(["export GOPATH=%s" % go_path])
    os.environ['GOPATH'] = go_path
    run_ut(".")

    print "-----------------------------------"
    print "all is well ^_^"
