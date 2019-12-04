#!/usr/bin/env python

# -*- coding:utf-8 -*-

import sys
import sys
import json
import time
import getopt
import random
import pymongo
from bson import ObjectId

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
    print("INFO  [%s] %s " % (time.strftime('%Y-%m-%d %H:%M:%S'), message))


def log_error(message):
    print("ERROR [%s] %s " % (time.strftime('%Y-%m-%d %H:%M:%S'), message))


class MongoCluster(object):

    def __init__(self, url):
        self.url = url
        self.conn = None

    def connect(self):
        self.conn = pymongo.MongoClient(self.url)

    def close(self):
        self.conn.close()


# """ start of Json diff module """
class Symbol(object):
    def __init__(self, label):
        self.label = label

    def __repr__(self):
        return self.label

    def __str__(self):
        return "$" + self.label


missing = Symbol('missing')
identical = Symbol('identical')
delete = Symbol('delete')
insert = Symbol('insert')
update = Symbol('update')
add = Symbol('add')
discard = Symbol('discard')
replace = Symbol('replace')
left = Symbol('left')
right = Symbol('right')

_all_symbols_ = [
    missing,
    identical,
    delete,
    insert,
    update,
    add,
    discard,
    replace,
    left,
    right
]

__all__ = [
    'missing',
    'identical',
    'delete',
    'insert',
    'update',
    'add',
    'discard',
    'replace',
    'left',
    'right',
    '_all_symbols_'
]

__version__ = '1.1.2'


# rules
# - keys and strings which start with $ are escaped to $$
# - when source is dict and diff is a dict -> patch
# - when source is list and diff is a list patch dict -> patch
# - else -> replacement

# Python 2 vs 3
PY3 = sys.version_info[0] == 3

if PY3:
    string_types = str


class JsonDumper(object):
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __call__(self, obj, dest=None):
        if dest is None:
            return json.dumps(obj, **self.kwargs)
        else:
            return json.dump(obj, dest, **self.kwargs)


default_dumper = JsonDumper()


class JsonLoader(object):
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __call__(self, src):
        if isinstance(src, string_types):
            return json.loads(src, **self.kwargs)
        else:
            return json.load(src, **self.kwargs)


default_loader = JsonLoader()


class JsonDiffSyntax(object):
    def emit_set_diff(self, a, b, s, added, removed):
        raise NotImplementedError()

    def emit_list_diff(self, a, b, s, inserted, changed, deleted):
        raise NotImplementedError()

    def emit_dict_diff(self, a, b, s, added, changed, removed):
        raise NotImplementedError()

    def emit_value_diff(self, a, b, s):
        raise NotImplementedError()

    def patch(self, a, d):
        raise NotImplementedError()

    def unpatch(self, a, d):
        raise NotImplementedError()


class CompactJsonDiffSyntax(object):
    def emit_set_diff(self, a, b, s, added, removed):
        if s == 0.0 or len(removed) == len(a):
            return {replace: b} if isinstance(b, dict) else b
        else:
            d = {}
            if removed:
                d[discard] = removed
            if added:
                d[add] = added
            return d

    def emit_list_diff(self, a, b, s, inserted, changed, deleted):
        if s == 0.0:
            return {replace: b} if isinstance(b, dict) else b
        elif s == 1.0:
            return {}
        else:
            d = changed
            if inserted:
                d[insert] = inserted
            if deleted:
                d[delete] = [pos for pos, value in deleted]
            return d

    def emit_dict_diff(self, a, b, s, added, changed, removed):
        if s == 0.0:
            return {replace: b} if isinstance(b, dict) else b
        elif s == 1.0:
            return {}
        else:
            changed.update(added)
            if removed:
                changed[delete] = list(removed.keys())
            return changed

    def emit_value_diff(self, a, b, s):
        if s == 1.0:
            return {}
        else:
            return {replace: b} if isinstance(b, dict) else b

    def patch(self, a, d):
        if isinstance(d, dict):
            if not d:
                return a
            if replace in d:
                return d[replace]
            if isinstance(a, dict):
                a = dict(a)
                for k, v in d.items():
                    if k is delete:
                        for kdel in v:
                            del a[kdel]
                    else:
                        av = a.get(k, missing)
                        if av is missing:
                            a[k] = v
                        else:
                            a[k] = self.patch(av, v)
                return a
            elif isinstance(a, (list, tuple)):
                original_type = type(a)
                a = list(a)
                if delete in d:
                    for pos in d[delete]:
                        a.pop(pos)
                if insert in d:
                    for pos, value in d[insert]:
                        a.insert(pos, value)
                for k, v in d.items():
                    if k is not delete and k is not insert:
                        k = int(k)
                        a[k] = self.patch(a[k], v)
                if original_type is not list:
                    a = original_type(a)
                return a
            elif isinstance(a, set):
                a = set(a)
                if discard in d:
                    for x in d[discard]:
                        a.discard(x)
                if add in d:
                    for x in d[add]:
                        a.add(x)
                return a
        return d


class ExplicitJsonDiffSyntax(object):
    def emit_set_diff(self, a, b, s, added, removed):
        if s == 0.0 or len(removed) == len(a):
            return b
        else:
            d = {}
            if removed:
                d[discard] = removed
            if added:
                d[add] = added
            return d

    def emit_list_diff(self, a, b, s, inserted, changed, deleted):
        if s == 0.0:
            return b
        elif s == 1.0:
            return {}
        else:
            d = changed
            if inserted:
                d[insert] = inserted
            if deleted:
                d[delete] = [pos for pos, value in deleted]
            return d

    def emit_dict_diff(self, a, b, s, added, changed, removed):
        if s == 0.0:
            return b
        elif s == 1.0:
            return {}
        else:
            d = {}
            if added:
                d[insert] = added
            if changed:
                d[update] = changed
            if removed:
                d[delete] = list(removed.keys())
            return d

    def emit_value_diff(self, a, b, s):
        if s == 1.0:
            return {}
        else:
            return b


class SymmetricJsonDiffSyntax(object):
    def emit_set_diff(self, a, b, s, added, removed):
        if s == 0.0 or len(removed) == len(a):
            return [a, b]
        else:
            d = {}
            if added:
                d[add] = added
            if removed:
                d[discard] = removed
            return d

    def emit_list_diff(self, a, b, s, inserted, changed, deleted):
        if s == 0.0:
            return [a, b]
        elif s == 1.0:
            return {}
        else:
            d = changed
            if inserted:
                d[insert] = inserted
            if deleted:
                d[delete] = deleted
            return d

    def emit_dict_diff(self, a, b, s, added, changed, removed):
        if s == 0.0:
            return [a, b]
        elif s == 1.0:
            return {}
        else:
            d = changed
            if added:
                d[insert] = added
            if removed:
                d[delete] = removed
            return d

    def emit_value_diff(self, a, b, s):
        if s == 1.0:
            return {}
        else:
            return [a, b]

    def patch(self, a, d):
        if isinstance(d, list):
            _, b = d
            return b
        elif isinstance(d, dict):
            if not d:
                return a
            if isinstance(a, dict):
                a = dict(a)
                for k, v in d.items():
                    if k is delete:
                        for kdel, _ in v.items():
                            del a[kdel]
                    elif k is insert:
                        for kk, vv in v.items():
                            a[kk] = vv
                    else:
                        a[k] = self.patch(a[k], v)
                return a
            elif isinstance(a, (list, tuple)):
                original_type = type(a)
                a = list(a)
                if delete in d:
                    for pos, value in d[delete]:
                        a.pop(pos)
                if insert in d:
                    for pos, value in d[insert]:
                        a.insert(pos, value)
                for k, v in d.items():
                    if k is not delete and k is not insert:
                        k = int(k)
                        a[k] = self.patch(a[k], v)
                if original_type is not list:
                    a = original_type(a)
                return a
            elif isinstance(a, set):
                a = set(a)
                if discard in d:
                    for x in d[discard]:
                        a.discard(x)
                if add in d:
                    for x in d[add]:
                        a.add(x)
                return a
        raise Exception("Invalid symmetric diff")

    def unpatch(self, b, d):
        if isinstance(d, list):
            a, _ = d
            return a
        elif isinstance(d, dict):
            if not d:
                return b
            if isinstance(b, dict):
                b = dict(b)
                for k, v in d.items():
                    if k is delete:
                        for kk, vv in v.items():
                            b[kk] = vv
                    elif k is insert:
                        for kk, vv in v.items():
                            del b[kk]
                    else:
                        b[k] = self.unpatch(b[k], v)
                return b
            elif isinstance(b, (list, tuple)):
                original_type = type(b)
                b = list(b)
                for k, v in d.items():
                    if k is not delete and k is not insert:
                        k = int(k)
                        b[k] = self.unpatch(b[k], v)
                if insert in d:
                    for pos, value in reversed(d[insert]):
                        b.pop(pos)
                if delete in d:
                    for pos, value in reversed(d[delete]):
                        b.insert(pos, value)
                if original_type is not list:
                    b = original_type(b)
                return b
            elif isinstance(b, set):
                b = set(b)
                if discard in d:
                    for x in d[discard]:
                        b.add(x)
                if add in d:
                    for x in d[add]:
                        b.discard(x)
                return b
        raise Exception("Invalid symmetric diff")


builtin_syntaxes = {
    'compact': CompactJsonDiffSyntax(),
    'symmetric': SymmetricJsonDiffSyntax(),
    'explicit': ExplicitJsonDiffSyntax()
}


class JsonDiffer(object):

    class Options(object):
        pass

    def __init__(self, syntax='compact', load=False, dump=False, marshal=False, loader=default_loader, dumper=default_dumper):
        self.options = JsonDiffer.Options()
        self.options.syntax = builtin_syntaxes.get(syntax, syntax)
        self.options.load = load
        self.options.dump = dump
        self.options.marshal = marshal
        self.options.loader = loader
        self.options.dumper = dumper
        self._symbol_map = {
            '$' + symbol.label: symbol
            for symbol in _all_symbols_
        }

    def _list_diff_0(self, C, X, Y, i, j):
        if i > 0 and j > 0:
            d, s = self._obj_diff(X[i-1], Y[j-1])
            if s > 0 and C[i][j] == C[i-1][j-1] + s:
                for annotation in self._list_diff_0(C, X, Y, i-1, j-1):
                    yield annotation
                yield (0, d, j-1, s)
                return
        if j > 0 and (i == 0 or C[i][j-1] >= C[i-1][j]):
            for annotation in self._list_diff_0(C, X, Y, i, j-1):
                yield annotation
            yield (1, Y[j-1], j-1, 0.0)
            return
        if i > 0 and (j == 0 or C[i][j-1] < C[i-1][j]):
            for annotation in self._list_diff_0(C, X, Y, i-1, j):
                yield annotation
            yield (-1, X[i-1], i-1, 0.0)
            return

    def _list_diff(self, X, Y):
        # LCS
        m = len(X)
        n = len(Y)
        # An (m+1) times (n+1) matrix
        C = [[0 for j in range(n+1)] for i in range(m+1)]
        for i in range(1, m+1):
            for j in range(1, n+1):
                _, s = self._obj_diff(X[i-1], Y[j-1])
                # Following lines are part of the original LCS algorithm
                # left in the code in case modification turns out to be problematic
                #if X[i-1] == Y[j-1]:
                #    C[i][j] = C[i-1][j-1] + 1
                #else:
                C[i][j] = max(C[i][j-1], C[i-1][j], C[i-1][j-1] + s)
        inserted = []
        deleted = []
        changed = {}
        tot_s = 0.0
        for sign, value, pos, s in self._list_diff_0(C, X, Y, len(X), len(Y)):
            if sign == 1:
                inserted.append((pos, value))
            elif sign == -1:
                deleted.insert(0, (pos, value))
            elif sign == 0 and s < 1:
                changed[pos] = value
            tot_s += s
        tot_n = len(X) + len(inserted)
        if tot_n == 0:
            s = 1.0
        else:
            s = tot_s / tot_n
        return self.options.syntax.emit_list_diff(X, Y, s, inserted, changed, deleted), s

    def _set_diff(self, a, b):
        removed = a.difference(b)
        added = b.difference(a)
        if not removed and not added:
            return {}, 1.0
        ranking = sorted(
            (
                (self._obj_diff(x, y)[1], x, y)
                for x in removed
                for y in added
            ),
            reverse=True,
            key=lambda x: x[0]
        )
        r2 = set(removed)
        a2 = set(added)
        n_common = len(a) - len(removed)
        s_common = float(n_common)
        for s, x, y in ranking:
            if x in r2 and y in a2:
                r2.discard(x)
                a2.discard(y)
                s_common += s
                n_common += 1
            if not r2 or not a2:
                break
        n_tot = len(a) + len(added)
        s = s_common / n_tot if n_tot != 0 else 1.0
        return self.options.syntax.emit_set_diff(a, b, s, added, removed), s

    def _dict_diff(self, a, b):
        removed = {}
        nremoved = 0
        nadded = 0
        nmatched = 0
        smatched = 0.0
        added = {}
        changed = {}
        for k, v in a.items():
            w = b.get(k, missing)
            if w is missing:
                nremoved += 1
                removed[k] = v
            else:
                nmatched += 1
                d, s = self._obj_diff(v, w)
                if s < 1.0:
                    changed[k] = d
                smatched += 0.5 + 0.5 * s
        for k, v in b.items():
            if k not in a:
                nadded += 1
                added[k] = v
        n_tot = nremoved + nmatched + nadded
        s = smatched / n_tot if n_tot != 0 else 1.0
        return self.options.syntax.emit_dict_diff(a, b, s, added, changed, removed), s

    def _obj_diff(self, a, b):
        if a is b:
            return self.options.syntax.emit_value_diff(a, b, 1.0), 1.0
        if isinstance(a, dict) and isinstance(b, dict):
            return self._dict_diff(a, b)
        elif isinstance(a, tuple) and isinstance(b, tuple):
            return self._list_diff(a, b)
        elif isinstance(a, list) and isinstance(b, list):
            return self._list_diff(a, b)
        elif isinstance(a, set) and isinstance(b, set):
            return self._set_diff(a, b)
        elif a != b:
            return self.options.syntax.emit_value_diff(a, b, 0.0), 0.0
        else:
            return self.options.syntax.emit_value_diff(a, b, 1.0), 1.0

    def diff(self, a, b, fp=None):
        if self.options.load:
            a = self.options.loader(a)
            b = self.options.loader(b)

        d, s = self._obj_diff(a, b)

        if self.options.marshal or self.options.dump:
            d = self.marshal(d)

        if self.options.dump:
            return self.options.dumper(d, fp)
        else:
            return d

    def similarity(self, a, b):
        if self.options.load:
            a = self.options.loader(a)
            b = self.options.loader(b)

        d, s = self._obj_diff(a, b)

        return s

    def patch(self, a, d, fp=None):
        if self.options.load:
            a = self.options.loader(a)
            d = self.options.loader(d)

        if self.options.marshal or self.options.load:
            d = self.unmarshal(d)

        b = self.options.syntax.patch(a, d)

        if self.options.dump:
            return self.options.dumper(b, fp)
        else:
            return b

    def unpatch(self, b, d, fp=None):
        if self.options.load:
            b = self.options.loader(b)
            d = self.options.loader(d)

        if self.options.marshal or self.options.load:
            d = self.unmarshal(d)

        a = self.options.syntax.unpatch(b, d)

        if self.options.dump:
            return self.options.dumper(a, fp)
        else:
            return a


    def _unescape(self, x):
        if isinstance(x, string_types):
            sym = self._symbol_map.get(x, None)
            if sym is not None:
                return sym
            if x.startswith('$'):
                return x[1:]
        return x

    def unmarshal(self, d):
        if isinstance(d, dict):
            return {
                self._unescape(k): self.unmarshal(v)
                for k, v in d.items()
            }
        elif isinstance(d, (list, tuple)):
            return type(d)(
                self.unmarshal(x)
                for x in d
            )
        else:
            return self._unescape(d)

    def _escape(self, o):
        if type(o) is Symbol:
            return "$" + o.label
        if isinstance(o, string_types) and o.startswith('$'):
            return "$" + o
        return o

    def marshal(self, d):
        if isinstance(d, dict):
            return {
                self._escape(k): self.marshal(v)
                for k, v in d.items()
            }
        elif isinstance(d, (list, tuple)):
            return type(d)(
                self.marshal(x)
                for x in d
            )
        else:
            return self._escape(d)


def diff(a, b, fp=None, cls=JsonDiffer, **kwargs):
    return cls(**kwargs).diff(a, b, fp)


def patch(a, d, fp=None, cls=JsonDiffer, **kwargs):
    return cls(**kwargs).patch(a, d, fp)


def similarity(a, b, cls=JsonDiffer, **kwargs):
    return cls(**kwargs).similarity(a, b)


__all__ = [
    "similarity",
    "diff",
    "JsonDiffer",
    "JsonDumper",
    "JsonLoader",
]


jsondiff = JsonDiffer()


class Diff(object):
    def __init__(self, left_value=None, right_value=None):
        self.left = left_value
        self.right = right_value
        self.change = None

    def get_values(self):
        values = {'left': self.left, 'right': self.right}
        if self.left is None or self.right is None:
            return {'error': values}
        return values

    def diff_content(self):
        self.change = jsondiff.diff(self.left, self.right, syntax='explicit', dump=True)
        return self.change

    def get_change_list(self, method):
        try:
            change = jsondiff.diff(self.left, self.right, syntax='explicit', dump=True)
            return json.loads(change)['$' + method]
        except Exception as e:
            return None


def precision_check(src_doc, dst_doc):
    diff_content = jsondiff.diff(src_doc, dst_doc)
    log_error("DIFF DETAIL => src_record_id[ ObjectId(\"%s\") ], diff_content[%s]" % (src_doc['_id'],diff_content))
    return None

# """ end of Json diff module """


def filter_check(m):
    new_m = {}
    for k in CheckList:
        new_m[k] = m[k]
    return new_m


def check(src, dst):
    """
        check meta data. include db.collection names and stats()
    """
    srcDbNames = src.conn.database_names()
    dstDbNames = dst.conn.database_names()
    srcDbNames = [db for db in srcDbNames if db not in configure[EXCLUDE_DBS]]
    dstDbNames = [db for db in dstDbNames if db not in configure[EXCLUDE_DBS]]
    if len(srcDbNames) != len(dstDbNames):
        log_error("DIFF => database count not equals. src[%s], dst[%s]" % (srcDbNames, dstDbNames))
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
        srcStats = srcDb.command("dbstats")
        dstStats = dstDb.command("dbstats")

        srcStats = filter_check(srcStats)
        dstStats = filter_check(dstStats)

        if srcStats != dstStats:
            log_error("DIFF => database [%s] stats not equals src[%s], dst[%s]" % (db, srcStats, dstStats))
            return False
        else:
            log_info("EQUL => database [%s] stats equals" % db)

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
                log_error("DIFF => collection [%s] index number not equals: src[%r], dst[%r]" % (
                    coll, src_index_length, dst_index_length))
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


def data_comparison(srcColl, dstColl, mode):
    """
        check sample data. comparison every entry
    """
    if mode == "no":
        return True
    elif mode == "sample":
        # srcColl.count() mus::t equals to dstColl.count()
        count = configure[COMPARISION_COUNT] if configure[COMPARISION_COUNT] <= srcColl.count() else srcColl.count()
    else:  # all
        count = srcColl.count()

    if count == 0:
        return True

    rec_count = count
    batch = 16
    show_progress = (batch * 64)
    total = 0
    while count > 0:
        # sample a bounch of docs

        docs = srcColl.aggregate([{"$sample": {"size": batch}}])
        while docs.alive:
            doc = next(docs)
            migrated = dstColl.find_one(doc["_id"])
            # both origin and migrated bson is Map . so use ==
            if doc != migrated:
                log_error("DIFF => src_record[%s], dst_record[%s]" % (doc, migrated))
                precision_check(doc, migrated)
                return False

        total += batch
        count -= batch

        if total % show_progress == 0:
            log_info("  ... process %d docs, %.2f %% !" % (total, total * 100.0 / rec_count))

    return True


def usage():
    print("|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|")
    print("| Usage: ./comparison.py --src=\"localhost:27017/db?\" --dest=\"localhost:27018/db?\" --count=10000 (the sample number) --excludeDbs=admin,local --excludeCollections=system.profile --comparisonMode=sample/all/no (sample: comparison sample number, default; all: comparison all data; no: only comparison outline without data)  |")
    print("|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|")
    print("| Like : ./comparison.py --src=\"localhost:3001\" --dest=\"localhost:3100\"  --count=1000  --excludeDbs=admin,local,mongoshake --excludeCollections=system.profile --comparisonMode=sample  |")
    print("|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|")
    print("| python3 ./comparison.py --src="localhost:27017" --dest="localhost:27018"  --count=1000  --excludeDbs=admin,local,mongoshake,config --excludeCollections=system.profile --comparisonMode=sample  |")
    exit(0)


if __name__ == "__main__":
    opts, args = getopt.getopt(
        sys.argv[1:],
        "hs:d:n:e:x:",
        ["help", "src=", "dest=", "count=", "excludeDbs=", "excludeCollections=", "comparisonMode="]
    )

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
            print(value)
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
    log_info("Configuration [sample=%s, count=%d, excludeDbs=%s, excludeColls=%s]" % (
        configure[SAMPLE], configure[COMPARISION_COUNT], configure[EXCLUDE_DBS], configure[EXCLUDE_COLLS]))

    try:
        src, dst = MongoCluster(srcUrl), MongoCluster(dstUrl)
        print("[src = %s]" % srcUrl)
        print("[dst = %s]" % dstUrl)
        src.connect()
        dst.connect()
    except Exception as e:
        print(e, log_error("create mongo connection failed %s|%s" % (srcUrl, dstUrl)))
        exit(-1)

    if check(src, dst):
        print("SUCCESS")
        exit(0)
    else:
        print("FAIL")
        exit(-1)

    src.close()
    dst.close()

