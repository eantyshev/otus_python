#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import gzip
import sys
import glob
import logging
import collections
from optparse import OptionParser
# brew install protobuf
# protoc  --python_out=. ./appsinstalled.proto
# pip install protobuf
import appsinstalled_pb2
# pip install python-memcached
import memcache
import functools
import time

import multiprocessing as mp

NPROCESSES = 4

NORMAL_ERR_RATE = 0.01
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])

# TODO:

def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))

def retry(period=10, ntries=5):
    def _wrapped(f):
        @functools.wraps(f)
        def f_with_retries(*args, **kwargs):
            tries_left = ntries
            while True:
                try:
                    return f(*args, **kwargs)
                except Exception as exc:
                    if tries_left:
                        logging.error("Failed, %d retries left: %s", tries_left, exc)
                        tries_left -= 1
                        time.sleep(period)
                    else:
                        raise
        return f_with_retries
    return _wrapped


class MemcLoader(object):
    def __init__(self, memc_addr, dry_run=False):
        self.memc_addr = memc_addr
        self.dry_run = dry_run
        self._memc = None


    def __str__(self):
        return self.memc_addr

    @property
    def memc(self):
        if not self._memc:
            self._memc = memcache.Client([self.memc_addr])
        return self._memc

    @retry(period=1, ntries=10)
    def load(self, key, val):
        if not self.memc.set(key, val):
            self._memc = None
            raise Exception("memcached service is unavail: %s" % self)

    def _construct_protobuf(self, appsinstalled):
        ua = appsinstalled_pb2.UserApps()
        ua.lat = appsinstalled.lat
        ua.lon = appsinstalled.lon
        key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
        ua.apps.extend(appsinstalled.apps)
        packed = ua.SerializeToString()
        return (key, packed)

    def insert_appsinstalled(self, appsinstalled):
        key, packed = self._construct_protobuf(appsinstalled)
        try:
            if self.dry_run:
                logging.debug("%s - %s -> %s" % (self.memc_addr, key, str(packed).replace("\n", " ")))
            else:
                self.load(key, packed)
        except Exception, e:
            logging.exception("Cannot write to memc %s: %s" % (self, e))
            return False
        return True


def parse_appsinstalled(line):
    line_parts = line.strip().split("\t")
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
        logging.info("Not all user apps are digits: `%s`" % line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def worker_func(options, child_conn, processed, errors):
    logging.info("Worker pid %d started...", os.getpid())
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }
    memc_loaders = {key: MemcLoader(addr, options.dry) for key, addr in device_memc.items()}
    while True:
        line = child_conn.recv()
        #logging.info("Worker pid %d recv: %s", os.getpid(), line)
        if line == "quit":
            break

        appsinstalled = parse_appsinstalled(line)
        if not appsinstalled:
            errors.value += 1
            continue
        memc_loader = memc_loaders.get(appsinstalled.dev_type)
        if not memc_loader:
            errors.value += 1
            logging.error("Unknown device type: %s" % appsinstalled.dev_type)
            continue
        ok = memc_loader.insert_appsinstalled(appsinstalled)
        if ok:
            processed.value += 1
        else:
            errors.value += 1
        if processed.value % 1000 == 0:
            sys.stdout.write(".")
            sys.stdout.flush()

    logging.info("Worker pid %d exit", os.getpid())


def main(options):

    for fn in glob.iglob(options.pattern):
        logging.info('Processing %s' % fn)

        processed = mp.Value('i')
        errors = mp.Value('i')

        processes = []
        parent_conns = []
        for iproc in range(NPROCESSES):
            parent_conn, child_conn = mp.Pipe()
            p = mp.Process(
                target=worker_func,
                args=(options, child_conn, processed, errors)
            )
            p.start()
            processes.append(p)
            parent_conns.append(parent_conn)

        fd = gzip.open(fn)
        iproc = 0
        for line in fd:
            iproc = (iproc + 1) % NPROCESSES
            line = line.strip()
            if not line:
                continue
            parent_conns[iproc].send(line)

        for iproc in range(NPROCESSES):
            parent_conns[iproc].send("quit")
            processes[iproc].join()
        logging.info("processed = %d, errors = %d", processed.value, errors.value)
        if processed.value > 0:
            err_rate = float(errors.value) / processed.value
            if err_rate < NORMAL_ERR_RATE:
                logging.info("Acceptable error rate (%s). Successfull load" % err_rate)
            else:
                logging.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))
        fd.close()
        dot_rename(fn)



def prototest():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="/data/appsinstalled/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        main(opts)
    except Exception, e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
