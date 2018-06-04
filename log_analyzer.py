#!/usr/bin/env python
# -*- coding: utf-8 -*-

from array import array
import argparse
import bisect
from collections import namedtuple, defaultdict
from datetime import datetime
import gzip
import json
import logging
from logging import info, error, exception
import os
import yaml
import re

LogRecord = namedtuple('LogRecord', ['url', 'request_time'])
NginxLogInfo = namedtuple('NginxLogInfo', ['fpath', 'is_gz', 'date'])

# log_format ui_short '$remote_addr $remote_user
#                     '$http_x_real_ip [$time_local] "$request" '
#                     '$status $body_bytes_sent "$http_referer" '
#                     '"$http_user_agent" "$http_x_forwarded_for" '
#                     '"$http_X_REQUEST_ID" "$http_X_RB_USER" '
#                     '$request_time';

UI_SHORT_REGEXP = ('(?P<remote_addr>[0-9a-fA-F.:]+) '
                   '(?P<remote_user>\S+)\s+'
                   '(?P<http_x_real_ip>\S+) '
                   '\[(?P<time_local>[^]]+)\] '
                   '"(?P<request>[^"]+)" '
                   '(?P<status>\d+) '
                   '(?P<body_bytes_sent>\d+) '
                   '"(?P<http_referer>[^"]+)" '
                   '"(?P<http_user_agent>[^"]+)" '
                   '"(?P<http_x_forwarded_for>[^"]+)" '
                   '"(?P<http_X_REQUEST_ID>[^"]+)" '
                   '"(?P<http_X_RB_USER>[^"]+)" '
                   '(?P<request_time>[0-9.]*)$')
HTTP_GET_REGEXP = "^[A-Z]+ (\S+) HTTP/\d\.\d"
REPORT_HTML = "report.html"

CONFIG = {
    "REPORT_SIZE": 1000,
    "REPORT_DIR": "./reports",
    "LOG_DIR": "./log",
    "ERROR_RATIO": 0.0
}


def get_config(conf_file):
    if os.path.exists(conf_file):
        with open(conf_file) as f_cfg:
            file_config = yaml.load(f_cfg)
        CONFIG.update(file_config)
    # expand paths
    for var in ['REPORT_DIR', 'LOG_DIR']:
        if var in CONFIG:
            CONFIG[var] = os.path.expanduser(CONFIG[var])
    return CONFIG


def median(lst):
    l = len(lst)
    if l == 0:
        return None
    elif l % 2 == 0:
        return (lst[l / 2 - 1] + lst[l / 2]) / 2.0
    else:
        return lst[l / 2]


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--config",
                   default="config.yaml",
                   help="Config file."
                   )
    return p.parse_args()


def setup_logging(config):
    logging.basicConfig(level=logging.INFO,
                        datefmt='%Y.%m.%d %H:%M:%S',
                        format='[%(asctime)s] %(levelname).1s %(message)s',
                        filename=config.get('MONITORING_LOGFILE'))


def _last_nginx_info(path):
    """
    Last by date matching nginx-access-ui.log-%Y%m%d(.gz)?
    @:returns (fname, is_gz, date) or None if not found
    """
    NGINX_LOG_REGEXP = "nginx-access-ui.log-(?P<date>\d{8})(?P<gz>\.gz)?"
    reg = re.compile(NGINX_LOG_REGEXP)
    for fname in sorted(os.listdir(path), reverse=True):
        m = reg.match(fname)
        if m:
            date = m.group('date')
            is_gz = bool(m.group('gz'))
            try:
                date = datetime.strptime(date, "%Y%m%d").strftime("%Y.%m.%d")
            except ValueError as exc:
                info("Log file %s has bad date format: %s", fname, exc)
                continue
            return NginxLogInfo(fpath=os.path.join(path, fname),
                                is_gz=is_gz,
                                date=date)
    else:
        info("No matching nginx log files under path: ", path)
        return None


def _open(fpath, is_gz):
    if is_gz:
        return gzip.open(fpath, 'rb')
    else:
        return open(fpath, 'rb')


def _extract_url_from_request(request):
    m = re.match(HTTP_GET_REGEXP, request)
    if not m:
        raise RuntimeError("Failed to get url from request: ", request)
    return m.group(1)


def _parse_single_line(record_regexp, line):
    res = record_regexp.match(line)
    if not res:
        raise RuntimeError("Failed to parse line: ", line)
    d = res.groupdict()
    return LogRecord(
        url=_extract_url_from_request(d['request']),
        request_time=float(d['request_time'])
    )


def nginx_log_parser(fpath, is_gz, error_ratio):
    record_regexp = re.compile(UI_SHORT_REGEXP)
    error_cnt = 0
    cnt = 0
    with _open(fpath, is_gz) as f_obj:
        for line in f_obj:
            cnt += 1
            try:
                yield _parse_single_line(record_regexp, line)
            except RuntimeError as exc:
                exception(exc)
                error_cnt += 1
    if error_cnt > error_ratio * cnt:
        raise RuntimeError("Error ratio limit exceeded: ", error_cnt)


def _collect_stats(records, report_size):
    time_list = defaultdict(lambda: array('d'))
    for rec in records:
        bisect.insort_left(
            time_list[rec.url],
            rec.request_time
        )
    stats = {}
    count_total = 0
    time_total = 0.0
    for url, times in time_list.items():
        stats[url] = {
            'count': len(times),
            "time_sum": sum(times),
            "time_max": max(times),
            "time_med": median(times)
        }
        count_total += len(times)
        time_total += sum(times)
    for url in time_list:
        rec = stats[url]
        rec['count_perc'] = 100.0 * rec['count'] / count_total
        rec['time_perc'] = 100.0 * rec['time_sum'] / time_total
        rec['time_avg'] = rec['time_sum'] / rec['count']
    items_list = [dict(url=url, **vals) for url, vals in stats.items()]
    items_list.sort(key=lambda rec: rec['time_sum'], reverse=True)
    stats = items_list[:report_size]
    _round_floats(stats)
    return stats


def _round_floats(stats):
    for rec in stats:
        for field in ['time_sum', 'time_max',
                      'time_med', 'count_perc',
                      'time_perc', 'time_avg']:
            rec[field] = round(rec[field], 3)


def create_report_html(report_html, target_path, stats):
    report_template = open(report_html).read()
    stats_json = json.dumps(stats)
    if "$table_json" not in report_template:
        raise RuntimeError("Report template is not valid!")
    report_template = report_template.replace("$table_json", stats_json)
    with open(target_path, "w") as f:
        f.write(report_template)


def ensure_report_files(report_html, report_dir, date):
    if not os.path.isfile(report_html):
        raise RuntimeError("%s not found" % report_html)
    basename = "report-%s.html" % date
    if not os.path.isdir(report_dir):
        os.makedirs(report_dir)
    target_path = os.path.join(report_dir, basename)
    if os.path.isfile(target_path):
        return None
    return target_path


def main():
    args = parse_args()
    config = get_config(args.config)
    setup_logging(config)
    info(config)

    fpath, is_gz, date = _last_nginx_info(config['LOG_DIR'])
    info("Found log file: %s, is_gz: %s", fpath, is_gz)
    target_path = ensure_report_files(REPORT_HTML,
                                      config['REPORT_DIR'],
                                      date)
    if not target_path:
        info("Log is already parsed: %s", fpath)
        return

    it = nginx_log_parser(fpath, is_gz, config['ERROR_RATIO'])
    stats = _collect_stats(it, config['REPORT_SIZE'])
    create_report_html(REPORT_HTML, target_path, stats)


if __name__ == "__main__":
    try:
        main()
    except (Exception, KeyboardInterrupt) as exc:
        exception(exc)
        raise
