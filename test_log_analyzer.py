#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import json
import mock
import unittest

import log_analyzer
from log_analyzer import *

FAKE_STATS = [
    {'count': 1,
     'count_perc': 33.333,
     'time_avg': 5.0,
     'time_max': 5.0,
     'time_med': 5.0,
     'time_perc': 92.593,
     'time_sum': 5.0,
     'url': 'url3'},
    {'count': 1,
     'count_perc': 33.333,
     'time_avg': 0.3,
     'time_max': 0.3,
     'time_med': 0.3,
     'time_perc': 5.556,
     'time_sum': 0.3,
     'url': 'url2'}
]


class TetsLogAnalyzer(unittest.TestCase):

    def test_median_odd(self):
        self.assertAlmostEquals(
            median([1.4, 2.1, 3.0, 4.1, 5.5]),
            3.0)

    def test_median_even(self):
        self.assertAlmostEquals(
            median([1.4, 2.1, 4.1, 5.5]),
            3.1)

    def test_regexp(self):
        test_line = ('1.166.85.48 -  - [30/Jun/2017:03:28:20 +0300] '
                     '"GET /export/appinstall_raw/2017-06-30/ HTTP/1.0" 200 25652'
                     ' "-" "Mozilla/5.0 (Windows; U; Windows NT 6.0; ru; rv:1.9.0.12)'
                     ' Gecko/2009070611 Firefox/3.0.12 (.NET CLR 3.5.30729)" "-" "-" "-" 0.003')
        m = re.match(UI_SHORT_REGEXP, test_line)
        self.assertEqual(m.groupdict(),
                         {'status': '200',
                          'body_bytes_sent': '25652',
                          'remote_user': '-',
                          'request_time': '0.003',
                          'http_referer': '-',
                          'remote_addr': '1.166.85.48',
                          'http_x_forwarded_for': '-',
                          'http_X_REQUEST_ID': '-',
                          'request': 'GET /export/appinstall_raw/2017-06-30/ HTTP/1.0',
                          'http_user_agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.0; ru; rv:1.9.0.12) Gecko/2009070611 Firefox/3.0.12 (.NET CLR 3.5.30729)',
                          'time_local': '30/Jun/2017:03:28:20 +0300',
                          'http_X_RB_USER': '-',
                          'http_x_real_ip': '-'}
                         )

    def test_extract_url_from_request(self):
        request = 'GET /api/v2/banner/25019354 HTTP/1.1'
        self.assertEqual('/api/v2/banner/25019354',
                         log_analyzer._extract_url_from_request(request))

    def test_parse_single_line(self):
        regexp = re.compile(UI_SHORT_REGEXP)
        test_lines = [
            ('bad fmt line',
             None
             ),
            (
                '1.202.56.176 -  - [29/Jun/2017:03:59:15 +0300]'
                ' "0" 400 166 "-" "-" "-" "-" "-" 0.000\n',
                None
            ),
            (
                '1.169.137.128 -  - [30/Jun/2017:03:28:23 +0300] '
                '"GET /api/v2/group/1240146/banners HTTP/1.1" 200 994 "-" '
                '"Configovod" "-" "1498782502-2118016444-4707-10488733" '
                '"712e90144abee9" 0.643\n',
                LogRecord(url="/api/v2/group/1240146/banners",
                          request_time=0.643)
            ),
        ]
        for line, res in test_lines:
            if res:
                self.assertEqual(
                    log_analyzer._parse_single_line(
                        regexp, line),
                    res
                )
            else:
                self.assertRaises(RuntimeError,
                                  log_analyzer._parse_single_line,
                                  regexp, line)

    @mock.patch('os.listdir')
    def test_last_nginx_info_plain(self, mock_listdir):
        mock_listdir.return_value = ['nginx-access-ui.log-20180304',
                                     'nginx-access-ui.log-20180303']
        res = log_analyzer._last_nginx_info('fake_path')
        mock_listdir.assert_called_once_with('fake_path')
        self.assertEqual(res,
                         ('fake_path/nginx-access-ui.log-20180304',
                          False,
                          '2018.03.04'))

    @mock.patch('os.listdir')
    def test_last_nginx_info_gz(self, mock_listdir):
        mock_listdir.return_value = ['1234.txt',
                                     'nginx-access-ui.log-20180302',
                                     'nginx-access-ui.log-20180303.gz']
        res = log_analyzer._last_nginx_info('fake_path')
        mock_listdir.assert_called_once_with('fake_path')
        self.assertEqual(res,
                         ('fake_path/nginx-access-ui.log-20180303.gz',
                          True,
                          '2018.03.03'))

    @mock.patch('log_analyzer.open', return_value='plain_fobj')
    def test_open_plain(self, mock_open):
        res_plain = log_analyzer._open('fake_log', False)
        mock_open.assert_called_once_with('fake_log', 'rb')
        self.assertEqual(res_plain, 'plain_fobj')

    @mock.patch('gzip.open', return_value='gz_fobj')
    def test_open_gz(self, mock_gzip_open):
        res_gz = log_analyzer._open('fake_log', True)
        mock_gzip_open.assert_called_once_with('fake_log', 'rb')
        self.assertEqual(res_gz, 'gz_fobj')

    @mock.patch('log_analyzer._parse_single_line')
    @mock.patch('log_analyzer._open')
    def test_nginx_log_parser(self, mock_open, mock_parse_line):
        ctx = mock_open.return_value
        f_obj = ctx.__enter__.return_value
        f_obj.readlines.return_value = ['line1\n', 'line2']
        records = [
            LogRecord(url='url1',
                      request_time=0.1),
            LogRecord(url='url2',
                      request_time=0.2)
        ]
        mock_parse_line.side_effect = records
        res = list(log_analyzer.nginx_log_parser("fake_log", True, 0.0))
        mock_open.assert_called_once_with("fake_log", True)
        self.assertEqual(mock_parse_line.call_count, 2)
        self.assertEqual(res, records)

    def test_collect_stats(self):
        records = [
            LogRecord(url='url1',
                      request_time=0.1),
            LogRecord(url='url2',
                      request_time=0.3),
            LogRecord(url='url3',
                      request_time=5.0)
        ]
        res = log_analyzer._collect_stats(records, 2)
        self.assertEqual(res, FAKE_STATS)

    def test_create_report_html(self):
        with mock.patch('log_analyzer.open',
                        mock.mock_open(
                            read_data='{"data":\n$table_json}\n'),
                        create=True) as mopen:
            log_analyzer.create_report_html("fake_report_html",
                                            "fake_target_path",
                                            FAKE_STATS)
        mopen.assert_any_call("fake_report_html")
        mopen.assert_any_call("fake_target_path", "w")
        written = mopen().write.call_args[0][0]
        self.assertEqual(json.loads(written)['data'], FAKE_STATS)

    @mock.patch("log_analyzer.os.makedirs")
    @mock.patch("log_analyzer.os.path")
    def test_ensure_report_files_no_template(self, mock_ospath, mock_makedirs):
        # If no template html, raise RuntimeError
        mock_ospath.isfile.side_effect = [False, False]
        self.assertRaises(
            RuntimeError,
            log_analyzer.ensure_report_files,
            "fake_report_html",
            "fake_report_dir",
            "2000.12.12")

    @mock.patch("log_analyzer.os.makedirs")
    @mock.patch.object(log_analyzer.os.path, "isfile")
    @mock.patch.object(log_analyzer.os.path, "isdir")
    def test_ensure_report_files_exist(self, mock_isdir,
                                       mock_isfile, mock_makedirs):
        # If the report file exists, return None
        mock_isfile.side_effect = [True, True]
        self.assertEqual(
            log_analyzer.ensure_report_files(
                "fake_report_html",
                "fake_report_dir",
                "2000.12.12"),
            None
        )
        self.assertEqual(mock_isfile.call_args[0][0],
                         "fake_report_dir/report-2000.12.12.html")

    @mock.patch("log_analyzer.os.makedirs")
    @mock.patch.object(log_analyzer.os.path, "isfile")
    @mock.patch.object(log_analyzer.os.path, "isdir")
    def test_ensure_report_files_ok(self, mock_isdir,
                                    mock_isfile, mock_makedirs):
        # If the report file doesn't exist
        mock_isfile.side_effect = [True, False]
        self.assertEqual(
            log_analyzer.ensure_report_files(
                "fake_report_html",
                "fake_report_dir",
                "2000.12.12"),
            "fake_report_dir/report-2000.12.12.html"
        )


if __name__ == '__main__':
    unittest.main()
