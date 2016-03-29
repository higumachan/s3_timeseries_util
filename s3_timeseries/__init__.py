import logging
import datetime
from itertools import (
    takewhile,
    dropwhile
)
from boto.s3.connection import S3Connection
from collections import namedtuple
from rx.observable import Observable
from rx.subjects.subject import (
    Subject
)
from rx.linq.observable import (
    interval,
    repeat,
    select,
    selectmany,
    fromiterable,
    startswith
)
from util import (
    s3path2datetime
)

KeyWithTimeSeriese = namedtuple(
    "KeyWithTimeSeriese",
    ["key", "datetime"]
)


class S3TimeSeriese(object):
    """
    s3://bucketname/path/to/{year}/{month}/{day}/{hour}/{minute}/{second}/{year}_{month}_{day}_{hour}_{minute}_{second}
    """
    DOWNLOADABLE_DURATION_MIN = 15
    DOWNLOAD_WAIT_SECONDS = 60

    DAY = "DAY"
    HOUR = "HOUR"
    MINUTE = "MINUTE"

    def __init__(self, s3_path, hours, s_date, e_date,
                 position_filepath,
                 access_key, access_secret, bucket_name, s3_host=None,
                 s3_proxy=None, s3_proxy_port=None,
                 search_future_days=7):
        self.s3_path = s3_path
        self.hours = hours
        self.s_date = s_date
        self.e_date = e_date
        self.position_filepath = position_filepath
        self.search_future_days = int(search_future_days)
        self.bucket = self._get_bucket(access_key, access_secret, bucket_name, s3_host, s3_proxy, s3_proxy_port)
        self.raw_stream = Subject()
        self.initialize_downloadable(self.s_date)

    @classmethod
    def _get_bucket(cls, key, secret, bucket_name, host, proxy, proxy_port):
        if host is not None:
            conn = S3Connection(key, secret, host=host, proxy=proxy, proxy_port=proxy_port)
        else:
            conn = S3Connection(key, secret, proxy=proxy, proxy_port=proxy_port)
        return conn.get_bucket(bucket_name)

    def initialize_downloadable(self, current_date):
        def on_next(current_datetime):
            self.raw_stream.on_next(self._fetch_downloadable_files(current_datetime))

        Observable.interval(1000)\
            .zip(
            self.raw_stream,
            self.raw_stream.filter(lambda x: len(x) > 0),
            lambda x, y, z: y[-1].datetime if len(y) > 0 else z[-1]
        )\
            .subscribe(on_next)
        self.raw_stream.on_next([KeyWithTimeSeriese(None, self.s_date)])

        self.downloadable_file_stream = self.raw_stream\
            .flat_map(lambda x: Observable.from_iterable(x))\
            .publish()

    def run(self):
        self.disposable = self.downloadable_file_stream.connect()

    def close(self):
        self.disposable.dispose()

    def _fetch_downloadable_files(self, current_datetime):
        """

        :param _: not use
        :return: downloadable keys
        """
        span_list = [self.MINUTE, self.HOUR, self.DAY]

        for span in span_list:
            fetched_list = list(self._fetch_downloadable_files_detail(current_datetime, span))
            #logging.debug("fetching span current date", span=span, datetime=current_datetime)
            if len(fetched_list) > 0:
                return fetched_list

        for i in range(1, self.search_future_days):
            dt = current_datetime + datetime.timedelta(days=i)
            #logging.debug("fetching span current date", span=span, datetime=dt)
            fetched_list = list(self._fetch_downloadable_files_detail(dt, self.DAY))
            if len(fetched_list) > 0:
                return fetched_list
        return []

    def _fetch_downloadable_files_detail(self, current_datetime, span):
        """

        :param current_date:
        :param span: span element number DAY, HOUR or MINUTE
        :return:
            list of s3path after current_date files
        """
        span2pathformat = {
            self.DAY: "/{:%Y/%m/%d/}",
            self.HOUR: "/{:%Y/%m/%d/%H}",
            self.MINUTE: "/{:%Y/%m/%d/%H/%M}"
        }
        span2timedelta = {
            self.DAY: datetime.timedelta(1),
            self.HOUR: datetime.timedelta(seconds=60 * 60),
            self.MINUTE: datetime.timedelta(seconds=60)
        }

        """
        :param current (current datetime)
        :param span (span in (DAY, HOUR, MINUTE))
        :param now (now time)
        :return list of soreted s3files between now and self.e_date
        """
        def f(current, span, now=current_datetime):
            paths = self.s3_path + span2pathformat[span].format(current)
            files = sorted(map(lambda x: x.key, list(self.bucket.list(paths))))

            files_datetime = map(s3path2datetime, files)
            return takewhile(
                lambda x: x.datetime < self.e_date,
                dropwhile(
                    lambda x: x.datetime <= now,
                    map(lambda x: KeyWithTimeSeriese(*x), zip(files, files_datetime))
                )
            )
        files = list(f(current_datetime, span))
        if len(files) > 0:
            return files
        return list(f(current_datetime + span2timedelta[span], span, current_datetime))

