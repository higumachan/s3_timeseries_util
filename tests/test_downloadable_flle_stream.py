from s3_timeseries import S3TimeSeriese, KeyWithTimeSeriese
import unittest
from nose.tools import ok_, eq_
import datetime
import boto
from boto.s3.key import Key
from moto import mock_s3
from rx.linq.observable import (
    toiterable
)
from rx.testing.reactive_assert import (
    are_elements_equal,
    assert_equal
)
import time

class TestPlaybackFilelisting:
    def setUp(self):
        self.mock = mock_s3()
        self.mock.start()
        self.conn = boto.connect_s3("nadeko", "nadeko")
        self.conn.create_bucket('playback_test')
        self.s3_path = "nadeko/102"

    def tearDown(self):
        self.mock.stop()

    def set_files(self):
        self.bucket = self.conn.get_bucket("playback_test")
        self.base = self.s3_path
        self.current_date = datetime.datetime(2015, 1, 1, 0, 0, 0)
        for i in range(360 * 24 * 2):
            k = Key(self.bucket)
            k.key = '{0}/{1:%Y/%m/%d/%H/%M}/{1:%Y_%m_%d_%H_%M_00_000000.ts}'.format(
                self.base,
                self.current_date + datetime.timedelta(minutes=i * 4)
            )
            k.set_contents_from_string('nadeko {}'.format(i))

    def set_files_mini(self):
        self.bucket = self.conn.get_bucket("playback_test")
        self.base = self.s3_path
        self.current_date = datetime.datetime(2015, 1, 1, 0, 0, 0)
        for i in range(100):
            k = Key(self.bucket)
            k.key = '{0}/{1:%Y/%m/%d/%H/%M}/{1:%Y_%m_%d_%H_%M_00_000000.ts}'.format(
                self.base,
                self.current_date + datetime.timedelta(minutes=i * 4)
            )
            k.set_contents_from_string('nadeko {}'.format(i))


    def create_pb(self):
        self.pb = S3TimeSeriese(
            self.s3_path,
            range(0, 24),
            self.current_date,
            self.current_date + datetime.timedelta(days=30),
            "/nadeko",
            "nadeko",
            "nadeko",
            "playback_test",
        )
        self.pb.bucket = self.bucket

    def simple_test(self):
        self.set_files_mini()
        self.create_pb()
        print "nadeko", list(self.pb.bucket.list("record/nadeko/102"))


        def p(x):
            print x
        self.pb.downloadable_file_stream.subscribe(
            p
        )
        self.pb.run()

if __name__ == '__main__':
    t = TestPlaybackFilelisting()
    t.setUp()
    t.simple_test()