from s3_timeseries import S3TimeSeriese
import unittest
from nose.tools import ok_, eq_
import datetime
import boto
from boto.s3.key import Key
from moto import mock_s3

class TestPlaybackFilelisting(unittest.TestCase):
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

    def fetch_detail_test(self):
        self.set_files()
        self.create_pb()
        print "nadeko", list(self.pb.bucket.list("record/nadeko/102"))

        eq_(len(self.pb._fetch_downloadable_files_detail(self.current_date, S3TimeSeriese.MINUTE)), 0)
        eq_(len(self.pb._fetch_downloadable_files_detail(self.current_date, S3TimeSeriese.HOUR)), 14)
        eq_(len(self.pb._fetch_downloadable_files_detail(self.current_date, S3TimeSeriese.DAY)), 15 * 24 - 1)

    def fetch_test(self):
        self.set_files()
        self.create_pb()
        eq_(len(self.pb._fetch_downloadable_files(self.current_date)), 14)
        current_date = datetime.datetime(2015, 1, 1, 0, 4, 0)
        eq_(len(self.pb._fetch_downloadable_files(current_date)), 13)
        current_date = datetime.datetime(2015, 1, 1, 0, 8, 0)
        eq_(len(self.pb._fetch_downloadable_files(current_date)), 12)
        current_date = datetime.datetime(2015, 1, 1, 0, 56, 0)
        eq_(len(self.pb._fetch_downloadable_files(current_date)), 15)
        current_date = datetime.datetime(2015, 1, 1, 23, 56, 0)
        print len(list(self.bucket.list("record/nadeko/102/2015/01/02")))
        eq_(len(self.pb._fetch_downloadable_files(current_date)), 15)

    def e_date_test(self):
        self.set_files_mini()
        self.create_pb()
        self.pb.e_date = datetime.datetime(2015, 1, 1, 0, 30, 0)
        eq_(len(self.pb._fetch_downloadable_files(self.current_date)), 7)
        eq_(len(self.pb._fetch_downloadable_files(self.current_date + datetime.timedelta(minutes=30))), 0)