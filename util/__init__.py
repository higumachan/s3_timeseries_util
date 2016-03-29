import logging
import datetime

def datetime2s3path(dt, s3_path):
    """
    translate datime to s3path
    :param dt:
    :param s3_id:
    :return:
    """
    return "{}/{:%Y/%m/%d/%H/%M}".format(s3_path.strip("/"), dt)


def s3path2datetime(s3_path):
    """

    :param s3_path:
    :return:
        datetime
    """
    try:
        timelist = map(int, s3_path.strip("/").rsplit("/", 6)[1:-1]) # parse /path/to/YYYY/MM/DD/hh/mm/ss/filename
    except Exception as e:
        print "invalid path s3_path"
        raise e

    return datetime.datetime(*timelist)