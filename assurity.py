__author__ = 'lloyd'

import json
import progressbar
import re
import calendar

from pprint import pprint
from boto.s3.connection import S3Connection
from boto.exception import S3ResponseError
from boto.s3.key import Key


def main():

    credentials_file = open('./Config/credentials.conf')
    credentials = json.load(credentials_file)
    pprint(credentials)
    credentials_file.close()

    # create regular expression for date
    p = re.compile('\d\d\d\d_\d\d_\d\d')

    print 'Connecting to S3...'
    conn = S3Connection(credentials['aws_access_key_id'], credentials['aws_secret_access_key'])
    s3_backups = conn.get_bucket('mecca-database-backup')
    for backup_key in s3_backups.list():
        m = p.search(str(backup_key))
        if m:
            print backup_key.bucket
            print backup_key.key
            print backup_key.key.split('/''')
            print backup_key.key.split('/''')[-1]
            print str(backup_key)[m.start():m.end()]
            year = int(str(backup_key)[m.start():m.end()][0:4])
            month = int(str(backup_key)[m.start():m.end()][5:7])
            day = int(str(backup_key)[m.start():m.end()][8:10])
            print calendar.weekday(year, month, day)
            print calendar.monthrange(year, month)[1]


    # this gives array of week, 0 = Monday
    # print calendar.weekday(2014, 04, 02)

    # this gives the last day of the month
    # print calendar.monthrange(2013, 04)[1]



def upload_file():
    print 'blah'


if __name__ == '__main__':
    main()