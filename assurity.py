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
    credentials_file.close()
    pprint(credentials)

    settings_file = open('./Config/settings.conf')
    settings = json.load(settings_file)
    settings_file.close()
    pprint(settings)

    # create regular expression for date
    p = re.compile('\d\d\d\d_\d\d_\d\d')

    get_backup_keys(settings, credentials)


def get_backup_keys(settings, credentials):
        # create regular expression for date
    p = re.compile('\d\d\d\d_\d\d_\d\d')

    print 'Connecting to S3...'
    conn = S3Connection(credentials['aws_access_key_id'], credentials['aws_secret_access_key'])
    s3_backups = conn.get_bucket(settings['s3_bucket'])
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
            # what day of the week is it
            print calendar.weekday(year, month, day)
            # find the last day of the month
            print calendar.monthrange(year, month)[1]


def upload_file():
    print 'blah'


if __name__ == '__main__':
    main()