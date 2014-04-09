__author__ = 'Lloyd Shanks'

import json
import calendar
import re
import os
import math

from pprint import pprint
from boto.s3.connection import S3Connection
from boto.exception import S3ResponseError
from boto.s3.key import Key
from filechunkio import FileChunkIO
from progressbar import *


def main():

    credentials_file = open('./Config/credentials.conf')
    credentials = json.load(credentials_file)
    credentials_file.close()
    #pprint(credentials)

    settings_file = open('./Config/settings.conf')
    settings = json.load(settings_file)
    settings_file.close()
    #pprint(settings)

    local_files = get_local_files(settings)
    #pprint(local_files)
    remote_files = get_remote_files(settings, credentials)
    #pprint(remote_files)
    for filename, details in local_files.items():
        if filename not in remote_files:
            #print filename
            upload_file(filename, details, settings, credentials)


def restart_line():
    sys.stdout.write('\r')
    sys.stdout.flush()


def get_local_files(settings):
    local_files = {}
    p = re.compile('\d\d\d\d_\d\d_\d\d')
    for root, dirs, files in os.walk(settings['backup_directory']):
        for filename in files:
            m = p.search(str(filename))
            if m:
                local_files[filename] = {}
                local_files[filename]['year'] = int(str(filename)[m.start():m.end()][0:4])
                local_files[filename]['month'] = int(str(filename)[m.start():m.end()][5:7])
                local_files[filename]['day'] = int(str(filename)[m.start():m.end()][8:10])
    return local_files


def get_remote_files(settings, credentials):
    remote_files = {}
    # create regular expression for date
    p = re.compile('\d\d\d\d_\d\d_\d\d')

    print 'Connecting to S3...'
    conn = S3Connection(credentials['aws_access_key_id'], credentials['aws_secret_access_key'])
    # test for exception
    s3_backups = conn.get_bucket(settings['s3_bucket'])
    for backup_key in s3_backups.list():
        m = p.search(str(backup_key))
        if m:
            filename = backup_key.key.split('/''')[-1]
            remote_files[filename] = {}
            remote_files[filename]['year'] = int(str(backup_key)[m.start():m.end()][0:4])
            remote_files[filename]['month'] = int(str(backup_key)[m.start():m.end()][5:7])
            remote_files[filename]['day'] = int(str(backup_key)[m.start():m.end()][8:10])
    return remote_files

def upload_file(name, details, settings, credentials):
    conn = S3Connection(credentials['aws_access_key_id'], credentials['aws_secret_access_key'])
    # test for exception
    s3_backups = conn.get_bucket(settings['s3_bucket'])
    k = Key(s3_backups)

    # set up progress bar
    widgets = [name, ': ', Percentage(), ' ', Bar(marker='*', left='[', right=']'),
               ' ', ETA(), ' ', FileTransferSpeed()]


    if calendar.monthrange(details['year'], details['month'])[1] == details['day']:
        k.key = settings['s3_path'] + '/monthly/' + name
    elif calendar.weekday(details['year'], details['month'], details['day']) == 6:
        k.key = settings['s3_path'] + '/weekly/' + name
    else:
        k.key = settings['s3_path'] + '/daily/' + name
    print k.key

    filename = settings['backup_directory'] + name

    mp = s3_backups.initiate_multipart_upload(k.key)

    size = os.stat(filename).st_size
    chunk_size = max(int(math.ceil(size/10000)), 10240)
    num_chunks = int(math.ceil(size / float(chunk_size)))

    pbar = ProgressBar(widgets=widgets, maxval=num_chunks)
    pbar.start()

    offset = 0
    part_id = 1
    while (offset <= size):
        restart_line()

        fp = FileChunkIO(filename, 'r', offset=offset, bytes=chunk_size)

        mp.upload_part_from_file(fp, part_id)

        pbar.update(part_id)
        part_id = part_id+1
        offset = offset + chunk_size + 1
        fp.close()
    mp.complete_upload()
    pbar.finish()


if __name__ == '__main__':
    main()