__author__ = 'Lloyd Shanks'

import json
import calendar
import re
import os
import math
import sys

from boto.s3.connection import S3Connection
from boto.exception import S3ResponseError
from boto.s3.key import Key
from filechunkio import FileChunkIO
from multiprocessing import Pool


def main():
    credentials_file = open('./Config/credentials.conf')
    credentials = json.load(credentials_file)
    credentials_file.close()

    settings_file = open('./Config/settings.conf')
    settings = json.load(settings_file)
    settings_file.close()

    #clean up multipart uploads (temporary)
    conn = S3Connection(credentials['aws_access_key_id'], credentials['aws_secret_access_key'])
    # test for exception
    s3_backups = conn.get_bucket(settings['s3_bucket'])
    for mpu in s3_backups.get_all_multipart_uploads():
        mpu.cancel_upload()
        print 'cancelled'

    local_files = get_local_files(settings)
    remote_files = get_remote_files(settings, credentials)
    for filename, details in local_files.items():
        if filename not in remote_files:
            upload_file(filename, details, settings, credentials)


def progress_line(filename, current, total):
    os.system('cls' if os.name == 'nt' else 'clear')
    percent_done = float(current) / float(total)
    sys.stdout.write(filename + ': ')
    sys.stdout.write('[')
    for i in range(int(math.floor(percent_done * 20))):
        sys.stdout.write('#')
    for i in range(int(math.ceil((1 - percent_done) * 20))):
        sys.stdout.write(' ')
    sys.stdout.write('] ' + str(percent_done * 100) + '%')


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


def upload_part(settings, credentials, multipart_id, part_id, filename, offset, bytes, num_chunks):
    conn = S3Connection(credentials['aws_access_key_id'], credentials['aws_secret_access_key'])
    s3_backups = conn.get_bucket(settings['s3_bucket'])
    for mp in s3_backups.get_all_multipart_uploads():
        if mp.id == multipart_id:
            fp = FileChunkIO(filename, 'r', offset=offset, bytes=bytes)
            mp.upload_part_from_file(fp, part_id)
            fp.close()
            progress_line(filename, part_id, num_chunks)


def upload_file(name, details, settings, credentials):
    conn = S3Connection(credentials['aws_access_key_id'], credentials['aws_secret_access_key'])
    # test for exception
    s3_backups = conn.get_bucket(settings['s3_bucket'])

    k = Key(s3_backups)

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
    print size
    chunk_size = max(int(math.ceil(size / 9999)), 10240)
    print chunk_size
    num_chunks = max(int(math.ceil(size / float(chunk_size))), 1)
    print num_chunks

    pool = Pool(processes=10)
    for chunk in range(num_chunks):
        offset = chunk * chunk_size
        remaining_bytes = size - offset
        part_id = chunk + 1
        num_bytes = min([chunk_size, remaining_bytes])
        pool.apply_async(upload_part, [settings, credentials, mp.id, part_id, filename, offset, num_bytes, num_chunks])
    pool.close()
    pool.join()

    if len(mp.get_all_parts()) == num_chunks:
        mp.complete_upload()
    else:
        mp.cancel_upload()


if __name__ == '__main__':
    main()