#!/usr/bin/env python

import argparse
from Queue import Queue
import os
import ntpath
import time
import threading
import hashlib

from boto.s3.connection import S3Connection
from boto.s3.bucket import Bucket
from retrying import retry

enqueued_counter = 0
de_queue_counter = 0

downloadable_keys_queue = Queue()

all_downloaded = False


def enqueue_s3_keys(s3_bucket, prefix, destination_folder):
    '''
    En-queues S3 Keys to be downloaded.

    :param s3_bucket:               Boto Bucket object that contains the keys to be downloaded
    :param prefix:                  The path to the S3 folder to be downloaded, exp bucket_root/folder_1
    :param destination_folder:      The relative or absolute path to the folder you wish to download to
    '''
    global enqueued_counter

    bucket_list = s3_bucket.list(prefix=prefix)
    for key in bucket_list:
        # prepare local destination structure
        destination = destination_folder + key.name.replace(prefix, '', 1) if prefix else ('/' + key.name)
        folder_hierarchy_builder(destination)
        
        # enqueue
        downloadable_keys_queue.put((key, destination))

        enqueued_counter += 1


def folder_hierarchy_builder(destination):
    '''
    Builds up the folder hierarchy for the destination file path.

    :param destination:         The file path the downloaded file is supposed to be saved to.
    '''
    if not os.path.exists(destination):
        containing_dir = ntpath.dirname(destination)
        if not os.path.exists(containing_dir):
            os.makedirs(containing_dir)


def retry_download_key(key, local_destination_path):
    '''
    Retries Downloading the S3 key into the local destination for 5 times,
    waits 1 sec between each retry.

    :param key:                         The S3 key object.
    :param local_destination_path:      (str), path to download the key to
    '''
    try:
        download_key(key, local_destination_path)
    except:
        print('Error downloading file with key: {0}'.format(key.name))


@retry(stop_max_attempt_number=5, wait_fixed=1000)
def download_key(key, local_destination_path):
    '''
    Downloads the S3 key into the local destination.

    :param key:                         The S3 key object.
    :param local_destination_path:      (str), path to download the key to
    '''
    key.get_contents_to_filename(local_destination_path)


def download_required(key, local_destination_path):
    '''
    Checks if the local file is identical to the S3 key by using the file's md5 hash.

    :param key:                         The S3 key object.
    :param local_destination_path:      (str), path to download the key to
    '''
    download_needed = True
    if os.path.exists(local_destination_path):
        s3_md5 = key.etag
        local_md5 = '"' + hashlib.md5(open(local_destination_path, 'rb').read()).hexdigest() + '"'
        if s3_md5 == local_md5:
            download_needed = False
    
    return download_needed


def consume_download_queue(enqueue_thread, thread_pool_size):
    '''
    Consumes the download queue with the designated thread poll size by downloading the keys to
    their respective destinations.

    :param enqueue_thread:          The thread used to en-queue the download queue. It's used to indicate if
                                    en-queuing is done.
    :param thread_pool_size:        The Designated thread pool size. (how many concurrent threads to download files.)
    '''
    global de_queue_counter

    thread_pool = []
    while enqueue_thread.is_alive() or not downloadable_keys_queue.empty():
        # de-pool the done threads
        for t in thread_pool:
            if not t.is_alive():
                thread_pool.remove(t)

        # en-pool new threads
        if not downloadable_keys_queue.empty() and len(thread_pool) <= thread_pool_size:
            t = threading.Thread(target=retry_download_key, args=downloadable_keys_queue.get())
            t.start()
            thread_pool.append(t)

            de_queue_counter += 1


def print_status():
    '''
    Reports the download situation to console.
    '''
    global all_downloaded
    while not all_downloaded:
        # report progress every 10 secs
        time.sleep(10)        
        print('{0} keys enqueued, and {1} keys downloaded'.format(enqueued_counter, de_queue_counter))


def action(S3_KEY, S3_SECRET, bucket_name, prefix, destination_folder):
    '''
    Orchestrates the en-queuing and consuming threads in conducting:
    1. Local folder structure construction
    2. S3 key en-queuing
    3. S3 key downloading if file updated

    :param S3_KEY:                  Your S3 API Key
    :param S3_SECRET:               Your S3 API Secret
    :param bucket_name:             Your S3 bucket name
    :param prefix:                  Your path to the S3 folder to be downloaded, exp bucket_root/folder_1
    :param destination_folder:      The destination folder you are downloading S3 files to
    :return:                        True is all downloaded, false if interrupted in any way
    '''
    conn = S3Connection(S3_KEY, S3_SECRET)
    bucket = Bucket(connection=conn, name=bucket_name)

    enqueue_thread = threading.Thread(target=enqueue_s3_keys, args=(bucket, prefix, destination_folder))
    enqueue_thread.daemon = True
    enqueue_thread.start()

    report_thread = threading.Thread(target=print_status, args=())
    report_thread.daemon = True
    report_thread.start()

    # take 1 sec to wait for the first batch of keys to be enqueued
    time.sleep(1)

    consume_download_queue(enqueue_thread, 20)

    global all_downloaded
    all_downloaded = True

    if all_downloaded:
        print('All keys are downloaded')
    else:
        print('Download Interrupted')


def main(command_line_args=None):
    parser = argparse.ArgumentParser(prog='s3download')
    parser.add_argument('S3_KEY', help="Your S3 API Key")
    parser.add_argument('S3_SECRET', help="Your S3 API Secret")
    parser.add_argument('bucket_name', help="Your S3 bucket name")
    parser.add_argument('--prefix', default=None, help="Your path to the S3 folder to be downloaded, exp bucket_root/folder_1")
    parser.add_argument('--destination_folder', default='.', help="The destination folder you are downloading S3 files to.")

    args = parser.parse_args(command_line_args)

    if args.S3_KEY and args.S3_SECRET and args.bucket_name:
        action(args.S3_KEY, args.S3_SECRET, args.bucket_name, args.prefix, args.destination_folder)

    return all_downloaded


if __name__ == "__main__":
    main()