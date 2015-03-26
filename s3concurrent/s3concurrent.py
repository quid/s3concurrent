#!/usr/bin/env python

import argparse
import os
import ntpath
import time
import threading
import hashlib
import logging
import sys
from Queue import Queue

from boto.s3.connection import S3Connection
from boto.s3.bucket import Bucket

from retrying import retry

# configure logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class DownloadKeyQueue:
    '''
    DownloadKeyQueue implements the queuing functions needed for s3concurrent download.
    '''

    def __init__(self):
        self.downloadable_keys_queue = Queue()
        self.enqueued_counter = 0
        self.de_queue_counter = 0
        self.all_downloaded = False

    def enqueue_key(self, key, destination):
        '''
        Enqueues a key to be downloaded to the local destination.

        :param key:             S3 key object to be downloaded
        :param destination:     (string) the path the file is supposed to be downloaded to
        '''
        self.downloadable_keys_queue.put((key, destination))
        self.enqueued_counter += 1

    def is_empty(self):
        '''
        Checks if the queue is empty.

        :return:                (bool) true if the queue is empty
        '''
        return self.downloadable_keys_queue.empty()

    def de_queue(self):
        '''
        De-queues a key from the queue to be downloaded.

        :return:                a tuple of S3 key object and its local destination in string,
                                None if nothing to de-queue
        '''
        value = None

        if not self.is_empty():
            value = self.downloadable_keys_queue.get()
            self.de_queue_counter += 1

        return value


def enqueue_s3_keys(s3_bucket, prefix, destination_folder, queue):
    '''
    En-queues S3 Keys to be downloaded.

    :param s3_bucket:               Boto Bucket object that contains the keys to be downloaded
    :param prefix:                  The path to the S3 folder to be downloaded, exp bucket_root/folder_1
    :param destination_folder:      The relative or absolute path to the folder you wish to download to
    :param queue:                   A DownloadKeyQueue instance to enqueue all the keys in
    '''
    bucket_list = s3_bucket.list(prefix=prefix)
    for key in bucket_list:
        # prepare local destination structure
        destination = destination_folder + key.name.replace(prefix, '', 1) if prefix else ('/' + key.name)
        try:
            containing_dir = ntpath.dirname(destination)
            if not os.path.exists(containing_dir):
                os.makedirs(containing_dir)
        
            # enqueue
            queue.enqueue_key(key, destination)

        except:
            logger.exception('Cannot enqueue key: {0}'.format(key.name))


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
        logger.exception('Error downloading file with key: {0}'.format(key.name))


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


def consume_download_queue(enqueue_thread, thread_pool_size, queue):
    '''
    Consumes the download queue with the designated thread poll size by downloading the keys to
    their respective destinations.

    :param enqueue_thread:          The thread used to en-queue the download queue. It's used to indicate if
                                    en-queuing is done.
    :param thread_pool_size:        The Designated thread pool size. (how many concurrent threads to download files.)
    :param queue:                   A DownloadKeyQueue instance to consume all the keys from
    '''
    thread_pool = []
    while enqueue_thread.is_alive() or not queue.empty():
        # de-pool the done threads
        for t in thread_pool:
            if not t.is_alive():
                thread_pool.remove(t)

        # en-pool new threads
        if not queue.empty() and len(thread_pool) <= thread_pool_size:
            t = threading.Thread(target=retry_download_key, args=queue.de_queue())
            t.start()
            thread_pool.append(t)


def print_status(queue):
    '''
    Reports the download situation to console.

    :param queue:                   A DownloadKeyQueue instance where all the queuing info is stored
    '''
    while not queue.all_downloaded:
        # report progress every 10 secs
        time.sleep(10)
        logger.info('{0} keys enqueued, and {1} keys downloaded'.format(queue.enqueued_counter, queue.de_queue_counter))


def download_all(s3_key, s3_secret, bucket_name, prefix, destination_folder, queue):
    '''
    Orchestrates the en-queuing and consuming threads in conducting:
    1. Local folder structure construction
    2. S3 key en-queuing
    3. S3 key downloading if file updated

    :param s3_key:                  Your S3 API Key
    :param S3_SECRET:               Your S3 API Secret
    :param bucket_name:             Your S3 bucket name
    :param prefix:                  Your path to the S3 folder to be downloaded, exp bucket_root/folder_1
    :param destination_folder:      The destination folder you are downloading S3 files to
    :param queue:                   A DownloadKeyQueue instance to enqueue all the keys in
    :return:                        True is all downloaded, false if interrupted in any way
    '''
    conn = S3Connection(s3_key, s3_secret)
    bucket = Bucket(connection=conn, name=bucket_name)

    enqueue_thread = threading.Thread(target=enqueue_s3_keys, args=(bucket, prefix, destination_folder, queue))
    enqueue_thread.daemon = True
    enqueue_thread.start()

    report_thread = threading.Thread(target=print_status, args=(queue))
    report_thread.daemon = True
    report_thread.start()

    consume_download_queue(enqueue_thread, 20, queue)

    queue.all_downloaded = True


def main(command_line_args=None):
    parser = argparse.ArgumentParser(prog='s3download')
    parser.add_argument('s3_key', help="Your S3 API Key")
    parser.add_argument('s3_secret', help="Your S3 API Secret")
    parser.add_argument('bucket_name', help="Your S3 bucket name")
    parser.add_argument('--prefix', default=None, help="Your path to the S3 folder to be downloaded, exp bucket_root/folder_1")
    parser.add_argument('--destination_folder', default='.', help="The destination folder you are downloading S3 files to.")

    args = parser.parse_args(command_line_args)

    queue = DownloadKeyQueue()

    if args.s3_key and args.s3_secret and args.bucket_name:
        download_all(args.s3_key, args.s3_secret, args.bucket_name, args.prefix, args.destination_folder, queue)

        if queue.all_downloaded:
            logger.info('All keys are downloaded')
        else:
            logger.info('Download Interrupted')

    return queue.all_downloaded


if __name__ == "__main__":
    main()