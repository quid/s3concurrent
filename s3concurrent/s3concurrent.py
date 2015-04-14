#!/usr/bin/env python

import argparse
import hashlib
import logging
import ntpath
import os
import sys
import time
import threading

from boto.s3.bucket import Bucket
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from Queue import Queue

# configure logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class ProcessKeyQueue:
    '''
    ProcessKeyQueue implements the queuing functions needed for s3concurrent upload/download.
    '''

    def __init__(self):
        self.process_able_keys_queue = Queue()
        self.enqueued_counter = 0
        self.de_queue_counter = 0
        self.all_processed = False
        self.queuing = False

    def enqueue_item(self, key, local_file_path, enqueue_count=1):
        '''
        Enqueues an item to be downloaded to the local destination.

        :param key:                 the S3 key instance to be uploaded or downloaded
        :param local_file_path:     the local file path corresponding to the s3 key
        :param enqueue_count:       the count that the same key is enqueued
        '''
        self.process_able_keys_queue.put((key, local_file_path, enqueue_count))
        self.enqueued_counter += 1

    def is_empty(self):
        '''
        Checks if the queue is empty.

        :return:                (bool) true if the queue is empty
        '''
        return self.process_able_keys_queue.empty()

    def de_queue_an_item(self):
        '''
        De-queues an item from the queue

        :return:                an item previously enqueued
        '''
        value = None

        if not self.is_empty():
            value = self.process_able_keys_queue.get()
            self.de_queue_counter += 1

        return value

    def is_queuing(self):
        '''
        Checks if queuing all the process-able keys from S3

        :return                 True if still queuing
        '''
        return self.queuing

    def queuing_stopped(self):
        '''
        Stops the queuing from S3.
        '''
        self.queuing = False

    def queuing_started(self):
        '''
        Starts the queuing from S3.
        '''
        self.queuing = True


def enqueue_s3_keys_for_download(s3_bucket, prefix, destination_folder, queue):
    '''
    En-queues S3 Keys to be downloaded.

    :param s3_bucket:               Boto Bucket object that contains the keys to be downloaded
    :param prefix:                  The path to the S3 folder to be downloaded, exp bucket_root/folder_1
    :param destination_folder:      The relative or absolute path to the folder you wish to download to
    :param queue:                   A ProcessKeyQueue instance to enqueue all the keys in
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
            queue.enqueue_item(key, destination)

        except:
            logger.exception('Cannot enqueue key: {0}'.format(key.name))

    logger.info('Initial queuing has completed. {0} keys has been enqueued.'.format(queue.enqueued_counter))
    queue.queuing_stopped()


def enqueue_s3_keys_for_upload(s3_bucket, prefix, from_folder, queue):
    '''
    En-queues S3 Keys to be uploaded.

    :param s3_bucket:               Boto Bucket object that contains the keys to be uploaded to
    :param prefix:                  The path to the S3 folder to be uploaded to, exp bucket_root/folder_1
    :param from_folder:             The relative or absolute path to the folder you wish to upload from
    :param queue:                   A ProcessKeyQueue instance to enqueue all the keys in
    '''
    abs_from_folder_path = os.path.abspath(from_folder)

    for root, dirs, files in os.walk(abs_from_folder_path):
        for single_file in files:
            abs_file_path = os.path.join(root, single_file)

            s3_key_name = abs_file_path.replace(abs_from_folder_path, '', 1)
            if not s3_key_name.startswith('/') and prefix != '':
                s3_key_name = '/' + s3_key_name
            s3_key_name = prefix + s3_key_name

            key = Key(s3_bucket)
            key.key = s3_key_name

            queue.enqueue_item(key, abs_file_path)

    logger.info('Initial queuing has completed. {0} keys has been enqueued.'.format(queue.enqueued_counter))
    queue.queuing_stopped()


def is_sync_needed(key, local_file_path):
    '''
    Checks if the local file is identical to the S3 key by using the file's md5 hash.

    :param key:                         The S3 key object.
    :param local_file_path:             (str), path to download the key to
    '''
    sync_needed = True
    if os.path.exists(local_file_path) and key.exists():
        try:
            key_etag = key.etag
            if not key_etag:
                key_etag = key.bucket.lookup(key.name).etag

            local_md5 = '"' + hashlib.md5(open(local_file_path, 'rb').read()).hexdigest() + '"'
            sync_needed = key_etag != local_md5

        except:
            logger.exception(
                'Cannot compare local file {0} against remote file {1}. s3concurrent will process it anyway.'
                .format(local_file_path, key.name))

    return sync_needed


def process_a_key(queue, action, max_retry):
    '''
    Process (download or upload) a S3 key from/to respective local path.

    :param queue:                   A ProcessKeyQueue instance to de-queue a key from
    :param action:                  download or upload
    :param max_retry:               The max times for s3copncurrent to retry uploading/downloading a key
    '''
    if not queue.is_empty():
        key, local_path, enqueue_count = queue.de_queue_an_item()

        try:

            if is_sync_needed(key, local_path) and enqueue_count <= max_retry:

                # wait accordingly to enqueue_count
                if enqueue_count > 1:
                    wait_time = enqueue_count ** 2
                    logger.info('Attempt no.{0} to {1} {2}. Wait {3} secs.'.format(enqueue_count, action, key.name, wait_time))
                    time.sleep(wait_time)

                # conduct upload/download
                if action == 'download':
                    key.get_contents_to_filename(local_path)
                else:
                    key.set_contents_from_filename(local_path)

            elif enqueue_count > max_retry:
                logger.error('Ignoring {0} since s3concurrent had tried downloading {1} times.'.format(key.name, max_retry))

        except:
            logger.warn('Error {0}ing file with key: {1}, putting it back to the queue'.format(action, key.name))
            queue.enqueue_item(key, local_path, enqueue_count=enqueue_count + 1)

    else:
        # do nothing when the queue is empty
        pass


def consume_queue(queue, action, thread_pool_size, max_retry):
    '''
    Consumes the queue with the designated thread poll size by uploading/downloading the keys to
    their respective destinations.

    :param queue:                   A ProcessKeyQueue instance to consume all the keys from
    :param action:                  download or upload
    :param thread_pool_size:        The Designated thread pool size. (how many concurrent threads to process files.)
    :param max_retry:               The max times for s3copncurrent to retry uploading/downloading a key
    '''
    thread_pool = []

    while queue.is_queuing() or not queue.is_empty() or len(thread_pool) != 0:
        # de-pool the done threads
        for t in thread_pool:
            if not t.is_alive():
                thread_pool.remove(t)

        # en-pool new threads
        if not queue.is_empty() and len(thread_pool) <= thread_pool_size:
            t = threading.Thread(target=process_a_key, args=[queue, action, max_retry])
            t.start()
            thread_pool.append(t)

    queue.all_processed = True


def process_all(action, s3_key, s3_secret, bucket_name, prefix, local_folder, queue, thread_count, max_retry):
    '''
    Orchestrates the en-queuing and consuming threads in conducting:
    1. Local folder structure construction
    2. S3 key en-queuing
    3. S3 key uploading/downloading if file updated

    :param action:                  download or upload
    :param s3_key:                  Your S3 API Key
    :param s3_secret:               Your S3 API Secret
    :param bucket_name:             Your S3 bucket name
    :param prefix:                  Your path to the S3 folder to be uploaded/downloaded, exp bucket_root/folder_1
    :param local_folder:            The local folder you wish to upload/download the files from/to
    :param queue:                   A ProcessKeyQueue instance to enqueue all the keys in
    :param thread_count:            The number of threads that you wish s3concurrent to use
    :param max_retry:               The max times for s3copncurrent to retry uploading/downloading a key
    :return:                        True is all processed, false if interrupted in any way
    '''
    conn = S3Connection(s3_key, s3_secret)
    bucket = Bucket(connection=conn, name=bucket_name)

    if action == 'download':
        target_function = enqueue_s3_keys_for_download
    else:
        target_function = enqueue_s3_keys_for_upload

    enqueue_thread = threading.Thread(target=target_function, args=(bucket, prefix, local_folder, queue))
    enqueue_thread.daemon = True
    enqueue_thread.start()

    queue.queuing_started()

    consume_thread = threading.Thread(target=consume_queue, args=(queue, action, thread_count, max_retry))
    consume_thread.daemon = True
    consume_thread.start()

    while not queue.all_processed:
        # report progress every 10 secs
        logger.info('{0} keys enqueued, and {1} keys {2}ed'.format(queue.enqueued_counter, queue.de_queue_counter, action))
        time.sleep(10)

    logger.info('{0} keys enqueued, and {1} keys {2}ed'.format(queue.enqueued_counter, queue.de_queue_counter, action))


def main(action, command_line_args):
    parser = argparse.ArgumentParser(prog='s3concurrent_{0}'.format(action))
    parser.add_argument('s3_key', help="Your S3 API Key")
    parser.add_argument('s3_secret', help="Your S3 API Secret")
    parser.add_argument('bucket_name', help="Your S3 bucket name")
    parser.add_argument('--prefix', default=None, help="Your path to the S3 folder to be {0}ed, exp bucket_root/folder_1".format(action))
    parser.add_argument('--local_folder', default='.', help="The local folder you are {0}ing S3 files to.".format(action))
    parser.add_argument('--thread_count', default=10, help="The number of threads that you wish s3concurrent to use")
    parser.add_argument('--max_retry', default=10, help="The max times for s3copncurrent to retry uploading/downloading a key")

    args = parser.parse_args(command_line_args)

    queue = ProcessKeyQueue()

    if args.s3_key and args.s3_secret and args.bucket_name:
        process_all(action, args.s3_key, args.s3_secret, args.bucket_name, args.prefix, args.local_folder, queue, int(args.thread_count), int(args.max_retry))

        if queue.all_processed:
            logger.info('All keys are {0}ed'.format(action))
        else:
            logger.info('{0} interrupted'.format(action))

    return queue.all_processed


def s3concurrent_download(command_line_args=None):
    main('download', command_line_args=command_line_args)


def s3concurrent_upload(command_line_args=None):
    main('upload', command_line_args=command_line_args)
