#!/usr/bin/env python

import mock
import os
import shutil
import time
import unittest

from s3concurrent import s3concurrent

sandbox = os.path.dirname(os.path.realpath(__file__)) + '/sandbox'


class TestS3Concurrent(unittest.TestCase):

    @mock.patch('boto.s3.bucket.Bucket')
    def test_enqueue_s3_keys(self, mocked_bucket_list):
        mock_folder1 = 'a/b/'
        mocked_key1 = mock.Mock()
        mocked_key1.name = mock_folder1 + 'c'

        mock_folder2 = 'b/c/'
        mocked_key2 = mock.Mock()
        mocked_key2.name = mock_folder2 + 'd'

        mock_folder3 = 'c/d/'
        mocked_key3 = mock.Mock()
        mocked_key3.name = mock_folder3 + 'e'

        mocked_bucket = mock.Mock()
        mocked_bucket.list = lambda prefix: [mocked_key1, mocked_key2, mocked_key3]

        queue = s3concurrent.DownloadKeyQueue()

        s3concurrent.enqueue_s3_keys(mocked_bucket, 'test/prefix', sandbox, queue)

        self.assertTrue(os.path.exists(sandbox + mock_folder1))
        self.assertTrue(os.path.exists(sandbox + mock_folder2))
        self.assertTrue(os.path.exists(sandbox + mock_folder3))

        self.assertEquals(queue.enqueued_counter, 3)
        self.assertFalse(queue.is_empty())

        self.assertFalse(queue.is_queuing())

    @mock.patch('boto.s3.bucket.Bucket')
    @mock.patch('ntpath.dirname', side_effect=Exception)
    def test_enqueue_s3_keys_error(self, mocked_dirname, mocked_bucket_list):
        mock_folder1 = 'a/b/'
        mocked_key1 = mock.Mock()
        mocked_key1.name = mock_folder1 + 'c'

        mocked_bucket = mock.Mock()
        mocked_bucket.list = lambda prefix: [mocked_key1]

        queue = s3concurrent.DownloadKeyQueue()

        s3concurrent.enqueue_s3_keys(mocked_bucket, 'test/prefix', sandbox, queue)

        self.assertEquals(queue.enqueued_counter, 0)
        self.assertTrue(queue.is_empty())

        self.assertFalse(queue.is_queuing())

    def test_consume_a_key(self):
        mock_folder1 = 'a/b/'
        mocked_key1 = mock.Mock()
        mocked_key1.name = mock_folder1 + 'c'
        mocked_key1.get_contents_to_filename = mock.Mock()

        queue = s3concurrent.DownloadKeyQueue()
        queue.enqueue_key(mocked_key1, sandbox)

        self.assertEquals(queue.enqueued_counter, 1)

        s3concurrent.consume_a_key(queue)

        self.assertEquals(queue.de_queue_counter, 1)
        self.assertTrue(queue.is_empty())
        mocked_key1.get_contents_to_filename.assert_called_once_with(sandbox)

    def test_consume_a_key_error(self):
        mock_folder1 = 'a/b/'
        mocked_key1 = mock.Mock()
        mocked_key1.name = mock_folder1 + 'c'
        mocked_key1.get_contents_to_filename = mock.Mock()
        mocked_key1.get_contents_to_filename.side_effect = Exception

        queue = s3concurrent.DownloadKeyQueue()
        queue.enqueue_key(mocked_key1, sandbox)

        self.assertEquals(queue.enqueued_counter, 1)

        s3concurrent.consume_a_key(queue)

        self.assertEquals(queue.de_queue_counter, 1)
        mocked_key1.get_contents_to_filename.assert_called_once_with(sandbox)
        self.assertFalse(queue.is_empty())

        self.assertEquals(queue.enqueued_counter, 2)

    def test_download_required(self):
        mocked_key1 = mock.Mock()
        mocked_key1.etag = ''

        mocked_file_path = sandbox + '/a.txt'

        with open(mocked_file_path, 'wb') as f:
            f.write('mocked file')

        download = s3concurrent.download_required(mocked_key1, mocked_file_path)
        self.assertTrue(download)

    def test_download_not_required(self):
        mocked_key1 = mock.Mock()
        mocked_key1.etag = '"de3a2ccff42d63dc60c6955634d122da"'

        mocked_file_path = sandbox + '/a.txt'

        with open(mocked_file_path, 'wb') as f:
            f.write('mocked file')

        download = s3concurrent.download_required(mocked_key1, mocked_file_path)
        self.assertFalse(download)

    @mock.patch('hashlib.md5', side_effect=Exception)
    def test_download_required_error(self, mocked_read_md5):
        mocked_key1 = mock.Mock()
        download = s3concurrent.download_required(mocked_key1, sandbox + '/a.txt')
        self.assertTrue(download)

    @mock.patch('s3concurrent.s3concurrent.consume_a_key')
    def test_consume_download_queue(self, mocked_consume_a_key):
        # mock the function consume_a_key
        # mock the queue
        # check if de-queued and files are stored in places
        mocked_key1 = mock.Mock()
        mocked_key2 = mock.Mock()
        mocked_key3 = mock.Mock()

        queue = s3concurrent.DownloadKeyQueue()

        queue.queuing_started()
        queue.enqueue_key(mocked_key1, sandbox)
        queue.enqueue_key(mocked_key2, sandbox)
        queue.enqueue_key(mocked_key3, sandbox)
        queue.queuing_stopped()

        def mock_dequeue_a_key(queue):
            queue.de_queue_a_key()

        mocked_consume_a_key.side_effect = mock_dequeue_a_key

        self.assertFalse(queue.is_empty())
        self.assertEquals(3, queue.enqueued_counter)

        s3concurrent.consume_download_queue(3, queue)
        time.sleep(0.1)

        self.assertTrue(queue.is_empty())
        self.assertEquals(3, queue.de_queue_counter)

    def setUp(self):
        if os.path.exists(sandbox):
            shutil.rmtree(sandbox)
        os.makedirs(sandbox)

    def tearDown(self):
        shutil.rmtree(sandbox)