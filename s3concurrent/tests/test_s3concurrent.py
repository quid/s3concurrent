#!/usr/bin/env python

import mock
import os
import shutil
import time
import unittest

from s3concurrent import s3concurrent

sandbox = os.path.dirname(os.path.realpath(__file__)) + '/sandbox/'


class TestS3Concurrent(unittest.TestCase):

    def test_enqueue_s3_keys_for_download(self):

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

        queue = s3concurrent.ProcessKeyQueue()

        s3concurrent.enqueue_s3_keys_for_download(mocked_bucket, 'test/prefix', sandbox, queue)

        self.assertTrue(os.path.exists(sandbox + mock_folder1))
        self.assertTrue(os.path.exists(sandbox + mock_folder2))
        self.assertTrue(os.path.exists(sandbox + mock_folder3))

        self.assertEquals(queue.enqueued_counter, 3)
        self.assertFalse(queue.is_empty())

        self.assertFalse(queue.is_queuing())

    @mock.patch('ntpath.dirname', side_effect=Exception)
    def test_enqueue_s3_keys_for_download_error(self, mocked_dirname):
        mock_folder1 = 'a/b/'
        mocked_key1 = mock.Mock()
        mocked_key1.name = mock_folder1 + 'c'

        mocked_bucket = mock.Mock()
        mocked_bucket.list = lambda prefix: [mocked_key1]

        queue = s3concurrent.ProcessKeyQueue()

        s3concurrent.enqueue_s3_keys_for_download(mocked_bucket, 'test/prefix', sandbox, queue)

        self.assertEquals(queue.enqueued_counter, 0)
        self.assertTrue(queue.is_empty())

        self.assertFalse(queue.is_queuing())

    def test_enqueue_s3_keys_for_upload(self):
        # fake files to be enqueued
        for item in ['a', 'b', 'c']:
            with open(sandbox + '{0}.txt'.format(item), 'wb') as f:
                f.write('mocked file')

        mocked_bucket = mock.Mock()

        queue = s3concurrent.ProcessKeyQueue()

        s3concurrent.enqueue_s3_keys_for_upload(mocked_bucket, 'test/prefix', sandbox, queue)

        self.assertEquals(queue.enqueued_counter, 3)
        self.assertFalse(queue.is_empty())

        self.assertFalse(queue.is_queuing())

    def test_download_a_key(self):
        mock_folder1 = 'a/b/'
        mocked_key1 = mock.Mock()
        mocked_key1.name = mock_folder1 + 'c'
        mocked_key1.get_contents_to_filename = mock.Mock()

        queue = s3concurrent.ProcessKeyQueue()
        queue.enqueue_item(mocked_key1, sandbox)

        self.assertEquals(queue.enqueued_counter, 1)

        s3concurrent.process_a_key(queue, 'download', 1)

        self.assertEquals(queue.de_queue_counter, 1)
        self.assertTrue(queue.is_empty())
        mocked_key1.get_contents_to_filename.assert_called_once_with(sandbox)

    def test_download_a_key_error(self):
        mock_folder1 = 'a/b/'
        mocked_key1 = mock.Mock()
        mocked_key1.name = mock_folder1 + 'c'
        mocked_key1.get_contents_to_filename = mock.Mock()
        mocked_key1.get_contents_to_filename.side_effect = Exception

        queue = s3concurrent.ProcessKeyQueue()
        queue.enqueue_item(mocked_key1, sandbox)

        self.assertEquals(queue.enqueued_counter, 1)

        s3concurrent.process_a_key(queue, 'download', 1)

        self.assertEquals(queue.de_queue_counter, 1)
        mocked_key1.get_contents_to_filename.assert_called_once_with(sandbox)
        self.assertFalse(queue.is_empty())

        self.assertEquals(queue.enqueued_counter, 2)

    def test_upload_a_key(self):
        test_key_name = sandbox + 'test.txt'

        with open(test_key_name, 'wb') as f:
            f.write('mocked file')

        mocked_key1 = mock.Mock()
        mocked_key1.name = test_key_name
        mocked_key1.set_contents_from_filename = mock.Mock()

        queue = s3concurrent.ProcessKeyQueue()
        queue.enqueue_item(mocked_key1, test_key_name)

        self.assertEquals(queue.enqueued_counter, 1)

        s3concurrent.process_a_key(queue, 'upload', 1)

        self.assertEquals(queue.de_queue_counter, 1)
        self.assertTrue(queue.is_empty())
        mocked_key1.set_contents_from_filename.assert_called_once_with(test_key_name)

    def test_upload_a_key_error(self):
        test_key_name = sandbox + 'test.txt'

        mocked_key1 = mock.Mock()
        mocked_key1.name = test_key_name
        mocked_key1.set_contents_from_filename = mock.Mock()
        mocked_key1.set_contents_from_filename.side_effect = Exception

        queue = s3concurrent.ProcessKeyQueue()
        queue.enqueue_item(mocked_key1, sandbox)

        self.assertEquals(queue.enqueued_counter, 1)

        s3concurrent.process_a_key(queue, 'upload', 1)

        self.assertEquals(queue.de_queue_counter, 1)
        mocked_key1.set_contents_from_filename.assert_called_once_with(sandbox)
        self.assertFalse(queue.is_empty())

        self.assertEquals(queue.enqueued_counter, 2)

    def test_is_sync_needed(self):
        mocked_key1 = mock.Mock()
        mocked_key1.etag = ''

        mocked_file_path = sandbox + '/a.txt'

        with open(mocked_file_path, 'wb') as f:
            f.write('mocked file')

        download = s3concurrent.is_sync_needed(mocked_key1, mocked_file_path)
        self.assertTrue(download)

    def test_is_sync_not_needed(self):
        mocked_key1 = mock.Mock()
        mocked_key1.etag = '"de3a2ccff42d63dc60c6955634d122da"'

        mocked_file_path = sandbox + '/a.txt'

        with open(mocked_file_path, 'wb') as f:
            f.write('mocked file')

        download = s3concurrent.is_sync_needed(mocked_key1, mocked_file_path)
        self.assertFalse(download)

    @mock.patch('hashlib.md5', side_effect=Exception)
    def test_is_sync_needed_error(self, mocked_read_md5):
        mocked_key1 = mock.Mock()
        download = s3concurrent.is_sync_needed(mocked_key1, sandbox + '/a.txt')
        self.assertTrue(download)

    @mock.patch('s3concurrent.s3concurrent.process_a_key')
    def test_consume_download_queue(self, mocked_consume_a_key):
        mocked_key1 = mock.Mock()
        mocked_key2 = mock.Mock()
        mocked_key3 = mock.Mock()

        queue = s3concurrent.ProcessKeyQueue()

        queue.queuing_started()
        queue.enqueue_item(mocked_key1, sandbox)
        queue.enqueue_item(mocked_key2, sandbox)
        queue.enqueue_item(mocked_key3, sandbox)
        queue.queuing_stopped()

        def mock_dequeue_a_key(queue, action, max_retry):
            queue.de_queue_an_item()

        mocked_consume_a_key.side_effect = mock_dequeue_a_key

        self.assertFalse(queue.is_empty())
        self.assertEquals(3, queue.enqueued_counter)

        s3concurrent.consume_queue(queue, 'download', 3, 1)
        time.sleep(0.1)

        self.assertTrue(queue.is_empty())
        self.assertEquals(3, queue.de_queue_counter)

    @mock.patch('s3concurrent.s3concurrent.process_a_key')
    def test_consume_upload_queue(self, mocked_consume_a_key):
        mocked_key1 = mock.Mock()
        mocked_key2 = mock.Mock()
        mocked_key3 = mock.Mock()

        queue = s3concurrent.ProcessKeyQueue()

        queue.queuing_started()
        queue.enqueue_item(mocked_key1, sandbox)
        queue.enqueue_item(mocked_key2, sandbox)
        queue.enqueue_item(mocked_key3, sandbox)
        queue.queuing_stopped()

        def mock_dequeue_a_key(queue, action, max_retry):
            queue.de_queue_an_item()

        mocked_consume_a_key.side_effect = mock_dequeue_a_key

        self.assertFalse(queue.is_empty())
        self.assertEquals(3, queue.enqueued_counter)

        s3concurrent.consume_queue(queue, 'upload', 3, 1)
        time.sleep(0.1)

        self.assertTrue(queue.is_empty())
        self.assertEquals(3, queue.de_queue_counter)

    @mock.patch('time.sleep')
    @mock.patch('s3concurrent.s3concurrent.is_sync_needed', return_value=True)
    def test_process_a_key_waiting(self, mocked_is_sync_needed, mocked_sleep):
        mock_folder1 = 'a/b/'
        mocked_key1 = mock.Mock()
        mocked_key1.name = mock_folder1 + 'c'
        mocked_key1.get_contents_to_filename = mock.Mock()

        queue = s3concurrent.ProcessKeyQueue()
        queue.enqueue_item(mocked_key1, sandbox, 2)

        s3concurrent.process_a_key(queue, 'download', 3)

        mocked_sleep.assert_called_once_with(4)

    @mock.patch('time.sleep')
    @mock.patch('s3concurrent.s3concurrent.is_sync_needed', return_value=True)
    def test_process_a_key_max_retry(self, mocked_is_sync_needed, mocked_sleep):
        mock_folder1 = 'a/b/'
        mocked_key1 = mock.Mock()
        mocked_key1.name = mock_folder1 + 'c'
        mocked_key1.get_contents_to_filename = mock.Mock()

        queue = s3concurrent.ProcessKeyQueue()
        queue.enqueue_item(mocked_key1, sandbox, 2)

        s3concurrent.process_a_key(queue, 'download', 1)

        self.assertEquals(0, mocked_sleep.call_count)

    def setUp(self):
        if os.path.exists(sandbox):
            shutil.rmtree(sandbox)
        os.makedirs(sandbox)

    def tearDown(self):
        shutil.rmtree(sandbox)