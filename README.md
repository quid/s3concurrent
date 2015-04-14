# Introduction

s3concurrent is designed to upload and download (not too big in size) files with deep file structures out from S3 in a quick and concurrent manner.

Since S3 does not have a real folder structure, all the files are put into buckets using their key names to mimic logical hierarchy.
For example, S3 files with key names a_folder/file_1.png, a_folder/file_2.png, and a_folder/file_3.png are considered to be in a logical folder `a_folder`.

This key-value model grant S3 a easier approach to handle redundancy and direct file access.
However when it comes to reconstructing folder structure from remote, current practices in multiple open source projects traverse through all the
related keys (with prefix filters in boto) multiple times to pinpoint the exact keys/files to upload/download.
This is approach is inefficient for mass file syncing.

s3concurrent takes a different approach.
It loops through all the targeted keys only once, construct local/s3 folder structure as it goes, and en-queue all the to-be-processed keys into a queue.
With multiple (10 by default) different threads, it starts to consume the queue and upload/download all the files into their respective locations.

# Installation

```
pip install s3concurrent
```

# Detailed Usages

s3concurrent brings you a local command you can use directly in your terminals.

For example, you would like to download the entire clone of pip repositories into your local mirror from S3 with 20 threads:

```
s3concurrent_download <your_S3_Key> <your_S3_Secret> <your_S3_bucket_name> --local_folder my_local_mirror_dir --prefix mirror/pypi --thread_count 20
```

When it comes to upload the clone up to a different bucket in S3:

```
s3concurrent_upload <your_S3_Key> <your_S3_Secret> <your_anothger_S3_bucket_name> --local_folder my_local_mirror_dir --prefix mirror/pypi --thread_count 20
```

## Optional
### local_folder

Absolute or relative path to the local folder. The default value is the current directory that you are in.

### prefix

The folder prefix in S3. If your wish to download all the files under folder_a/folder_b in a bucket.
All you have to do here is to specify the prefix to `folder_a/folder_b`

### thread_count

The number of threads that s3concurrent will use to download the files. The default value is 10.

### max_retry

The max times for s3copncurrent to retry uploading/downloading a key