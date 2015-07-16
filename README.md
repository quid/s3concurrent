# Introduction

s3concurrent uploads files to or download files from S3.  Features include:

* Handles deep folder structures with many files.  
* Uploads/downloads many files concurrently.
* Maintains folder structure between a S3 bucket and local file system.
* Only uploads/downloads a file when a file has changed between S3 bucket and
local file system.

# Installation

```
git clone https://github.com/quid/s3concurrent.git
pip install s3concurrent/
```

# Usage

## s3concurrent_download

    usage: s3concurrent_download [-h] [--prefix PREFIX]
                           [--local_folder LOCAL_FOLDER]
                           [--thread_count THREAD_COUNT]
                           [--max_retry MAX_RETRY]
                           s3_key s3_secret bucket_name

    positional arguments:
      s3_key                Your S3 API Key
      s3_secret             Your S3 secret key
      bucket_name           Your S3 bucket name

    optional arguments:
      -h, --help            show this help message and exit
      --prefix PREFIX       Path to a folder in the S3 bucket (e.g. my/dest/folder/)
      --local_folder LOCAL_FOLDER
                            Path to a a local filesystem folder (e.g. /my/src/folder)
      --thread_count THREAD_COUNT
                            Number of concurrent files to upload/download
      --max_retry MAX_RETRY
                            Max retries for uploading/downloading a file

## s3concurrent_upload

    usage: s3concurrent_upload [-h] [--prefix PREFIX]
                           [--local_folder LOCAL_FOLDER]
                           [--thread_count THREAD_COUNT]
                           [--max_retry MAX_RETRY]
                           s3_key s3_secret bucket_name

    positional arguments:
      s3_key                Your S3 API Key
      s3_secret             Your S3 secret key
      bucket_name           Your S3 bucket name

    optional arguments:
      -h, --help            show this help message and exit
      --prefix PREFIX       Path to a folder in the S3 bucket (e.g. my/dest/folder/)
      --local_folder LOCAL_FOLDER
                            Path to a a local filesystem folder (e.g. /my/src/folder)
      --thread_count THREAD_COUNT
                            Number of concurrent files to upload/download
      --max_retry MAX_RETRY
                            Max retries for uploading/downloading a file


# Examples

Download files from the folder 'mirror/pypi' in a S3 bucket to a local folder 
'/path/to/mirror/pypi' with 20 concurrent downloads.


```
s3concurrent_download <your_S3_Key> <your_S3_Secret> <your_S3_Bucket> --local_folder /path/to/mirror/pypi --prefix mirror/pypi --thread_count 20
```

Upload files from the folder '/tmp/benchmark' to a 'benchmark' folder on S3 with 
10 concurrent uploads and 3 retries per upload.

```
s3concurrent_upload <your_S3_Key> <your_S3_Secret> <your_S3_Bucket> --local_folder /tmp/benchmark --prefix benchmark --thread_count 10 --max_retry 3
```

