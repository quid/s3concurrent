from distutils.core import setup

install_requires=[
    "boto>=2.32.1",
    "argparse>=1.3.0",
    "mock==1.0.1",
    "nose==1.3.4"
]

setup(
    name='s3concurrent',
    version='0.2.0',
    author='Quid Inc.',
    author_email='ops@quid.com',
    packages=['s3concurrent'],
    scripts=[],
    url='https://github.com/quid/s3concurrent',
    license='MIT',
    description='A fast S3 downloader/uploader for deep file structures.',
    keywords='s3 download upload tools',
    long_description=open('README.md').read(),
    install_requires=install_requires,
    entry_points={
    'console_scripts': [
        's3concurrent_download=s3concurrent.s3concurrent:s3concurrent_download',
        's3concurrent_upload=s3concurrent.s3concurrent:s3concurrent_upload'
    ]}
)
