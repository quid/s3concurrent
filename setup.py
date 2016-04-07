from distutils.core import setup
from pip.req import parse_requirements

setup(
    name='s3concurrent',
    version='0.2.2',
    author='Quid Inc.',
    author_email='ops@quid.com',
    packages=['s3concurrent'],
    scripts=[],
    url='https://github.com/quid/s3concurrent',
    license='MIT',
    description='A fast S3 downloader/uploader for deep file structures.',
    keywords='s3 download upload tools',
    long_description=open('README.md').read(),
    install_requires=(str(ir.req) for ir in \
        parse_requirements('requirements.txt', session=False)
    ),
    entry_points={
    'console_scripts': [
        's3concurrent_download=s3concurrent.s3concurrent:s3concurrent_download',
        's3concurrent_upload=s3concurrent.s3concurrent:s3concurrent_upload'
    ]}
)
