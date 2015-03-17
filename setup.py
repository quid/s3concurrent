from distutils.core import setup

install_requires=[
    "boto>=2.32.1",
    "argparse>=1.3.0"
]

setup(
    name='s3concurrent',
    version='0.1.0',
    author='',
    author_email='',
    packages=['s3concurrent'],
    scripts=[],
    url='',
    license='LICENSE.txt',
    description='The Fastest S3 downloader for deep file structures',
    long_description=open('README.txt').read(),
    install_requires=install_requires,
    entry_points={
    'console_scripts': [
        's3concurrent=s3concurrent.s3concurrent:main',
    ]}
)