from distutils.core import setup

setup(
    name='S3MassDownloader',
    version='0.1.0',
    author='',
    author_email='',
    packages=['S3MassDownloader'],
    scripts=[],
    url='',
    license='LICENSE.txt',
    description='The Fastest S3 downloader for deep file structures',
    long_description=open('README.txt').read(),
    install_requires=[
        "boto>=2.32.1",
        "argparse>=1.3.0"
    ],
)