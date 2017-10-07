from setuptools import setup, find_packages

VERSION = '0.1'

setup(
    name='shelver',
    packages=find_packages(),
    version=VERSION,
    description="Continuous deployment assistant to Hashicorp's Packer",
    long_description=open('README.rst').read(),
    url='https://github.com/Cobliteam/shelver',
    download_url='https://github.com/Cobliteam/shelver/archive/{}.tar.gz'.format(VERSION),
    author='Daniel Miranda',
    author_email='daniel@cobli.co',
    license='MPL2',
    install_requires=[
        'pyyaml',
        'Jinja2',
        'boto3',
        'aiofiles',
        'click'
    ],
    scripts=['bin/shelver'],
    keywords='packer aws ami cloud cd continuous-deployment')
