from setuptools import setup, find_packages

VERSION = '0.1'

install_requires = [
    'pyyaml',
    'Jinja2',
    'boto3',
    'aiofiles',
    'click<8',
]

extras_require = {
    'testing': [
        'pytest',
        'pytest-asyncio>=0.6.0',
        'pytest-xdist',
        'coverage',
        'flake8',
        'tox',
    ]
}

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
    install_requires=install_requires,
    extras_require=extras_require,
    scripts=['bin/shelver'],
    keywords='packer aws ami cloud cd continuous-deployment')
