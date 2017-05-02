from __future__ import absolute_import, print_function

import os
import argparse
import logging
from fnmatch import fnmatch

import yaml
from shelver.provider import Provider
from shelver.registry import Registry
from shelver.builder import Builder
from shelver.provider.amazon import AmazonProvider

def do_build(opts, provider, registry):
    def filter_img(img):
        return any(fnmatch(img.name, pat) for pat in opts.images)

    if not opts.images:
        opts.images = ['*']

    image_batches = registry.build_order(filter_img)
    with provider.make_builder(registry,
                               base_dir=opts.base_dir,
                               tmp_dir=opts.tmp_dir,
                               cache_dir=opts.cache_dir,
                               keep_tmp=opts.keep_tmp,
                               packer_cmd=opts.packer_cmd) as builder:
        for batch in image_batches:
            builder.build_all(batch)

def do_list(opts, provider, registry):
    images = sorted(registry.images.items())
    artifacts = set(registry.artifacts.values())

    for name, image in images:
        print('==', name)
        for version, artifact in registry.get_image_versions(image):
            artifacts.remove(artifact)
            print('{}: {}'.format(version, artifact))

        print()

    print('==', 'Unmanaged artifacts')
    for artifact in sorted(artifacts, key=lambda a: a.name):
        print(artifact)

def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('shelver').setLevel(logging.DEBUG)
    logging.getLogger('botocore').setLevel(logging.WARN)
    logging.getLogger('boto3').setLevel(logging.WARN)

    args = argparse.ArgumentParser(description='Cloud compute image continuous '
                                               'delivery assistant for Packer')
    args.add_argument('-p', '--provider', metavar='PROVIDER', required=True,
        choices=Provider.available_names())
    args.add_argument('-d', '--base-dir', metavar='DIR',
        help='Base directory to make paths in the config relative to. '
             'If not specified, the directory containing the config will be '
             'used')
    args.add_argument('-c', '--config', metavar='FILE', default='shelver.yml',
        help='Path to configuration file in YAML/Jinja format. '
             'YAML values are templated using Jinja instead of the whole file')
    args.add_argument('--tmp-dir', metavar='DIR',
        help='Override path to store temporary files into')
    args.add_argument('--cache-dir', metavar='DIR',
        help='Override path to store cached files into (between builds)')
    args.add_argument('--keep-tmp', action='store_true', default=False,
        help='Do not delete temporary files after finishing (for debugging)')
    args.add_argument('--packer-cmd', default='packer',
        help='Path to packer executable')

    cmds = args.add_subparsers(dest='command')

    build_cmd = cmds.add_parser('build')
    build_cmd.add_argument('images', nargs='*',
        help='Names of images to build from the config. Can use wildcard '
             'patterns. Images that serve as bases for other images will be '
             'automatically included if any image that requires them is '
             'included, wether they match the patterns or not')

    list_cmd = cmds.add_parser('list')

    ##

    opts = args.parse_args()
    if not opts.base_dir:
        opts.base_dir = os.path.dirname(os.path.abspath(opts.config))

    provider = Provider.new(opts.provider)
    with open(opts.config, 'rb') as f:
        image_config = yaml.safe_load(f)

    registry = provider.make_registry(image_config)

    ##

    if opts.command == 'list':
        do_list(opts, provider, registry)
    elif opts.command == 'build':
        do_build(opts, provider, registry)
    else:
        print("Error: invalid command '{}'".format(opts.command),
              file=sys.stderr)
        sys.exit(1)
