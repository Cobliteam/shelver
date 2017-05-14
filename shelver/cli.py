import sys
import os
import argparse
import logging
import asyncio
from functools import partial
from fnmatch import fnmatch
from signal import SIGHUP, SIGINT, SIGTERM

import yaml
from shelver.image import Image
from shelver.provider import Provider
from shelver.build import Coordinator
from shelver.errors import ShelverError
from shelver.util import LoopManager


logger = logging.getLogger('shelver.cli')



def _filter_img(patterns, image):
    return any(fnmatch(image.name, pat) for pat in patterns)


def _build_done(image, fut):
    try:
        artifacts = list(fut.result())
        print('{}: Build succeeded, {} artifacts produced'.format(
            image.name, len(artifacts)))
        for artifact in artifacts:
            print('{}: {}'.format(image.name, artifact.id))
    except ShelverError as e:
        print('{}: Build failed: {}'.format(image.name, e))
    except Exception:
        logger.exception('%s: Build failed with unexpected exception',
                         image.name)


@asyncio.coroutine
def do_build(opts, provider, config):
    if not opts.images:
        opts.images = ['*']

    registry = provider.make_registry(Image.parse_config(config))
    yield from registry.load_existing_artifacts()

    with provider.make_builder(registry,
                               base_dir=opts.base_dir,
                               tmp_dir=opts.tmp_dir,
                               cache_dir=opts.cache_dir,
                               keep_tmp=opts.keep_tmp,
                               packer_cmd=opts.packer_cmd) as builder:
        coordinator = builder.make_coordinator(max_builds=opts.max_builds)
        for name, image in registry.images.items():
            if not _filter_img(opts.images, image):
                continue

            artifact = registry.get_image_artifact(image, default=None)
            if artifact:
                continue

            logger.info('Scheduling build for %s', name)
            build = coordinator.get_or_run_build(image)
            build.add_done_callback(partial(_build_done, image))

        yield from coordinator.run_all()


@asyncio.coroutine
def do_list(opts, provider, config):
    registry = provider.make_registry(Image.parse_config(config))
    yield from registry.load_existing_artifacts()

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


def parse_args():
    args = argparse.ArgumentParser(
        description='Cloud compute image continuous delivery assistant for '
                    'Packer')
    args.add_argument(
        '-p', '--provider', metavar='PROVIDER',
        choices=Provider.available_names())
    args.add_argument(
        '-d', '--base-dir', metavar='DIR',
        help='Base directory to make paths in the config relative to. '
             'If not specified, the directory containing the config will be '
             'used')
    args.add_argument(
        '-c', '--config', metavar='FILE', default='shelver.yml',
        help='Path to configuration file in YAML/Jinja format. '
             'YAML values are templated using Jinja instead of the whole file')
    args.add_argument(
        '-r', '--region', metavar='region',
        help='Use non-default region for providers that support it')
    args.add_argument(
        '-j', '--max-builds', metavar='JOBS', type=int,
        help='Maximum number of concurrent builds to run')
    args.add_argument(
        '--tmp-dir', metavar='DIR',
        help='Override path to store temporary files into')
    args.add_argument(
        '--cache-dir', metavar='DIR',
        help='Override path to store cached files into (between builds)')
    args.add_argument(
        '--keep-tmp', action='store_true', default=False,
        help='Do not delete temporary files after finishing (for debugging)')
    args.add_argument(
        '--packer-cmd', default='packer',
        help='Path to packer executable')

    cmds = args.add_subparsers(dest='command')

    build_cmd = cmds.add_parser('build')
    build_cmd.add_argument(
        'images', nargs='*',
        help='Names of images to build from the config. Can use wildcard '
             'patterns. Images that serve as bases for other images will be '
             'automatically included if any image that requires them is '
             'included, wether they match the patterns or not')
    cmds.add_parser('list')

    return args.parse_args()


def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('shelver').setLevel(logging.DEBUG)
    logging.getLogger('botocore').setLevel(logging.WARN)
    logging.getLogger('boto3').setLevel(logging.WARN)

    opts = parse_args()
    if not opts.base_dir:
        opts.base_dir = os.path.dirname(os.path.abspath(opts.config))

    with open(opts.config, 'rb') as f:
        config = yaml.safe_load(f)

    provider_config = config.pop('provider', {})
    config_provider_name = provider_config.pop('name', None)

    opts.provider = opts.provider or config_provider_name
    if not opts.provider:
        print('Error: no provider specified in command line or config file',
              file=sys.stderr)
        return 1

    ##

    loop = asyncio.get_event_loop()
    with LoopManager(loop):
        provider = Provider.new(opts.provider, config=provider_config, loop=loop)
        if opts.command == 'list':
            run = do_list(opts, provider, config)
        elif opts.command == 'build':
            run = do_build(opts, provider, config)
        else:
            print("Error: invalid command '{}'".format(opts.command),
                  file=sys.stderr)
            return 1

        run_fut = asyncio.ensure_future(run, loop=loop)
        try:
            loop.run_until_complete(run_fut)
            return 0
        except ShelverError as e:
            # Already logged in the _build_done callback
            return 1
        except Exception as e:
            logger.exception('Unexpected exception')
            return 1
