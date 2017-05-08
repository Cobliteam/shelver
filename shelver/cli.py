import sys
import os
import argparse
import logging
import asyncio
from functools import partial
from fnmatch import fnmatch

import yaml
from shelver.provider import Provider
from shelver.build import Coordinator
from shelver.errors import ShelverError


logger = logging.getLogger('shelver.cli')


def do_build(opts, provider, registry):
    if not opts.images:
        opts.images = ['*']

    loop = asyncio.get_event_loop()
    with provider.make_builder(registry,
                               base_dir=opts.base_dir,
                               tmp_dir=opts.tmp_dir,
                               cache_dir=opts.cache_dir,
                               keep_tmp=opts.keep_tmp,
                               packer_cmd=opts.packer_cmd,
                               loop=loop) as builder:
        if not opts.images:
            opts.images = ['*']

        def filter_img(image):
            return any(fnmatch(image.name, pat) for pat in opts.images)

        def build_done(image, fut):
            try:
                artifacts = fut.result()
                for artifact in artifacts:
                    print('Built succeeded for image {}: {}'.format(
                        image.name, artifact))
            except ShelverError as e:
                print('Build failed for image {}: {}'.format(
                    image.name, e))
            except Exception:
                logger.exception('Build failed with unexpected exception')

        coordinator = Coordinator(builder, max_builds=opts.max_builds,
                                  loop=loop)
        for name, image in registry.images.items():
            if not filter_img(image):
                continue

            logger.info('Scheduling build for %s', name)
            build = coordinator.get_or_run_build(image)
            build.add_done_callback(partial(build_done, image))

        run_all = asyncio.ensure_future(coordinator.run_all())
        try:
            results = loop.run_until_complete(run_all)
            failed = any(f.cancelled() or f.exception()
                         for f in results.values())
            return 1 if failed else 0
        except KeyboardInterrupt:
            print('Received interrupt, stopping tasks', file=sys.stderr)
            run_all.cancel()
            loop.run_forever()
            run_all.exception()
            return 1
        except Exception:
            logger.exception('Unexpected exception')
            run_all.cancel()
            loop.run_forever()
            run_all.exception()
            return 1


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

    return 0


def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('shelver').setLevel(logging.DEBUG)
    logging.getLogger('botocore').setLevel(logging.WARN)
    logging.getLogger('boto3').setLevel(logging.WARN)

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

    ##

    opts = args.parse_args()
    if not opts.base_dir:
        opts.base_dir = os.path.dirname(os.path.abspath(opts.config))

    with open(opts.config, 'rb') as f:
        config = yaml.safe_load(f)

    provider_config = config.pop('provider', {})
    config_provider_name = provider_config.pop('name', None)
    if config_provider_name and not opts.provider:
        opts.provider = config_provider_name
    elif not opts.provider:
        print('Error: no provider specified, and not defined in config file',
              file=sys.stderr)
        return 1

    provider = Provider.new(opts.provider, config=provider_config)
    registry = provider.make_registry(config)

    ##

    if opts.command == 'list':
        return do_list(opts, provider, registry)
    elif opts.command == 'build':
        return do_build(opts, provider, registry)
    else:
        print("Error: invalid command '{}'".format(opts.command),
              file=sys.stderr)
        return 1
