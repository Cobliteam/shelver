import sys
import os
import json
import logging
import shlex
import asyncio
from collections import namedtuple
from fnmatch import fnmatch
from functools import wraps
from asyncio.futures import CancelledError, TimeoutError
try:
    from asyncio import ensure_future
except ImportError:
    from asyncio import async as ensure_future

import yaml
import click
from shelver.provider import Provider
from shelver.build import Builder
from shelver.image import Image
from shelver.errors import ShelverError
from shelver.util import AsyncLoopSupervisor


logger = logging.getLogger('shelver.cli')

ShelverContext = namedtuple('ShelverContext', 'loop provider registry base_dir')


@click.group()
@click.option('-p', '--provider', 'provider_name',
              type=click.Choice(Provider.available_names()))
@click.option('-d', '--base-dir',
              type=click.Path(exists=True, file_okay=False, resolve_path=True))
@click.option('-c', '--config-file', default='./shelver.yml',
              type=click.Path(exists=True, dir_okay=False, resolve_path=True, readable=True))
@click.pass_context
def main(ctx, provider_name, base_dir, config_file):
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('shelver').setLevel(logging.DEBUG)
    logging.getLogger('botocore').setLevel(logging.WARN)
    logging.getLogger('boto3').setLevel(logging.WARN)

    if not base_dir:
        base_dir = os.path.dirname(config_file)

    with click.open_file(config_file) as f:
        config = yaml.safe_load(f)
    provider_config = config.pop('provider', {})
    config_provider_name = provider_config.pop('name', None)

    if not provider_name:
        if not config_provider_name:
            ctx.fail('No provider specified in command line or config file')
            return

        provider_name = config_provider_name

    loop = asyncio.get_event_loop()
    provider = Provider.new(provider_name, provider_config, loop=loop)
    registry = provider.make_registry(Image.parse_config(config), loop=loop)

    ctx.obj = ShelverContext(loop=loop, provider=provider, registry=registry, base_dir=base_dir)


def shelver_async_cmd(f):
    f = asyncio.coroutine(f)

    @click.pass_context
    @wraps(f)
    def wrapper(ctx, *args, **kwargs):
        try:
            ret = AsyncLoopSupervisor(ctx.obj.loop).supervise(f(ctx, *args, **kwargs))
            if ret:
                ctx.exit(ret)
        except ShelverError as e:
            ctx.fail(str(e))
        except Exception:
            logger.exception('Unexpected exception')
            ctx.fail('Unexpected exception')

    return wrapper


@main.command()
@click.argument(
    'image_patterns', metavar='IMAGES', nargs=-1)
@click.option(
    '-j', '--max-builds',
    type=click.IntRange(min=0),
    help='Maximum number of builds to run concurrently. Omit or set to 0 to run as many builds '
         'as allowed by the dependency tree (as base images need to be built before those that '
         'depend on them)')
@click.option(
    '--temp-dir', default=Builder.default_tmp_dir('.'),
    type=click.Path(file_okay=False, writable=True, resolve_path=True),
    help='Directory to store temporary build files, such as in-progress archives, Packer '
         'templates and processed instance metadata')
@click.option(
    '--cache-dir', default=Builder.default_cache_dir('.'),
    type=click.Path(file_okay=False, writable=True, resolve_path=True),
    help='Directory to store finished archives and other data that is expensive to build and can '
         'be shared between different invocations')
@click.option(
    '--log-dir', default=Builder.default_log_dir('.'),
    type=click.Path(file_okay=False, writable=True, resolve_path=True),
    help='Directory to store build logs')
@click.option(
    '--clean-temp-dir/--no-clean-temp-dir', default=True,
    help='Whether to clean or leave files written to the temp dir after the build is finished. '
         'Mostly useful for inspection and debugging.')
@click.option(
    '--packer-cmd', default='packer',
    type=shlex.split,
    help='Command to use to invoke Packer. Can include arguments other than the executable path, '
         'separated by spaces and quotes according to shell rules (but NOT actually interpreted '
         'by a shell, i.e. variable expansion is not possible)')
@shelver_async_cmd
def build(ctx, image_patterns, max_builds, temp_dir, cache_dir, log_dir, clean_temp_dir,
          packer_cmd):
    """
    Build and tag images.

    If IMAGES are specified, they will be treated as wildcard patterns filtering which images will
    be built. Images which are not matched by the patterns, but are base images for others that do
    will be included implicitly.
    """

    def build_done(image, version, fut):
        try:
            artifacts = list(fut.result())
            print('{}: Build succeeded, {} artifacts produced'.format(
                image.name, len(artifacts)))
            for artifact in artifacts:
                print('{}: {}'.format(image.name, artifact.id))
        except ShelverError as e:
            print('{}: Build failed: {}'.format(image.name, e))
        except CancelledError:
            print('{}: Build cancelled'.format(image.name))
        except Exception as e:
            print('{}: Build failed with unexpected exception: {}'.format(
                image.name, type(e).__name__))
            logger.exception('%s:', image.name)

    loop, provider, registry, base_dir = ctx.find_object(ShelverContext)
    builder = provider.make_builder(registry, base_dir=base_dir, tmp_dir=temp_dir,
                                    cache_dir=cache_dir, log_dir=log_dir,
                                    keep_tmp=not clean_temp_dir, packer_cmd=packer_cmd)
    with builder:
        yield from registry.load_existing_artifacts()

        coordinator = builder.make_coordinator(max_builds=max_builds, cancel_timeout=60)
        coordinator.add_build_done_callback(build_done)

        for name, image in registry.images.items():
            # Image was not specified in command line, do not build it
            if image_patterns and not any(fnmatch(image.name, pat) for pat in image_patterns):
                continue

            # Image is already built, do not build it
            artifact = registry.get_image_artifact(image, default=None)
            if artifact:
                continue

            logger.info('Scheduling build for %s', name)
            coordinator.get_or_run_build(image)

        try:
            # The coordinator already handles cancellation. It will only throw
            # such an exception if waiting for a graceful finish timed out or
            # was interrupted by a second signal.
            results = yield from ensure_future(coordinator.run_all())
        except (CancelledError, TimeoutError):
            logger.error('Failed to stop builds cleanly, aborting')
            return 130

        # The _build_done callback already prints results, so just check for
        # any errors to set the exit code.
        failed = any(r.cancelled() or r.exception() for r in results.values())
        if failed:
            return 1


@main.command('list')
@shelver_async_cmd
def list_cmd(ctx):
    """
    List configured images.
    """
    loop, provider, registry, base_dir = ctx.find_object(ShelverContext)
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


@main.command('get-artifact')
@click.argument(
    'image', required=True)
@click.argument(
    'version', required=False)
@click.option(
    '-f', '--format', 'fmt', default='plain',
    type=click.Choice(['plain', 'json', 'id']),
    help='Output format')
@shelver_async_cmd
def get_artifact(ctx, image, version, fmt):
    """
    Retrieve artifact information.

    Get information about an artifact matching a given IMAGE and VERSION. If VERSION is omitted,
    the current version as specified in the configuration file will be used.

    The output FORMAT can be one of:
        plain: user-readable description of artifact properties
        json:  machine-parsable description in JSON format
        id:    provider-specific ID only, in a single line

    A non-zero exit code will be returned if the specified IMAGE is not configured, or if there
    is no artifact matching the specified IMAGE and VERSION.
    """
    loop, provider, registry, base_dir = ctx.find_object(ShelverContext)
    yield from registry.load_existing_artifacts()

    artifact = registry.get_image_artifact(image, version=version, default=None)
    if not artifact:
        return 1

    if fmt == 'id':
        print(artifact.id)
    elif fmt == 'json':
        json.dump(artifact.to_dict(), sys.stdout, indent=2)
    elif fmt == 'plain':
        print(artifact)
        # TODO: pretty print
