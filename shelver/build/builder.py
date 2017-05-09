import sys
import os
import shutil
import json
import tempfile
import logging
import subprocess
import asyncio
from functools import partial
from collections import Mapping
from ast import literal_eval

import yaml
import aiofiles
from jinja2 import Template
from icicle import FrozenDictEncoder

from shelver.archive import Archive
from shelver.util import AsyncBase, deep_merge, is_collection
from shelver.errors import ConfigurationError, ConcurrentBuildError
from .watcher import Watcher

logger = logging.getLogger('shelver.builder')


class Builder(AsyncBase):
    LOCAL_DIR_PREFIX = '.shelver'

    @classmethod
    def default_log_dir(cls, base_dir):
        return os.path.join(base_dir, cls.LOCAL_DIR_PREFIX, 'log')

    @classmethod
    def default_tmp_dir(cls, base_dir):
        return os.path.join(base_dir, cls.LOCAL_DIR_PREFIX, 'tmp')

    @classmethod
    def default_cache_dir(cls, base_dir):
        return os.path.join(base_dir, cls.LOCAL_DIR_PREFIX, 'cache')

    def __init__(self, registry, base_dir, *, tmp_dir=None, cache_dir=None,
                 log_dir=None, keep_tmp=True, packer_cmd='packer', **kwargs):

        super().__init__(**kwargs)

        tmp_dir = tmp_dir or self.default_tmp_dir(base_dir)
        cache_dir = cache_dir or self.default_cache_dir(base_dir)
        log_dir = log_dir or self.default_log_dir(base_dir)

        logger.debug('builder dirs: tmp = %s, cache = %s, log = %s', tmp_dir,
                     cache_dir, log_dir)

        self.registry = registry
        self.base_dir = base_dir
        self.tmp_dir = tmp_dir
        self.cache_dir = cache_dir
        self.log_dir = log_dir
        self.keep_tmp = bool(keep_tmp)
        self.packer_cmd = packer_cmd

        self._build_tmp_dir = None

    def close(self):
        if self._build_tmp_dir and not self.keep_tmp:
            try:
                logger.info('Cleaning up temporary build dir %s',
                            self._build_tmp_dir)
                shutil.rmtree(self._build_tmp_dir)
            except Exception:
                pass
        self._build_tmp_dir = None

    @staticmethod
    def _create_tmp_dir(base):
        if not os.path.isdir(base):
            logger.debug('Creating tmp dir: %s', base)

            os.makedirs(base)

        return tempfile.mkdtemp(dir=base)

    @asyncio.coroutine
    def get_build_tmp_dir(self):
        if self._build_tmp_dir:
            return self._build_tmp_dir

        self._build_tmp_dir = yield from self.delay(
            self._create_tmp_dir, self.tmp_dir)
        return self._build_tmp_dir

    @asyncio.coroutine
    def build_archive(self, opts):
        tmp = yield from self.get_build_tmp_dir()
        archive = Archive.from_config(
            self.base_dir, opts, tmp_dir=tmp, cache_dir=self.cache_dir)
        yield from archive.get_or_build()
        return archive

    @asyncio.coroutine
    def get_template_context(self, image, version, archive, base_artifact=None):
        if base_artifact:
            logger.info('Using base artifact: %s', base_artifact)

        archive_path = yield from archive.get_or_build()
        logger.info('Generated provision archive: %s', archive_path)

        # Prepare packer template
        context = {
            'name': image.name,
            'version': version,
            'description': image.description,
            'environment': image.environment,
            'instance_type': image.instance_type,
            'base': image.base,
            'provision': image.provision,
            'base_artifact': base_artifact,
            'archive': archive.to_dict()
        }
        return context

    def process_template(self, data, context):
        if isinstance(data, Mapping):
            return dict((k, self.process_template(v, context))
                        for (k, v) in data.items())
        elif is_collection(data):
            return list(self.process_template(v, context) for v in data)
        elif isinstance(data, str):
            result = Template(data).render(context)
            try:
                result_obj = literal_eval(result)
            except (SyntaxError, ValueError):
                result_obj = result

            return result_obj

    @asyncio.coroutine
    def load_template(self, path, context):
        f = yield from aiofiles.open(path, 'rb', loop=self._loop)
        try:
            content = yield from f.read()
            return self.process_template(yaml.safe_load(content), context)
        finally:
            yield from f.close()

    def post_process_template(self, data, image):
        try:
            data['builders'] = list(map(
                lambda d: deep_merge(d, image.builder_opts),
                data['builders']))
            return data
        except KeyError:
            raise ConfigurationError('No builders found in template')

    @staticmethod
    def _create_tmp_file(*args, mode='wb', **kwargs):
        fd, path = tempfile.mkstemp(*args, **kwargs)
        f = os.fdopen(fd, mode)
        f.path = path
        return f

    @asyncio.coroutine
    def write_template(self, data):
        tmp = yield from self.get_build_tmp_dir()

        fd, path = yield from self.delay(
            partial(tempfile.mkstemp, suffix='.json', dir=tmp))
        f = yield from aiofiles.open(fd, 'w', encoding='utf-8',
                                     loop=self._loop, executor=self._executor)
        try:
            content = json.dumps(data, indent=2, cls=FrozenDictEncoder)
            logger.debug('Generated packer template: \n%s', content)

            yield from f.write(content)
            return path
        finally:
            yield from f.close()

    @asyncio.coroutine
    def _open_log_file(self, name, version):
        if not os.path.isdir(self.log_dir):
            yield from self.delay(os.makedirs, self.log_dir)

        fname = '{}_{}.log'.format(name, version)
        path = os.path.join(self.log_dir, fname)
        f = yield from aiofiles.open(path, mode='ab', loop=self._loop)
        return f

    @asyncio.coroutine
    def _get_build_cmd(self, image, version, base_artifact=None, logger=logger):
        logger.info('Starting build: %s, version %s', image, version)

        archive = \
            yield from self.build_archive(image.archive)
        context = \
            yield from self.get_template_context(
                image, version, archive, base_artifact=base_artifact)
        packer_data = \
            yield from self.load_template(image.template_path, context)
        packer_data = \
            self.post_process_template(packer_data, image)
        template_path = \
            yield from self.write_template(packer_data)

        return self.packer_cmd, ['build', '-machine-readable', template_path]

    @asyncio.coroutine
    def run_build(self, image, version, base_artifact=None, msg_stream=None):
        close_stream = False
        if not msg_stream:
            msg_stream = yield from aiofiles.open(
                sys.stdout.fileno(), 'wb', closefd=False, loop=self._loop,
                executor=self._executor)
            close_stream = True

        try:
            log_stream = yield from self._open_log_file(image.name, version)
            try:
                program, args = yield from self._get_build_cmd(
                    image, version, base_artifact=base_artifact)

                proc = yield from asyncio.create_subprocess_exec(
                    program, *args, stdin=None, stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE, loop=self._loop)

                watcher = Watcher(image.name, msg_stream, log_stream,
                                  loop=self._loop)
                return (yield from watcher.run(proc))
            finally:
                yield from log_stream.close()
        finally:
            if close_stream:
                yield from msg_stream.close()
