import sys
import os
import shutil
import json
import tempfile
import logging
import subprocess
import asyncio
from functools import partial
from collections.abc import Mapping
from ast import literal_eval

import yaml
import aiofiles
from jinja2 import Template

from shelver.archive import Archive
from shelver.util import AsyncBase, deep_merge, is_collection
from shelver.errors import ConfigurationError
from .coordinator import Coordinator
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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def make_coordinator(self, **kwargs):
        kwargs.setdefault('loop', self._loop)
        kwargs.setdefault('executor', self._executor)
        return Coordinator(self, **kwargs)

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

    async def get_build_tmp_dir(self):
        if self._build_tmp_dir:
            return self._build_tmp_dir

        self._build_tmp_dir = await self.delay(
            self._create_tmp_dir, self.tmp_dir)
        return self._build_tmp_dir

    async def build_archive(self, opts):
        tmp = await self.get_build_tmp_dir()
        archive = Archive.from_config(
            self.base_dir, opts, tmp_dir=tmp, cache_dir=self.cache_dir)
        await archive.get_or_build()
        return archive

    async def get_template_context(self, image, version, archive,
                                   base_artifact=None):
        if base_artifact:
            logger.info('Using base artifact: %s', base_artifact)

        archive_path = await archive.get_or_build()

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

    async def load_template(self, path, context):
        f = await aiofiles.open(path, 'rb', loop=self._loop)
        try:
            content = await f.read()
            return self.process_template(yaml.safe_load(content), context)
        finally:
            await f.close()

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

    async def write_template(self, data):
        tmp = await self.get_build_tmp_dir()

        fd, path = await self.delay(
            partial(tempfile.mkstemp, suffix='.json', dir=tmp))
        f = await aiofiles.open(fd, 'w', encoding='utf-8',
                                loop=self._loop, executor=self._executor)
        try:
            def default(o):
                if isinstance(o, Mapping) and not isinstance(o, dict):
                    return dict(o)

                raise TypeError

            content = json.dumps(data, default=default, indent=2)
            logger.debug('Generated packer template: \n%s', content)

            await f.write(content)
            return path
        finally:
            await f.close()

    async def _open_log_file(self, name, version):
        if not os.path.isdir(self.log_dir):
            await self.delay(os.makedirs, self.log_dir)

        fname = '{}_{}.log'.format(name, version)
        path = os.path.join(self.log_dir, fname)
        f = await aiofiles.open(path, mode='ab', loop=self._loop)
        return f

    async def _get_build_cmd(self, image, version, base_artifact=None,
                             logger=logger):
        logger.info('Starting build: %s, version %s', image, version)

        archive = \
            await self.build_archive(image.archive)
        context = \
            await self.get_template_context(
                image, version, archive, base_artifact=base_artifact)
        packer_data = \
            await self.load_template(image.template_path, context)
        packer_data = \
            self.post_process_template(packer_data, image)
        template_path = \
            await self.write_template(packer_data)

        cmd = list(self.packer_cmd) + ['build', '-machine-readable',
                                       template_path]
        return cmd[0], cmd[1:]

    async def _get_build_env(self):
        return os.environ.copy()

    async def run_build(self, image, version, base_artifact=None,
                        msg_stream=None):
        close_stream = False
        if not msg_stream:
            msg_stream = await aiofiles.open(
                sys.stderr.fileno(), 'wb', closefd=False, loop=self._loop,
                executor=self._executor)
            close_stream = True

        try:
            log_stream = await self._open_log_file(image.name, version)
            try:
                env = await self._get_build_env()
                program, args = await self._get_build_cmd(
                    image, version, base_artifact=base_artifact)

                proc = await asyncio.create_subprocess_exec(
                    program, *args, env=env, stdin=None,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    limit=2 ** 32, loop=self._loop)

                watcher = Watcher(image.name, msg_stream, log_stream,
                                  loop=self._loop)
                return (await watcher.run(proc))
            finally:
                await log_stream.close()
        finally:
            if close_stream:
                await msg_stream.close()
