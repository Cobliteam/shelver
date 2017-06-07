import os
import shutil
import asyncio
import logging
from abc import ABCMeta, abstractmethod

from shelver.errors import ConfigurationError
from shelver.util import AsyncBase
from .file_lock import FileLock

logger = logging.getLogger('shelver.archive.base')


class Archive(AsyncBase, metaclass=ABCMeta):
    _types = {}

    @classmethod
    def register_type(cls, archive_cls):
        for name in archive_cls.NAMES:
            cls._types[name] = archive_cls

    @classmethod
    def from_config(cls, base_dir, cfg, **defaults):
        archive_opts = dict(defaults)
        archive_opts.update(cfg)

        source_dir = os.path.join(base_dir, archive_opts.pop('dir'))
        archive_type = archive_opts.pop('type')
        try:
            archive_cls = cls._types[archive_type]
        except KeyError:
            raise ConfigurationError(
                "Unknown archive type '{}'".format(archive_type))

        return archive_cls(source_dir=source_dir,
                           **archive_opts)

    def __init__(self, source_dir, tmp_dir, cache_dir, **kwargs):
        super().__init__(**kwargs)

        self.source_dir = source_dir
        self.tmp_dir = tmp_dir
        self.cache_dir = cache_dir
        self._path = None

        if not os.path.isdir(self.cache_dir):
            os.makedirs(self.cache_dir)

    @asyncio.coroutine
    @abstractmethod
    def basename(self):
        pass

    @asyncio.coroutine
    @abstractmethod
    def build(self):
        pass

    @asyncio.coroutine
    def get_or_build(self):
        if self._path:
            return self._path

        basename = yield from self.basename()
        path = os.path.join(self.cache_dir, basename)

        try:
            # Try to create the file if it does not exist, then lock it while
            # the builder is running to avoid simulatenous builds. Then finally
            # swap the tmp file with the final one.
            with open(path, 'x') as f:
                lock = FileLock(f, loop=self._loop, executor=self._executor)
                yield from lock.acquire()
                try:
                    tmp_archive = yield from self.build()
                    mv = self._loop.run_in_executor(
                        self._executor, shutil.move, tmp_archive, path)
                    yield from mv

                    logger.info('Generated provision archive: %s', path)
                finally:
                    lock.release()
        except FileExistsError:
            logger.info('Using cached provision archive: %s', path)

            with open(path, 'rb') as f:
                # Acquire the read lock and release it immediately, just to wait
                # until a running build finishes
                lock = FileLock(f, loop=self._loop, executor=self._executor)
                yield from lock.acquire(exclusive=False)
                lock.release()
        except Exception:
            logger.exception('Failed to build archive')
            os.unlink(path)

        self._path = path
        return path

    def to_dict(self):
        return {
            'source_dir': self.source_dir,
            'path': self._path
        }
