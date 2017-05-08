import os
import shutil
import asyncio
from abc import ABCMeta, abstractmethod

from shelver.errors import ConfigurationError


class Archive(metaclass=ABCMeta):
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

    def __init__(self, source_dir, tmp_dir, cache_dir, *, loop=None,
                 executor=None):
        self.source_dir = source_dir
        self.tmp_dir = tmp_dir
        self.cache_dir = cache_dir
        self._loop = loop or asyncio.get_event_loop()
        self._executor = executor
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
        if not self._path:
            basename = yield from self.basename()
            cached = os.path.join(self.cache_dir, basename)
            if not os.path.isfile(cached):
                tmp_archive = yield from self.build()
                mv = self._loop.run_in_executor(self._executor, shutil.move,
                                                tmp_archive, cached)
                yield from mv

            self._path = cached

        return self._path

    def to_dict(self):
        return {
            'source_dir': self.source_dir,
            'path': self._path
        }
