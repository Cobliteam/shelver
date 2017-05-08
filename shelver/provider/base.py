import asyncio
from abc import ABCMeta, abstractmethod

from shelver.registry import Registry
from shelver.artifact import Artifact
from shelver.build import Builder
from shelver.util import AsyncBase


class Provider(AsyncBase, metaclass=ABCMeta):
    Registry = Registry
    Builder = Builder
    Artifact = Artifact

    _providers = {}

    @classmethod
    def register(cls, provider_cls):
        for name in provider_cls.NAMES:
            cls._providers[name] = provider_cls

    @classmethod
    def available_names(cls):
        return cls._providers.keys()

    @classmethod
    def new(cls, name, *args, **kwargs):
        try:
            provider_cls = cls._providers[name]
        except KeyError:
            raise RuntimeError("Unknown provider '{}".format(name))

        return provider_cls(*args, **kwargs)

    def __init__(self, config, **kwargs):
        super().__init__(**kwargs)

        self.config = config

    @asyncio.coroutine
    def make_registry(self, image_config, *args, **kwargs):
        kwargs.setdefault('loop', self._loop)
        kwargs.setdefault('executor', self._executor)
        registry = self.Registry.from_config(
            self, image_config, *args, **kwargs)
        yield from registry.load_existing_artifacts()
        return registry

    @asyncio.coroutine
    def make_builder(self, *args, **kwargs):
        kwargs.setdefault('loop', self._loop)
        kwargs.setdefault('executor', self._executor)
        return self.Builder(*args, **kwargs)
