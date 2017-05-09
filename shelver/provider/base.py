import asyncio
from abc import ABCMeta, abstractmethod

from shelver.registry import Registry
from shelver.artifact import Artifact
from shelver.build import Builder
from shelver.errors import ConfigurationError
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
            raise ConfigurationError("Unknown provider '{}'".format(name))

        return provider_cls(*args, **kwargs)

    def __init__(self, config, **kwargs):
        super().__init__(**kwargs)

        self.config = config

    def make_registry(self, images, *args, **kwargs):
        kwargs.setdefault('loop', self._loop)
        kwargs.setdefault('executor', self._executor)
        registry = self.Registry(images, provider=self, **kwargs)
        return registry

    def make_builder(self, *args, **kwargs):
        kwargs.setdefault('loop', self._loop)
        kwargs.setdefault('executor', self._executor)
        return self.Builder(*args, **kwargs)

    def make_artifact(self, **kwargs):
        return self.Artifact(provider=self, **kwargs)
