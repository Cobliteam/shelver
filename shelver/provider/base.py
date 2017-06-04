from abc import ABCMeta, abstractclassmethod

from shelver.registry import Registry
from shelver.artifact import Artifact
from shelver.build import Builder
from shelver.errors import ConfigurationError
from shelver.util import AsyncBase


class Provider(AsyncBase, metaclass=ABCMeta):
    registry_class = Registry
    builder_class = Builder
    artifact_class = Artifact

    _providers = {}

    @property
    @abstractclassmethod
    def name(cls):
        raise NotImplementedError

    @property
    @abstractclassmethod
    def aliases(cls):
        raise NotImplementedError

    @property
    @abstractclassmethod
    def registry_class(cls):
        raise NotImplementedError

    @property
    @abstractclassmethod
    def builder_class(cls):
        raise NotImplementedError

    @property
    @abstractclassmethod
    def artifact_class(cls):
        raise NotImplementedError

    @classmethod
    def register(cls, provider_cls):
        names = [provider_cls.name]
        names.extend(provider_cls.aliases)

        conflict = next((n for n in names if n in cls._providers), None)
        if conflict:
            raise ValueError(
                'Provider with name {} already registered'.format(conflict))

        for name in names:
            cls._providers[name] = provider_cls

    @classmethod
    def available_names(cls):
        return list(cls._providers.keys())

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
        registry = self.registry_class(images, provider=self, **kwargs)
        return registry

    def make_builder(self, *args, **kwargs):
        kwargs.setdefault('loop', self._loop)
        kwargs.setdefault('executor', self._executor)
        return self.builder_class(*args, **kwargs)

    def make_artifact(self, **kwargs):
        return self.artifact_class(provider=self, **kwargs)
