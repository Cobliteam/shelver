from shelver.registry import Registry
from shelver.artifact import Artifact
from shelver.builder import Builder

class Provider(object):
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

    def __init__(self, config):
        self.config = config

    def make_registry(self, image_config):
        registry = self.Registry(self, image_config)
        registry.load_existing_artifacts()
        return registry

    def make_builder(self, *args, **kwargs):
        return self.Builder(*args, **kwargs)


from shelver.provider.amazon import AmazonProvider
Provider.register(AmazonProvider)

