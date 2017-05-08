from abc import ABCMeta, abstractmethod


class Artifact(metaclass=ABCMeta):
    def __init__(self, provider, name=None, image=None, version=None,
                 environment=None):
        if image:
            if not version:
                raise ValueError(
                    'Version must be specified together with image')

            if not name:
                name = image.name
        elif not name:
            raise ValueError(
                'At least one of name and image must be specified')

        self._provider = provider
        self._name = name
        self._image = image
        self._version = version
        self._environment = environment

    @property
    def provider(self):
        return self._provider

    @property
    def name(self):
        return self._name

    @property
    def image(self):
        return self._image

    @property
    def version(self):
        return self._version

    @property
    def environment(self):
        return self._environment

    @property
    @abstractmethod
    def id(self):
        pass

    def __str__(self):
        return \
            '{}(id={}, name={}, image={}, version={}, environment={})'.format(
                type(self).__name__, self.id, self.name, self.image,
                self.version, self.environment)
