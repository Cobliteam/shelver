import logging
import asyncio

from shelver.artifact import Artifact
from shelver.registry import Registry
from shelver.build import Builder
from .base import Provider


logger = logging.getLogger('shelver.provider.test')


class TestArtifact(Artifact):
    def __init__(self, provider, id, **kwargs):
        super().__init__(provider, **kwargs)
        self._id = id

    @property
    def id(self):
        return self._id


class TestRegistry(Registry):
    def load_artifact_by_id(self, id, region=None):
        name, version = id.split(':')
        image = self.get_image(name)
        artifact = TestArtifact(self.provider, id, image=image,
                                version=version, environment='test')
        self.register_artifact(artifact)
        self.associate_artifact(artifact, image, version)
        return artifact

    def load_existing_artifacts(self, region=None):
        return self


class TestBuilder(Builder):
    @asyncio.coroutine
    def run_build(self, image, version, base_artifact=None, msg_stream=None):
        image = self.registry.get_image(image)
        if not version:
            version = image.current_version
        else:
            assert version == image.current_version

        logging.debug('Faking build for image %s, version %s', image.name,
                      version)

        id = '{}:{}'.format(image.name, version)
        artifact = {'id': id}
        return [artifact]


class TestProvider(Provider):
    NAMES = ('test',)

    Artifact = TestArtifact
    Registry = TestRegistry
    Builder = TestBuilder
