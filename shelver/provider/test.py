import logging
import asyncio

from shelver.artifact import Artifact
from shelver.registry import Registry
from shelver.build import Builder
from .base import Provider


logger = logging.getLogger('shelver.provider.test')


class TestArtifact(Artifact):
    def __init__(self, id, **kwargs):
        super().__init__(**kwargs)
        self._id = id

    @property
    def id(self):
        return self._id


class TestRegistry(Registry):
    @asyncio.coroutine
    def load_artifact_by_id(self, id, region=None, image=None):
        name, version = id.split(':')
        if not image:
            image = self.get_image(name)

        artifact = TestArtifact(self.provider, id, image=image,
                                version=version, environment='test')
        self.register_artifact(artifact)
        self.associate_artifact(artifact, image, version)
        return artifact

    @asyncio.coroutine
    def load_existing_artifacts(self, region=None):
        pass


class TestBuilder(Builder):
    @asyncio.coroutine
    def run_build(self, image, version, base_artifact=None, msg_stream=None):
        image = self.registry.get_image(image)
        if not version:
            version = image.current_version
        else:
            assert version == image.current_version

        id = '{}:{}'.format(image.name, version)
        logging.info('Built fake artifact %s for image %s:%s', id,
                     image.name, version)

        artifact = {'id': id}
        return [artifact]


class TestProvider(Provider):
    name = 'test'
    aliases = ()

    artifact_class = TestArtifact
    registry_class = TestRegistry
    builder_class = TestBuilder
