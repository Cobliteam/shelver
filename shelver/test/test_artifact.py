import pytest

from shelver.artifact import Artifact
from shelver.provider.test import TestArtifact


def test_artifact_construct(provider, image):
    with pytest.raises(ValueError):
        TestArtifact('test', provider=provider, name=None, image=None)

    with pytest.raises(ValueError):
        TestArtifact('test', provider=provider, image=image, version=None)

    artifact = TestArtifact('test', provider=provider, image=image,
                            version='1', environment='test')

    assert artifact.id == 'test'
    assert artifact.name == image.name
    assert artifact.provider == provider
    assert artifact.image == image
    assert artifact.version == '1'
    assert artifact.environment == 'test'


def test_artifact_to_dict(artifact):
    d = artifact.to_dict()
    assert d['id'] == artifact.id
    assert d['provider'] == artifact.provider.name
    assert d['image'] == artifact.image.name
    assert d['name'] == artifact.name
    assert d['version'] == artifact.version
    assert d['environment'] == artifact.environment


def test_artifact_to_dict_without_image(external_artifact):
    d = external_artifact.to_dict()
    assert d['id'] == external_artifact.id
    assert d['provider'] == external_artifact.provider.name
    assert d['image'] is None
    assert d['name'] == external_artifact.name
    assert d['version'] == external_artifact.version
    assert d['environment'] == external_artifact.environment


def test_artifact_subclass_requirements(provider):
    class BrokenArtifact(Artifact):
        pass

    with pytest.raises(TypeError):
        BrokenArtifact(provider=provider, name='test')
