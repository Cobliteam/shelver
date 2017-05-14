import pytest
import shelver.provider
from shelver.image import Image
from shelver.errors import UnknownImageError


images = {
    'test1': Image(
        name='test1',
        current_version='1',
        environment='prod',
        description='Test Image 1',
        template_path='test.yml',
        instance_type='test'),
    'test2': Image(
        name='test2',
        current_version='2',
        environment='prod',
        description='Test Image 2',
        template_path='test.yml',
        instance_type='test',
        base='test1'),
    'test3': Image(
        name='test3',
        current_version='1',
        environment='prod',
        description='Test Image 3',
        template_path='test.yml',
        instance_type='test',
        base='test1:v1')
}


@pytest.fixture
def provider():
    return shelver.provider.TestProvider({})


@pytest.fixture
def registry(provider):
    return provider.make_registry(images)


@pytest.fixture
def artifacts(provider):
    return {
        'test1-v1': provider.make_artifact(
            id='test1-v1',
            image=images['test1'],
            version='1',
            environment='prod'),
        'test2-v1': provider.make_artifact(
            id='test2-v1',
            image=images['test2'],
            version='1',
            environment='prod'),
        'test2-v2': provider.make_artifact(
            id='test2-v1',
            image=images['test2'],
            version='2',
            environment='prod'),
        'test3-v1': provider.make_artifact(
            id='test3-v1',
            image=images['test3'],
            version='1',
            environment='prod')
    }


@pytest.fixture
def registry_with_artifacts(artifacts, registry):
    for artifact in artifacts.values():
        registry.register_artifact(artifact)
        registry.associate_artifact(artifact)

    return registry


def test_reject_unknown_base(provider):
    with pytest.raises(ValueError):
        provider.make_registry({
            'bad_base': Image(
                name='bad_base',
                current_version='1',
                environment='prod',
                description='Test Image with bad base',
                template_path='test.yml',
                instance_type='test',
                base='whatever')
        })


@pytest.mark.xfail
def test_reject_cycles(provider):
    with pytest.raises(ValueError):
        provider.make_registry({
            'left': Image(
                name='left',
                current_version='1',
                environment='prod',
                description='Test Image (left leaning)',
                template_path='test.yml',
                instance_type='test',
                base='right'),
            'right': Image(
                name='right',
                current_version='1',
                environment='prod',
                description='Test Image (right leaning)',
                template_path='test.yml',
                instance_type='test',
                base='left')
        })


def test_images(registry):
    assert images == registry.images


def test_get_image(registry):
    for name, img in images.items():
        assert registry.get_image(name) == img

    img = images['test1']
    assert registry.get_image(img) == img

    with pytest.raises(UnknownImageError):
        registry.get_image('whatever')

    new_img = img._replace(current_version=None)
    with pytest.raises(UnknownImageError):
        registry.get_image(new_img)

    assert registry.get_image('whatever', None) is None


def test_artifact_registration(artifacts, registry_with_artifacts):
    for name, artifact in artifacts:
        full_name = name + ':' + artifact.version
        assert registry.get_artifact(full_name) == artifact


def test_artifact_registration_alt_name(artifacts, registry):
    artifact = next(artifacts.values())
    registry.register_artifact(artifact, name='whatever')
    assert registry.get_artifact('whatever') == artifact


def test_artifact_association(artifacts, registry_with_artifacts):
    for artifact in artifacts.values():
        image = artifact.image
        version = artifact.version
        assert registry.get_image_artifact(image, version) == artifact

    assert registry.get_image_artifact('test1', '1') == artifacts['test1_v1']
    assert registry.get_image_artifact('test2', '1') == artifacts['test2_v1']
    assert registry.get_image_artifact('test2', '2') == artifacts['test2_v2']
    assert registry.get_image_artifact('test1') == artifacts['test1_v1']
    assert registry.get_image_artifact('test2') == artifacts['test2_v2']

    with pytest.raises(KeyError):
        registry.get_image_artifact('test1', '2')
    with pytest.raises(KeyError):
        registry.get_image_artifact('test3')
    assert registry.get_image_artifact('test3', default=None) is None

    assert (list(registry.get_image_versions('test2')) ==
            [('1', artifacts['test2_v1']),
             ('2', artifacts['test2_v2'])])


def test_base_artifact_discovery(artifacts, registry_with_artifacts):
    assert registry.get_image_base_artifact('test1') is None
    assert (registry.get_image_base_artifact('test2') ==
            artifacts['test2_v2'])
    assert (registry.get_image_base_artifact('test3') ==
            artifacts['test2_v1'])
