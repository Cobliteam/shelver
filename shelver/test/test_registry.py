import pytest
import shelver.provider
from shelver.image import Image
from shelver.errors import UnknownImageError, ConfigurationError, UnknownArtifactError

images = {
    'fedora': Image.from_dict({
        'name': 'fedora',
        'current_version': '25',
        'environment': 'prod',
        'description': 'Fedora 25',
        'template_path': 'fedora.yml',
        'instance_type': 't2.micro'}),
    'server': Image.from_dict({
        'name': 'server',
        'current_version': '2',
        'environment': 'prod',
        'description': 'Base server',
        'template_path': 'server.yml',
        'instance_type': 't2.micro',
        'base': 'fedora'}),
    'web': Image.from_dict({
        'name': 'web',
        'current_version': '1',
        'environment': 'prod',
        'description': 'Web server',
        'template_path': 'web.yml',
        'instance_type': 't2.micro',
        'base': 'server:1'})
}


@pytest.fixture
def provider():
    return shelver.provider.TestProvider({})


@pytest.fixture
def empty_registry(provider):
    return provider.make_registry(images)


@pytest.fixture
def artifacts(provider):
    return {
        'fedora-v24': provider.make_artifact(
            id='fedora-v24',
            image=images['fedora'],
            version='24',
            environment='prod'),
        'fedora-v25': provider.make_artifact(
            id='fedora-v25',
            image=images['fedora'],
            version='25',
            environment='prod'),
        'server-v1': provider.make_artifact(
            id='server-v1',
            image=images['server'],
            version='1',
            environment='prod'),
        'server-v2': provider.make_artifact(
            id='server-v2',
            image=images['server'],
            version='2',
            environment='prod'),
        'web-v1': provider.make_artifact(
            id='web-v1',
            image=images['web'],
            version='1',
            environment='prod')
    }


@pytest.fixture
def registry(artifacts, empty_registry):
    registry = empty_registry

    for artifact in artifacts.values():
        registry.register_artifact(artifact)
        registry.associate_artifact(artifact)

    return registry


def test_reject_unknown_base(provider):
    registry = provider.make_registry({
        'bad_base': Image.from_dict({
            'name': 'bad_base',
            'current_version': '1',
            'environment': 'prod',
            'description': 'Test Image with bad base',
            'template_path': 'fedora.yml',
            'instance_type': 'test',
            'base': 'whatever'})
    })
    with pytest.raises(UnknownImageError):
        registry.check_cycles()


@pytest.mark.xfail
def test_reject_cycles(provider):
    registry = provider.make_registry({
        'left': Image.from_dict({
            'name': 'left',
            'current_version': '1',
            'environment': 'prod',
            'description': 'Test Image (left leaning)',
            'template_path': 'fedora.yml',
            'instance_type': 'test',
            'base': 'right'}),
        'right': Image.from_dict({
            'name': 'right',
            'current_version': '1',
            'environment': 'prod',
            'description': 'Test Image (right leaning)',
            'template_path': 'fedora.yml',
            'instance_type': 'test',
            'base': 'left'})
    })
    with pytest.raises(ConfigurationError):
        registry.check_cycles()


def test_images(registry):
    assert registry.images == images


def test_get_image(registry):
    for name, img in images.items():
        assert registry.get_image(name) == img

    img = images['fedora']
    assert registry.get_image(img) == img

    with pytest.raises(UnknownImageError):
        registry.get_image('whatever')

    new_img = img._replace(current_version=None)
    with pytest.raises(UnknownImageError):
        registry.get_image(new_img)

    assert registry.get_image('whatever', None) is None


def test_artifact_registration(artifacts, registry):
    for name, artifact in artifacts.items():
        full_name = artifact.image.name + ':' + artifact.version
        assert registry.get_artifact(full_name) == artifact


def test_artifact_registration_alt_name(artifacts, empty_registry):
    registry = empty_registry

    artifact = next(iter(artifacts.values()))
    registry.register_artifact(artifact, name='whatever')
    assert registry.get_artifact('whatever') == artifact


def test_artifact_association(artifacts, registry):
    for artifact in artifacts.values():
        image = artifact.image
        version = artifact.version
        assert registry.get_image_artifact(image, version) == artifact

    assert registry.get_image_artifact('fedora', '24') == artifacts['fedora-v24']
    assert registry.get_image_artifact('fedora') == artifacts['fedora-v25']
    assert registry.get_image_artifact('server', '1') == artifacts['server-v1']
    assert registry.get_image_artifact('server') == artifacts['server-v2']
    assert registry.get_image_artifact('web', '1') == artifacts['web-v1']
    assert registry.get_image_artifact('web') == artifacts['web-v1']

    with pytest.raises(UnknownArtifactError):
        registry.get_image_artifact('fedora', '26')
    with pytest.raises(UnknownImageError):
        registry.get_image_artifact('debian')

    assert registry.get_image_artifact('fedora', '26', default=None) is None
    with pytest.raises(UnknownImageError):
        assert registry.get_image_artifact('debian', default=None)

    assert (list(registry.get_image_versions('server')) ==
            [('1', artifacts['server-v1']),
             ('2', artifacts['server-v2'])])


def test_base_artifact_discovery(artifacts, registry):
    assert registry.get_image_base_artifact('fedora') is None
    assert (registry.get_image_base_artifact('server') ==
            artifacts['fedora-v25'])
    assert (registry.get_image_base_artifact('web') ==
            artifacts['server-v1'])
