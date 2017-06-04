import pytest
from shelver.image import Image
from shelver.errors import (UnknownImageError, ConfigurationError,
                            UnknownArtifactError)


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


def test_images(registry, images):
    assert registry.images == images


def test_get_image(registry, images):
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
        expected_name = artifact.name
        if artifact.image:
            expected_name = artifact.image.name + ':' + artifact.version

        assert registry.get_artifact(expected_name) == artifact


def test_artifact_registration_alt_name(artifacts, empty_registry):
    registry = empty_registry

    artifact = next(iter(artifacts.values()))
    registry.register_artifact(artifact, name='whatever')
    assert registry.get_artifact('whatever') == artifact


def test_artifact_association(artifacts, registry):
    for artifact in artifacts.values():
        if artifact.image:
            assert registry.get_image_artifact(artifact.image,
                                               artifact.version) == artifact

    assert \
        registry.get_image_artifact('fedora', '24') == artifacts['fedora-v24']
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
