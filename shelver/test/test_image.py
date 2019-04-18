from collections.abc import Mapping, Sequence

import pytest
from shelver.errors import ConfigurationError
from shelver.image import Image


def test_image_from_dict_defaults():
    img = Image.from_dict({'name': 'test', 'version': '1'})
    assert img.name == 'test'
    assert img.current_version == '1'
    assert isinstance(img.environment, str) and img.environment
    assert isinstance(img.description, str) and img.description
    assert isinstance(img.template_path, str) and img.template_path
    assert img.base is None
    assert img.provision is None
    assert isinstance(img.metadata, Sequence)
    assert isinstance(img.archive, Mapping)
    assert isinstance(img.packer_builder_overrides, Mapping)


def test_image_from_dict_overrides():
    img = Image.from_dict(
        {'name': 'test', 'current_version': '1',
         'description': 'override desc'},
        defaults={'packer_builder_overrides': {'some-option': 'some-value'},
                  'description': 'default desc',
                  'base': 'some-base'})
    assert img.name == 'test'
    assert img.current_version == '1'
    assert img.description == 'override desc'
    assert img.packer_builder_overrides['some-option'] == 'some-value'


def test_image_from_dict_invalid_defaults():
    with pytest.raises((TypeError, ValueError)):
        Image.from_dict(
            {'name': 'test', 'version': '1'},
            defaults={'packer_builder_overrides': 'bad-string'})


def test_image_parse_config_invalid():
    with pytest.raises(ConfigurationError):
        Image.parse_config('not-a-dict')


@pytest.fixture
def example_config(images):
    config = {name: img.to_dict() for name, img in images.items()}
    return config


def test_image_parse_config_no_mutation(example_config):
    original_config = example_config.copy()
    Image.parse_config(example_config)
    assert example_config == original_config


def test_image_parse_config(images, example_config):
    parsed_images = Image.parse_config(example_config)
    assert parsed_images == images


def test_image_parse_config_with_defaults(images, example_config):
    config = example_config.copy()
    config['defaults'] = {'packer_builder_overrides': {'hello': 'world'}}

    parsed_images = Image.parse_config(config)
    assert parsed_images.keys() == images.keys()

    for name, image in parsed_images.items():
        expected_attrs = images[name].to_dict()
        expected_attrs.update(config['defaults'])
        assert expected_attrs == image.to_dict()
