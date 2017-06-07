import pytest
from shelver.image import Image
from shelver.provider.test import TestProvider


@pytest.fixture
def provider():
    return TestProvider({})


@pytest.fixture
def images():
    return {
        'fedora': Image.from_dict({
            'name': 'fedora',
            'current_version': '25',
            'environment': 'test',
            'description': 'Fedora 25',
            'template_path': 'fedora.yml',
            'instance_type': 't2.micro'}),
        'server': Image.from_dict({
            'name': 'server',
            'current_version': '2',
            'environment': 'test',
            'description': 'Base server',
            'template_path': 'server.yml',
            'instance_type': 't2.micro',
            'base': 'fedora'}),
        'web': Image.from_dict({
            'name': 'web',
            'current_version': '1',
            'environment': 'test',
            'description': 'Web server',
            'template_path': 'web.yml',
            'instance_type': 't2.micro',
            'base': 'server:1'})
    }


@pytest.fixture
def image(images):
    return images['fedora']


@pytest.fixture
def artifacts(provider, images):
    return {
        'fedora-v24': provider.make_artifact(
            id='fedora-v24',
            image=images['fedora'],
            version='24',
            environment='test'),
        'fedora-v25': provider.make_artifact(
            id='fedora-v25',
            image=images['fedora'],
            version='25',
            environment='test'),
        'server-v1': provider.make_artifact(
            id='server-v1',
            image=images['server'],
            version='1',
            environment='test'),
        'server-v2': provider.make_artifact(
            id='server-v2',
            image=images['server'],
            version='2',
            environment='test'),
        'web-v1': provider.make_artifact(
            id='web-v1',
            image=images['web'],
            version='1',
            environment='test'),
        'vpn-v1': provider.make_artifact(
            id='vpn-v1',
            name='vpn-v1',
            environment='test')
    }


@pytest.fixture
def artifact(artifacts):
    return artifacts['fedora-v24']


@pytest.fixture
def external_artifact(artifacts):
    return artifacts['vpn-v1']


@pytest.fixture
def empty_registry(provider, images):
    return provider.make_registry(images)


@pytest.fixture
def registry(artifacts, empty_registry):
    registry = empty_registry

    for artifact in artifacts.values():
        registry.register_artifact(artifact)
        if artifact.image:
            registry.associate_artifact(artifact)

    return registry
