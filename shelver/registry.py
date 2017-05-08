from abc import ABCMeta, abstractmethod
from collections import defaultdict, deque

from distutils.version import LooseVersion
from shelver.image import Image
from shelver.util import freeze
from shelver.errors import *


class Registry(object, metaclass=ABCMeta):
    _GET_IMAGE_DEFAULT = object()

    version_key = LooseVersion

    @classmethod
    def from_config(cls, provider, config):
        images = Image.load_all(config)
        return cls(provider, images)

    def __init__(self, provider, images):
        self.provider = provider
        self._images = freeze(images)
        self._image_set = frozenset(self._images.values())
        self._versions = defaultdict(dict)
        self._artifacts = {}

        # self._check_cycles()

    @property
    def images(self):
        return self._images

    def __getitem__(self, key):
        try:
            return self._images[key]
        except KeyError:
            raise UnknownImageError(key)

    def get_image(self, image, default=_GET_IMAGE_DEFAULT):
        if isinstance(image, Image):
            if image not in self._image_set:
                raise UnknownImageError(image.name)

            return image

        try:
            return self[image]
        except KeyError:
            if default is not self._GET_IMAGE_DEFAULT:
                raise

            return default

    @property
    def artifacts(self):
        return self._artifacts

    def _check_artifact(self, artifact):
        if not isinstance(artifact, self.provider.Artifact):
            raise TypeError(
                'Unsupported artifact type: {}'.format(type(artifact)))

    def register_artifact(self, artifact, name=None):
        self._check_artifact(artifact)

        if not name:
            name = artifact.name
            if artifact.version:
                name += ':' + artifact.version

        existing = self._artifacts.get(name)
        if existing:
            if existing != artifact:
                raise ValueError(
                    'Artifact already registered with name {}'.format(name))
        else:
            self._artifacts[name] = artifact

        return self

    def get_artifact(self, name, default=None):
        return self._artifacts.get(name, default)

    @abstractmethod
    def load_artifact_by_id(self, id, region=None, image=None):
        pass

    @abstractmethod
    def load_existing_artifacts(self, region=None):
        return self

    def associate_artifact(self, artifact, image=None, version=None):
        if not image:
            image = artifact.image
        if not version:
            version = artifact.version

        if not image or not version:
            raise ValueError('Image and/or version are unset and not present '
                             'in artifact')

        image = self.get_image(image)
        versions = self._versions[image]
        if version in versions:
            raise ValueError(
                'Image {} already has artifact for version {}'.format(
                    image, version))

        if artifact.provider is not self.provider:
            raise ValueError(
                'Cannot associate artifact not registered with this provider')

        versions[version] = artifact
        return self

    def get_image_artifact(self, image, version=None,
                           default=_GET_IMAGE_DEFAULT):
        image = self.get_image(image)
        version = version or image.current_version

        versions = self._versions[image]
        try:
            return versions[version]
        except KeyError:
            if default is self._GET_IMAGE_DEFAULT:
                raise UnknownArtifactError(image.name + ':' + version)

            return default

    def get_image_versions(self, image):
        image = self.get_image(image)

        return sorted(self._versions[image].items(),
                      key=lambda v: self.version_key(v[0]))

    def get_image_base_artifact(self, image):
        image = self.get_image(image)
        base_name, base_version = image.base_with_version
        if not base_name:
            return None

        base_image = self.get_image(base_name, None)
        if base_image:
            base_artifact = self.get_image_artifact(base_image, base_version)
        else:
            base_artifact = self.get_artifact(image.base)

        return base_artifact

    def _build_graph(self, filter_by=None):
        images = set()
        to_visit = deque(filter(filter_by, self._image_set))
        edges = {}

        while to_visit:
            image = to_visit.popleft()
            if image in images:
                continue

            images.add(image)
            base_image = image.base and self.get_image(image.base)
            if not base_image:
                continue

            edges[image] = [base_image]
            to_visit.append(base_image)

        return images, edges

    def build_order(self, filter_by=None):
        levels, unsatisfied_edges = self._build_order(filter_by)
        if unsatisfied_edges:
            cycles = ', '.join(
                '{} <- {}'.format(dest, tuple(sources))
                for dest, sources in unsatisfied_edges.items())

            raise ConfigurationError(
                'Unsatisfied dependencies or cycles found: {}'.format(cycles))

        return levels
