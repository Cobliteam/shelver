from abc import ABCMeta, abstractmethod
from collections import defaultdict

from distutils.version import LooseVersion
from shelver.image import Image
from shelver.util import (AsyncBase, TopologicalSortError, freeze,
                          topological_sort)
from shelver.errors import (ConfigurationError, UnknownArtifactError,
                            UnknownImageError)


class Registry(AsyncBase, metaclass=ABCMeta):
    _GET_IMAGE_DEFAULT = object()

    version_key = LooseVersion

    def __init__(self, images, *, provider, **kwargs):
        super().__init__(**kwargs)

        self.provider = provider
        self._images = freeze(images)
        self._image_set = frozenset(self._images.values())
        self._versions = defaultdict(dict)
        self._artifacts = {}

    @property
    def images(self):
        return self._images

    def __getitem__(self, key):
        return self._images[key]

    def get_image(self, image, default=_GET_IMAGE_DEFAULT):
        if isinstance(image, Image):
            if image not in self._image_set:
                raise UnknownImageError(image.name)

            return image

        try:
            return self._images[image]
        except KeyError:
            if default is self._GET_IMAGE_DEFAULT:
                raise UnknownImageError(image)

            return default

    @property
    def artifacts(self):
        return self._artifacts

    def _check_artifact(self, artifact):
        if not isinstance(artifact, self.provider.artifact_class):
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
            self._artifacts[artifact.id] = artifact

        return self

    def get_artifact(self, name, default=_GET_IMAGE_DEFAULT):
        try:
            return self._artifacts[name]
        except KeyError:
            if default is self._GET_IMAGE_DEFAULT:
                raise UnknownArtifactError(name)

            return default

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

    @abstractmethod
    async def load_existing_artifacts(self, region=None):
        pass

    @abstractmethod
    async def load_artifact_by_id(self, id, region=None, image=None):
        pass

    def get_image_artifact(self, image, version=None,
                           default=_GET_IMAGE_DEFAULT):
        image = self.get_image(image)
        version = version or image.current_version

        versions = self._versions[image]
        try:
            return versions[version]
        except KeyError:
            if default is self._GET_IMAGE_DEFAULT:
                raise UnknownArtifactError(image.name, version)

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

        base_image = self.get_image(base_name, default=None)
        if base_image:
            base_artifact = self.get_image_artifact(base_image, base_version)
        else:
            base_artifact = self.get_artifact(image.base)

        return base_artifact

    def check_cycles(self):
        edges = defaultdict(list)
        for image in self._image_set:
            base_name, base_version = image.base_with_version
            if not base_name:
                continue

            if base_name in self._artifacts:
                continue

            base_image = self.get_image(base_name)
            edges[base_image].append(image)

        # Will raise when a cycle is found
        try:
            topological_sort(self._image_set, edges)
        except TopologicalSortError as e:
            cycles_msg = ', '.join(' <- '.format(dest, srcs)
                                   for dest, srcs in e.cycles.items())
            raise ConfigurationError(
                'Image dependency graph contains cycles: {}'.format(
                    cycles_msg))
