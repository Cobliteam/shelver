from __future__ import absolute_import, unicode_literals
from future.utils import iteritems

from collections import defaultdict
from itertools import groupby

import yaml
from distutils.version import LooseVersion
from icicle import FrozenDict
from sortedcontainers import SortedDict
from shelver.image import Image
from shelver.artifact import Artifact
from shelver.util import topological_sort


class Registry(object):
    version_key = LooseVersion

    def __init__(self, provider, image_config):
        images = Image.load_all(image_config)

        self.provider = provider
        self._images = FrozenDict(images)
        self._image_set = frozenset(images.values())
        self._versions = defaultdict(lambda: SortedDict(self.version_key))
        self._artifacts = {}

    @property
    def images(self):
        return self._images

    def _get_image(self, image):
        try:
            if isinstance(image, Image):
                if image not in self._image_set:
                    raise KeyError(image.name)

                return image
            else:
                return self._images[image]
        except KeyError as e:
            raise RuntimeError('Unregistered image {}'.format(e.args[0]))

    def get_image(self, name, default=None):
        try:
            return self._get_image(name)
        except KeyError:
            return default

    def _check_artifact(self, artifact):
        if not isinstance(artifact, self.provider.Artifact):
            raise TypeError(
                'Unsupported artifact type: {}'.format(type(artifact)))

    @property
    def artifacts(self):
        return FrozenDict(self._artifacts)

    def register_artifact(self, artifact, name=None):
        self._check_artifact(artifact)

        if not name:
            name = artifact.name

        existing = self._artifacts.get(name)
        if existing:
            if existing != artifact:
                raise ValueError(
                    'Artifact already registered with name {}'.format(name))
        else:
            self._artifacts[name] = artifact

        return self

    def load_existing_artifacts(self):
        return self

    def get_artifact(self, name, default=None):
        return self._artifacts.get(name, default)

    def register_image_artifact(self, image, version, artifact):
        image = self._get_image(image)

        versions = self._versions[image]
        existing = versions.get(version)
        if existing:
            if existing != artifact:
                raise ValueError(
                    'Image {} already has artifact for version {}'.format(
                        image, version))
        else:
            self.register_artifact(artifact)
            versions[version] = artifact

        return self

    def get_image_artifact(self, image, version=None, default=None):
        image = self._get_image(image)
        if not version:
            version = image.current_version

        versions = self._versions[image]
        return versions.get(version, default)

    def get_image_versions(self, image):
        image = self._get_image(image)

        for version, artifact in iteritems(self._versions[image]):
            yield version, artifact

    def get_image_base_artifact(self, image):
        if not image.base:
            return None

        try:
            base_name, base_version = image.base.split(':')
        except ValueError:
            base_name, base_version = image.base, None

        base = self.get_image(base_name)
        if base:
            base_artifact = self.get_image_artifact(base, base_version)
        else:
            base_artifact = self.get_artifact(image.base)

        if not base_artifact:
            raise RuntimeError(
                "Unable to find existing image or artifact '{}'. "
                "Check your config for errors.".format(
                    image.base))

        return base_artifact

    def build_order(self, filter_by=None):
        images = set(filter(filter_by, self._image_set))
        edges = {}

        for image in images:
            base_image = image.base and self.get_image(image.base)
            if not base_image:
                continue

            # Make sure we add parent images even if they were filtered out
            images.add(base_image)
            edges[image] = [base_image]

        ordered_images = topological_sort(images, edges)
        groups = groupby(ordered_images, lambda (i, _): i)
        for _, group in groups:
            yield [image for _, image in group]
