import asyncio
import logging

from shelver.errors import ConfigurationError, PackerError, ShelverError
from shelver.util import AsyncBase

logger = logging.getLogger('shelver.build.coordinator')


class Coordinator(AsyncBase):
    def __init__(self, builder, *, msg_stream=None, max_builds=None,
                 cancel_timeout=60, **kwargs):
        super().__init__(**kwargs)

        self.builder = builder
        self.registry = builder.registry
        self.cancel_timeout = cancel_timeout
        self.stopping = False
        self._msg_stream = msg_stream
        self._build_counter = asyncio.BoundedSemaphore(
            max_builds or 999, loop=self._loop)
        self._builds = {}

    def _wait_builds(self, timeout=None):
        return asyncio.wait_for(
            asyncio.gather(*self._builds.values()), timeout=timeout)

    @asyncio.coroutine
    def run_all(self):
        self.registry.check_cycles()

        try:
            yield from self._wait_builds()
        except asyncio.CancelledError:
            # Don't stop builds on the first cancel. Wait a timeout before
            # killling them.

            self.stopping = True
            try:
                yield from self._wait_builds(self.cancel_timeout)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                # If the timeout is reached, cancel all builds and ourselves
                for build in self._builds.values():
                    build.cancel()

        return self._builds

    def get_or_run_build(self, image, version=None):
        if not version:
            version = image.current_version

        try:
            return self._builds[(image.name, version)]
        except KeyError:
            if self.stopping:
                raise asyncio.InvalidStateError('Coordinator is closed')

            build = self._builds[(image.name, version)] = \
                asyncio.ensure_future(self._run_build(image, version))
            return build

    @asyncio.coroutine
    def _get_base_artifact(self, image, version):
        base_name, base_version = image.base_with_version
        if not base_name:
            return None

        base_image = self.registry.get_image(base_name, None)
        if base_image:
            # If we depend on a registered image, try to look up the artifact
            # with the current version. If it does not yet exist, a recursive
            # build will be triggered and waited upon, after which ther artifact
            # should be available.
            # Notice how the second call to get_image_artifact does not provide
            # a default, as it should fail if the artifact was not registered
            # even when it's build completed.

            base_artifact = self.registry.get_image_artifact(
                base_image, base_version, default=None)
            if base_artifact:
                return base_artifact

            build = yield from self.get_or_run_build(base_image, base_version)
            yield from build

            return self.registry.get_image_artifact(base_image, base_version)
        else:
            # If we depend on an unregistered image, just try to grab it
            # immediately and fail if it is not available.
            return self.registry.get_artifact(base_name)

    @asyncio.coroutine
    def _run_build(self, image, version):
        if not version:
            version = image.current_version
        elif version != image.current_version:
            raise ConfigurationError(
                'Cannot build image {}: wanted version ({}) differs from '
                'current version ({})'.format(image.name, version,
                                              image.current_version))

        # Getting the base artifact may trigger other builds, which will acquire
        # the build semaphore, so delay our own call until we'll actually start
        # to build.
        try:
            base_artifact = yield from self._get_base_artifact(image, version)
        except PackerError:
            raise ShelverError(
                'Build for base image {} failed'.format(image.base))

        yield from self._build_counter.acquire()
        try:
            results = yield from self.builder.run_build(
                image, version, base_artifact=base_artifact,
                msg_stream=self._msg_stream)
        finally:
            self._build_counter.release()

        artifacts = []
        for result in results:
            try:
                id = result['id']
                region = result.get('region')
                artifact = yield from self.registry.load_artifact_by_id(
                    id, region=region)
                artifacts.append(artifact)
            except (KeyError, ValueError) as e:
                logger.warn('Failed to register created artifact: %s',
                            result)

        return artifacts
