class ShelverError(Exception):
    pass


class ConfigurationError(ShelverError):
    pass


class UnknownArtifactError(ShelverError):
    pass


class UnknownImageError(ShelverError):
    pass


class ConcurrentBuildError(ShelverError):
    pass


class PackerError(ShelverError):
    def __init__(self, exitcode, errors):
        self.exitcode = exitcode
        self.errors = errors

    def __str__(self):
        return 'Packer failed with exit code {}. Reported errors: {}'.format(
            self.exitcode, '\n'.join(self.errors))
