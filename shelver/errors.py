class ShelverError(Exception):
    pass


class ConfigurationError(ShelverError):
    pass


class UnknownArtifactError(ShelverError):
    def __init__(self, name, version):
        self.name = name
        self.version = version


class UnknownImageError(ShelverError):
    pass


class ConcurrentBuildError(ShelverError):
    pass


class PackerError(ShelverError):
    def __init__(self, exitcode, errors):
        self.exitcode = exitcode
        self.errors = errors

    def __str__(self):
        msg = ''
        if self.errors:
            errors = '\n'.join(map('- {}'.format, self.errors))
            msg = ' Reported errors:\n' + errors

        return 'Packer failed with exit code {}.{}'.format(
            self.exitcode, msg)
