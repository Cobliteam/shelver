from collections import namedtuple

from shelver.util import deep_merge, freeze


class Image(namedtuple('Image', 'name current_version environment description '
                                'template_path base '
                                'archive provision '
                                'instance_type metadata '
                                'builder_opts')):
    __slots__ = ()

    DEFAULTS = {
        'environment': 'prod',
        'description': '{{ name }} - version {{ current_version }}',
        'template_path': 'packer.yml',
        'archive': {},
        'base': None,
        'metadata': [],
        'provision': None,
        'builder_opts': {}
    }

    @classmethod
    def from_dict(cls, data, defaults=None):
        if defaults is not None:
            actual_defaults = deep_merge(cls.DEFAULTS, defaults)
        else:
            actual_defaults = cls.DEFAULTS

        d = deep_merge(actual_defaults, data)
        d['current_version'] = d.pop('version')
        d = freeze(d)

        return cls(**d)

    @classmethod
    def load_all(cls, data):
        if not isinstance(data, dict):
            raise ValueError('Configuration must be a dict of image specs')

        data = data.copy()
        defaults = data.pop('defaults', None)
        images = {}
        for name, config in data.items():
            config = config.copy()
            config['name'] = name
            images[name] = Image.from_dict(config, defaults=defaults)

        return images

    @property
    def base_with_version(self):
        if not self.base:
            return None, None

        try:
            name, version = self.base.split(':', 1)
            return name, version
        except ValueError:
            return self.base, None
