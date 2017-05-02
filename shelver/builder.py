from __future__ import absolute_import, print_function
from builtins import map, filter
from past.builtins import basestring
from future.utils import iteritems

import sys
import os
import shutil
import json
import tempfile
import logging
from collections import Mapping
from ast import literal_eval
from fcntl import F_GETFL, F_SETFL, fcntl

if sys.version_info[0] < 3:
    import subprocess32 as subprocess
else:
    import subprocess

try:
    from selectors import DefaultSelector
except ImportError:
    from selectors2 import DefaultSelector

import yaml
import colored
from jinja2 import Template
from icicle import FrozenDictEncoder
from shelver.archive import Archive
from shelver.process_watcher import ProcessWatcher
from shelver.util import deep_merge, is_collection


logger = logging.getLogger('shelver.builder')


class PackerWatcher(ProcessWatcher):
    COLORS = [
        '\033[31m', # red
        '\033[32m', # green
        '\033[33m', # yellow
        '\033[34m', # blue
        '\033[35m', # magenta
        '\033[36m', # cyan
    ]
    COLOR_RESET = '\033[39m'

    @classmethod
    def colored(cls, s):
        start = cls.COLORS[hash(s) % len(cls.COLORS)]
        return start + s + cls.COLOR_RESET

    def print_message(self, name, target, data):
        dest, msg = data[0], data[1:]
        dest_file = sys.stderr if dest == 'error' else sys.stdout

        if target:
            target += ': '

        if dest_file.isatty():
            text = '{name}{pad}{target}{msg}\n'.format(
                name=self.colored(name + ':'),
                pad=max(1, 15 - len(name)) * ' ',
                target=target,
                msg=','.join(msg))
        else:
            text = '{name}: {target}{msg}\n'.format(
                name=name,
                target=target,
                msg=','.join(msg))

        dest_file.write(text)
        dest_file.flush()

    def handle_line(self, name, line):
        timestamp, target, type_, data = line.rstrip().split(',', 3)
        data = data.split(',')
        timestamp = int(timestamp)

        for i in range(len(data)):
            data[i] = data[i] \
                .replace('%!(PACKER_COMMA)', ',') \
                .replace('\\n', '\n') \
                .replace('\\r', '\r')

        if type_ == 'ui':
            self.print_message(name, target, data)
        elif type_ == 'artifact':
            logger.debug('Received artifact line: %s', line)

class Builder(object):
    LOCAL_DIR_PREFIX = '.shelver'

    def __init__(self, registry, base_dir, tmp_dir=None, cache_dir=None,
                 keep_tmp=True, packer_cmd='packer'):
        if not tmp_dir:
            tmp_dir = os.path.join(base_dir, self.LOCAL_DIR_PREFIX, 'tmp')
        logger.debug('using tmp dir %s', tmp_dir)

        if not cache_dir:
            cache_dir = os.path.join(base_dir, self.LOCAL_DIR_PREFIX, 'cache')
        logger.debug('using cache dir %s', cache_dir)

        self.registry = registry
        self.base_dir = base_dir
        self.tmp_dir = tmp_dir
        self.cache_dir = cache_dir
        self.keep_tmp = bool(keep_tmp)
        self.packer_cmd = packer_cmd

        self._build_tmp_dir = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._build_tmp_dir and not self.keep_tmp:
            try:
                logger.info('Cleaning up temporary build dir %s',
                            self._build_tmp_dir)
                shutil.rmtree(self._build_tmp_dir)
            except Exception:
                pass

        return False

    def get_build_tmp_dir(self):
        if not os.path.isdir(self.tmp_dir):
            logger.debug('Creating tmp dir: %s', self.tmp_dir)
            os.makedirs(self.tmp_dir)

        if not self._build_tmp_dir:
            self._build_tmp_dir = tempfile.mkdtemp(dir=self.tmp_dir)

        return self._build_tmp_dir

    def template_context(self, image, version):
        return {
            'name': image.name,
            'version': version,
            'description': image.description,
            'environment': image.environment,
            'instance_type': image.instance_type,
            'base': image.base,
            'provision': image.provision
        }

    def template_apply(self, data, context):
        if isinstance(data, Mapping):
            return dict((k, self.template_apply(v, context))
                        for (k, v) in data.items())
        elif is_collection(data):
            return list(self.template_apply(v, context) for v in data)
        elif isinstance(data, str):
            result = Template(data).render(context)
            try:
                result_obj = literal_eval(result)
            except (SyntaxError, ValueError):
                result_obj = result

            return result_obj

    def run_build(self, image, version):
        logger.info('Starting build: %s, version %s', image, version)

        if self.find_running_build(image, version):
            raise RuntimeError('Build already running for image {}, '
                               'stopping'.format(image.name))

        # Find base image
        base_artifact = self.registry.get_image_base_artifact(image)
        if base_artifact:
            logger.debug('Found base artifact: %s', base_artifact)

        # Generate archive
        archive = Archive.from_config(self.base_dir, image.archive,
                                      tmp_dir=self.get_build_tmp_dir(),
                                      cache_dir=self.cache_dir)
        archive_path = archive.get_or_build()
        logger.debug('Generated provision archive: %s', archive_path)

        # Prepare packer template
        context = self.template_context(image, version)
        context.update(archive.template_context())
        context.update({
            'base_artifact': base_artifact,
            'repo_archive': archive_path
        })

        with open(image.template_path, 'rb') as f:
            packer_data = self.template_apply(yaml.safe_load(f), context)

        # Merge builder configuration in the template
        try:
            packer_data['builders'] = list(map(
                lambda d: deep_merge(d, image.builder_opts),
                packer_data['builders']))
        except KeyError as e:
            raise ValueError('No builders key found in template')

        logger.debug('Final packer template: %s', packer_data)

        fd, template_dest = tempfile.mkstemp(
            suffix='.json', dir=self.get_build_tmp_dir())

        with os.fdopen(fd, 'wb') as f:
            json.dump(packer_data, f, cls=FrozenDictEncoder)

        # Run packer
        cmd = [self.packer_cmd, 'build', '-machine-readable', template_dest]
        logger.debug('Running packer: %s', cmd)

        proc = subprocess.Popen(cmd,
            stdout=subprocess.PIPE, stdin=subprocess.PIPE)
        proc.stdin.close()

        return proc

    def build_all(self, images):
        for image in images:
            if not self.registry.get_image(image):
                raise ValueError("Unregisted image '{}'".format(image.name))

        image_names = ', '.join(img.name for img in images)
        logger.info('building images in parallel: %s', image_names)

        procs = {image.name: self.run_build(image, image.current_version)
                 for image in images}
        with PackerWatcher(procs) as watcher:
            try:
                watcher.watch()
            except Exception:
                logger.info('Terminating packer processes due to error')
                watcher.terminate_all(kill_timeout=10)
