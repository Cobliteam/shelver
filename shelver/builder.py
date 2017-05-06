from __future__ import absolute_import, print_function, unicode_literals
from builtins import map, filter
from past.builtins import basestring
from future.utils import iteritems

import sys
import os
import shutil
import json
import tempfile
import logging
import signal
from datetime import datetime
from collections import Mapping, defaultdict
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
from jinja2 import Template
from icicle import FrozenDictEncoder
from shelver.archive import Archive
from shelver.process_watcher import ProcessWatcher
from shelver.util import deep_merge, is_collection, TeeWriter


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

    def __init__(self, procs, out_streams, error_streams=None, **kwargs):
        super(PackerWatcher, self).__init__(procs, **kwargs)

        if not error_streams:
            error_streams = out_streams

        self.builds = set(procs)
        self.artifacts = defaultdict(dict)
        self.errors = defaultdict(list)
        self._out_streams = out_streams
        self._error_streams = error_streams

    def print_message(self, name, msg, target='', error=False):
        dest_files = (self._error_streams[name] if error
                      else self._out_streams[name])

        if target:
            target += ': '

        for dest_file in dest_files:
            if dest_file.isatty():
                text = '{name}{pad}{target}{msg}\n'.format(
                    name=self.colored(name + ':'),
                    pad=max(1, 15 - len(name)) * ' ',
                    target=target,
                    msg=msg)
            else:
                text = '{name}: {target}{msg}\n'.format(
                    name=name,
                    target=target,
                    msg=msg)

            dest_file.write(text.encode('utf-8'))
            dest_file.flush()

    def handle_line(self, name, line):
        try:
            timestamp, target, type_, data = line.rstrip().split(',', 3)
        except ValueError:
            self.print_message(name, line)
            return

        data = data.split(',')
        timestamp = int(timestamp)

        for i in range(len(data)):
            data[i] = data[i].replace('%!(PACKER_COMMA)', ',')

        if type_ == 'ui':
            dest, msg = data[0], ','.join(data[1:])
            self.print_message(name, target, msg, error=(dest == 'error'))
        else:
            print(line)

        if type_ == 'error':
            self.errors[name].append(','.join(data))
        elif type_ == 'artifact':
            i, data_key, data_val = int(data[0]), data[1], data[2:]
            artifact = self.artifacts[name].setdefault(i, {})

            if data_key == 'id':
                region, artifact_id = data[2].split(':', 1)
                artifact['region'] = region
                artifact['id'] = artifact_id
            elif data_key == 'end':
                pass
            elif len(data_val) == 1:
                artifact[data_key] = data_val[0]
            else:
                artifact[data_key] = data_val

    def handle_finish(self, name, proc):
        if proc.returncode != 0:
            if not self.errors[name]:
                self.errors[name].append(
                    'Packer failed with status {}'.format(proc.returncode))
        elif not self.artifacts.get(name):
            logger.warn('Packer finished successfully for build {}, '
                        'but no artifacts were returned')

    def results(self):
        self.wait_all()
        for name in self.builds:
            yield name, (self.artifacts[name], self.errors[name])


class Builder(object):
    LOCAL_DIR_PREFIX = '.shelver'

    def __init__(self, registry, base_dir, tmp_dir=None, cache_dir=None,
                 log_dir=None, keep_tmp=True, packer_cmd='packer'):
        if not log_dir:
            log_dir = os.path.join(base_dir, self.LOCAL_DIR_PREFIX, 'log')
        logger.debug('using log dir %s', cache_dir)

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
        self.log_dir = log_dir
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
        self._build_tmp_dir = None

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

    def _open_log(self, image, version):
        if not os.path.isdir(self.log_dir):
            os.makedirs(self.log_dir)

        path = '{}_{}_{}.log'.format(image.name, version,
            datetime.now().strftime('%Y-%m-%d_%H-%M-%S'))
        path = os.path.join(self.log_dir, path)

        return open(path, 'w')

    def run_build(self, image, version, logger=logger):
        logger.info('Starting build: %s, version %s', image, version)

        # Find base image
        base_artifact = self.registry.get_image_base_artifact(image)
        if base_artifact:
            logger.info('Found base artifact: %s', base_artifact)

        # Generate archive
        archive = Archive.from_config(self.base_dir, image.archive,
                                      tmp_dir=self.get_build_tmp_dir(),
                                      cache_dir=self.cache_dir)
        archive_path = archive.get_or_build()
        logger.info('Generated provision archive: %s', archive_path)

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

        fd, template_dest = tempfile.mkstemp(
            suffix='.json', dir=self.get_build_tmp_dir())

        with os.fdopen(fd, 'wb') as f:
            template_text = json.dumps(packer_data, indent=2,
                                       cls=FrozenDictEncoder)

            logger.debug('Generated packer template: \n%s', template_text)
            f.write(template_text.encode('utf-8'))

        # Run packer
        cmd = [self.packer_cmd, 'build', '-machine-readable', template_dest]
        logger.info('Packer command: %s', cmd)

        proc = subprocess.Popen(cmd,
            stdout=subprocess.PIPE, stdin=subprocess.PIPE)
        proc.stdin.close()

        return proc

    def _run_build_with_log(self, image, version, fileobj):
        build_logger = logger.getChild(image.name)

        handler = logging.StreamHandler(fileobj)
        handler.setLevel(logging.DEBUG)
        build_logger.addHandler(handler)

        try:
            return self.run_build(image, version, logger=build_logger)
        finally:
            build_logger.removeHandler(handler)
            handler.close()

    def _get_unbuilt_images(self, images):
        unbuilt = set()
        for image in images:
            if not self.registry.get_image(image):
                raise ValueError("Unregisted image '{}'".format(image.name))

            current_artifact = self.registry.get_image_artifact(image)
            if current_artifact:
                logger.info(
                    'found existing artifact - {}:{} = {}'.format(
                        image.name, image.current_version, current_artifact.id))
                continue

            if self.find_running_build(image, image.current_version):
                raise RuntimeError('Build already running for image {}, '
                                   'stopping'.format(image.name))

            unbuilt.add(image)

        return unbuilt

    def build_all(self, images):
        unbuilt_images = {img.name: img
                          for img in self._get_unbuilt_images(images)}
        if not unbuilt_images:
            return

        logger.info('building images in parallel: %s', ','.join(unbuilt_images))

        procs = {}
        log_files = {}
        try:
            for name, img in iteritems(unbuilt_images):
                version = img.current_version

                log_file = log_files[name] = self._open_log(img, version)
                procs[name] = self._run_build_with_log(img, version, log_file)

            out_streams = {k: [f, sys.stderr] for k, f in iteritems(log_files)}
            with PackerWatcher(procs, out_streams=out_streams) as watcher:
                try:
                    watcher.watch()
                except Exception as e:
                    logger.exception('Terminating packer processes due to error')
                    watcher.terminate_all(signal.SIGINT, kill_timeout=10)

                results = watcher.results()
                for name, (result, errors) in results:
                    if errors:
                        watcher.print_msg(name, '==> Build failed')
                        for error in errors:
                            watcher.print_msg(name, unicode(error))

                    img = unbuilt_images[name]

                    for artifact in result.values():
                        region = artifact.get('region')
                        logger.info('Got artifact for image %s (region %s): %s',
                                    name, region, artifact['id'])

                        self.registry.load_artifact_by_id(
                            artifact['id'], region=region, image=img)
        finally:
            for f in log_files.values():
                if not f.closed:
                    f.close()
