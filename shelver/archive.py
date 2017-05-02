from __future__ import absolute_import, unicode_literals
from future.utils import with_metaclass

import sys
import os
import shutil
from abc import ABCMeta, abstractmethod, abstractproperty
if sys.version_info[0] < 3:
    import subprocess32 as subprocess
else:
    import subprocess

class Archive(with_metaclass(ABCMeta, object)):
    _types = {}

    @classmethod
    def register_type(cls, archive_cls):
        for name in archive_cls.NAMES:
            cls._types[name] = archive_cls

    @classmethod
    def from_config(cls, base_dir, cfg, **defaults):
        archive_opts = dict(defaults)
        archive_opts.update(cfg)

        source_dir = os.path.join(base_dir, archive_opts.pop('dir'))
        archive_type = archive_opts.pop('type')
        try:
            archive_cls = cls._types[archive_type]
        except KeyError:
            raise RuntimeError(
                "Unknown archive type '{}'".format(archive_type))

        return archive_cls(source_dir=source_dir,
                           **archive_opts)

    def __init__(self, source_dir, tmp_dir, cache_dir):
        self.source_dir = source_dir
        self.tmp_dir = tmp_dir
        self.cache_dir = cache_dir

        self._path = None

    @abstractproperty
    def basename(self):
        pass

    @abstractmethod
    def build(self):
        pass

    def get_or_build(self):
        if not self._path:
            cached = os.path.join(self.cache_dir, self.basename)
            if not os.path.isfile(cached):
                if not os.path.isdir(self.cache_dir):
                    os.makedirs(self.cache_dir)

                shutil.move(self.build(), cached)

            self._path = cached

        return self._path

    def template_context(self):
        return {
            'repo_source_dir': self.source_dir
        }

class GitArchive(Archive):
    NAMES = ['git']

    def __init__(self, source_dir, tmp_dir, cache_dir, revision=None,
                 git_cmd='git'):
        cache_dir = os.path.join(cache_dir, 'git-archive')
        super(GitArchive, self).__init__(source_dir, tmp_dir, cache_dir)

        self.git_cmd = git_cmd
        self.revision = revision or 'HEAD'
        self._basename = None
        self._rev_hash = None

    @property
    def rev_hash(self):
        if not self._rev_hash:
            self._rev_hash = subprocess.check_output(
                [self.git_cmd, 'rev-parse', self.revision],
                cwd=self.source_dir).strip()

        return self._rev_hash

    @property
    def basename(self):
        if not self._basename:
            repo_name = os.path.basename(os.path.abspath(self.source_dir))
            self._basename =  '{}-{}.tar.xz'.format(repo_name, self.rev_hash)

        return self._basename

    def build(self):
        worktree = os.path.join(self.tmp_dir, 'worktree')
        archive = os.path.join(self.tmp_dir, self.basename)

        try:
            subprocess.check_call(
                [self.git_cmd, 'worktree', 'add', '--detach', worktree],
                cwd=self.source_dir)
            subprocess.check_call(
                [self.git_cmd, 'checkout', '--detach', self.rev_hash],
                cwd=worktree)
            subprocess.check_call(
                [self.git_cmd, 'submodule', 'update', '--init', '--recursive',
                 '--checkout', '--force'],
                cwd=worktree)
            subprocess.check_call(
                ['tar', '-c', '--exclude=.git', '--exclude=.git/*',
                 '-f', archive, '.'],
                cwd=worktree)
        finally:
            try:
                shutil.rmtree(worktree)
                subprocess.check_call([self.git_cmd, 'worktree', 'prune'],
                                      cwd=self.source_dir)
            except Exception:
                pass

        return archive

    def template_context(self):
        context = super(GitArchive, self).template_context()
        context.update({
            'repo_rev': self.revision,
            'repo_commit': self.rev_hash,
        })
        return context

Archive.register_type(GitArchive)
