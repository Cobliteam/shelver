import os
import shutil
import subprocess
import asyncio

from shelver.util import async_subprocess_run
from .base import Archive


class GitArchive(Archive):
    NAMES = ('git',)

    def __init__(self, source_dir, tmp_dir, cache_dir, *, revision=None,
                 git_cmd='git', **kwargs):
        cache_dir = os.path.join(cache_dir, 'git-archive')
        super().__init__(source_dir, tmp_dir, cache_dir, **kwargs)

        self.git_cmd = git_cmd
        self.revision = revision or 'HEAD'
        self._basename = None
        self._revision_hash = None

    @asyncio.coroutine
    def _run_git(self, *args, capture=False, **kwargs):
        stdout = subprocess.PIPE if capture else None
        out, err = yield from async_subprocess_run(
            self.git_cmd, *args, stdout=stdout, loop=self._loop, **kwargs)
        if not capture:
            return None, None

        return out, err

    @asyncio.coroutine
    def revision_hash(self):
        if not self._revision_hash:
            out, _ = yield from \
                self._run_git('rev-parse', self.revision, cwd=self.source_dir,
                              capture=True)
            self._revision_hash = out.rstrip().decode('utf-8')

        return self._revision_hash

    @asyncio.coroutine
    def basename(self):
        if not self._basename:
            repo_name = os.path.basename(os.path.abspath(self.source_dir))
            rev = yield from self.revision_hash()
            self._basename = '{}-{}.tar.xz'.format(repo_name, rev)

        return self._basename

    @asyncio.coroutine
    def build(self):
        rev = yield from self.revision_hash()
        basename = yield from self.basename()

        work_tree = os.path.join(self.tmp_dir, 'worktree')
        archive = os.path.join(self.tmp_dir, basename)

        try:
            yield from self._run_git(
                'worktree', 'add', '--detach', work_tree, cwd=self.source_dir)
            yield from self._run_git(
                'checkout', '--detach', rev, cwd=work_tree)
            yield from self._run_git(
                'submodule', 'update', '--init', '--recursive', '--checkout',
                '--force', cwd=work_tree)
            yield from async_subprocess_run(
                'tar', '-c', '--exclude=.git', '--exclude=.git/*', '-f',
                archive, '.', cwd=work_tree)
        finally:
            try:
                clean = self._loop.run_in_executor(
                    self._executor, shutil.rmtree, work_tree)
                yield from clean

                yield from self._run_git('worktree', 'prune',
                                         cwd=self.source_dir)
            except Exception:
                pass

        return archive

    def to_dict(self):
        d = super().to_dict()
        d['revision'] = self.revision
        d['commit'] = self._revision_hash
        return d
