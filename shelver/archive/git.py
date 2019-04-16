import asyncio
import os
import shutil
import subprocess

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
        self._git_lock = asyncio.Lock()
        self._basename = None
        self._revision_id = None

    async def _run_git(self, *args, capture=False, **kwargs):
        stdout = subprocess.PIPE if capture else None
        await self._git_lock.acquire()
        try:
            out, err = await async_subprocess_run(
                self.git_cmd, *args, stdout=stdout, loop=self._loop, **kwargs)
            if not capture:
                return None, None

            return out, err
        finally:
            self._git_lock.release()

    async def revision_id(self):
        if not self._revision_id:
            rev = self.revision + '^{commit}'
            out, _ = await \
                self._run_git('rev-parse', '--verify', rev,
                              cwd=self.source_dir, capture=True)
            self._revision_id = out.rstrip().decode('utf-8')

        return self._revision_id

    async def basename(self):
        if not self._basename:
            repo_name = os.path.basename(os.path.abspath(self.source_dir))
            rev = await self.revision_id()
            self._basename = '{}-{}.tar.xz'.format(repo_name, rev)

        return self._basename

    async def build(self):
        rev = await self.revision_id()
        basename = await self.basename()

        work_tree = os.path.join(self.tmp_dir, 'worktree')
        archive = os.path.join(self.tmp_dir, basename)

        try:
            await self._run_git(
                'worktree', 'add', '--detach', work_tree, cwd=self.source_dir)
            await self._run_git(
                'checkout', '--detach', rev, cwd=work_tree)
            await self._run_git(
                'submodule', 'update', '--init', '--recursive', '--checkout',
                '--force', cwd=work_tree)
            await async_subprocess_run(
                'tar', '-c', '--exclude=.git', '--exclude=.git/*', '-f',
                archive, '.', cwd=work_tree)
        finally:
            try:
                clean = self._loop.run_in_executor(
                    self._executor, shutil.rmtree, work_tree)
                await clean

                await self._run_git('worktree', 'prune', cwd=self.source_dir)
            except Exception:
                pass

        return archive

    def to_dict(self):
        d = super().to_dict()
        d['revision'] = self.revision
        d['commit'] = self._revision_id
        return d
