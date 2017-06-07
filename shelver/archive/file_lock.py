import fcntl
import asyncio

from shelver.util import AsyncBase


class FileLock(AsyncBase):
    def __init__(self, file, **kwargs):
        super().__init__(**kwargs)
        self._file = file

    @asyncio.coroutine
    def acquire(self, exclusive=True, *, timeout=None):
        flags = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
        do_lock = self.delay(fcntl.flock, self._file, flags)

        yield from asyncio.wait_for(do_lock, timeout, loop=self._loop)
        return self._file

    def release(self):
        fcntl.flock(self._file, fcntl.LOCK_UN)
