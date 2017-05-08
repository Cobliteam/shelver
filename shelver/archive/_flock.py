import fcntl
import asyncio

class FileLock():
    def __init__(self, file, loop=None, executor=None):
        self._file = file
        self._loop = loop or asyncio.get_event_loop()
        self._executor = executor

    @asyncio.coroutine
    def acquire(self, exclusive=True, *, timeout=None):
        flags = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
        do_lock = self._loop.run_in_executor(
            self._executor, fcntl.flock, self._file.fileno(), flags)

        yield from asyncio.wait_for(do_lock, timeout, loop=self._loop)

    def release(self):
        fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)

