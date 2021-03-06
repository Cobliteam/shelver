import os
import asyncio
import random
import tempfile

import pytest
from shelver.archive.file_lock import FileLock


class FakeFileLock(FileLock):
    """A FileLock that does nothing, for canary testing"""
    async def acquire(self, *args, **kwargs):
        return self._file

    def release(self):
        pass


async def _run_competing_file_writes(make_lock):
    """
    Write to the same file in two coroutines and return the final contents

    make_lock is a function that receives a file object and returns a FileLock
    object.
    """

    fd, fname = tempfile.mkstemp()
    try:
        # Open the temp file two times, so that locks are not shared between
        # coroutines
        with os.fdopen(fd, 'w+b', 0) as f1, open(fname, 'w+b', 0) as f2:
            # Replace the whole content of the file with data, but write it in
            # a random fashion, jumping between positions, so that we can
            # observe any races. If the lock is functioning correctly, the file
            # will end up as if one of the two pieces of data was written
            # sequentially.

            async def write(lock, data):
                f = await lock.acquire()

                f.truncate(len(data))
                positions = list(range(len(data)))
                random.shuffle(positions)

                for i in positions:
                    # Write one char then yield so that other coroutines can
                    # run and step over us if the lock is broken.
                    f.seek(i)
                    f.write(data[i:i + 1])
                    await asyncio.sleep(0)

                lock.release()

            await asyncio.gather(
                write(make_lock(f1), b'hello'),
                write(make_lock(f2), b'world'))

        # Read the contents of the file after the competing writes.
        with open(fname, 'rb') as f3:
            content = f3.read()

        return content
    finally:
        os.unlink(fname)


@pytest.mark.asyncio
async def test_filelock_exclusive():
    for i in range(10):
        result = await _run_competing_file_writes(FileLock)

        # We must get correct results every single time to be sure the lock
        # works
        assert result in (b'hello', b'world')


@pytest.mark.asyncio
async def test_filelock_canary():
    """Run tests with a do-nothing lock to validate the test above"""

    results = set()
    for i in range(10):
        result = await _run_competing_file_writes(FakeFileLock)
        results.add(result)

    # Consider a failure if there are any non-deterministic results. This way
    # we can confirm that we have races without the working file lock.
    assert len(results) != 1
