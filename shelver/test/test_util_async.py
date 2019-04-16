import os
import asyncio
import threading
import time
import signal
try:
    from asyncio import ensure_future
except ImportError:
    ensure_future = getattr(asyncio, 'async')

import pytest
from shelver.util import AsyncBase, AsyncLoopSupervisor


def test_async_base_init_def():
    async_obj = AsyncBase()
    assert isinstance(async_obj._loop, asyncio.AbstractEventLoop)
    assert async_obj._executor is None


def test_async_base_init_explicit(event_loop):
    executor = object()
    async_obj = AsyncBase(loop=event_loop, executor=executor)

    assert async_obj._loop is event_loop
    assert async_obj._executor is executor


@pytest.mark.asyncio
def test_async_base_delay(event_loop):
    async_obj = AsyncBase(loop=event_loop)

    # Run a sleep in the background with delay, get the time in a second
    # coroutine and compare with the time after the sleep to check for blocking
    def get_time():
        time.sleep(0.2)
        return event_loop.time()

    f = ensure_future(async_obj.delay(get_time))
    t1 = event_loop.time()
    t2 = yield from f

    # If running get_time() blocked, it would have finished before running the
    # subsequent lines, making t2 >= t1.
    assert t1 < t2


@pytest.fixture
def supervisor(event_loop):
    return AsyncLoopSupervisor(event_loop)


def test_async_loop_supervisor_init(event_loop):
    supervisor = AsyncLoopSupervisor(event_loop, timeout=10)
    assert supervisor.loop is event_loop
    assert supervisor.timeout == 10


def _count_cancellations(timeout, times=1, loop=None):
    cancel_count = 0

    for _ in range(times):
        try:
            yield from asyncio.sleep(timeout, loop=loop)
        except asyncio.CancelledError:
            cancel_count += 1

    return cancel_count


def _interrupt(timeout, times=1):
    os.kill(os.getpid(), signal.SIGINT)

    if times > 1:
        t = threading.Timer(timeout, _interrupt, (timeout, times - 1))
        t.start()


@pytest.mark.boxed
def test_async_loop_supervisor_supervise(event_loop, supervisor):
    supervisor.timeout = 1
    cancellations = supervisor.supervise(
        _count_cancellations(0.2, times=1, loop=event_loop))

    assert not supervisor.loop.is_running()
    assert cancellations == 0
    assert not supervisor.timed_out


@pytest.mark.boxed
def test_async_loop_supervisor_interrupt(event_loop, supervisor):
    # Set up the supervisor with a short timeout. Also send a signal a short
    # time after starting. The supervisor should pick up the first signal, send
    # the first cancellation, then afterwards send the second cancellation when
    # the timeout expires.

    t = threading.Timer(0.2, _interrupt, (0.2, 1))
    t.start()
    supervisor.timeout = 0.2

    cancellations = supervisor.supervise(
        _count_cancellations(0.5, times=2, loop=event_loop))

    assert not supervisor.loop.is_running()
    assert cancellations == 2
    assert supervisor.timed_out


@pytest.mark.boxed
def test_async_loop_supervisor_double_interrupt(event_loop, supervisor):
    # Same as above, but send signals twice to check if the second one triggers
    # a second cancellation, just as a timeout would.

    t = threading.Timer(0.2, _interrupt, (0.2, 2))
    t.start()
    supervisor.timeout = 2

    cancellations = supervisor.supervise(
        _count_cancellations(0.5, times=2, loop=event_loop))

    assert not supervisor.loop.is_running()
    assert cancellations == 2
    assert not supervisor.timed_out
