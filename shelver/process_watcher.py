from __future__ import absolute_import, unicode_literals
from future.utils import iteritems

import os
from fcntl import F_GETFL, F_SETFL, fcntl
from threading import Timer
try:
    from selectors import EVENT_READ, DefaultSelector
except ImportError:
    from selectors2 import EVENT_READ, DefaultSelector


class ProcessWatcher(object):
    @staticmethod
    def _extract_lines(buf):
        while True:
            line_end = buf.find(b'\n')
            if line_end == -1:
                break

            line, buf[:] = buf[:line_end + 1], buf[line_end + 1:]
            yield line.decode('utf-8')

    def __init__(self, procs, timeout=60):
        self.procs = procs
        self.timeout = timeout

        self.selector = DefaultSelector()
        self.results = {}
        self.buffers = {}

        for name, proc in iteritems(procs):
            # make process stdout non blocking
            fobj = proc.stdout
            fcntl(fobj, F_SETFL, fcntl(fobj, F_GETFL) | os.O_NONBLOCK)

            # register the fobj and initialize an empty buffer
            self.selector.register(fobj, EVENT_READ, data=name)
            self.buffers[name] = bytearray()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.selector:
            self.selector.close()
            self.selector = None

    def handle_line(self, name, line):
        pass

    def _reap_procs(self):
        for name, proc in list(self.procs.items()):
            ret = proc.poll()
            if ret is not None:
                results[name] = ret
                del self.procs[name]

        return bool(self.procs)


    def watch(self):
        while self._reap_procs():
            ready = self.selector.select(self.timeout)
            for key, events in ready:
                if not events & EVENT_READ:
                    continue

                buf = self.buffers[key.data]
                data = key.fileobj.read(1024)
                if not data:
                    self.handle_line(key.data, buf.decode('utf-8'))
                    self.selector.unregister(key.fileobj)
                    key.fileobj.close()
                    continue

                buf += data
                for line in self._extract_lines(buf):
                    self.handle_line(key.data, line)


    def kill_all(self):
        for proc in self.procs.values():
            if proc.poll() is None:
                proc.kill()

    def terminate_all(self, kill_timeout=None):
        for proc in self.procs.values():
            if proc.poll() is None:
                proc.terminate()

        if kill_timeout:
            timer = Timer(kill_timeout, self.kill_all)
            try:
                timer.start()
                self.wait_all()
            finally:
                timer.cancel()

    def wait_all(self):
        for name, proc in list(self.procs.items()):
            self.results[name] = proc.wait()
            del self.procs[name]
