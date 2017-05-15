import signal
import logging
import asyncio

from shelver.errors import PackerError


logger = logging.getLogger('shelver.build.watcher')


class Watcher(object):
    COLORS = [
        b'\033[31m',  # red
        b'\033[32m',  # green
        b'\033[33m',  # yellow
        b'\033[36m',  # cyan
        b'\033[34m',  # blue
        b'\033[35m',  # magenta
    ]
    COLOR_RESET = b'\033[39m'

    @classmethod
    def colored(cls, s):
        start = cls.COLORS[hash(s) % len(cls.COLORS)]
        return start + s + cls.COLOR_RESET

    def __init__(self, prefix, msg_stream, log_stream, *, loop=None):
        if not isinstance(prefix, bytes):
            prefix = prefix.encode('utf-8')

        self.prefix = prefix
        self.errors = []
        self.artifacts = []
        self._msg_stream = msg_stream
        self._log_stream = log_stream
        self._loop = loop or asyncio.get_event_loop()
        self._future = None

    @staticmethod
    def _parse_line(line):
        try:
            _, target, type_, data = line.rstrip().split(b',', 3)
        except ValueError:
            return None, None, None

        data = data.replace(b'%!(PACKER_COMMA)', b',')
        return target, type_, data

    @asyncio.coroutine
    def write_message(self, line):
        isatty = yield from self._msg_stream.isatty()
        if isatty:
            prefix = self.colored(self.prefix + b':')
        else:
            prefix = self.prefix + b':'

        yield from self._msg_stream.write(prefix + b' ' + line + b'\n')
        yield from self._msg_stream.flush()
        yield from self._log_stream.write(line + b'\n')
        yield from self._log_stream.flush()

    @asyncio.coroutine
    def handle_stdout(self, stream):
        while True:
            line = yield from stream.readline()
            if not line:
                break

            target, type_, data = self._parse_line(line)
            if data is None:
                yield from self.write_message(line)
                continue

            # We only keep the data as bytes if it is possibly coming from
            # program output.
            if type_ == b'ui':
                msg = data.split(b',', 1)[-1]
                if target:
                    msg = target + b': ' + msg

                yield from self.write_message(msg)
                continue

            # Otherwise, we transform everything to unicode and work from there.
            data = data.decode('utf-8')

            if type_ == b'error':
                self.errors.append(data)
            elif type_ == b'artifact':
                i, data_key, *data_val = data.split(',')
                i = int(i)

                while i >= len(self.artifacts):
                    self.artifacts.append({})
                artifact = self.artifacts[i]

                if data_key == 'id':
                    region, artifact_id = data_val[0].split(':', 1)
                    artifact['region'] = region
                    artifact['id'] = artifact_id
                elif data_key == 'end':
                    pass
                elif len(data_val) == 1:
                    artifact[data_key] = data_val[0]
                else:
                    artifact[data_key] = data_val

    @asyncio.coroutine
    def handle_stderr(self, stream):
        while True:
            line = yield from stream.readline()
            if not line:
                break

            yield from self.write_message(line)

    @staticmethod
    def _send_signal(proc, signame):
        sig = getattr(signal, signame)
        logger.debug('Sending %s to pid %d', signame, proc.pid)
        proc.send_signal(sig)

    @asyncio.coroutine
    def run(self, proc):
        io = asyncio.gather(
            self.handle_stdout(proc.stdout),
            self.handle_stderr(proc.stderr),
            loop=self._loop)

        try:
            # Wait until we finish consuming IO and the processes finishes,
            # but shield the IO from cancellation, as we will keep running
            # after sending the SIGINT and waiting for Packer to finish
            # gracefully
            _, ret = yield from asyncio.gather(asyncio.shield(io),
                                               proc.wait())
        except asyncio.CancelledError:
            self.errors.append('Canceled by signal')
            self._send_signal(proc, 'SIGINT')

            try:
                # Cancel everything right away when we receive the second
                # cancellation (either from the user, or from a timeout from
                # an outer layer)
                _, ret = yield from asyncio.gather(io, proc.wait())
            except asyncio.CancelledError:
                # Kill the process with prejudice for immediate return.
                self._send_signal(proc, 'SIGKILL')
                yield from proc.wait()
                raise

        if ret != 0:
            raise PackerError(ret, self.errors)

        return self.artifacts
