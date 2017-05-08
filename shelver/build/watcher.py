import asyncio
from signal import SIGINT, SIGKILL

from shelver.errors import PackerError


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

    def __init__(self, prefix, msg_stream, log_stream, *,
                 kill_timeout=60, loop=None):
        if not isinstance(prefix, bytes):
            prefix = prefix.encode('utf-8')

        self.prefix = prefix
        self.kill_timeout = kill_timeout
        self.errors = []
        self.artifacts = []
        self._msg_stream = msg_stream
        self._log_stream = log_stream
        self._loop = loop or asyncio.get_event_loop()

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

    @asyncio.coroutine
    def run(self, proc, canceled=False):
        try:
            yield from asyncio.gather(self.handle_stdout(proc.stdout),
                                      self.handle_stderr(proc.stderr),
                                      loop=self._loop)

            ret = yield from proc.wait()
            if ret != 0:
                self.errors.append('Command failed')
                raise PackerError(ret, self.errors)

            return self.artifacts
        except asyncio.CancelledError:
            if canceled:
                proc.send_signal(SIGKILL)
                raise

            proc.send_signal(SIGINT)
            yield from self.run(proc, canceled=True)


