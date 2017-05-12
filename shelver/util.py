import subprocess
import asyncio
from itertools import chain
from collections import Hashable, Iterable, Mapping, MutableMapping, Set, deque
from types import MappingProxyType


class FrozenDict(Mapping):  # pragma: nocover
    def __init__(self, *args, **kwargs):
        self._data = dict(*args, **kwargs)
        self._hash = None

    def __getitem__(self, key):
        return self._data[key]

    def __len__(self):
        return len(self._data)

    def __iter__(self):
        return iter(self._data)

    def __hash__(self):
        if not self._hash:
            self._hash = hash(frozenset(self._data.items()))

        return self._hash

    def __repr__(self):
        return '{}({})'.format(type(self), repr(self._data))

    def __eq__(self, other):
        if isinstance(other, type(self)):
            return self._data == other._data
        elif isinstance(other, Mapping):
            return self._data == other


class AsyncBase():
    def __init__(self, **kwargs):
        self._loop = kwargs.pop('loop', asyncio.get_event_loop())
        self._executor = kwargs.pop('executor', None)

    def delay(self, fn, *args):
        return self._loop.run_in_executor(self._executor, fn, *args)


def is_collection(v):
    return isinstance(v, Iterable) and not isinstance(v, (bytes, str))


def wrap_as_coll(v, coll=list):
    if is_collection(v):
        return coll(v)
    else:
        return coll([v])


def _merge_mutable_mapping(left, right):
    res = left.copy()
    for k, right_v in right.items():
        if k in res:
            res[k] = deep_merge(res[k], right_v)
        else:
            res[k] = right_v

    return res


def _merge_mapping(left, right):
    memo = set()
    for k, left_v in left.items():
        if k in right:
            v = deep_merge(left_v, right[k])
        else:
            v = left_v

        yield k, v
        memo.add(k)

    for k, right_v in right.items():
        if k not in memo:
            yield k, right_v


def deep_merge(left, right):
    if isinstance(left, Mapping):
        if not isinstance(right, Mapping):
            raise ValueError('Cannot merge Mapping and non-Mapping')

        if isinstance(left, MutableMapping) \
           and callable(getattr(left, 'copy', None)):
            return _merge_mutable_mapping(left, right)
        else:
            tpe = type(left)
            return tpe(_merge_mapping(left, right))
    elif isinstance(left, Set):
        if not is_collection(right):
            raise ValueError('Cannot merge Set and non-Collection')

        return left | set(right)
    elif is_collection(left):
        if not is_collection(right):
            raise ValueError('Cannot merge Collection and non-Collection')

        tpe = type(left)
        return tpe(chain(left, right))
    else:
        return right


def freeze(obj):
    if isinstance(obj, Hashable):
        return obj
    elif isinstance(obj, bytearray):
        return bytes(obj)
    elif isinstance(obj, Mapping):
        return FrozenDict((k, freeze(v)) for (k, v) in obj.items())
    elif isinstance(obj, Set):
        return frozenset(freeze(v) for v in obj)
    elif is_collection(obj):
        return tuple(freeze(v) for v in obj)
    else:  # pragma: nocover
        raise ValueError('Cannot freeze object of type {}'.format(type(obj)))


def topological_sort(nodes, edges):
    result = []
    edges = {dest: set(sources) for (dest, sources) in edges.items()}
    leaves = deque(filter(lambda n: n not in edges, nodes))

    while leaves:
        result.append(set())
        while leaves:
            leaf = leaves.popleft()
            result[-1].add(leaf)

            for dest, sources in edges.items():
                try:
                    sources.remove(leaf)
                except ValueError:
                    pass

        for dest in list(edges):
            if not edges[dest]:
                del edges[dest]
                leaves.append(dest)

    if edges:
        return None, edges

    return result, None


@asyncio.coroutine
def async_subprocess_run(program, *args, input=None, stdout=subprocess.PIPE,
                         stderr=None, loop=None, **kwargs):
    loop = loop or asyncio.get_event_loop()
    cmd = [program, *args]
    proc = yield from asyncio.create_subprocess_exec(
        *cmd, stdout=stdout, stderr=stderr, loop=loop, **kwargs)
    out, err = yield from proc.communicate(input)
    ret = yield from proc.wait()

    if ret != 0:
        exc = subprocess.CalledProcessError(ret, cmd, output=out)
        # These attributes are not added automatically pre-Py3.5
        if not hasattr(exc, 'stdout'):
            exc.stdout = stdout and out
        if not hasattr(exc, 'stderr'):
            exc.stderr = stderr and err

        raise exc

    return out, err
