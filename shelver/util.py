from __future__ import absolute_import, unicode_literals
from builtins import filter
from past.builtins import basestring
from future.utils import iteritems

from itertools import chain, count
from collections import Hashable, Iterable, Mapping, MutableMapping, Set

from icicle import FrozenDict


def is_collection(v):
    return isinstance(v, Iterable) and not isinstance(v, basestring)


def wrap_as_coll(v, coll=list):
    if is_collection(v):
        return coll(v)
    else:
        return coll(iter(lambda: v, v))


def _merge_mutable_mapping(left, right):
    res = left.copy()
    for k, right_v in iteritems(right):
        if k in res:
            res[k] = deep_merge(res[k], right_v)
        else:
            res[k] = right_v

    return res


def _merge_mapping(left, right):
    memo = set()
    for k, left_v in iteritems(left):
        if k in right:
            v = deep_merge(left_v, right[k])
        else:
            v = left_v

        yield k, v
        memo.add(k)

    for k, right_v in iteritems(right):
        if k not in memo:
            yield k, right_v


def deep_merge(left, right):
    if isinstance(left, Mapping):
        if not isinstance(right, Mapping):
            raise ValueError('Cannot merge Mapping and non-Mapping')

        if isinstance(left, MutableMapping):
            return _merge_mutable_mapping(left, right)
        else:
            tpe = type(left)
            return tpe(_merge_mapping(left, right))
    elif isinstance(left, Set):
        if not is_collection(right):
            raise ValueError('Cannot merge Set and non-Collection')

        return left.union(right)
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
    elif isinstance(obj, Mapping):
        return FrozenDict((k, freeze(v)) for (k, v) in iteritems(obj))
    elif isinstance(obj, Set):
        return frozenset(freeze(v) for v in obj)
    elif is_collection(obj):
        return tuple(freeze(v) for v in obj)
    else:
        raise ValueError("Cannot freeze object of type {}".format(type(obj)))


def topological_sort(nodes, edges):
    nodes = list(nodes)
    result = []
    edges = dict((dest, list(sources)) for (dest, sources) in iteritems(edges))
    leaves = list(filter(lambda n: n not in edges, nodes))

    for level in count():
        while leaves:
            leaf = leaves.pop()
            result.append((level, leaf))

            for dest, sources in iteritems(edges):
                sources.remove(leaf)

        for dest in list(edges):
            if not edges[dest]:
                del edges[dest]
                leaves.append(dest)

        if not leaves:
            break

    if edges:
        cycles = ', '.join(
            '{} <- {}'.format(dest, tuple(sources))
            for dest, sources in iteritems(edges))

        raise ValueError('Dependency cycles found: ' + cycles)

    return result
