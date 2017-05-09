import pytest
from collections import OrderedDict, Mapping

from icicle import FrozenDict
from shelver.util import (is_collection, wrap_as_coll, deep_merge, freeze,
                          topological_sort)


class ListMapping(Mapping):
    def __init__(self, items):
        self._items = list(items)

    def __getitem__(self, item):
        for k, v in self._items:
            if k == item:
                return v

        raise KeyError(item)

    def __len__(self):
        return len(self._items)

    def __iter__(self):
        return iter(k for (k, v) in self._items)

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return 'ListMapping({!r})'.format(self._items)


@pytest.mark.parametrize('obj,is_coll', [
    ([], True),
    (set(), True),
    (dict(), True),
    ('str', False),
    (range(5), True)
])
def test_is_collection(obj, is_coll):
    assert is_collection(obj) == is_coll


@pytest.mark.parametrize('obj,coll,result', [
    ([], list, []),
    (1, list, [1]),
    ([], set, set()),
    ('str', list, ['str']),
    ('str', set, {'str'})
])
def test_wrap_as_coll(obj, coll, result):
    assert wrap_as_coll(obj, coll=coll) == result


@pytest.mark.parametrize('left,right,merged', [
    # two scalars = right scalar
    (1, 2, 2),
    # two lists = concat
    ([1], [1], [1, 1]),
    # two sets = merge
    ({1}, {2}, {1, 2}),
    # set and list = merge
    ({1}, [1], {1}),
    # two tuples = concat as tuple (preserve type)
    ((1, 2), (3, 4), (1, 2, 3, 4)),
    # two dicts = merge
    ({'a': 1}, {'b': 2}, {'a': 1, 'b': 2}),
    # two dics with same key = merge key
    ({'a': 1}, {'a': 2}, {'a': 2}),
    # nested dicts = merge nested
    ({'a': {'1': 1}},
     {'a': {'2': 2}},
     {'a': {'1': 1, '2': 2}}),
    # two ordered dicts, preserve type and order
    (OrderedDict([('a', 1), ('b', 2)]),
     OrderedDict([('a', 2), ('c', 3), ('d', 4)]),
     OrderedDict([('a', 2), ('b', 2), ('c', 3), ('d', 4)])),
    # immutable mappings
    (ListMapping([('a', 1), ('b', 2)]),
     ListMapping([('a', 2), ('c', 3), ('d', 4)]),
     ListMapping([('a', 2), ('b', 2), ('c', 3), ('d', 4)])),
    # collection and non-collection
    ([], 1, None),
    # set and non-collection
    (set(), 1, None),
    # mapping and non-mapping
    ({}, [], None)
])
def test_deep_merge(left, right, merged):
    if merged is not None:
        assert deep_merge(left, right) == merged
    else:
        with pytest.raises(ValueError):
            deep_merge(left, right)


@pytest.mark.parametrize('obj,frozen', [
    ('str', 'str'),
    ((), ()),
    ([], ()),
    (set(), frozenset()),
    (bytearray(), bytes()),
    ({'a': 1}, FrozenDict(a=1)),
    ({'a': []}, FrozenDict(a=())),

])
def test_freeze(obj, frozen):
    res = freeze(obj)
