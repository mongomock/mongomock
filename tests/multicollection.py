from .diff import diff
import copy
import functools
import re

class MultiCollection(object):
    def __init__(self, conns):
        super(MultiCollection, self).__init__()
        self.conns = conns.copy()
        self.do = Foreach(self.conns, compare=False)
        self.compare = Foreach(self.conns, compare=True)
        self.compare_ignore_order = Foreach(self.conns, compare=True, ignore_order=True)
class Foreach(object):
    def __init__(self, objs, compare, ignore_order=False, method_result_decorators=()):
        self.___objs = objs
        self.___compare = compare
        self.___ignore_order = ignore_order
        self.___decorators = list(method_result_decorators)
    def __getattr__(self, method_name):
        return ForeachMethod(self.___objs, self.___compare, self.___ignore_order, method_name, self.___decorators)
    def __call__(self, *decorators):
        return Foreach(self.___objs, self.___compare, self.___ignore_order, self.___decorators + list(decorators))

class ForeachMethod(object):
    def __init__(self, objs, compare, ignore_order, method_name, decorators):
        super(ForeachMethod, self).__init__()
        self.___objs = objs
        self.___compare = compare
        self.___ignore_order = ignore_order
        self.___method_name = method_name
        self.___decorators = decorators
    def __call__(self, *args, **kwargs):
        results = dict(
            # copying the args and kwargs is important, because pymongo changes the dicts (fits them with the _id)
            (name, self.___apply_decorators(getattr(obj, self.___method_name)(*_deepcopy(args), **_deepcopy(kwargs))))
            for name, obj in self.___objs.items()
        )
        if self.___compare:
            _assert_no_diff(results, ignore_order=self.___ignore_order)
        return results
    def ___apply_decorators(self, obj):
        for d in self.___decorators:
            obj = d(obj)
        return obj

def _assert_no_diff(results, ignore_order):
    if _result_is_cursor(results):
        value_processor = functools.partial(_expand_cursor, sort=ignore_order)
    else:
        assert not ignore_order
        value_processor = None
    prev_name = prev_value = None
    for index, (name, value) in enumerate(results.items()):
        if value_processor is not None:
            value = value_processor(value)
        if index > 0:
            d = diff(prev_value, value)
            assert not d, _format_diff_message(prev_name, name, d)
        prev_name = name
        prev_value = value

def _result_is_cursor(results):
    return any(type(result).__name__ == "Cursor" for result in results.values())

def _expand_cursor(cursor, sort):
    returned = [result.copy() for result in cursor]
    if sort:
        returned.sort(key=lambda document: str(document.get('_id', str(document))))
    for result in returned:
        result.pop("_id", None)
    return returned

def _format_diff_message(a_name, b_name, diff):
    msg = "Unexpected Diff:"
    for (path, a_value, b_value) in diff:
        a_path = [a_name] + path
        b_path = [b_name] + path
        msg += "\n\t{} != {} ({} != {})".format(
            ".".join(map(str, a_path)), ".".join(map(str, b_path)), a_value, b_value
        )
    return msg

def _deepcopy(x):
    """
    Deepcopy, but ignore regex objects...
    """
    if isinstance(x, re._pattern_type):
        return x
    if isinstance(x, list) or isinstance(x, tuple):
        return type(x)(_deepcopy(y) for y in x)
    if isinstance(x, dict):
        return dict((_deepcopy(k), _deepcopy(v)) for k, v in x.items())
    return copy.deepcopy(x)
