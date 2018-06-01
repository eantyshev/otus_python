#!/usr/bin/env python
# -*- coding: utf-8 -*-

from functools import update_wrapper


def disable(f):
    '''
    Disable a decorator by re-assigning the decorator's name
    to this function. For example, to turn off memoization:

    >>> memo = disable

    '''
    return f


def decorator(f):
    '''
    Decorate a decorator so that it inherits the docstrings
    and stuff from the function it's decorating.
    '''
    wrapper = lambda *args, **kwargs: f(*args, **kwargs)
    update_wrapper(wrapper, f)
    return wrapper


def countcalls(f):
    '''Decorator that counts calls made to the function decorated.'''
    class CounterFunc(object):
        def __init__(self):
            self.calls = 0
        def __call__(self, *args, **kwargs):
            self.calls += 1
            return f(*args, **kwargs)
    wrapper = CounterFunc()
    update_wrapper(wrapper, f)
    return wrapper


def memo(func):
    '''
    Memoize a function so that it caches all return values for
    faster future lookups.
    '''
    _cache = {}
    def _pickle_args(args, kwargs):
        return (repr(args), repr(kwargs))
    def _memo_func(*args, **kwargs):
        key = _pickle_args(args, kwargs)
        if _cache.has_key(key):
            print "return from cache"
            return _cache[key]
        else:
            res = func(*args, **kwargs)
            _cache[key] = res
            return res
    update_wrapper(_memo_func, func)
    return _memo_func


def n_ary(f):
    '''
    Given binary function f(x, y), return an n_ary function such
    that f(x, y, z) = f(x, f(y,z)), etc. Also allow f(x) = x.
    '''
    def _wrapper(*args):
        res = f(args[-2], args[-1])
        for arg in args[-3::-1]:
            res = f(arg, res)
        return res
    update_wrapper(_wrapper, f)
    return _wrapper


class trace(object):
    '''Trace calls made to function decorated.

    @trace("____")
    def fib(n):
        ....

    >>> fib(3)
     --> fib(3)
    ____ --> fib(2)
    ________ --> fib(1)
    ________ <-- fib(1) == 1
    ________ --> fib(0)
    ________ <-- fib(0) == 1
    ____ <-- fib(2) == 2
    ____ --> fib(1)
    ____ <-- fib(1) == 1
     <-- fib(3) == 3

    '''
    def __init__(self, prefix):
        self._prefix_len = 0
        self._step = prefix
    def __call__(self, f):
        def _traced(*args):
            args_str = "(" + ",".join(map(str, args)) + ")"
            print self._step * self._prefix_len, "-->", f.func_name + args_str
            self._prefix_len += 1
            res = f(*args)
            self._prefix_len -= 1
            print self._step * self._prefix_len, "<--", f.func_name + args_str, "==", res
            return res

        update_wrapper(_traced, f)
        return _traced

# countcalls should be the outmost decorator,
# as others aren't aware of passing through its' ".calls" attribute
@countcalls
@memo
@n_ary
def foo(a, b):
    return a + b


@countcalls
@memo
@n_ary
def bar(a, b):
    return a * b


@countcalls
@trace("####")
@memo
def fib(n):
    """Some doc"""
    return 1 if n <= 1 else fib(n-1) + fib(n-2)


def main():
    print foo(4, 3)
    print foo(4, 3, 2)
    print foo(4, 3)
    print "foo was called", foo.calls, "times"

    print bar(4, 3)
    print bar(4, 3, 2)
    print bar(4, 3, 2, 1)
    print "bar was called", bar.calls, "times"

    print fib.__doc__
    fib(3)
    print fib.calls, 'calls made'


if __name__ == '__main__':
    main()
