from math import floor
from itertools import groupby, count
from heapq import merge
from operator import itemgetter


def RBFi(C, T, J):
    """Infinite stream of arrivial times, conforming to the request
    bound function (rbf) of a flow v_i

    >>> stream = RBFi(C=15.0, T=60.0, J=80.0)
    >>> next(stream)
    (0.0, [15.0, 15.0])
    >>> next(stream)
    (40.0, [15.0])
    >>> next(stream)
    (100.0, [15.0])
    """
    k = 1 + floor(J / T)
    yield 0.0, [C] * k
    for i in count(k):
        yield i * T - J, [C]


def RBF(CTJs):
    """Infinite stream of arrivial times, conforming to the request
    bound function (rbf) of a set of flows

    >>> flows = ((15.0, 60.0, 80.0), (10.0, 50.0, 0.0))
    >>> streams = RBF(flows)
    >>> next(streams)
    (0.0, [10.0, 15.0, 15.0])
    >>> next(streams)
    (40.0, [15.0])
    >>> next(streams)
    (50.0, [10.0])
    >>> next(streams)
    (100.0, [10.0, 15.0])
    """
    RBFs = (RBFi(C, T, J) for C, T, J in CTJs)
    yield from merge_C_streams(RBFs)


def RBFi_times(C, T, J):
    """Infinite stream of arrivial times, conforming to the request
    bound function (rbf) of a flow v_i

    >>> stream = RBFi_times(C=15.0, T=60.0, J=80.0)
    >>> next(stream)
    0.0
    >>> next(stream)
    40.0
    >>> next(stream)
    100.0
    """
    k = 1 + floor(J / T)
    yield 0.0
    for i in count(k):
        yield i * T - J


def RBF_times(CTJs):
    """Infinite stream of arrivial times, conforming to the request
    bound function (rbf) of a set of flows

    >>> flows = ((15.0, 60.0, 80.0), (10.0, 50.0, 0.0))
    >>> streams = RBF_times(flows)
    >>> next(streams)
    0.0
    >>> next(streams)
    40.0
    >>> next(streams)
    50.0
    >>> next(streams)
    100.0
    """
    RBFs = (RBFi_times(*CTJ) for CTJ in CTJs)
    yield from merge_t_streams(RBFs)


def RBF_val(CTJs, t):
    """Compute a sum of rbf functions at time t.

    >>> flows = ((15.0, 60.0, 80.0), (10.0, 50.0, 0.0))
    >>> RBF_val(flows, 0)
    40.0
    >>> RBF_val(flows, 40)
    55.0
    >>> RBF_val(flows, 50)
    65.0
    """
    return sum((1 + floor((t + J) / T)) * C for C, T, J in CTJs)


def merge_t_streams(streams):
    for t, _ in groupby(merge(*streams)):
        yield t


def merge_C_streams(streams):
    for t, flows in groupby(merge(*streams), key=itemgetter(0)):
        yield t, [C for _, Cs in flows for C in Cs]


def stream_tagger(tag):
    def _tagger(tCs, tag=tag):
        t, Cs = tCs
        return t, tag, Cs
    return _tagger


class StepList(list):
    """List of steps as tuples (value, start_time, end_time)."""

    def append(self, t, val):
        """Append new step at time t if value has changed.

        >>> sl = StepList()
        >>> sl.append(0.0, 5.0)
        >>> sl.append(3.0, 5.0)  # Ignored, as value did not change
        >>> sl.append(6.0, 3.0)
        >>> sl
        [(5.0, 0.0, 6.0), (3.0, 6.0, inf)]
        """
        if self:
            last_val, last_t, _ = self[-1]
            if val == last_val:
                return
            self[-1] = last_val, last_t, t
        super().append((val, t, float('+inf')))
