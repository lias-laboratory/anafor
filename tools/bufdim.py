import heapq
from itertools import groupby
from functools import lru_cache
from enum import IntEnum, unique
from sortedcontainers import SortedList
from util.helpers import MaxFinder
from tools.rbf import RBF, merge_C_streams, stream_tagger
from . import base


def LPT(stream):
    """Apply a longest processing time strategy on top of a stream of
    arrival times."""
    Cs = []
    t = 0.0
    for next_arrival, new_Cs in stream:
        if next_arrival <= t:
            for C in new_Cs:
                heapq.heappush(Cs, -C)
            continue
        while Cs:
            C = -heapq.heappop(Cs)
            t += C
            yield t, [C]
            if t >= next_arrival:
                break
        t = max(t, next_arrival)
        for C in new_Cs:
            heapq.heappush(Cs, -C)


def SPT(stream):
    """Apply a shortest processing time strategy on top of a stream of
    arrival times."""
    Cs = SortedList()
    t = 0.0
    idle = True
    for next_arrival, new_Cs in stream:
        if next_arrival <= t:
            Cs.update(new_Cs)
            continue
        if Cs and idle:
            yield t, [Cs.pop()]  # take last elem (largest)
        while Cs:
            C = Cs.pop(0)  # take first elem (smallest)
            t += C
            yield t, [C]
            if t >= next_arrival:
                break
        idle = next_arrival > t
        if idle:
            t = next_arrival
        Cs.update(new_Cs)


@unique
class Event(IntEnum):
    IN = 0
    OUT = 1


class Node(base.Node):
    "FA model of a node"
    suffix = ""

    def get_streams(self, c_node):
        "Get output and input stream for a node"
        CTJs = c_node._get_CTJs()
        out_stream = LPT(RBF(CTJs))
        in_stream = RBF(CTJs)
        return out_stream, in_stream

    @lru_cache(maxsize=None)
    def Bklg(self):
        c_node = self.tool.comp_node(self)
        out_stream, in_stream = self.get_streams(c_node)
        arrivals = groupby(
            heapq.merge(
                map(stream_tagger(Event.OUT), out_stream),
                map(stream_tagger(Event.IN), in_stream),
            ),
            key=lambda x: round(x[0], 5),
        )

        max_bklg = MaxFinder(f"Bklg for {self} frames opt")
        arrived, departed = 0, 0
        for t, events in arrivals:
            for _, tag, Cs in events:
                if tag == Event.IN:
                    arrived += len(Cs)
                    self.export("in", t, Cs, arrived)
                    # print(self, 'in', t, Cs, arrived)
                elif tag == Event.OUT:
                    departed += len(Cs)
                    self.export("out", t, Cs, departed)
                    # print(self, 'out', t, Cs, departed)
            backlog = arrived - departed
            max_bklg.check(backlog, t)
            if backlog == 0:
                break

        self.export(
            "res", (f"bklg_f_opt{self.suffix}", "times"), max_bklg.value, max_bklg.times
        )
        return max_bklg


class NodeSerial(Node):
    "FA model of a node with serialization"
    suffix = "_s"

    def get_streams(self, c_node):
        "Get output and input stream for a node with serialization"
        SPT_streams = []
        CTJs = c_node._get_CTJs()
        for source, flows in c_node.flows_by_src.items():
            CTJx = c_node._get_CTJs(tuple(flows))
            SPT_streams.append(RBF(CTJx) if source is c_node else SPT(RBF(CTJx)))
            # print(self, source, CTJx)
        in_stream = merge_C_streams(SPT_streams)
        out_stream = LPT(RBF(CTJs))
        return out_stream, in_stream

    def Bklg(self):
        return super().Bklg()


class BufDim(base.Tool):
    "Buffer dimensionning model of a network configuration made of flows and of nodes"
    objTypes = {
        # serialisation : (NodeType, FlowType)
        False: (Node, base.Flow),
        True: (NodeSerial, base.Flow),
    }

    def __init__(self, config, comp, serialization=True):
        "Create BufDim computation model from config"
        self.serialization = serialization
        self.comp = comp
        super().__init__(config, *BufDim.objTypes[serialization])

    def __repr__(self):
        return super().__repr__() + (
            " with serialisation" if self.serialization else ""
        )

    def comp_node(self, node):
        return self.comp.nodes[node._model]

    def compute_all(self):
        "Launch the computation for every node in each flow"
        for node in self.nodes.values():
            node.Bklg()
