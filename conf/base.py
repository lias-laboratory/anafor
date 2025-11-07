from collections import defaultdict
from functools import reduce


class Flow():
    def __init__(self, flow_id, period, s_max, s_min, prio):
        self.flow_id = flow_id
        self.T = float(period)
        self.s_min = float(s_min)
        self.s_max = float(s_max)
        self.paths = defaultdict(set)
        self.sources = dict()
        self.prio = int(prio)

    def __repr__(self):
        return f'{type(self).__name__}({self.flow_id})'

    def __iter__(self):
        return iter(self.sources)

    def C(self, node):
        return self.s_max / node.R

    def Crate(self, rate):
        return self.s_max / rate

    def add_path(self, source, dest):
        assert dest not in self.sources
        self.sources[dest] = source
        if source is not None:
            self.paths[source].add(dest)


class Node():
    def __init__(self, node_id, rate, idle_slopes, latency):
        self.node_id = node_id
        self.R = rate
        self.L = latency
        self.idle_slopes = idle_slopes
        self.flows_by_src = defaultdict(set)
        self.minC = float('inf')
        self.maxC = float('-inf')

    def add_flow(self, source, flow):
        self.flows_by_src[self if source is None else source].add(flow)
        self.minC = min(self.minC, flow.s_min / self.R)
        self.maxC = max(self.maxC, flow.s_max / self.R)

    def __iter__(self):
        return iter({flow for flows in self.flows_by_src.values()
                          for flow in flows})

    def __repr__(self):
        return f'{type(self).__name__}({self.node_id})'


class Configuration():
    def __init__(self, name=''):
        self.flows = {}
        self.nodes = {}
        self.exporters = []
        self.name = name

    def register(self, exporter, *args, **kwargs):
        self.exporters.append(exporter(self, *args, **kwargs))

    def render_all(self):
        for exporter in self.exporters:
            if exporter.renderable():
                exporter.render()
