import inspect
from functools import cached_property


class Component():
    """Generic computation component"""

    def __init__(self, tool):
        self.tool = tool
        self.exporters = tool.exporters

    def export(self, *args, **kwargs):
        outerframe = inspect.currentframe().f_back
        calling_function = outerframe.f_code.co_name
        for exporter in self.exporters:
            exporter.export(self.tool, self._model, calling_function, *args, **kwargs)


class Node(Component):
    """Generic model of a node"""

    def __init__(self, tool, node):
        super().__init__(tool)
        self._model = node
        self.R = node.R
        self.L = node.L
        self.minC = node.minC
        self.maxC = node.maxC

    @cached_property
    def flows_by_src(self):
        return {
            self.tool.nodes[node]: set(map(self.tool.flows.get, flows))
            for node, flows in self._model.flows_by_src.items()
        }

    @cached_property
    def flows(self):
        return {flow for flows in self.flows_by_src.values()
                for flow in flows}

    def __repr__(self):
        return repr(self._model)


class Flow(Component):
    """Generic model of a flow."""

    def __init__(self, tool, flow):
        super().__init__(tool)
        self._model = flow
        self.prio = flow.prio
        self.T = flow.T

    def __repr__(self):
        return repr(self._model)

    def prev(self, node):
        """Get previous node in a path or current if no previous."""
        return self.tool.nodes.get(self._model.sources[node._model], node)

    def C(self, node):
        """Worst case transmission in a node."""
        return self._model.C(node._model)

    def __iter__(self):
        """All the nodes from the path."""
        return map(self.tool.nodes.get, iter(self._model))


class Tool():
    """Generic model of a network configuration made of flows and of nodes"""

    def __init__(self, config, NodeType, FlowType):
        """Monkey-patch conf, flows and nodes with attributes for a specific tool."""
        self.config = config
        self.exporters = config.exporters
        self.nodes = {node: NodeType(self, node)
                      for node in config.nodes.values()}
        self.flows = {flow: FlowType(self, flow)
                      for flow in config.flows.values()}

    def __repr__(self):
        return f'{type(self).__name__}'
