from math import floor, ceil
from functools import lru_cache
from collections import defaultdict
from tools.rbf import RBF_times, RBF_val, StepList, merge_t_streams
from util.helpers import MaxFinder
from . import base


ERR = 1e-7


class Node(base.Node):
    """FA model of a node."""

    suffix = ''

    def __init__(self, tool, node):
        super().__init__(tool, node)

    @lru_cache(maxsize=None)
    def _get_CTJs(self, flows=None):
        """Get a collection of CTJ from a collection of flows."""
        CTJs = []
        for flow in self.flows if flows is None else flows:
            Smax, Smin = flow.Sextr(self)
            J = Smax - Smin
            CTJs.append((flow.C(self), flow.T, J))
        return tuple(CTJs)

    @lru_cache(maxsize=None)
    def Bklg(self):
        """Get the worst case backlog in a node."""
        CTJs = self._get_CTJs()
        bklg_max = MaxFinder(f'Bklg for {self}', 'µs')

        for t in RBF_times(CTJs):
            W = RBF_val(CTJs, t)
            bklg_max.check(W - t, t)
            if W < t:
                break

        self.export('res', ('bklg_b', 'times'), bklg_max.value, bklg_max.times)
        self.export('res', ('bklg_f', ), ceil(bklg_max.value / self.minC))
        return bklg_max


class NodeSerial(Node):
    """FA model of a node, specialized for flow serialization."""

    suffix = '_s'

    def _get_CTJs_by_src(self):
        """"Get for each source a collection of CTJs, with additionnal
        max_C and source link rate info.
        """
        for src, flows in self.flows_by_src.items():
            CTJs = self._get_CTJs(tuple(flows))
            max_C = float('inf') if src is self else max(C for C, _, _ in CTJs)
            rratio = self.R / src.R
            yield (src, (CTJs, max_C, rratio))

    @staticmethod
    @lru_cache(maxsize=None)
    def _get_stimes_by_src(rbfsx, CTJx, max_C, rratio):
        """Find intersections times between rbfx and LinkRate curves."""
        bklg = RBF_val(CTJx, 0) - max_C
        rate = rratio - sum(C / T for C, T, _ in CTJx)
        tmax = bklg / rate

        for W, t0, t1 in rbfsx:
            if t0 > tmax:
                break
            t = (W - max_C) / rratio
            if t0 <= t <= t1:
                yield t

    def _get_stimes(self, rbfs, IP):
        """Find intersections times for each input link."""
        streams = [self._get_stimes_by_src(tuple(rbfsx), *IP[src])
                   for src, rbfsx in rbfs.items()]
        yield from merge_t_streams(streams)

    def _bklg(self, IP, times, bklg_max):
        rbfs = defaultdict(StepList)
        for t in times:
            W = 0.0
            for src, (CTJx, max_C, rratio) in IP.items():
                rbfx = RBF_val(CTJx, t)
                if src is self:  # No serialization
                    W += rbfx
                else:
                    linkrate = rratio * t + max_C
                    W += min(rbfx, linkrate)
                    rbfs[src].append(t, rbfx)
            bklg_max.check(W - t, t)
            if W - t < ERR:
                break
        return rbfs

    @lru_cache(maxsize=None)
    def Bklg(self):
        """Get the worst case backlog in a node with serialization."""
        CTJs = self._get_CTJs()
        IP = dict(self._get_CTJs_by_src())
        bklg_max = MaxFinder(f'Bklg for {self}', 'µs')

        times = RBF_times(CTJs)
        rbfs = self._bklg(IP, times, bklg_max)
        serial_times = self._get_stimes(rbfs, IP)
        self._bklg(IP, serial_times, bklg_max)

        self.export('res', ('bklg_b_s', 'times'), bklg_max.value, bklg_max.times)
        self.export('res', ('bklg_f_s', ), ceil(bklg_max.value / self.minC))

        return bklg_max


class NodePrio(Node):
    """FA model of a node, with static priorities."""
    suffix = '_p'

    def _get_CTJs_by_prio(self, prio):
        """Get a collection of CTJs for self and higher priority flows."""
        WLP, CTJsp, CTJhp = 0.0, [], []
        for flow in self.flows:
            Smax, Smin = flow.Sextr(self)
            C = flow.C(self)
            CTJ = C, flow.T, Smax - Smin
            if flow.prio > prio:
                WLP = max(WLP, C)
            elif flow.prio == prio:
                CTJsp.append(CTJ)
            else:
                CTJhp.append(CTJ)
        return WLP, tuple(CTJsp), tuple(CTJhp)

    @lru_cache(maxsize=None)
    def Bklg(self, Ci, prio):
        """Get the worst case backlog in a node."""
        WLP, CTJsp, CTJhp = self._get_CTJs_by_prio(prio)
        bklg_max = MaxFinder(f'Bklg for {self} (P={prio})', 'µs')

        for t in RBF_times(CTJsp):
            W_old, W = 0.0, Ci
            WLSP = WLP + RBF_val(CTJsp, t)
            while abs(W - W_old) > ERR:
                W_old, W = W, WLSP + RBF_val(CTJhp, W - Ci)
            bklg_max.check(W - t, t)
            if W - t < ERR:
                break

        self.export('res', ('bklg_b_p', 'times'), bklg_max.value, bklg_max.times)
        self.export('res', ('bklg_f_p', ), ceil(bklg_max.value / self.minC))
        return bklg_max


class NodePrioSerial(NodeSerial):
    """FA model of a node, with static priorities, specialized for flow serialization"""
    suffix = '_sp'

    @staticmethod
    @lru_cache(maxsize=None)
    def _BHP(CTJ, t):
        return sum((floor((t+J)/T) - floor(J/T)) * C for C, T, J in CTJ)

    def _get_CTJs_by_src_and_prio(self, prio):
        WLP, CTJhp, CTJsp, IP = 0.0, [], [], {}

        for src, flows in self.flows_by_src.items():
            CTJspx, CTJhpx = [], []
            max_C = float('inf') if src is self else 0.0
            rratio = self.R / src.R
            for flow in flows:
                Smax, Smin = flow.Sextr(self)
                C = flow.C(self)
                CTJ = C, flow.T, Smax - Smin
                if flow.prio > prio:  # lp
                    WLP = max(WLP, C)
                else:  # shp
                    max_C = max(C, max_C)
                    if flow.prio == prio:  # sp
                        CTJspx.append(CTJ)
                    else:  # hp
                        CTJhpx.append(CTJ)
            CTJhp += CTJhpx
            CTJsp += CTJspx
            if CTJspx or CTJhpx:
                IP[src] = tuple(CTJspx), tuple(CTJhpx), max_C, rratio

        CTJhp = tuple(CTJhp)
        CTJsp = tuple(CTJsp)

        return WLP, CTJhp, CTJsp, IP

    def _bklg(self, Ci, WLP, CTJhp, CTJsp, IP, times, bklg_max):
        rbfs = defaultdict(StepList)
        for t in times:
            WSP = 0.0
            for src, (CTJspx, CTJhpx, max_C, rratio) in IP.items():
                rbfx = RBF_val(CTJspx, t)
                if src is self:  # No serialization
                    WSP += rbfx
                else:
                    linkrate = rratio * t + max_C - self._BHP(CTJhpx, t)
                    WSP += min(rbfx, linkrate)
                    rbfs[src].append(t, rbfx)
            WLSP = WLP + WSP
            W_old, W = 0.0, Ci
            while abs(W - W_old) > ERR:
                W_old, W = W, WLSP + RBF_val(CTJhp, W - Ci)
            bklg_max.check(W - t, t)
            if W - t < ERR:
                break
        return rbfs

    @staticmethod
    @lru_cache(maxsize=None)
    def _get_stimes_by_src(rbfsx, CTJspx, CTJhpx, max_C, rratio):
        """Find intersections times between rbfx and LinkRate curves."""
        bklg = (RBF_val(CTJspx, 0)
                + sum(C for C, _, _ in CTJhpx)
                - max_C)
        rate = (rratio
                - sum(C / T for C, T, _ in CTJspx)
                - sum(C / T for C, T, _ in CTJhpx))
        tmax = bklg / rate

        rbfhp0 = RBF_val(CTJhpx, 0.0)

        for W, t0, t1 in rbfsx:
            if t0 > tmax:
                break
            tau0 = 0.0
            hp_times = RBF_times(CTJhpx) if CTJhpx else [float('+inf')]
            for tau1 in hp_times:
                if tau1 <= t0:
                    continue
                if tau0 >= min(tmax, t1):  # take tmax if t1 = +inf
                    break
                a, b = max(t0, tau0), min(t1, tau1)
                rbfhp = RBF_val(CTJhpx, a)
                t = (W - max_C + rbfhp - rbfhp0) / rratio
                if a <= t <= b:
                    yield t
                tau0 = tau1

    @lru_cache(maxsize=None)
    def Bklg(self, Ci, prio):
        """Get the worst case backlog in a node with serialization."""
        WLP, CTJhp, CTJsp, IP = self._get_CTJs_by_src_and_prio(prio)
        bklg_max = MaxFinder(f'Bklg for {self} (P={prio})', 'µs')

        times = RBF_times(CTJsp)
        rbfs = self._bklg(Ci, WLP, CTJhp, CTJsp, IP, times, bklg_max)
        serial_times = self._get_stimes(rbfs, IP)
        self._bklg(Ci, WLP, CTJhp, CTJsp, IP, serial_times, bklg_max)

        self.export('res', ('bklg_b_sp', 'times'), bklg_max.value, bklg_max.times)
        self.export('res', ('bklg_f_sp', ), ceil(bklg_max.value / self.minC))
        
        print(self, bklg_max)

        return bklg_max


class Flow(base.Flow):
    """FA model of a flow."""

    def _get_node_Bklg(self, node):
        """Get maximum Bklg for current flow in a given node."""
        return node.Bklg()

    @lru_cache(maxsize=None)
    def Sextr(self, node):
        """Get Smin and Smax in a node."""
        prev_node = self.prev(node)
        if prev_node is node:
            return 0.0, 0.0
        C = self.C(prev_node)
        L = node.L
        Bklg = self._get_node_Bklg(prev_node)
        Smax_prev, Smin_prev = self.Sextr(prev_node)
        Smin = Smin_prev + C + L
        Smax = Smax_prev + Bklg.value + L

        return Smax, Smin

    def R(self, node):
        """Compute the worst-case e2e delay R in a node."""
        Smax, Smin = self.Sextr(node)
        Bklg = self._get_node_Bklg(node)
        R, times = Smax + Bklg.value, Bklg.times

        self.export('res_Sextr', node._model, ('Smin', 'Smax'), Smin, Smax)
        self.export('res_R', node._model, (f'R{node.suffix}', 'times'), R, times)
        return R, times


class FlowPrio(Flow):
    """FA model of a flow."""

    def _get_node_Bklg(self, node):
        """Get maximum Bklg for current flow in a given node."""
        return node.Bklg(self.C(node), self.prio)


class FA(base.Tool):
    """FA model of a network configuration made of flows and of nodes."""

    objTypes = {
        # (serialisation, prio): (NodeType, FlowType)
        (False, False): (Node,           Flow),
        (True,  False): (NodeSerial,     Flow),
        (False, True):  (NodePrio,       FlowPrio),
        (True,  True):  (NodePrioSerial, FlowPrio),
    }

    def __init__(self, config, serialization=True, prio=True):
        """Create FA computation model from config."""
        self.serialization = serialization
        self.prio = prio
        super().__init__(config, *FA.objTypes[(serialization, prio)])

    def __repr__(self):
        return (super().__repr__()
                + (' with serialisation' if self.serialization else '')
                + (' with static priorities' if self.prio else ''))

    def compute_all(self):
        """Launch the computation for every node in each flow."""
        for flow in self.flows.values():
            for node in flow:
                flow.R(node)
