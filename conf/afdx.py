from collections import deque
from . import base


class VL(base.Flow):
    def __init__(self, num, bag, s_max, s_min, prio):
        super().__init__(num, bag, s_max, s_min, prio)

    @property
    def num(self):
        return int(self.flow_id)

    @property
    def bag(self):
        return self.T


class Port(base.Node):
    def __init__(self, comp, port_num, rate, idle_slopes, latency=16):
        self.port_id = f'{comp.comp_id} {port_num}'
        super().__init__(self.port_id, rate, idle_slopes, latency)
        self.component = comp
        self.num = port_num


class Component(object):
    def __init__(self, name):
        self.comp_id = name
        self.ports = {}

    def add_port(self, port_num : int, rate : float, idle_slopes, latency=16):
        port = Port(self, port_num, rate, idle_slopes, latency)
        assert port_num not in self.ports
        self.ports[port_num] = port

    def __getitem__(self, port_num):
        return self.ports[int(port_num)]

    def __iter__(self):
        return iter(self.ports.values())

    def __repr__(self):
        return f'{type(self).__name__}({self.comp_id})'


class Es(Component):
    pass


class Switch(Component):
    pass


class Configuration(base.Configuration):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.vls = self.flows
        self.ports = self.nodes
        self.components = {}

    def get_port(self, comp_name, port_num):
        return self.components[comp_name][port_num]

    @staticmethod
    def from_mod_file(confname, latency=16):

        def read_comp(file, CompType):
            name, port_count = next(file).split()
            assert name not in conf.components
            component = CompType(name)
            conf.components[name] = component
            for _ in range(int(port_count)):
                num, rate, *cbs_fields = next(file).split()
                idle_slopes = list(map(float, cbs_fields))
                component.add_port(int(num), float(rate), idle_slopes, latency)
            conf.ports.update({p.port_id: p for p in component})

        def read_vl(file):

            def build(arcs, source=None):
                if not arcs:
                    arcs = deque(next(file).split())
                name, num = arcs.popleft(), int(arcs.popleft())
                dest = conf.get_port(name, num)
                vl.add_path(source, dest)
                dest.add_flow(source, vl)
                forks = int(arcs.popleft())
                for _ in range(forks):
                    build(arcs, dest)

            num, bag, s_min, s_max, prio = next(file).split()
            num = int(num)
            assert num not in conf.vls
            vl = VL(num=num,
                    bag=float(bag) / 12.5,     # Bytes to usec
                    s_max=float(s_max) * 8.0,  # Bytes to bits
                    s_min=float(s_min) * 8.0,  # Bytes to bits
                    prio=int(prio))
            conf.vls[num] = vl
            arcs = deque(next(file).split())
            net_count = int(arcs.popleft())
            for _ in range(net_count):
                build(arcs)

        conf = Configuration(name=confname)

        with open(f'./assets/{confname}.mod', 'r') as mod_file:
            es_count = int(next(mod_file))
            for _ in range(es_count):
                read_comp(mod_file, Es)
            es_count = int(next(mod_file))
            for _ in range(es_count):
                read_comp(mod_file, Switch)
            vl_count = int(next(mod_file))
            for _ in range(vl_count):
                read_vl(mod_file)

        return conf
