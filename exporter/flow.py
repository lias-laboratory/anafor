from os import makedirs
from collections import defaultdict
from exporter.base import DispatchExporter


class FlowCSV(DispatchExporter):
    flow_cols = [
        'flow_id',
        'T',
        'prio',
        's_min',
        's_max',
    ]

    node_cols = [
        'node_id',
        'R',
        'L',
    ]

    def __init__(self, *args, sep=';', **kwargs):
        super().__init__(*args, **kwargs)
        self.sep = sep
        self.res = defaultdict(dict)
        self.data_cols = set()
    
    def dispatch(self, tool, cls, fn, hook, obj, *args):
        if cls.startswith('Flow') and fn == 'R' and hook.startswith('res_'):
            node, col, *max_r = args
            self.data_cols.add(col)
            self.res[(obj, node)][col] = max_r

    def title_line(self):
        for col in self.flow_cols:
            yield col
        for col in self.node_cols:
            yield col
        for col in sorted(self.data_cols):
            for item in col:
                yield item

    def data_line(self, flow, node, res):
        for attr in self.flow_cols:
            yield getattr(flow, attr)
        for attr in self.node_cols:
            yield getattr(node, attr)
        for col in sorted(self.data_cols):
            for item in res.get(col, [''] * len(res)):
                yield item

    def renderable(self):
        return bool(self.res)

    def render(self):
        makedirs(self.folder, exist_ok=True)
        timestamp = f'-{self.timestamp}' if self.timestamp else ''
        f_name = f'{self.folder}/flow{timestamp}.csv'
        fl_nd_sort = lambda item: (item[0][0].flow_id, item[0][1].node_id)
        with open(f_name, 'w') as f:
            print(*self.title_line(), sep=self.sep, file=f)
            for (flow, node), res in sorted(self.res.items(), key=fl_nd_sort):
                print(*self.data_line(flow, node, res), sep=self.sep, file=f)
