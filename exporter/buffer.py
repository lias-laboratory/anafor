from os import makedirs
from collections import defaultdict
from exporter.base import FunExporter, DispatchExporter
from util.helpers import list_str


class BufferCSV(DispatchExporter):
    node_cols = [
        'node_id',
        'R',
        'L',
        'minC',
        'maxC',
    ]

    def __init__(self, *args, sep=';', **kwargs):
        super().__init__(*args, **kwargs)
        self.sep = sep
        self.res = defaultdict(dict)
        self.data_cols = set()

    def dispatch(self, tool, cls, fn, hook, obj, *args):
        if cls.startswith('Node') and fn == 'Bklg' and hook == 'res':
            col, *max_bklg = args
            self.data_cols.add(col)
            self.res[obj][col] = max_bklg

    def title_line(self):
        for col in self.node_cols:
            yield col
        for col in sorted(self.data_cols):
            for item in col:
                yield item

    def data_line(self, node, res):
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
        f_name = f'{self.folder}/bklg{timestamp}.csv'

        def node_sort(item):
            return item[0].node_id

        with open(f_name, 'w') as f:
            print(*self.title_line(), sep=self.sep, file=f)
            for node, res in sorted(self.res.items(), key=node_sort):
                print(*self.data_line(node, res), sep=self.sep, file=f)


class BufferGraph(FunExporter):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.out_curve = defaultdict(list)
        self.in_curve = defaultdict(list)
        self.res = {}

    def BufDim_Node_Bklg_out(self, obj, t, Cs, departed):
        self.out_curve[obj].append((t, Cs, departed))

    def BufDim_Node_Bklg_in(self, obj, t, Cs, arrived):
        self.in_curve[obj].append((t, Cs, arrived))

    def BufDim_Node_Bklg_res(self, obj, *args):
        _, max_value, max_times = args
        self.res[obj] = (max_value, max_times)

    def renderable(self):
        return bool(self.out_curve)

    def render(self):
        timestamp = f'-{self.timestamp}' if self.timestamp else ''
        folder = f'{self.folder}/{self.name}{timestamp}'
        makedirs(folder, exist_ok=True)
        xscale = 7
        for node, out in self.out_curve.items():
            inp = self.in_curve[node]
            width, _, height = out[-1]
            _, burst, _ = inp.pop(0)
            in_times = ','.join(
                f"{t:g}/{arrived:g}/{list_str(Cs, sep='+')}/{len(Cs)}"
                for t, Cs, arrived in inp
            )
            max_value, max_times = self.res[node]
            res_times = ','.join(
                "{:g}/{:g}".format(t, [departed
                                       for tt, _, departed in [(0.0, [], 0)] + out
                                       if tt <= t][-1])
                for t in max_times
            )
            grid = fr"""
% GRID
% ----
\draw [helper, ystep=1, xstep={width}/{xscale}]
    (0,0) grid({width}/{xscale},{height + 1});
\draw (0,0) rectangle ({width}/{xscale}, {height + 1});
\foreach \i in {{0, ..., {height + 1}}}
    \draw (up: \i) -- ++(left:.2) node[left, font=\tiny] {{\i}} ;
"""

            out_curve = fr"""
% OUTPUT CURVE
% ------------
\draw[thick, dashed] (0,0)
    \foreach \x/\y in {{{list_str(out, '{0[0]:g}/{0[2]:g}')}}} {{
        -- (\x/{xscale}, \y-1) coordinate (here)
        edge[helper] (here |- 0,0)
        -- (\x/{xscale}, \y)
    }}
;
% Ticks at bottom
\foreach \x in {{{list_str(out, '{0[0]:g}')}}}
    \draw (\x/{xscale},0)  --++ (down:0.2)  --++(.2,-.2)
        node[rotate=-45, right=0, font=\tiny] {{\x}};
"""

            in_curve = fr"""
% INPUT CURVE
% -----------
\draw[thick] (0, 0) -- (up:{len(burst)})
    node[below right=-1pt]{{\tiny({list_str(burst, sep='+')})}}
    \foreach \x/\y/\n/\h in {{{in_times}}} {{
        -- (\x/{xscale},\y-\h) coordinate (here)
        -- (\x/{xscale}, \y)
        edge[helper] (here |- 0,{height + 1})
        node[below right=-1pt]{{\tiny(\n)}}
    }}
    -- ({width}/{xscale}, {height})
;
% Ticks at top
\foreach \x in {{{list_str(inp, '{0[0]:g}')}}}
    \draw (\x/{xscale}, {height + 1}) --++ (up:0.2) --++(.2,.2)
        node[rotate=45, right=0, font=\tiny] {{\x}};
"""

            bklg = fr"""
% MAXIMUM BKLG
% ------------
\foreach \x/\y in {{{res_times}}}
    \draw[stealth-stealth,thick] (\x/{xscale},\y) --++ (up:{max_value})
    node[midway, right=0, font=\scriptsize] {{{max_value}}}
    ;
"""

            title = fr"""
% TITLES
% ------
\path (0,0) -- ++(up:{height + 1})
    node[midway, above=4ex, sloped] {{Cumulative number of frames}};
\path (0,{height + 1}) -- ++(right:{width}/{xscale})
    node[midway, above=4ex] {{Frame arrival times ($\mu$s)}};
\path (0,0) -- ++(right:{width}/{xscale})
    node[midway, below=4ex] {{Frame departure times ($\mu$s)}};
"""

            with open(f'{folder}/{str(node)}.pgf', 'w') as f:
                f.write(fr"""
% Config: {self.config.name}
% Exporter: {self.__class__.__name__}
% For component: {node}
% Date: {self.timestamp}
\begin{{tikzpicture}}[xscale=0.5, yscale=0.5]
\tikzstyle{{helper}}=[thin, gray, dotted]
{title}
{grid}
{in_curve}
{out_curve}
{bklg}
\end{{tikzpicture}}
""")
