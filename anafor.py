"""Test AnaFor with various options."""

from datetime import datetime
from conf.afdx import Configuration
from exporter.buffer import BufferGraph, BufferCSV
from exporter.flow import FlowCSV
from tools.bufdim import BufDim
from tools.fa import FA

# Choice of a network configuration file from assets folder
CONF_NAME = 'fpfifo'
config = Configuration.from_mod_file(CONF_NAME, latency=16)

# Select several analysis tools
# (the existing analysis classes can be found in the tools folder)
fa = FA(config, serialization=False, prio=True)
fas = FA(config, serialization=True, prio=True)
bd = BufDim(config, fas, serialization=True)

# Log output as CSV or TikZ figures
# (the existing exporter classes can be found in the exporter folder)
config.register(BufferCSV, timestamp=False)
config.register(FlowCSV, timestamp=False)
config.register(BufferGraph, timestamp=False)

# Run each analysis
fa.compute_all()
fas.compute_all()
bd.compute_all()

# Render the output of each registered exporter to the export folder
config.render_all()
