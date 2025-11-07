"""Test AnaFor with various options."""

from datetime import datetime
import conf.afdx
from exporter.buffer import BufferGraph, BufferCSV
from exporter.flow import FlowCSV
from tools.bufdim import BufDim
from tools.fa import FA
import tools.fa as fa_
import exporter.base as exporter_
import resource

# Choice of a network configuration file from conf folder
CONF_NAME = 'fpfifo'
config = conf.afdx.Configuration.from_mod_file(CONF_NAME, latency=16)

# Select several analysis tools
fa = FA(config, serialization=False, prio=True)
fas = FA(config, serialization=True, prio=True)
bd = BufDim(config, fas, serialization=True)

# Log output as CSV or TikZ figures
config.register(BufferCSV, timestamp=False)
config.register(FlowCSV, timestamp=False)
config.register(BufferGraph, timestamp=False)

# Run each analysis
fa.compute_all()
fas.compute_all()
bd.compute_all()

# Render logs to the export folder
config.render_all()
