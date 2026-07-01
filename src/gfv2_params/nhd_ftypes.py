"""NHD FTYPE classification constants shared across download + depstor builders.

Kept dependency-free so depstor_builders can import the dprst/on-stream FTYPE
policy without pulling in the download stack (py7zr/requests via nhd_flowlines).
"""

from __future__ import annotations

# Waterbodies that are always depression storage — never promoted on-stream.
FORCE_DPRST_FTYPES = {"Playa"}
# Waterbodies excluded from the depstor waterbody classification entirely:
# neither dprst nor on-stream. A glacier/permanent ice mass is not depression
# storage; its cells fall back to land (perv/imperv via LULC).
EXCLUDE_WATERBODY_FTYPES = {"Ice Mass"}
# Union: FTYPEs that must never appear in the on-stream set.
NEVER_ONSTREAM_FTYPES = FORCE_DPRST_FTYPES | EXCLUDE_WATERBODY_FTYPES
