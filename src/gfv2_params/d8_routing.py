"""In-process D8 upslope traversal for the depstor routing step.

Replaces WhiteboxTools `Watershed`, which traces each cell downstream with no
memoization and no cycle guard and so hangs on flow cycles / pathological flats
in the CONUS FDR (it stalled mid-VPU-2 for 3+ hours; see
docs/superpowers/specs/2026-05-29-depstor-d8-routing-kernel-design.md).

`drains_to_dprst_kernel` answers, for every cell: does following the ESRI-D8
flow pointer downstream eventually reach a depression pour-point? It is the
upslope contributing area of the pour-point set on a functional (out-degree-1)
flow graph — a textbook O(N) traversal. Pour-point (dprst) cells seed as
draining; barrier cells (on-stream waterbodies) seed as non-draining termini,
so a flow path that reaches a barrier before a pour stops there and its
upslope land is not attributed to a downstream depression — the first
waterbody on the flow path wins (dprst wins any impossible-by-construction
overlap with a barrier).

`drains_to_dprst_labeled_kernel` extends this to per-depression attribution:
each cell receives the label of the specific depression its D8 path reaches,
enabling per-depression contributing-area counts.

This is the ONLY numba user in the package; it is deliberately isolated here so
the widely-imported `depstor.py` stays numba-free.

ESRI D8 encoding (value -> downstream neighbour):
    1=E  2=SE  4=S  8=SW  16=W  32=NW  64=N  128=NE
Any other value (notably nodata 255, or 0) is treated as a sink/terminus.
"""

from __future__ import annotations

import numpy as np
from numba import njit

# State coloring used during traversal.
_UNKNOWN = 0
_DRAINS = 1
_NOT = 2
_ACTIVE = 3  # currently on the path being walked (detects cycles)


@njit(cache=True)
def _resolve(fdr, pour, barrier, fdr_nodata):
    ny, nx = fdr.shape
    st = np.zeros((ny, nx), dtype=np.uint8)

    # Seed: every pour-point cell drains (to itself / the depression).
    for r in range(ny):
        for c in range(nx):
            if pour[r, c] == 1:
                st[r, c] = _DRAINS

    # Seed barriers as non-draining termini (on-stream waterbodies). A flow
    # path that reaches a barrier before a pour resolves _NOT and stops there,
    # so its upslope land is not attributed to a downstream depression. dprst
    # wins any overlap (disjoint by construction; only seed still-_UNKNOWN cells).
    for r in range(ny):
        for c in range(nx):
            if barrier[r, c] == 1 and st[r, c] == _UNKNOWN:
                st[r, c] = _NOT

    cap = 1 << 20
    stack_r = np.empty(cap, dtype=np.int64)
    stack_c = np.empty(cap, dtype=np.int64)
    n_cycles = 0

    for sr in range(ny):
        for sc in range(nx):
            if st[sr, sc] != _UNKNOWN:
                continue
            n = 0
            cr = sr
            cc = sc
            result = _NOT
            while True:
                s = st[cr, cc]
                if s == _DRAINS:
                    result = _DRAINS
                    break
                if s == _NOT:
                    result = _NOT
                    break
                if s == _ACTIVE:
                    n_cycles += 1
                    result = _NOT
                    break

                st[cr, cc] = _ACTIVE
                if n >= cap:
                    new_cap = cap * 2
                    nr_ = np.empty(new_cap, dtype=np.int64)
                    nc_ = np.empty(new_cap, dtype=np.int64)
                    nr_[:cap] = stack_r
                    nc_[:cap] = stack_c
                    stack_r = nr_
                    stack_c = nc_
                    cap = new_cap
                stack_r[n] = cr
                stack_c[n] = cc
                n += 1

                code = fdr[cr, cc]
                if code == fdr_nodata:
                    result = _NOT
                    break
                if code == 1:
                    dr = 0
                    dc = 1
                elif code == 2:
                    dr = 1
                    dc = 1
                elif code == 4:
                    dr = 1
                    dc = 0
                elif code == 8:
                    dr = 1
                    dc = -1
                elif code == 16:
                    dr = 0
                    dc = -1
                elif code == 32:
                    dr = -1
                    dc = -1
                elif code == 64:
                    dr = -1
                    dc = 0
                elif code == 128:
                    dr = -1
                    dc = 1
                else:
                    result = _NOT
                    break

                nr2 = cr + dr
                nc2 = cc + dc
                if nr2 < 0 or nr2 >= ny or nc2 < 0 or nc2 >= nx:
                    result = _NOT
                    break
                cr = nr2
                cc = nc2

            for i in range(n):
                rr = stack_r[i]
                ric = stack_c[i]
                st[rr, ric] = result

    out = np.zeros((ny, nx), dtype=np.uint8)
    for r in range(ny):
        for c in range(nx):
            if st[r, c] == _DRAINS:
                out[r, c] = 1
    return out, n_cycles


@njit(cache=True)
def _resolve_labeled(fdr, label, fdr_nodata):
    ny, nx = fdr.shape
    st = np.zeros((ny, nx), dtype=np.uint8)
    lab = np.zeros((ny, nx), dtype=np.int32)

    # Seed: each depression cell drains to its own label.
    for r in range(ny):
        for c in range(nx):
            if label[r, c] > 0:
                st[r, c] = _DRAINS
                lab[r, c] = label[r, c]

    cap = 1 << 20
    stack_r = np.empty(cap, dtype=np.int64)
    stack_c = np.empty(cap, dtype=np.int64)
    n_cycles = 0

    for sr in range(ny):
        for sc in range(nx):
            if st[sr, sc] != _UNKNOWN:
                continue
            n = 0
            cr = sr
            cc = sc
            result = _NOT
            result_lab = 0
            while True:
                s = st[cr, cc]
                if s == _DRAINS:
                    result = _DRAINS
                    result_lab = lab[cr, cc]
                    break
                if s == _NOT:
                    result = _NOT
                    break
                if s == _ACTIVE:
                    n_cycles += 1
                    result = _NOT
                    break

                st[cr, cc] = _ACTIVE
                if n >= cap:
                    new_cap = cap * 2
                    nr_ = np.empty(new_cap, dtype=np.int64)
                    nc_ = np.empty(new_cap, dtype=np.int64)
                    nr_[:cap] = stack_r
                    nc_[:cap] = stack_c
                    stack_r = nr_
                    stack_c = nc_
                    cap = new_cap
                stack_r[n] = cr
                stack_c[n] = cc
                n += 1

                code = fdr[cr, cc]
                if code == fdr_nodata:
                    result = _NOT
                    break
                if code == 1:
                    dr = 0
                    dc = 1
                elif code == 2:
                    dr = 1
                    dc = 1
                elif code == 4:
                    dr = 1
                    dc = 0
                elif code == 8:
                    dr = 1
                    dc = -1
                elif code == 16:
                    dr = 0
                    dc = -1
                elif code == 32:
                    dr = -1
                    dc = -1
                elif code == 64:
                    dr = -1
                    dc = 0
                elif code == 128:
                    dr = -1
                    dc = 1
                else:
                    result = _NOT
                    break

                nr2 = cr + dr
                nc2 = cc + dc
                if nr2 < 0 or nr2 >= ny or nc2 < 0 or nc2 >= nx:
                    result = _NOT
                    break
                cr = nr2
                cc = nc2

            for i in range(n):
                rr = stack_r[i]
                ric = stack_c[i]
                st[rr, ric] = result
                if result == _DRAINS:
                    lab[rr, ric] = result_lab

    out = np.zeros((ny, nx), dtype=np.int32)
    for r in range(ny):
        for c in range(nx):
            if st[r, c] == _DRAINS:
                out[r, c] = lab[r, c]
    return out, n_cycles


def drains_to_dprst_kernel(fdr_win, pour_win, barrier_win, fdr_nodata=255):
    """Mark cells whose ESRI-D8 path reaches a depression pour-point.

    Pour-point (dprst) cells seed as draining (`_DRAINS`); barrier cells
    (on-stream waterbodies) seed as non-draining termini (`_NOT`). A flow path
    that reaches a barrier before it reaches a pour stops there and is marked
    non-draining, so land upslope of an on-stream waterbody is not attributed
    to a downstream depression — the first waterbody on the flow path wins.
    dprst wins on any (impossible-by-construction) overlap between the two
    masks.

    Parameters
    ----------
    fdr_win : ndarray[uint8]
        ESRI-D8 flow-direction window. Values in {1,2,4,8,16,32,64,128} are
        flow directions; `fdr_nodata` and any other value terminate as sinks.
    pour_win : ndarray[uint8]
        Pour-point mask: 1 = depression cell, 0 = background.
    barrier_win : ndarray[uint8]
        Barrier mask, same shape as `fdr_win`/`pour_win`: 1 = barrier cell
        (on-stream waterbody), 0 = background. Required.
    fdr_nodata : int, default 255
        FDR nodata value (treated as a sink).

    Returns
    -------
    out : ndarray[uint8]
        1 where the cell drains to a pour-point (including the pour-points
        themselves), else 0.
    n_cycles : int
        Number of flow cycles encountered during the traversal. A cycle is a
        data defect in a hydro-conditioned FDR (it is what hung WBT Watershed);
        its cells are resolved as non-draining. A non-zero count is worth a
        warning at the call site.
    """
    fdr = np.ascontiguousarray(fdr_win, dtype=np.uint8)
    pour = np.ascontiguousarray(pour_win, dtype=np.uint8)
    barrier = np.ascontiguousarray(barrier_win, dtype=np.uint8)
    return _resolve(fdr, pour, barrier, np.uint8(fdr_nodata))


def drains_to_dprst_labeled_kernel(fdr_win, label_win, fdr_nodata=255):
    """Per-cell label of the depression its ESRI-D8 path reaches (0 = none).

    Like ``drains_to_dprst_kernel`` but attributes each draining cell to a
    specific depression. ``label_win`` carries each depression region's unique
    positive id at its cells (0 background); the return holds that id for every
    cell that drains there, enabling per-depression contributing-area counts via
    ``np.bincount(out.ravel())``. Returns ``(out_int32, n_cycles)``.
    """
    fdr = np.ascontiguousarray(fdr_win, dtype=np.uint8)
    label = np.ascontiguousarray(label_win, dtype=np.int32)
    return _resolve_labeled(fdr, label, np.uint8(fdr_nodata))
