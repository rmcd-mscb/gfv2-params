"""Rasterise each HRU's home VPU code onto the template grid.

Used by carea_map percentile-`vpu` mode on multi-VPU fabrics (gfv2): each cell
gets the integer VPU code of the HRU that covers it, so the builder can map
`vpu_code -> T_P` into a per-cell threshold array. Because the HRU polygon's
`vpu` value is burned, every cell of an HRU carries that HRU's home VPU — the
exact per-HRU home-VPU assignment the spec requires.

For single-VPU fabrics the profile declares `vpu:` and the raster is a constant
fill (or carea_map uses the scalar T_P directly and skips this step).
"""

from __future__ import annotations

import geopandas as gpd
import numpy as np
import rasterio
from rasterio.features import rasterize

from ..depstor import RasterInfo
from .context import BuildContext

VPU_NODATA = 0  # 0 is not a valid VPU code (VPUs are 01..18 -> 1..18)


def vpu_to_code(vpu: str) -> int:
    """'01'..'18' -> 1..18. Raises on anything that isn't a VPU label."""
    try:
        code = int(str(vpu).lstrip("0") or "0")
    except (TypeError, ValueError):
        raise ValueError(f"Not a VPU label: {vpu!r}")
    if not 1 <= code <= 21:  # NHDPlus VPUs run 01..18 (+ a few sub-regions)
        raise ValueError(f"VPU code out of range: {vpu!r} -> {code}")
    return code


def resolve_vpu_source(profile_vpu, fabric_has_vpu_attr: bool):
    """Resolution precedence: profile scalar > fabric attribute > error."""
    if profile_vpu:
        return "scalar", str(profile_vpu)
    if fabric_has_vpu_attr:
        return "attribute", "vpu"
    raise ValueError(
        "carea_map percentile `vpu` scope requires a profile `vpu` scalar "
        "(single-VPU fabric) or a `vpu` attribute on the HRU layer."
    )


def build(step_cfg: dict, ctx: BuildContext, logger) -> dict:
    out = ctx.resolve_output(step_cfg["output"])
    if out.exists() and not ctx.force:
        logger.info("  vpu_id exists — skipping (pass --force)")
        return {"vpu_id": out}

    info = RasterInfo.from_path(ctx.template_path)
    profile = {
        "driver": "GTiff", "height": info.height, "width": info.width, "count": 1,
        "dtype": "uint8", "crs": info.crs, "transform": info.transform,
        "nodata": VPU_NODATA, "compress": "LZW", "tiled": True,
        "blockxsize": 256, "blockysize": 256, "BIGTIFF": "YES",
    }

    hru = gpd.read_file(ctx.hru_gpkg, layer=ctx.hru_layer)
    kind, value = resolve_vpu_source(ctx.vpu, "vpu" in hru.columns)
    logger.info("--- vpu_id (%s) ---", kind)

    out.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(out, "w", **profile) as dst:
        if kind == "scalar":
            dst.write(np.full((info.height, info.width), vpu_to_code(value),
                              dtype="uint8"), 1)
        else:
            if hru.crs != info.crs:
                hru = hru.to_crs(info.crs)
            shapes = ((geom, vpu_to_code(v)) for geom, v in zip(hru.geometry, hru[value]))
            arr = rasterize(
                shapes, out_shape=(info.height, info.width),
                transform=info.transform, fill=VPU_NODATA, dtype="uint8",
                all_touched=True,
            )
            dst.write(arr, 1)
    logger.info("  Wrote vpu_id raster -> %s", out)
    return {"vpu_id": out}
