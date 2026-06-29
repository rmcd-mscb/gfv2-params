"""Builder-level tests for compute_breached_fdr (WBT mocked).

Mirrors tests/test_wbt.py: the WBT subprocess is never actually run — we
monkeypatch _run_wbt and assert the orchestration contract (reuse the staged
fixed DEM, then issue BreachDepressionsLeastCost followed by D8Pointer with the
ESRI pointer flag; skip when the output already exists).
"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

import gfv2_params.shared_rasters.compute_breached_fdr as cbf

LOGGER = logging.getLogger("test_compute_breached_fdr")


def _touch(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(b"")


def test_reuses_fixed_dem_then_breach_then_d8(monkeypatch, tmp_path):
    vpu = "09"
    out_dir = tmp_path / "per_vpu"
    vpu_dir = out_dir / vpu
    _touch(vpu_dir / f"Hydrodem_merged_fixed_{vpu}.tif")  # already staged

    calls: list[tuple[str, list[str]]] = []

    def fake_run_wbt(runner, tool, args, logger):
        calls.append((tool, args))
        # emulate WBT writing its --output so the next step's input exists
        for a in args:
            if a.startswith("--output="):
                _touch(Path(a.split("=", 1)[1]))

    def fail_fix(*a, **k):
        raise AssertionError("_fix_dem_nodata must not run when fixed DEM exists")

    monkeypatch.setattr(cbf, "_run_wbt", fake_run_wbt)
    monkeypatch.setattr(cbf, "_fix_dem_nodata", fail_fix)

    cbf._process_vpu(vpu, out_dir, out_dir, runner="wbt", force=False, logger=LOGGER)

    tools = [t for t, _ in calls]
    assert tools == ["BreachDepressionsLeastCost", "D8Pointer"]
    breach_args = " ".join(calls[0][1])
    assert f"Hydrodem_breached_{vpu}.tif" in breach_args
    assert f"--dist={cbf.BREACH_DIST}" in breach_args
    d8_args = " ".join(calls[1][1])
    assert "--esri_pntr" in d8_args
    assert (vpu_dir / f"Fdr_breached_{vpu}.tif").exists()


def test_skips_when_output_exists_and_not_force(monkeypatch, tmp_path):
    vpu = "16"
    out_dir = tmp_path / "per_vpu"
    _touch(out_dir / vpu / f"Fdr_breached_{vpu}.tif")

    def boom(*a, **k):
        raise AssertionError("must not invoke WBT when output exists")

    monkeypatch.setattr(cbf, "_run_wbt", boom)
    cbf._process_vpu(vpu, out_dir, out_dir, runner="wbt", force=False, logger=LOGGER)


def test_missing_fixed_and_source_raises(monkeypatch, tmp_path):
    vpu = "10"
    out_dir = tmp_path / "per_vpu"
    (out_dir / vpu).mkdir(parents=True)
    with pytest.raises(FileNotFoundError):
        cbf._process_vpu(vpu, out_dir, out_dir, runner="wbt", force=False, logger=LOGGER)


def test_force_reruns_fix_and_wbt(monkeypatch, tmp_path):
    vpu = "01"
    out_dir = tmp_path / "per_vpu"
    _touch(out_dir / vpu / f"Hydrodem_merged_{vpu}.tif")        # source Hydrodem
    _touch(out_dir / vpu / f"Hydrodem_merged_fixed_{vpu}.tif")  # exists, but force=True
    _touch(out_dir / vpu / f"Fdr_breached_{vpu}.tif")           # output exists too

    fix_calls = []
    wbt_calls = []

    def fake_fix(src, dst, logger):
        fix_calls.append(dst)

    def fake_run_wbt(runner, tool, args, logger):
        wbt_calls.append(tool)
        for a in args:
            if a.startswith("--output="):
                _touch(Path(a.split("=", 1)[1]))

    monkeypatch.setattr(cbf, "_fix_dem_nodata", fake_fix)
    monkeypatch.setattr(cbf, "_run_wbt", fake_run_wbt)

    cbf._process_vpu(vpu, out_dir, out_dir, runner="wbt", force=True, logger=LOGGER)

    assert len(fix_calls) == 1                                   # force re-creates the fixed DEM
    assert wbt_calls == ["BreachDepressionsLeastCost", "D8Pointer"]
