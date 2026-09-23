"""Tests for the --add-fabric profile-stub insertion in scripts/init_data_root.py"""

import importlib.util
import logging
import re
from pathlib import Path

import pytest
import yaml

_spec = importlib.util.spec_from_file_location(
    "init_data_root",
    Path(__file__).resolve().parent.parent / "scripts" / "init_data_root.py",
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

add_fabric_profile = _mod.add_fabric_profile

_logger = logging.getLogger("test")

# A minimal base_config with comments, gfv2 the only fabric, fabrics last.
_BASE = """\
# A leading comment that must survive the edit.
data_root: /fake/root
default_fabric: gfv2

fabrics:
  gfv2:
    expected_max_hru_id: 100
    batch_size: 10
    id_feature: nat_hru_id
"""


def _write_base(tmp_path) -> Path:
    p = tmp_path / "base_config.yml"
    p.write_text(_BASE)
    return p


def test_appends_parseable_profile_with_required_keys(tmp_path):
    p = _write_base(tmp_path)
    add_fabric_profile(p, "oregon", _logger)

    cfg = yaml.safe_load(p.read_text())
    assert "oregon" in cfg["fabrics"]
    oregon = cfg["fabrics"]["oregon"]
    assert oregon["batch_size"] == 10000
    # required keys present as stubs (id_feature defaults to nat_hru_id placeholder)
    assert "expected_max_hru_id" in oregon
    assert oregon["id_feature"] == "nat_hru_id"
    # hru_gpkg/hru_layer are required for EVERY fabric (prepare_fabric,
    # build_weights, gap-fill) — they must be active keys in the stub, not
    # buried in a commented block, or the first pipeline step
    # (prepare_fabric) fails with a KeyError.
    assert "hru_gpkg" in oregon
    assert oregon["hru_layer"] == "nhru"
    # existing fabric untouched
    assert cfg["fabrics"]["gfv2"]["id_feature"] == "nat_hru_id"


def test_stub_is_a_complete_active_depstor_profile(tmp_path):
    """Every depstor key is ACTIVE in the stub, wired to the real shared input.

    The old stub commented the depstor block out and pointed it at retired
    paths (input/depstor/<fabric>_segments_wbodies.gpkg, layer v2_wb, the ArcPy
    twi.vrt). Every real fabric runs depstor, so the first outside user had to
    reconstruct the whole block by hand from another profile. The stub is now
    the tjc/flaming_gorge shape: only the fabric-specific values carry a TODO.
    """
    p = _write_base(tmp_path)
    add_fabric_profile(p, "oregon", _logger)
    oregon = yaml.safe_load(p.read_text())["fabrics"]["oregon"]

    # Fabric-bounds FDR clip serves as both template and FDR.
    assert oregon["template_raster"] == "{data_root}/{fabric}/shared/{fabric}_fdr.vrt"
    assert oregon["fdr_raster"] == oregon["template_raster"]
    # Percentile-mode TWI source, never the legacy VPU-01-calibrated twi.vrt.
    assert oregon["twi_raster"] == "{data_root}/shared/conus/vrt/twi_hydrodem.vrt"
    # Single pre-merged gpkg: segments come from the same file as the HRUs.
    assert oregon["segments_gpkg"] == oregon["hru_gpkg"]
    assert oregon["segments_layer"] == "nsegment"
    # Shared CONUS inputs, identical for every fabric.
    assert oregon["waterbody_gpkg"] == "{data_root}/input/nhd/nhd_waterbodies.gpkg"
    assert oregon["waterbody_layer"] == "waterbodies"
    assert oregon["wbd_huc12_table"] == "{data_root}/input/wbd/wbd_huc12.parquet"
    assert oregon["burn_add_waterbody_table"] == "{data_root}/input/nhd/burn_add_waterbodies.parquet"
    assert oregon["sink_points_table"] == "{data_root}/input/nhd/sink_points.parquet"
    assert oregon["ecoregions_gpkg"] == "{data_root}/input/ecoregions/us_eco_l3.gpkg"
    # The real 3DEP tile inventory (#223 part 2), not the retired wesm_index hull.
    assert oregon["dem_1m_inventory"] == "{data_root}/input/3dep/dem_1m_tile_inventory.parquet"
    assert oregon["wesm_project_attrs"] == "{data_root}/input/wesm/wesm_project_attrs.parquet"
    assert "wesm_index" not in oregon
    # Single-VPU placeholder: present and active, so vpu_id cannot silently
    # fall through to a missing `vpu` HRU attribute.
    assert "vpu" in oregon
    # The two floors stay opt-in: a domain with no closed basin must NOT
    # declare min_endorheic_comids (an empty table is a legitimate result).
    assert "min_endorheic_comids" not in oregon
    assert "min_onstream_comids" not in oregon


def test_stub_round_trips_through_the_real_loader(tmp_path):
    """The stub must load via load_base_config with no unresolved placeholder.

    _resolve_placeholders raises on any `{word}` it does not know, so a stub
    that used a placeholder other than {data_root}/{fabric} would break the
    very next command the recipe runs.
    """
    from gfv2_params.config import load_base_config

    p = _write_base(tmp_path)
    add_fabric_profile(p, "oregon", _logger)
    cfg = load_base_config(p, fabric="oregon")
    assert cfg["hru_gpkg"] == "/fake/root/oregon/fabric/oregon.gpkg"
    assert cfg["template_raster"] == "/fake/root/oregon/shared/oregon_fdr.vrt"
    assert cfg["segments_gpkg"] == cfg["hru_gpkg"]


_ARCH_TABLE = Path(__file__).resolve().parent.parent / "docs" / "ARCHITECTURE.md"
_ROW = re.compile(r"^\| `(\w+)` \| (✓|—) \| (✓|—) \|")


def _required_profile_keys_table() -> dict[str, tuple[bool, bool]]:
    """Parse docs/ARCHITECTURE.md's 'Required profile keys' table.

    Returns {key: (always_required, depstor_only)}.
    """
    rows = {}
    for line in _ARCH_TABLE.read_text().splitlines():
        m = _ROW.match(line)
        if m:
            rows[m.group(1)] = (m.group(2) == "✓", m.group(3) == "✓")
    assert rows, "could not find the required-profile-keys table in ARCHITECTURE.md"
    return rows


def test_every_stub_key_is_documented_in_the_architecture_table(tmp_path):
    """The stub and the doc table are two hand-maintained lists of the same keys.

    A key added to one and not the other is exactly how the old stub rotted
    (the doc table gained wbd_huc12_table, ecoregions_gpkg, wesm_index ... and
    the stub never did). Bookkeeping keys (expected_max_hru_id, batch_size,
    id_feature, hru_layer) are documented too, so the whole active key set must
    appear in the table.
    """
    p = _write_base(tmp_path)
    add_fabric_profile(p, "oregon", _logger)
    stub_keys = set(yaml.safe_load(p.read_text())["fabrics"]["oregon"])
    documented = _required_profile_keys_table()
    undocumented = stub_keys - set(documented)
    assert not undocumented, f"stub keys missing from ARCHITECTURE.md table: {sorted(undocumented)}"


def test_every_always_required_key_is_active_in_the_stub(tmp_path):
    p = _write_base(tmp_path)
    add_fabric_profile(p, "oregon", _logger)
    stub_keys = set(yaml.safe_load(p.read_text())["fabrics"]["oregon"])
    always = {k for k, (req, _) in _required_profile_keys_table().items() if req}
    missing = always - stub_keys
    assert not missing, f"always-required keys absent from the stub: {sorted(missing)}"


def test_preserves_existing_comments(tmp_path):
    p = _write_base(tmp_path)
    add_fabric_profile(p, "oregon", _logger)
    assert "# A leading comment that must survive the edit." in p.read_text()


def test_raises_when_fabric_exists(tmp_path):
    p = _write_base(tmp_path)
    with pytest.raises(ValueError, match="already exists"):
        add_fabric_profile(p, "gfv2", _logger)


def test_rejects_invalid_name(tmp_path):
    p = _write_base(tmp_path)
    with pytest.raises(ValueError, match="Invalid fabric name"):
        add_fabric_profile(p, "bad name", _logger)


def test_raises_without_fabrics_mapping(tmp_path):
    p = tmp_path / "base_config.yml"
    p.write_text("data_root: /fake/root\ndefault_fabric: gfv2\n")
    with pytest.raises(ValueError, match="No top-level `fabrics:`"):
        add_fabric_profile(p, "oregon", _logger)
