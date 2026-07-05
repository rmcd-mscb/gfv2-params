from pathlib import Path

from gfv2_params.config import load_config


def test_snodas_source_entry_resolves():
    cfg = load_config(
        Path("configs/aggregate/aggregate_sources.yml"),
        base_config_path=Path("configs/base_config.yml"),
        fabric="oregon",
    )
    assert cfg["fabric"] == "oregon"
    src = next(s for s in cfg["sources"] if s["name"] == "snodas")
    assert src["output_prefix"] == "snodas"
    # output_dir is a top-level key, so load_config resolves it fully.
    assert cfg["output_dir"] == "{data_root}/oregon/snodas".format(data_root=cfg["data_root"])
    assert "{fabric}" not in cfg["output_dir"]
    assert cfg["weight_dir"] == "{data_root}/oregon/weights_agg".format(data_root=cfg["data_root"])
