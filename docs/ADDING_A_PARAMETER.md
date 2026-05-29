# Adding a new HRU parameter — end-to-end trace of `elevation`

If you're adding a new HRU parameter, here's the end-to-end trace of an
existing one (`elevation`) you can mirror. This walks the same path the
SLURM array task takes for one batch of HRUs, with file:line pointers and
what the `config` dict contains at each step.

The intended reader has numpy/pandas/rasterio experience but is not a
software engineer; the goal is to read this once and know exactly which 1-2
files to touch to add a new param. See
[ARCHITECTURE.md](ARCHITECTURE.md) for the high-level pattern.

## Prerequisites

You need three things on disk before adding a param:

- A fabric profile in [`configs/base_config.yml`](../configs/base_config.yml)
  with `hru_gpkg`, `hru_layer`, `id_feature`, `expected_max_hru_id`,
  `batch_size`.
- A source raster reachable under `data_root` (typically a CONUS VRT under
  `shared/conus/vrt/` or a per-fabric clip).
- A `prepare_fabric` run that produced `{data_root}/{fabric}/batches/`
  containing `batch_NNNN.gpkg` files and a `manifest.yml`.

## The 5 hops for `--param elevation`

### Hop 1 — Invocation

```bash
pixi run -e dev python scripts/derive_zonal_params.py \
    --mode zonal \
    --param elevation \
    --batch_id 0 \
    --config configs/zonal/zonal_params.yml \
    --base_config configs/base_config.yml \
    --fabric gfv2_vpu01
```

This is exactly what one SLURM array task runs. The submit wrapper
[`slurm_batch/submit_zonal_params.sh`](../slurm_batch/submit_zonal_params.sh)
loops every entry in `zonal_params.yml` and submits an array zonal job +
chained merge per param.

### Hop 2 — Orchestrator dispatch

[`scripts/derive_zonal_params.py:162-186`](../scripts/derive_zonal_params.py#L162-L186)
parses args and dispatches on `--mode`. For `--mode zonal` it calls
[`run_zonal`](../scripts/derive_zonal_params.py#L106-L119):

```python
config = _load_resolved_config(args)
entry = _find_param(config, args.param)
script_tag = entry.get("script")
param_cfg = _build_param_cfg(config, entry)
BATCH_RUNNERS[script_tag](param_cfg, args.batch_id, logger)
```

After `_load_resolved_config`
([scripts/derive_zonal_params.py:53-61](../scripts/derive_zonal_params.py#L53-L61))
`config` is the merge of `base_config.yml` (with the `gfv2_vpu01` profile
flattened on top) and `zonal_params.yml`, with every `{data_root}` /
`{fabric}` placeholder already substituted. `entry` is just the
`elevation` block from `zonal_params.yml`. `script_tag` is `"zonal"`,
which selects `run_zonal_batch` from the dispatch table (see Hop 4).

### Hop 3 — Building `param_cfg`

[`scripts/derive_zonal_params.py:74-103`](../scripts/derive_zonal_params.py#L74-L103)
flattens five sources into the single dict the runner consumes
(later wins on conflict):

```python
defaults = config.get("defaults", {})            # 1. zonal_params.yml `defaults:`
param_cfg = {**defaults, **entry}                # 2. the elevation entry
param_cfg["source_type"] = entry["name"]         # 3. "elevation"
param_cfg["fabric"] = config["fabric"]           # 4. "gfv2_vpu01"
# 5. fabric-profile required keys (and one optional one):
param_cfg["expected_max_hru_id"] = config["expected_max_hru_id"]
param_cfg["id_feature"] = require_config_key(config, "id_feature", ...)
param_cfg["hru_gpkg"]   = require_config_key(config, "hru_gpkg", ...)
param_cfg["hru_layer"]  = config.get("hru_layer", "nhru")
```

The five sources are:

1. **`defaults:`** block from
   [`configs/zonal/zonal_params.yml:31-38`](../configs/zonal/zonal_params.yml#L31-L38)
   — `batch_dir`, `target_layer`, `output_dir`, `merged_subdir`,
   `weight_dir`.
2. **The param entry** from
   [`configs/zonal/zonal_params.yml:43-47`](../configs/zonal/zonal_params.yml#L43-L47)
   for `elevation` — `name`, `script`, `source_raster`, `categorical`,
   `merged_file`.
3. **`source_type`** injected from `entry["name"]` so runners namespace
   their per-batch CSV subdirs.
4. **`fabric`** — the resolved active fabric.
5. **Fabric-profile keys** — `expected_max_hru_id`, `id_feature`,
   `hru_gpkg`, `hru_layer` from the active profile in `base_config.yml`,
   read via `require_config_key`
   ([`src/gfv2_params/config.py:86-100`](../src/gfv2_params/config.py#L86-L100))
   which raises a clear error if any required key is missing.

For `--param elevation --fabric gfv2_vpu01`, `param_cfg` ends up roughly:

```python
{
    "script":              "zonal",
    "source_type":         "elevation",
    "source_raster":       "{data_root}/shared/conus/vrt/elevation.vrt",  # already resolved
    "categorical":         False,
    "merged_file":         "nhm_elevation_params.csv",
    "batch_dir":           "{data_root}/gfv2_vpu01/batches",              # already resolved
    "target_layer":        "nhru",
    "output_dir":          "{data_root}/gfv2_vpu01/params",               # already resolved
    "merged_subdir":       "merged",
    "weight_dir":          "{data_root}/shared/conus/weights",            # already resolved
    "fabric":              "gfv2_vpu01",
    "expected_max_hru_id": 11278,
    "id_feature":          "nat_hru_id",
    "hru_gpkg":            "{data_root}/gfv2_vpu01/fabric/...gpkg",       # already resolved
    "hru_layer":           "nhru",
    # plus other top-level base_config keys (data_root, batch_size, ...)
}
```

### Hop 4 — Dispatch table

[`src/gfv2_params/zonal_runners/__init__.py:92-97`](../src/gfv2_params/zonal_runners/__init__.py#L92-L97)
maps each `script:` tag to the runner that handles it:

```python
BATCH_RUNNERS = {
    "zonal":  run_zonal_batch,    # continuous-zonal stats from one raster
    "soils":  run_soils_batch,
    "lulc":   run_lulc_batch,
    "ssflux": run_ssflux_batch,
}
```

`elevation` carries `script: zonal`, so the dispatch lands on
`run_zonal_batch` (imported from
[`src/gfv2_params/zonal_runners/zonal.py`](../src/gfv2_params/zonal_runners/zonal.py)).

### Hop 5 — The actual compute

[`src/gfv2_params/zonal_runners/zonal.py:16-72`](../src/gfv2_params/zonal_runners/zonal.py#L16-L72)
is the per-batch work — read the HRU batch gpkg, open the source raster,
hand both to `gdptools.ZonalGen`, write one CSV:

```python
nhru_gdf = gpd.read_file(batch_gpkg, layer=target_layer)
ned_da   = rioxarray.open_rasterio(raster_path, masked=True)
data = UserTiffData(source_var=source_type, source_ds=ned_da,
                    source_crs=ned_da.rio.crs, target_gdf=nhru_gdf,
                    target_id=id_feature, ...)
zonal_gen = ZonalGen(user_data=data, zonal_engine="exactextract",
                     zonal_writer="csv", out_path=output_dir,
                     file_prefix=file_prefix, jobs=4)
stats = zonal_gen.calculate_zonal(categorical=categorical)
```

Inputs resolved off `param_cfg`:

- `raster_path`  = `config["source_raster"]`
- `batch_gpkg`   = `{config["batch_dir"]}/batch_{batch_id:04d}.gpkg`
- `output_dir`   = `{config["output_dir"]}/{source_type}`
- `file_prefix`  = `base_nhm_{source_type}_{fabric}_batch_{batch_id:04d}_param`

For categorical params (e.g. `script: zonal` with `categorical: true`,
though `elevation` is `false`) `ZonalGen` returns one column per category;
for continuous params it returns the exactextract default stats.

## Where the output lands

One CSV per batch under:

```
{data_root}/{fabric}/params/{source_type}/base_nhm_{source_type}_{fabric}_batch_{NNNN}_param.csv
```

Real example for `oregon`, batch 0:

```
{data_root}/oregon/params/elevation/base_nhm_elevation_oregon_batch_0000_param.csv
```

Columns (continuous zonal, exactextract defaults):

```
<id_feature>, count, mean, std, min, 25%, 50%, 75%, max, sum
```

`<id_feature>` is whatever the fabric profile declares — `nat_hru_id` for
`gfv2`/`gfv2_vpu01`, `hru_id` for `oregon`.

After the array job finishes, `--mode merge` concatenates every batch CSV
into `{output_dir}/merged/{merged_file}` (one row per HRU, sorted by
`id_feature`); see
[`src/gfv2_params/zonal_runners/merge.py:15-75`](../src/gfv2_params/zonal_runners/merge.py#L15-L75).
For `elevation` that is
`{data_root}/{fabric}/params/merged/nhm_elevation_params.csv`.

## To add a new param

1. **Add a YAML entry** under `params:` in
   [`configs/zonal/zonal_params.yml`](../configs/zonal/zonal_params.yml).
   Mirror an existing entry of the same `script:` family — for a new
   continuous raster, copy the `elevation` block and change `name`,
   `source_raster`, and `merged_file`.
2. **Confirm the source raster exists on disk.** Resolve any
   `{data_root}` placeholders by hand and `ls` the path.
3. **Choose the `script:` tag** — `zonal` (continuous raster),
   `soils` (categorical/continuous from soils/litho), `lulc` (cov_type /
   covden / interception / retention bundle), or `ssflux` (litho-weighted
   PRMS flux params). The dispatch table is
   [`src/gfv2_params/zonal_runners/__init__.py:92-97`](../src/gfv2_params/zonal_runners/__init__.py#L92-L97).
   If your param doesn't fit any existing `script:` family you're adding
   a new runner, not a new param — see "How to add a new pipeline step"
   in [ARCHITECTURE.md](ARCHITECTURE.md#how-to-add-a-new-pipeline-step).
4. **Smoke-test one batch:**
   ```bash
   pixi run -e dev python scripts/derive_zonal_params.py \
       --mode zonal --param <newname> --batch_id 0 \
       --config configs/zonal/zonal_params.yml \
       --base_config configs/base_config.yml \
       --fabric gfv2_vpu01
   ```
   Check the per-batch CSV under `{data_root}/{fabric}/params/<newname>/`.
5. **Submit the full SLURM array** via
   [`slurm_batch/submit_zonal_params.sh`](../slurm_batch/submit_zonal_params.sh)
   from a shell that has `pixi` on `PATH`. This loops every param in the
   YAML and chains array zonal -> merge per param.

## See also

- [`docs/ARCHITECTURE.md`](ARCHITECTURE.md) — the high-level
  orchestrator + builder + unified-config pattern this walkthrough
  illustrates, plus the "How to add a new pipeline step" recipe.
- [`slurm_batch/HPC_REFERENCE.md`](../slurm_batch/HPC_REFERENCE.md) — Stage 4A walks the
  per-parameter submit + inspect path manually.
- Issue history: [#115](https://github.com/rmcd-mscb/gfv2-params/issues/115)
  — request for this walkthrough, from the 2026-05-26
  geoscientist-readability audit (PR #114).
