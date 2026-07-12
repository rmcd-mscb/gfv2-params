# gfv2-params

PRMS/NHM hydrologic-model parameter generation. Given a watershed fabric of
HRU polygons, this pipeline computes per-HRU parameters by running zonal
statistics against CONUS source rasters (DEM, soils, lithology, LULC,
depression-storage). Production runs target CONUS scale under SLURM on a
USGS HPC cluster.

## Where to start

- **New to the project?** → [Getting started](getting-started.md) walks
  through `pixi install`, environment setup, and a smoke command.
- **Running the pipeline on the HPC?** → [HPC workflow](hpc-workflow.md) is
  the authoritative step-by-step.
- **Want to understand the shape of the code?** →
  [Architecture / Canonical](ARCHITECTURE.md) for the
  orchestrator + builder pattern, fabric profiles, and the
  4-stage table.
- **Adding a new HRU parameter?** →
  [Adding a parameter](ADDING_A_PARAMETER.md) traces `--param elevation`
  end-to-end. The pattern transfers.
- **Hit an unfamiliar Python idiom?** →
  [Python patterns](python-patterns.md) explains the 10 non-obvious idioms
  this codebase uses (placeholder strings, `require_config_key`, the
  `BUILDERS` dispatch table, etc.).

## Pipeline reference

- [Depstor workflow](depstor_workflow.md) — depression-storage pipeline
  reference.
- [Depstor port summary](depstor_port_summary.md) — provenance notes from
  the depstor-pipeline consolidation (PR #72).
- [VPU 01 validation](depstor_vpu01_validation_results.md) — pipeline
  validation results on the VPU 01 small-scale target.
- [dprst_depth Phase 0 spike](dprst_depth_spike.md) — issue #173 go/no-go
  investigation into deriving `dprst_depth_avg` from 3DEP topography.

## API reference

The [API reference](api.md) is auto-generated from docstrings in
`src/gfv2_params/`. Coverage tracks what's documented; modules without
docstrings show only signatures.

## Conventions

Repo-specific rules (commits, doc audits, code style, the gotchas you
*must* know to not corrupt outputs) live in [Project conventions](conventions.md).
