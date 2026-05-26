# Geoscientist-Readability Audit (2026-05-26)

Targeted follow-on to the
[2026-05-23 fresh-eyes review](2026-05-23-repo-fresh-eyes.md), run after the
10-PR T1 + T2 sprint landed (PRs #104–#113).

**Lens.** A USGS hydrologist with 5 years of one-off Python (numpy / pandas /
rasterio / matplotlib / some xarray) who has NEVER built a Python package,
doesn't know what a "dispatch table" or "context manager" is in design-pattern
terms, finds `**kwargs` mildly intimidating, but WILL touch this codebase to
add a parameter or debug a failed run.

**Scope guard.** This is NOT a general code review. SWE pedantry (Protocol
classes, ABCs, deeper generics, mypy strictness, more abstraction) was
explicitly off-table. The question was: what trips THIS persona vs. what
flows for them.

## TL;DR

The sprint paid off. Domain language is consistent across files, dispatch
tables are small enough to grep, and the rich-WHY docstrings in
`compute_dem_derivatives.py` and `carea_map.py` read like annotated method
papers. The two real walls left for this persona:

1. The orchestrator + dispatch-table + flattened-config pattern still takes
   5 file-hops to trace `--param elevation` to the numpy that runs it.
2. The README hits a hydrologist with pixi / SLURM / lockfile jargon before
   they've run anything.

Both are fixable without code refactor: one walkthrough doc, one README
slim, three small comment patches, plus one tiny code change (gate the
startup heartbeat on `SLURM_ARRAY_TASK_ID`). One additional doc
(`python-patterns.md`) explains the 5–10 non-obvious idioms that appear
throughout the codebase. ~5 h total, filed as 6 issues; see "Filed issues"
below.

## Wins (the persona will find these clear)

- **Domain language is everywhere.** Variable and function names match the
  literature: `nhru_gdf`, `id_feature`, `Twi_hydrodem_<vpu>.tif`,
  `carea_max`, `smidx_coef`, `dprst_frac`, `fac`, `slope_deg`,
  `vpu_landmask_path`. The persona will recognize these from PRMS/NHM docs
  without an internal-glossary detour. (e.g.
  `src/gfv2_params/shared_rasters/compute_dem_derivatives.py:261-286`,
  `src/gfv2_params/depstor_builders/carea_map.py:122-130`.)
- **`compute_dem_derivatives.py:1-74`** is a model docstring for this
  persona — it explains *why* float64, *why* slope cap, *why* hybrid
  richdem + WBT, and which bug-shape the cap fixes ("1-pixel snake of
  holes"). This is exactly the kind of WHY-comment density the project
  policy nominally forbids but in fact tolerates here, and it's the right
  call.
- **`src/gfv2_params/depstor.py:97-113`** (`read_land_mask`) tells the
  persona which raster is the mask, why it isn't the hydro-DEM nodata,
  and where the mask is produced. One docstring, no detour.
- **Top-level CLIs are linear and predictable.** `scripts/derive_zonal_params.py`
  has a one-paragraph module docstring listing the three modes, a flat
  `main()` with argparse, and a 4-function dispatch — no decorators, no
  plugin discovery. (`scripts/derive_zonal_params.py:1-23`, `162-186`.)
- **`carea_map.py:90-101`** carries the threshold-mode guard inline with
  a warning that names the calibration corpus ("8.0/15.6 are calibrated
  to ArcPy TWI"). The persona who sets `threshold_mode: absolute` against
  `twi_hydrodem.vrt` is told *in the run log* why this is wrong.
- **No SWE-jargon hooks.** No `Protocol`, no `TypeVar`, no `Generic`, no
  `@overload`, no decorator-based registration. The dispatch tables are
  plain dicts (`BUILDERS`, `BATCH_RUNNERS`) — the persona can
  `grep BUILDERS` and read what's there.
  (`src/gfv2_params/shared_rasters/__init__.py:38-49`,
  `src/gfv2_params/zonal_runners/__init__.py:92-97`.)
- **Per-stage `__init__.py` modules double as design docs.** Each one
  names the pattern, lists the steps, and points at the orchestrator. The
  persona can read three short `__init__.py` files and have the whole
  architecture. (`src/gfv2_params/depstor_builders/__init__.py:1-48`,
  `src/gfv2_params/zonal_runners/__init__.py:1-25`.)
- **YAML config has prose comments at the top.**
  `configs/zonal/zonal_params.yml:1-29` explains what replaced what, why
  `lulc_<source>` is normalised, and which files stay. The persona will
  land in this file when they add a new param.

## HIGH pain points — actually block the persona

### #1 — Tracing "what does `--param elevation` actually do?" is 5 file-hops

Walking it:

- `scripts/derive_zonal_params.py:106-119` (`run_zonal`) → looks up
  `BATCH_RUNNERS[script_tag]`
- `src/gfv2_params/zonal_runners/__init__.py:92-97` → maps `"zonal"` →
  `run_zonal_batch`
- `src/gfv2_params/zonal_runners/zonal.py:16-72` → the actual
  numpy / gdptools work

That's 3 hops to find the compute logic. To understand what `config`
is, the persona ALSO has to read `_build_param_cfg` in the orchestrator
(`scripts/derive_zonal_params.py:74-103`) AND know that `defaults:` in
`configs/zonal/zonal_params.yml` are layered on top of the entry, AND
know that `{data_root}` placeholders get expanded by `_resolve_nested`
against the active fabric profile. That's not 3 hops, that's 5+ and a
mental model of profile flattening.

**Fix:** add a single "trace it" appendix to `docs/ARCHITECTURE.md`, or
a new `docs/ADDING_A_PARAMETER.md`, walking one parameter end-to-end
with file:line pointers and showing what `config` contains at each
step. Not a refactor — a one-page annotated trace.

### #2 — README's first 100 lines are jargon-dense

Lines 9–33 throw `pixi install` / `pixi.lock` / `pixi run --as-is` /
`--no-install --frozen` / `conda-meta` race / "deprecated fallback
`geoenv`" at a hydrologist who has never used pixi. The persona doesn't
need to know the SLURM-race rationale to clone-and-run; they need three
commands. (`README.md:17-22` is the worst — it's a 5-line tangent about
lock files and race conditions before they've run anything.)

**Fix:** demote the rationale paragraph to a "Why pixi --as-is?"
collapsible (`<details>`) section. Put `pixi install` +
`pixi shell -e dev` + a first run command in the first 15 lines.

### #3 — The `*_param_cfg` flattening is invisible magic

`_build_param_cfg` at `scripts/derive_zonal_params.py:74-103` merges
YAML `defaults:` + the param entry + 4 fabric-profile keys (`fabric`,
`expected_max_hru_id`, `id_feature`, `hru_gpkg`, `hru_layer`) into one
flat dict. The runner reads keys off it (`config["source_raster"]`,
`config["id_feature"]`) with no schema. The persona adding a new param
will not discover `id_feature` is injected by the orchestrator until
they get a `KeyError` mid-batch.

**Fix:** a 5-line comment block in `_build_param_cfg` listing the 5
origins of the config dict.

## MEDIUM pain points — slow the persona down

### #4 — `from __future__ import annotations`

…at the top of 35 files. The persona doesn't know what this does, will
assume it's load-bearing, and will copy it into new files cargo-cult-style.

### #5 — `ctx.require("perv")` key discoverability

`src/gfv2_params/depstor_builders/context.py:38-51`, used at e.g.
`carea_map.py:83-85`: builders look up upstream outputs by short string
key. The persona writing a new builder won't know what keys exist without
reading every prior `build()` return. **Fix:** add the key inventory as a
comment in `depstor_builders/__init__.py` next to `STEP_ORDER`.

### #6 — Startup heartbeat fires on import

`zonal_runners/__init__.py:39-64`: the heartbeat + GDAL exception toggle
run on `import`. The persona who imports
`from gfv2_params.zonal_runners import run_zonal_batch` in a notebook
will see `[startup pid=… host=…]` print and have no idea why. The
25-line comment block explains this clearly *for someone reading the
file* — but the persona will see the side effect first in a Jupyter cell
and confuse it for an error. **Fix:** gate the heartbeat on
`SLURM_ARRAY_TASK_ID` being set; it's only useful in SLURM anyway.

### #7 — `compute_dem_derivatives` is registered but opt-in

It's in `BUILDERS` but not in `shared_rasters.yml`'s default `steps:`
list (`src/gfv2_params/shared_rasters/__init__.py:34-37` explains, and
`build_shared_rasters.py:88-95` has a special-case path for it). A
persona who runs `--step compute_dem_derivatives` gets it; a persona who
runs nothing doesn't. It's documented in three places but never in one
obvious place.

## LOW pain points — noticeable but workaroundable

- The two `merge_rpu_by_vpu` registrations (`shared_rasters/__init__.py:44`)
  reuse one builder for two steps. The comment explains it, but the
  persona will still squint.
- `_resolve_nested` is duplicated in `scripts/derive_zonal_params.py:34-50`
  and `scripts/build_shared_rasters.py:42-60` with slightly different
  deferred-placeholder rules. Persona-impact is small (they'll never
  touch it).
- A few cryptic error messages:
  - `depstor.py:267` `"{name} transform mismatch with template"` — no
    values, no "what to check".
  - `intersect.py:28` `"intersect step '{name}' requires 'inputs: [a, b]'"`
    — doesn't say which YAML file or that `a` / `b` are short-name keys,
    not paths.
  - `wbt.py:108` `"WhiteboxTools {tool} failed (exit code {N})."` — true,
    but the message could end with "see WBT: … lines above".
- `dataclass` with `field(default_factory=dict)` in `BuildContext`
  (`depstor_builders/context.py:32`) — the persona will not recognize
  `field(default_factory=...)` syntax. Low-impact since they won't
  construct one by hand.

## Filed issues

This audit produced 6 follow-up issues:

| Lens-priority | Issue | Effort | Type |
|---|---|---|---|
| HIGH #1 | Add `docs/ADDING_A_PARAMETER.md` walkthrough tracing one param end-to-end | ~2 h | docs |
| HIGH #2 | Rewrite `README.md` §Setup to 15 lines of action + collapsible rationale | ~1 h | docs |
| HIGH #3 | Document the `config` dict's 5 origins in `_build_param_cfg` | 15 min | comment |
| MEDIUM #6 | Gate `zonal_runners` startup heartbeat on `SLURM_ARRAY_TASK_ID` | 30 min + test | **code change** |
| MEDIUM #5 | Document the `ctx.paths` key inventory next to `STEP_ORDER` | 30 min | comment |
| (Synthesis) | Add `docs/python-patterns.md` explaining the 5–10 non-obvious idioms | ~3 h | docs |

(The "synthesis" item — a single page on `from __future__ import annotations`,
the placeholder pattern, `require_config_key` vs `dict.get`, the orchestrator +
dispatch pattern, `BuildContext`, `pixi run --as-is` vs `pixi run -e dev`, and
why builders return paths instead of arrays — was contributed as a
recommendation drawn from a parallel review of a similar workflow. It
removes 30+ recurring questions per year by collecting all the
"why-is-this-here?" explanations in one place.)

Total: ~5 h of work across 6 small PRs.

## Methodology

Single autonomous-agent pass (general-purpose subagent) dispatched with a
persona-anchored rubric covering 10 dimensions:

1. Entry point (README path-to-running-code)
2. Variable / function names (domain language vs. abstract names)
3. Code shape (linear vs. indirect; file-hops to trace)
4. Magic / non-obvious mechanisms (dispatch tables, decorators, dynamic
   imports, placeholder expansion)
5. Comments and docstrings (WHY for geoscience choices)
6. Error messages (what / where / what-to-check)
7. Type hints (simple ones fine; flag complex generics)
8. Onboarding path (single-page "how to add a parameter?")
9. Scripts vs library boundary (which files are user-facing)
10. Surprises and footguns (`pixi run --as-is`, `FABRIC` env var precedence,
    `from __future__ import annotations`, the `{data_root}` placeholder
    pattern)

Sampled `README.md`, `docs/ARCHITECTURE.md`, `slurm_batch/RUNME.md`, three
builder modules (`compute_dem_derivatives.py`, `carea_map.py`,
`zonal_runners/ssflux.py`), three `__init__.py` files, two orchestrator
scripts (`derive_zonal_params.py`, `build_shared_rasters.py`), and the
shared module `depstor.py` + `config.py`. Cited file:line throughout.

**Out of scope.** SWE refactoring recommendations (Protocol classes, ABCs,
deeper generics, mypy strictness, package-level abstractions). The
audit's bar was "what trips this persona" — not "what would a senior
Python engineer want to see."

**Per the dated-snapshot convention in
[INDEX.md](../INDEX.md#section/Specs-and-plans-are-dated-snapshots):**
this document is a snapshot of the codebase on 2026-05-26. It will not be
updated after the filed issues land; the code becomes the source of truth.
