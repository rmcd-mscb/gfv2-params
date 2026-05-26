# Python patterns used in this codebase

If you're a hydrologist with 5 years of one-off Python who just landed in this
codebase and is wondering "why does this file have X at the top?" or "why is
this written this way?" — start here.

This is a tour of ~10 Python idioms you'll see on every page. Each entry
explains *why* we chose it (the rationale), not *what* it does (you already
read Python). Each links to a real example you can click into.

---

## 1. `from __future__ import annotations` at the top of most files

Almost every module under `src/` and `scripts/` begins with this single import.
The reason is type-hint ergonomics: with it on, every annotation (return
types, dataclass field types, parameter types) is stored as a string and never
evaluated at import time. That makes annotations effectively free — no
runtime overhead, no circular-import grief, and we can use forward references
(naming a class inside one of its own methods, e.g.
`def clone(self) -> RasterInfo`) without quoting them. We're on Python 3.12,
which already evaluates `X | Y` natively, so this import is more about
*consistency and forward-compatibility* than necessity. Once it's at the top
of one file in the package, having it at the top of all of them removes one
small thing to think about.

```python
# src/gfv2_params/depstor_builders/landmask.py
from __future__ import annotations

from pathlib import Path
```

See: `src/gfv2_params/depstor_builders/landmask.py:9`

---

## 2. Fabric-profile placeholder strings (`{data_root}`, `{fabric}`, `{vpu}`)

YAML configs are full of strings like
`"{data_root}/{fabric}/depstor_rasters/land_mask.tif"`. These are *not* Python
f-strings — they're plain strings that get resolved at config-load time by
`_resolve_placeholders` walking the merged config dict. This lets a single
YAML template serve every fabric (`gfv2`, `gfv2_vpu01`, `oregon`, ...): the
fabric profile in `configs/base_config.yml` supplies `data_root`, the active
`--fabric` (or `FABRIC` env var) supplies `fabric`, and per-VPU iteration
inside builders supplies `vpu` / `raster_vpu`. The result: no hardcoded paths
anywhere, and a new fabric is *only* a new profile entry — no script changes.

```python
# src/gfv2_params/config.py
for placeholder, replacement in replacements.items():
    value = value.replace(f"{{{placeholder}}}", replacement)
remaining = re.findall(r'\{(\w+)\}', value)
if remaining:
    raise ValueError(f"Unresolved placeholder(s) {remaining} in config key '{key}'. ...")
```

See: `src/gfv2_params/config.py:114-128`

---

## 3. `require_config_key(...)` vs `cfg.get(...)`

`cfg.get("twi_raster")` returns `None` silently when the key is missing —
fine for *optional* config. For *required* keys we use
`require_config_key(cfg, "twi_raster", "build_depstor_rasters")` which raises
a `KeyError` naming (a) the missing key, (b) the script that wanted it, and
(c) the fabric profile file where the user should add it. This is the
"fail loudly at the boundary" pattern: catch the misconfiguration the moment
the orchestrator first asks for the value, instead of letting `None` flow
through and surface 40 minutes later as a confusing `AttributeError` deep in
a builder. If a key is truly optional, use `.get()` — using
`require_config_key` for genuinely-optional keys is just as wrong as using
`.get()` for required ones.

```python
# src/gfv2_params/config.py
def require_config_key(config: dict, key: str, script_name: str) -> object:
    if key not in config:
        fabric = config.get("fabric", "<unknown>")
        raise KeyError(
            f"Required key '{key}' missing from merged config for "
            f"{script_name}. Expected from fabric profile '{fabric}' "
            f"in configs/base_config.yml."
        )
    return config[key]
```

See: `src/gfv2_params/config.py:86-100`. Call-site example:
`scripts/derive_zonal_params.py:101`.

---

## 4. Orchestrator + plain-dict dispatch table (no decorators, no plugins)

Each pipeline stage has a *registry* — a plain Python dict mapping step-name
strings to builder functions. There is no decorator-based registration, no
metaclass, no plugin discovery, no setuptools entry points. To find out what
steps exist, you `grep BUILDERS` or `grep BATCH_RUNNERS` and you see
everything in one place. To add a step, you add an import and an entry to the
dict. That's it. This is deliberate: the codebase favours boring,
greppable Python over clever frameworks. The orchestrator script just looks
up the function by name and calls it.

```python
# src/gfv2_params/depstor_builders/__init__.py
BUILDERS = {
    "landmask":      landmask.build,
    "imperv":        imperv.build,
    "streambuffer":  streambuffer.build,
    # ... etc.
}
```

See: `src/gfv2_params/depstor_builders/__init__.py:20-32` (the depstor
registry), `src/gfv2_params/zonal_runners/__init__.py:92-97` (the zonal
registry), and `src/gfv2_params/shared_rasters/__init__.py:38-49` (the
shared-rasters registry).

---

## 5. `BuildContext` — dataclass + `field(default_factory=dict)`

Builders pass intermediate results to each other through a single
`BuildContext` dataclass: paths to inputs (template raster, HRU geopackage,
etc.), the fabric name, the output directory, and a `paths` dict accumulating
each step's outputs. Two details worth naming. **(a)** It's a `@dataclass` so
each field is declared once with its type, and the constructor / repr come
for free. **(b)** The line `paths: dict[str, Path] = field(default_factory=dict)`
matters because writing `paths: dict[str, Path] = {}` would share *one* dict
across every instance (a classic Python gotcha for mutable defaults).
`default_factory=dict` makes each new `BuildContext` get its own fresh empty
dict.

Upstream outputs are fetched via `ctx.require("landmask")`, which raises a
clear `FileNotFoundError` if the upstream step hasn't run yet. The
orchestrator merges each builder's returned `{name: path}` dict back into
`ctx.paths` so the next step can find it.

```python
# src/gfv2_params/depstor_builders/context.py
@dataclass
class BuildContext:
    fabric: str
    template_path: Path
    output_dir: Path
    # ... more fields ...
    paths: dict[str, Path] = field(default_factory=dict)
```

See: `src/gfv2_params/depstor_builders/context.py:9-52`.

---

## 6. `pixi run --as-is` vs `pixi run -e dev`

You'll see both forms in different places. `pixi run -e dev <cmd>` activates
the `dev` environment (default deps plus pytest, ruff, pre-commit) and
checks the lockfile is up to date — what you want locally for testing and
linting. `pixi run --as-is <cmd>` is equivalent to `--no-install --frozen`:
it uses the already-installed env verbatim, no lock check, no env mutation.
This second form is **required** in SLURM batches because many array tasks
start at once, and any of them mutating `.pixi/envs/.../conda-meta` would
race with the others. Rule of thumb: `--as-is` for anything launched by
SLURM; `-e dev` for anything launched by a developer at a terminal.

```bash
# slurm_batch/derive_depstor_ratios.batch
pixi run --as-is python scripts/derive_depstor_params.py \
    --config configs/depstor/depstor_params.yml \
    --base_config "$BASE_CONFIG" \
    --fabric "$FABRIC" \
    --mode ratios
```

See: `slurm_batch/derive_depstor_ratios.batch:29` (SLURM use of `--as-is`)
and `CLAUDE.md:28` (the rationale).

---

## 7. Builders return paths, not arrays

Every `build(step_cfg, ctx, logger)` returns a `dict[str, Path]` — short
output names mapped to the GeoTIFF files those names refer to on disk. It
does **not** return a numpy array or rasterio dataset. The next builder
re-opens the file via `rasterio.open(...)` and reads only the windows it
needs. This is a deliberate streaming-intermediate-file design: CONUS-scale
FDR / TWI / Hydrodem grids are billions of cells each, do not fit in memory
end-to-end, and we need any individual step to be re-runnable in isolation
(via `--step <name>` on the orchestrator). Outputs on disk + path-passing in
the context is the only design that meets both constraints. The downside:
intermediate I/O isn't free. We accept that because the wall-clock cost of
re-reading a tiled LZW GeoTIFF is small next to fitting it in RAM.

```python
# src/gfv2_params/depstor_builders/landmask.py
def build(step_cfg: dict, ctx: BuildContext, logger) -> dict:
    output_path = ctx.resolve_output(step_cfg["output"])
    # ...
    return {"landmask": output_path}
```

See: `src/gfv2_params/depstor_builders/landmask.py:45-72`.

---

## 8. `STEP_ORDER` + `BUILDERS` decoupling (registered vs default-run)

`BUILDERS` is *what can run*. `STEP_ORDER` is the canonical DAG (the
dependency-respecting order to run them in). The orchestrator does NOT run
every step in `STEP_ORDER` blindly — it intersects `STEP_ORDER` with the
`steps:` list in the YAML config, so a step can be registered (and thus
available for `--step <name>`) without running by default. The Part 1
`compute_dem_derivatives` step is the canonical example: it's in `BUILDERS`
and `STEP_ORDER`, but is omitted from `configs/shared_rasters/shared_rasters.yml`'s
default `steps:` list because it produces an optional, parallel artifact.
The orchestrator honours the YAML's `steps:` selection, with `STEP_ORDER`
controlling sequence among whatever ran.

```python
# src/gfv2_params/shared_rasters/__init__.py
BUILDERS: dict = {
    # ...
    "compute_dem_derivatives": compute_dem_derivatives.build,
    # ...
}

STEP_ORDER: list[str] = [
    # ...
    "compute_dem_derivatives",  # optional / parallel
    # ...
]
```

See: `src/gfv2_params/shared_rasters/__init__.py:38-62` (registry +
ordering), and the comment block at `__init__.py:31-37` for the rationale.

---

## 9. `logger.info("WBT: %s", line)` — lazy log formatting

`logger.info("WBT: %s", line)` and `logger.info("WBT: " + line)` look
equivalent. They aren't. The first form passes the format string and the
argument *separately* to the logging library; if the log level is filtered
out (e.g. WARNING-only in production), the format never runs and the
substitution never happens. The second form builds the string
unconditionally before logging gets a chance to drop it. For
once-per-pipeline messages this doesn't matter; for the WBT subprocess
streamer that fires *per output line* of a multi-hour CONUS-scale
WhiteboxTools run, it adds up. So: prefer `logger.info("text: %s", x)` over
`logger.info(f"text: {x}")` in hot paths. (Outside hot paths f-strings are
fine and we use them happily.)

```python
# src/gfv2_params/wbt.py
for line in proc.stdout:
    stripped = line.rstrip()
    tail.append(stripped)
    logger.info("  WBT: %s", stripped)
```

See: `src/gfv2_params/wbt.py:93-96`.

---

## 10. `Path | None` vs `Optional[Path]` (PEP 604)

You'll see both in this codebase. `Optional[Path]` means "Path or None" via
`typing.Optional`. `Path | None` means the same thing via PEP 604 syntax,
which Python 3.10+ supports natively. They are interchangeable; we prefer
`|` for brevity and because it composes more cleanly with longer unions
(`int | float | str` is much nicer than
`Union[int, Union[float, str]]`). New code should use `|`. The handful of
`Optional[...]` sites remaining in older modules (e.g. `depstor.py`) are
fine — don't churn them just to rename.

```python
# src/gfv2_params/config.py
def load_base_config(
    base_config_path: Path | None = None,
    fabric: str | None = None,
) -> dict:
```

See: `src/gfv2_params/config.py:37-38` (modern `|` form) and
`src/gfv2_params/depstor.py:35` (the older `Optional[float]` form, still
present and still valid).

---

## Where to go next

- `docs/ARCHITECTURE.md` — the canonical map of the codebase
  (data-root layout, Part 1/2 split, the orchestrator + builder pattern).
- `CLAUDE.md` — project rules (atomic commits, doc-audit-before-merge,
  no-pytest-on-head-node).
- `slurm_batch/RUNME.md` — step-by-step HPC workflow walkthrough.
