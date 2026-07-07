# pyWatershed depression-storage requirements — gap analysis

**Purpose.** Confirm that this pipeline's parameter product
(`gfv2/params/merged/*.csv`) covers the parameter set that pyWatershed's
runoff/depression-storage module needs, and document the intended source for
the parameters it needs that we don't yet emit. This is analysis only — no
code, no new builder, no params emitted here.

**Verified against:** pyWatershed **2.0.4**, installed in the isolated
`reference` pixi environment (`pixi run -e reference ...`; python 3.10). The
module in question is `pywatershed.hydrology.prms_runoff.PRMSRunoff`. The
parameter set was read from `PRMSRunoff.get_parameters()`; defaults/units were
read from `pywatershed.meta.find_variables(...)`. See the verification
command output at the end of this file for the raw, freshly-printed values —
they match this plan's expected table exactly, with no discrepancies from a
prior transcript.

## Bucket 1 — spatial params, we produce

These vary per-HRU and come from zonal statistics against CONUS source
rasters; the depstor pipeline computes and emits all six.

| Param | pyWatershed default | units | Our product |
|---|---|---|---|
| `dprst_frac` | 0.0 | decimal fraction | `gfv2/params/merged/nhm_dprst_frac_params.csv` |
| `sro_to_dprst_perv` | 0.2 | decimal fraction | `nhm_sro_to_dprst_perv_params.csv` |
| `sro_to_dprst_imperv` | 0.2 | decimal fraction | `nhm_sro_to_dprst_imperv_params.csv` |
| `carea_max` | 0.6 | decimal fraction | `nhm_carea_max_params.csv` |
| `smidx_coef` | 0.005 | decimal fraction | `nhm_smidx_coef_params.csv` |
| `hru_percent_imperv` | 0.0 | decimal fraction | `nhm_hru_percent_imperv_params.csv` |

**CSV schema:** each file is two columns, `nat_hru_id,<param>`; the join key
across all `merged/` outputs is `nat_hru_id`. These six were rebuilt **Jul 5
2026** from the fully-grounded classifier (the dprst/on-stream union +
same-HRU-restricted `sro_to_dprst_*`; see `docs/ARCHITECTURE.md` and the
project's dprst-classifier history for provenance).

## Bucket 2 — constant defaults, legacy `0b` emitted

These are scalar (non-spatial) parameters. The legacy `0b_TB_depr_stor.py`
pipeline emitted them as fixed constants; pyWatershed's built-in defaults
match in all eight cases.

| Param | legacy `0b` value | pyWatershed default | units | note |
|---|---|---|---|---|
| `dprst_depth_avg` | 132 | 132.0 | inches | matches |
| `dprst_et_coef` | 1 | 1.0 | decimal fraction | matches |
| `dprst_frac_init` | 0.5 | 0.5 | decimal fraction | matches |
| `dprst_frac_open` | 1 | 1.0 | decimal fraction | matches |
| `imperv_stor_max` | 0.05 | 0.05 | inches | matches |
| `op_flow_thres` | 1 | 1.0 | decimal fraction | matches |
| `va_clos_exp` | 0.001 | 0.001 | none | matches |
| `va_open_exp` | 0.001 | 0.001 | none | matches |

(Units above are the freshly-printed `pywatershed.meta` strings, e.g.
"decimal fraction" rather than the shorthand "fraction" used in earlier
notes — no value discrepancy, just the verbatim metadata wording.)

## Bucket 3 — gaps: pyWatershed needs, legacy `0b` never emitted

| Param | pyWatershed default | units | recommendation |
|---|---|---|---|
| `dprst_flow_coef` | 0.05 | fraction/day | adopt pyWatershed default (a priori, no spatial basis) |
| `dprst_seep_rate_open` | 0.02 | fraction/day | adopt pyWatershed default |
| `smidx_exp` | 0.3 | 1.0/inch | adopt pyWatershed default |

## Naming + value discrepancies to flag explicitly

- **Rename:** legacy `dprst_seep_rate_close` → pyWatershed
  `dprst_seep_rate_clos` (drop the trailing `e`). pyWatershed splits seepage
  into `_open` and `_clos` (closed) variants; the legacy pipeline only carried
  a single closed-depression seepage constant under the old name.
- **Value discrepancy (call out prominently):** legacy `0b`
  `dprst_seep_rate_close` = **0.2**, but pyWatershed's own
  `dprst_seep_rate_clos` default = **0.02** — a **10x** difference. This is
  not a units mismatch; it changes closed-depression seepage rate by an order
  of magnitude. Recommendation: **adopt the pyWatershed default (0.02) unless
  the NHM paramdb specifies otherwise**, and treat this as needing a
  modeler's sign-off before either value is committed to a production param
  set.
- **`hru_area`** is a fabric-geometry input (per-HRU area, pyWatershed default
  1.0 acre used only as a metadata placeholder), not produced by legacy `0b`
  or by this pipeline's depstor stage. It is supplied by the watershed fabric
  itself (the HRU polygon geometry), consistent with this project's existing
  convention that geometry-derived quantities come from the fabric profile,
  not from a zonal-stats builder.

## Verdict

`gfv2/params/merged/` supplies all **6** spatial parameters that
pyWatershed's `PRMSRunoff` requires (bucket 1), each as a
`nat_hru_id,<param>` CSV. The remaining **11** non-spatial parameters
`PRMSRunoff` requires are a priori constants, not zonal-stats products:

- **8 already match** the legacy `0b` constants exactly (bucket 2) — no
  action needed, carry them forward as-is.
- **3 are gaps** the legacy pipeline never emitted (bucket 3) — action:
  adopt the pyWatershed defaults verbatim (`dprst_flow_coef=0.05`,
  `dprst_seep_rate_open=0.02`, `smidx_exp=0.3`); no spatial basis exists for
  any of them, so there is nothing further to derive.
- **1 needs a decision, not just adoption:** `dprst_seep_rate_clos` — action:
  get modeler sign-off on 0.02 (pyWatershed default) vs. 0.2 (legacy
  `dprst_seep_rate_close`) before it is written into any production param
  set; document whichever is chosen and why.

No code is produced by this document; it is a requirements/gap record for
Task 4's presentation and for whoever implements the constant-param emission
step.

## Verification command and output (evidence)

```bash
pixi run -e reference python -c "
import pywatershed as pws
from pywatershed.hydrology.prms_runoff import PRMSRunoff
params = set(PRMSRunoff.get_parameters())
print('pywatershed version:', pws.__version__)
for p in ['dprst_flow_coef','dprst_seep_rate_open','dprst_seep_rate_clos','smidx_exp',
          'dprst_depth_avg','dprst_et_coef','dprst_frac_init','dprst_frac_open',
          'imperv_stor_max','op_flow_thres','va_clos_exp','va_open_exp',
          'dprst_frac','sro_to_dprst_perv','sro_to_dprst_imperv','carea_max',
          'smidx_coef','hru_percent_imperv','hru_area']:
    m = pws.meta.find_variables([p]).get(p, {})
    print(f'{p:22s} in_runoff={p in params} default={m.get(\"default\")!r} units={m.get(\"units\")!r}')
"
```

```
pywatershed version: 2.0.4
dprst_flow_coef        in_runoff=True default=0.05 units='fraction/day'
dprst_seep_rate_open   in_runoff=True default=0.02 units='fraction/day'
dprst_seep_rate_clos   in_runoff=True default=0.02 units='fraction/day'
smidx_exp              in_runoff=True default=0.3 units='1.0/inch'
dprst_depth_avg        in_runoff=True default=132.0 units='inches'
dprst_et_coef          in_runoff=True default=1.0 units='decimal fraction'
dprst_frac_init        in_runoff=True default=0.5 units='decimal fraction'
dprst_frac_open        in_runoff=True default=1.0 units='decimal fraction'
imperv_stor_max        in_runoff=True default=0.05 units='inches'
op_flow_thres          in_runoff=True default=1.0 units='decimal fraction'
va_clos_exp            in_runoff=True default=0.001 units='none'
va_open_exp            in_runoff=True default=0.001 units='none'
dprst_frac             in_runoff=True default=0.0 units='decimal fraction'
sro_to_dprst_perv      in_runoff=True default=0.2 units='decimal fraction'
sro_to_dprst_imperv    in_runoff=True default=0.2 units='decimal fraction'
carea_max              in_runoff=True default=0.6 units='decimal fraction'
smidx_coef             in_runoff=True default=0.005 units='decimal fraction'
hru_percent_imperv     in_runoff=True default=0.0 units='decimal fraction'
hru_area               in_runoff=True default=1.0 units='acres'
```

(Stderr lines `prms_channel_flow_graph jit compiling with numba` and `Missing
optional dependency 'mpsplines'` are harmless and unrelated to this
verification.)
