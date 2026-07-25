# Test fixtures

## `segment_wbody_oregon_fixture.gpkg`

Real oregon geometry for `tests/test_segment_wbody_fixture.py`, which pins the
positive-length on-stream rule against the layers it actually runs on.

Layers: `nsegment` (every segment intersecting one of the COMIDs below) and
`waterbodies` (`COMID`, `FTYPE`, `member_comid`).

Cut from, on the HPC:
- segments: `{data_root}/oregon/fabric/model_layers 9.gpkg`, layer `nsegment`
- waterbodies: `{data_root}/input/nhd/nhd_waterbodies.gpkg`, layer `waterbodies`

COMIDs, chosen to cover every branch of the rule:

| COMID | role |
| --- | --- |
| 24083423, 24032328, 24051153, 24067917 | LakePond, zero-length shoreline graze — dropped |
| 24079145, 24079105 | Playa, zero-length graze — dropped |
| 23794331 | crossed by many segments; largest overlap in the fabric |
| 120052284, 120055365 | clean through-flow |
| 24032198 | Playa KEPT by geometry (the FTYPE guard in `wbody_connectivity` drops it) |
| 120050246 | Ice Mass KEPT by geometry (same) |

Regenerate with the snippet in Task 5/Step 1 of
`docs/superpowers/plans/2026-07-24-segment-driven-onstream-classifier.md`:

```bash
srun -p cpu -A impd --mem=16G --time=00:30:00 pixi run --as-is python - <<'PY'
from pathlib import Path
import geopandas as gpd, pyogrio, pandas as pd
root = Path('/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2')
out = Path('tests/data'); out.mkdir(parents=True, exist_ok=True)

COMIDS = [24083423, 24032328, 24051153, 24067917, 24079145, 24079105,
          23794331, 120052284, 120055365, 24032198, 120050246]

seg = gpd.read_file(root/'oregon/fabric/model_layers 9.gpkg', layer='nsegment')
wb = pyogrio.read_dataframe(
    root/'input/nhd/nhd_waterbodies.gpkg', layer='waterbodies',
    bbox=tuple(seg.total_bounds),
    columns=['COMID', 'FTYPE', 'member_comid'], use_arrow=True)
wb['COMID'] = pd.to_numeric(wb['COMID'], errors='coerce').astype('Int64')
wb_f = wb[wb.COMID.isin(COMIDS)].reset_index(drop=True)
hit = gpd.sjoin(seg, wb_f[['geometry']], how='inner', predicate='intersects')
seg_f = seg.loc[sorted(set(hit.index)), ['segment_id', 'geometry']].reset_index(drop=True)

fx = out/'segment_wbody_oregon_fixture.gpkg'
if fx.exists():
    fx.unlink()
seg_f.to_file(fx, layer='nsegment', driver='GPKG')
wb_f.to_file(fx, layer='waterbodies', driver='GPKG')

import shutil
shutil.copy(root/'input/staging/OR_waterbody_nseg_intersection.csv',
            out/'OR_waterbody_nseg_intersection.csv')
PY
```

Last cut 2026-07-25: 73 segments, 11 waterbody rows, 11 COMIDs, 380 KB. Derived
COMID sets matched the design spec's expected values exactly:
- positive-length: `[23794331, 24032198, 120050246, 120052284, 120055365]`
- graze-only: `[24032328, 24051153, 24067917, 24079105, 24079145, 24083423]`

## `OR_waterbody_nseg_intersection.csv`

The handoff crosswalk that motivated the segment-driven classifier: 1,629 rows, 783
distinct `wb_comid`, 1,353 distinct `nseg_id` (= the oregon `nsegment` layer's
`segment_id`, **not** `nhm_seg_id`).

Reproduced exactly by a bare
`gpd.sjoin(nsegment, waterbodies, predicate="intersects")` against the now-retired
`input/nhd/conus_waterbodies.gpkg`.

It is **provenance, not a reproduction target**: production reads
`nhd_waterbodies.gpkg`, where the same join gives 1,453 pairs (1,451 shared, 2 new, 178
CSV-only) and 776 COMIDs (7 CSV COMIDs absent). See
`docs/superpowers/specs/2026-07-24-segment-driven-onstream-classifier-design.md`.
