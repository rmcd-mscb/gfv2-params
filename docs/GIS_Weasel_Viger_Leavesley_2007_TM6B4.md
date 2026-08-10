# The GIS Weasel User's Manual (Viger & Leavesley, 2007) — TM 6-B4

Local copy: [`tm6b4.pdf`](tm6b4.pdf) (16.3 MB, 201 p.)

**Citation.** Viger, R.J., and Leavesley, G.H., 2007, Section 4 — The GIS Weasel
user's manual: U.S. Geological Survey Techniques and Methods, book 6, chap. B4,
201 p., <https://doi.org/10.3133/tm6B4>.
Landing page: <https://pubs.usgs.gov/tm/2007/06B04>

This document is cited by TM 6-B9 and by the Geospatial Fabric attribute-table
metadata as the source of per-parameter derivation methods. This page records
**what it actually contains**, because that turns out to differ from what those
citations promise.

---

## ⚠️ It does NOT document the Gleeson subsurface-flux parameters

The FGDC record for *Geospatial Fabric Attribute Tables for PRMS Subsurface Flux
Parameters based on Gleeson* ([doi:10.5066/F7CN71XR](https://doi.org/10.5066/F7CN71XR))
states:

> "The methodologies used to derive the individual attributes can be located in
> the Appendix of the GIS Weasel Users Manual by the name of the attribute,
> which is the same as the name of the corresponding PRMS parameter, in
> (Viger and Leavesley, 2007)."

**That pointer does not hold for the flux table.** Full-text search of TM 6-B4
returns **zero** occurrences of:

`soil2gw_max` · `ssr2gw_rate` · `slowcoef_lin` · `fastcoef_lin` ·
`gwflow_coef` · `k_perm` · `Gleeson` · `conductivity`

The reason is chronological and decisive: **Gleeson and others (2011) postdates
this 2007 manual by four years.** A 2007 document cannot describe a derivation
based on a 2011 dataset.

**Consequence for [issue #175](https://github.com/rmcd-mscb/gfv2-params/issues/175):**
the definitive statement of whether TM 6-B9's "*k_perm* cubed" means the log10
value or the linear permeability **is not in this document, and is not in any
reference TM 6-B9 cites.** Do not re-run this search. The best available
evidence is:

1. the FGDC attribute definitions in `GFAttsNhruPrms_subsurface-Gleeson.xml`
   (which establish "feature **average** hydraulic conductivity" and cube
   placement on `soil2gw_max` only), and
2. the reference implementation's actual per-region output, downloadable as
   `GeospatialFabricAttributes-PRMS_Gleeson_{01..21}.zip` from the same
   ScienceBase item — the only remaining way to settle the question, and then
   only distributionally.

See [`superpowers/specs/2026-08-10-ssflux-normalisation-design.md`](superpowers/specs/2026-08-10-ssflux-normalisation-design.md).

## What it does contain

The manual documents the **GIS Weasel**, an ArcInfo Workstation (8.0.2+) / GRID
application for preparing spatial information for lumped and distributed
hydrologic models. Body chapters cover the GUI workflow — DEM filling, flow
direction/accumulation, AOI delineation, drainage extraction, zone-map creation.
Much of this is obsolete as software instruction; the value is historical, in
recording how the Geospatial Fabric's geometric and topographic attributes were
originally produced.

**Appendix: Parameterization Methods** (p. 86 ff.) documents **37** methods, each
as `param_<name>.aml` with a standard entry format:

> DEFINITION · ORIGINAL PURPOSE · EXTRA DATA USED · SUBROUTINES CALLED ·
> DESCRIPTION · REFERENCES

The methods are generic zone-summary operations, not parameter-specific
derivations:

| group | methods |
|---|---|
| geometry | `param_area` `param_perimeter` `param_slope` `param_flowlength` `param_dist2headwater` |
| statistics | `param_max` `param_min` `param_range` `param_sum` `param_majority` `param_generic` |
| routing | `param_traveltime` `param_velocity` `param_nac` `param_nchan` `param_ntopchan` |
| land cover | `param_imperv` |
| feature counts | `param_nhru` `param_nssr` `param_nradpl` `param_nmru` `param_nshed` `param_nlink` `param_nreach` `param_ns` `param_nsc` `param_ngw` `param_ngwrow` `param_ngwcol` `param_nflowplane` `param_ndanode` `param_ndabranch` `param_ndajunction` `param_daf_pct_area` |
| misc | `param_id` `param_method1` `param_method2` |

Note the appendix explicitly declines to document the generic family
individually:

> "Despite the fact that all these routines have different names, the routines
> are computationally identical. The variations are limited only to the choice
> of summary statistic. ... Therefore, these routines will not be individually
> documented."

**There is no soils, land-cover-crosswalk, permeability, or subsurface-flux
method here.** For those, TM 6-B9 and the per-dataset ScienceBase FGDC records
are the authoritative sources.

## Relevance to this repo

Low-to-moderate, and mostly as provenance. `param_area`, `param_slope` and
`param_imperv` are conceptual ancestors of our zonal steps, but our own
derivations are documented far more specifically in
[`ARCHITECTURE.md`](ARCHITECTURE.md), [`parameter_index.md`](parameter_index.md)
and the `prms:` blocks in `configs/zonal/zonal_params.yml`. Keep this PDF as the
canonical citation target, not as a working reference.

## Reproducing the text extraction

No poppler or Python PDF library is installed in the pixi envs. Ghostscript
(`/usr/bin/gs`) is available and sufficient:

```bash
gs -q -dNOPAUSE -dBATCH -dSAFER -sDEVICE=txtwrite -o tm6b4.txt docs/tm6b4.pdf
```
