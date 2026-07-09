# Surface-Depression Storage and Runoff in the NHM — technical extract (Driscoll et al., 2020)

**Jessica M. Driscoll, Lauren E. Hay, Melanie K. Vanderhoof, and Roland J. Viger**

*Journal of the American Water Resources Association (JAWRA)* 56(1):16–29, February 2020.
DOI: [10.1111/1752-1688.12826](https://doi.org/10.1111/1752-1688.12826)

> **What this file is.** A *technical reference extract*, not a full conversion.
> The source PDF (`docs/J American Water Resour Assoc - 2020 - Driscoll - ...pdf`)
> is the Wiley-hosted version of the article; a verbatim full-text reproduction is
> blocked by content filtering, so this file records the model-relevant facts,
> parameter values, equations, and method — the parts this repo needs — in
> condensed form, with short attributed quotes where exact wording matters.
> The underlying article is a U.S. Government work in the public domain in the USA.
> Figures are described, not reproduced. Read the PDF for the full narrative,
> discussion, and literature cited.
>
> Companion: [`NHM_description_Regan_2018_TM6B9.md`](NHM_description_Regan_2018_TM6B9.md),
> which is a full conversion and is the authoritative parameter-derivation source.

---

## Why this paper matters to this repo

TM 6-B9 (Regan and others, 2018), appendix 1, states that five DPRST parameters
were set by **a calibration procedure by HRU**, using Monthly Water Balance Model
(MWBM) runoff as a proxy for change in depression storage. **This paper is the
description of that calibration.** It is therefore the primary source for what the
NHM production values of those five parameters actually *are* — as distinct from
the PRMS code defaults.

The load-bearing consequence: **`dprst_depth_avg`, `dprst_et_coef`,
`dprst_flow_coef`, `dprst_seep_rate_open`, and `op_flow_thres` are spatially
distributed calibrated values in the NHM, not constants.** Each HRU is assigned a
unique value.

---

## Scope and scale

- NHM Geospatial Fabric: **109,951 HRUs** over the CONUS, derived from NHDPlus V1.
- HRU size range 1 km² to >67,000 km², mean 74 km².
- **53,007 HRUs (48.2%) have surface-depression storage**; 56,944 have none and
  were excluded from the analysis.
- NHM-PRMS run 1980-01-01 → 2010-12-31; first 10 years spin-up; 1990–2010 (20 yr)
  used for calibration and analysis.
- Climate forcing: Daymet v2 (Thornton and others, 2016), area-weighted to HRUs
  via the USGS Geo Data Portal.

---

## How surface-depression storage is defined and parameterized

### Conceptual model — "merged, fill, and spill"

All independent depressions within an HRU are merged into a **single, lumped
depression-storage area** per HRU. Storage fills until a specified capacity is
reached, then the excess volume spills. No individual depressions are modeled.

Water **enters** aggregated DPRST by (1) direct precipitation and (2) overland
flow captured by the calculated contributing area within the HRU.
Water **leaves** by (1) evaporation, (2) seepage to groundwater recharge, and
(3) spillage — the sum of (a) interflow from storage above a threshold and
(b) overland flow when storage reaches the maximum volume.

> "In the current model representation, water cannot enter the aggregated surface
> depression through interflow or groundwater flow processes, though there is
> consideration for adding this capability to the code."

### Which water bodies count as depression storage

Surface-depression storage area is the aggregate sum of NHDPlus water body
features within HRUs that are **neither on-stream nor located where land cover
data indicate impervious surfaces exist**.

> "On-stream water bodies, such as lakes and reservoirs that are within or
> intersect with a 60-m buffer of NHM stream segments, are considered
> flow-through features and are not included in surface-depression storage."

The analysis relied on a **30-m DEM associated with NHDPlus**.

**Note for this repo:** the 60-m stream-segment buffer is the *legacy* on-stream
test. This repo deliberately superseded it with an NHD-topology classifier
(WBAREACOMI artificial-path topology ∪ geometric flow-through topology, both
gated on Network-Flowline membership). See `CLAUDE.md` and issues #145/#161.
The buffer test is documented here for provenance only — **do not reintroduce it.**

### Geometry: why `va_open_exp` / `va_clos_exp` = 0.001

> "The shape of the bottom of the surface depressions was specified to approximate
> a rectangular basin, though additional shapes could be specified... This
> consistent representation was chosen because there were not sufficient data to
> spatially distribute various shape parameters for each surface depression for
> the CONUS."

In TM 6-B9's Table 1–1, `va_open_exp` = `va_clos_exp` = 0.001 is annotated
"0.001 is an approximate rectangle; 1.0 is a triangle." The two statements agree.

The paper flags this as a known limitation: specifying bowl or triangular
geometry "would change ET by more accurately representing the portion of a given
volume of water is exposed to open-water evaporation," which "may then allow for
a more accurate representation of parameter `dprst_et_coef`."

### Open vs. closed

> "For this study all aggregated surface-depression storage within an HRU was
> specified to be open."

i.e. `dprst_frac_open` = 1.0 throughout, so the closed fraction is 0. This matches
TM 6-B9's narrative ("For the NHM the values of `dprst_frac_open` are set to the
default, 1.0"). The paper lists allowing "closed" depressions as future work.

### `dprst_frac` and `sro_to_dprst`

- `dprst_frac` — "The maximum fractional proportion of HRU area covered by surface
  depressions." Specified as a decimal fraction of HRU area "for convenience in
  calibration procedures."
- `sro_to_dprst` — "The contributing area to the aggregated surface-depression
  storage is a separate parameter (`sro_to_dprst`) that is calculated using the
  underlying DEM for all NHM parameters."

Both are *derived*, not calibrated — they are not in the calibration set below.

---

## The calibration (this is the key section)

### Parameter selection

A **Fourier Amplitude Sensitivity Test (FAST)** sensitivity analysis identified the
DPRST parameters with the most influence on surface-depression volume. Five were
selected for calibration.

### Method

- Algorithm: **Shuffled Complex Evolution (SCE)** global search (Duan and others,
  1992, 1993, 1994).
- Target: normalized (0–1) NHM-MWBM runoff, used as a **proxy for change in
  surface-depression storage**, compared against NHM-PRMS simulated fractional
  (0–1) depression storage (`dprst_vol_frac`) within each HRU.
- Two extra scaling factors, `scale_lower` and `scale_upper`, set the lower and
  upper bounds of the MWBM runoff normalization, so that a normalized value of 0
  (or 1) "could encompass more than just the very lowest (or highest) MWBM runoff
  values."

Objective function (Equation 1):

```
OF = (wgt1 × NRMSE) + (wgt2 × (1.0 − RS)) + (wgt3 × (1.0 − RSann))
```

where
- `NRMSE` = normalized root mean square error between normalized NHM-MWBM runoff
  and NHM-PRMS fractional depression storage, **monthly**;
- `RS` = Spearman's rank correlation of the same two series, **monthly**;
- `RSann` = Spearman's rank correlation of the same two series, **annual**;
- weights `wgt1 = 1.0`, `wgt2 = 0.5`, `wgt3 = 0.25`, "determined through trial and
  error."

### Calibrated parameter values (Table 1)

Summary statistics across the 53,007 HRUs with depression storage.

> ⚠️ **The published table transposes its first two column headers.** It prints
> "Maximum" then "Minimum", but every row's first value is the smaller one
> (e.g. `dprst_depth_avg` "Maximum 10, Minimum 300"). Verified against a 130-dpi
> render of the typeset table on p. 21. The columns below are relabeled to the
> values as they actually are; the ranges are unambiguous either way.

| PRMS term | Description | Min | Max | Median | Mean | StdDev |
|---|---|---|---|---|---|---|
| `dprst_depth_avg` | Average depth of storage depressions at maximum storage capacity | 10 | 300 | **49** | 80.4 | 79 |
| `dprst_et_coef` | Fraction of unsatisfied PET to apply to surface-depression storage | 0.75 | 1.25 | **1.04** | 1.02 | 0.2 |
| `dprst_flow_coef` | Coefficient in linear interflow equation for open surface depressions | 0.0001 | 0.1 | **0.048** | 0.049 | 0.029 |
| `dprst_seep_rate_open` | Coefficient used in linear seepage flow equation for open surface depressions | 0.00001 | 0.2 | **0.033** | 0.071 | 0.069 |
| `op_flow_thres` | Fraction of open depression storage above which interflow occurs | 0.75 | 1 | **0.83** | 0.85 | 0.07 |
| `scale_lower` | Scaling factor for MWBM lower normalization limit | 0.01 | 0.42 | 0.02 | 0.03 | 0.02 |
| `scale_upper` | Scaling factor for MWBM upper normalization limit | 0.51 | 1 | 0.91 | 0.87 | 0.14 |

`dprst_depth_avg` is in inches (TM 6-B9 Table 1–1). `scale_lower`/`scale_upper` are
calibration artifacts, not PRMS parameters.

**`dprst_seep_rate_clos` is not calibrated.** Per TM 6-B9: "Parameter
`dprst_seep_rate_clos` was set to values of `dprst_seep_rate_open`." Since all
NHM depressions are open (`dprst_frac_open` = 1.0), it is inert in the NHM
application but must still be supplied.

**Unit inconsistency to be aware of:** the Figure 6 caption gives
surface-depression seepage rate "in cm/day", whereas PRMS/pyWatershed define
`dprst_seep_rate_open` as `fraction/day`. Table 1's values (max 0.2) are consistent
with `fraction/day`. Treat the figure caption as an error.

---

## Analysis and results (context, not parameter-bearing)

Daily NHM-PRMS fractional depression storage was aggregated and correlated against
normalized NHM-MWBM runoff at monthly (n = 240) and annual (n = 20) time steps for
each of the 53,007 HRUs, using Spearman's rank correlation.

Categories: inversely correlated (−1.0 to −0.2); no correlation (>−0.2 to 0.2);
correlated (>0.2 to 0.75); highly correlated (>0.75 to 1.0).

**Table 2 — HRU counts by category**

| Category | Spearman's rank range | Monthly n | Monthly % | Annual n | Annual % |
|---|---|---|---|---|---|
| Highly correlated | >0.75 to 1.0 | 28,279 | 53.35 | 41,655 | 78.58 |
| Correlated | >0.2 to 0.75 | 23,825 | 44.95 | 11,016 | 20.78 |
| No correlation | >−0.2 to 0.2 | 881 | 1.66 | 304 | 0.57 |
| Inversely correlated | >−1.0 to −0.2 | 22 | 0.04 | 32 | 0.06 |

Key qualitative findings:

- Annual correlation is more extensive than monthly across the CONUS —
  interannual variability dominates for most of the country.
- Monthly correlation exceeds annual in areas of seasonal, short-term "dynamic"
  storage: the desert southwest, higher-elevation montane regions, and — by
  ecoregion — Cascades, Coast Range, Lake Agassiz Plain, Northern Glaciated
  Plains, and Southern Florida Plain. These five are "more likely to be good
  candidates for the proxy calibration strategy developed in the Prairie Pothole
  Region (Hay and others, 2018)."
- Of the geographically distributed variables tested (depression-storage density,
  number of depressions, depression depth), **only seepage rate and the ET
  coefficient** showed any difference in distribution across Spearman's rank
  categories.
- Higher seepage rates associate with inverse correlation; seepage rate decreases
  as storage–runoff correlation increases. ET coefficient increases with
  increasing correlation.
- HRUs with inverse correlation are sparse, in the deserts of California, Nevada,
  and Utah. No-correlation HRUs are in the basin-and-range west and eastern
  New York.

Stated limitations / future work: refined spatial representation (depression
geometry, dynamic surface-water extent), characterization allowing both open and
closed depressions, and groundwater contribution *into* depression storage.

---

## Figures

*[Figure 1. Schematic of the NHM infrastructure and components: physical models (daily PRMS, monthly MWBM), geospatial fabric, and model input data.]*

*[Figure 2. Four-panel schematic: (a) hydrologic processes and connections within PRMS, with the surface-depression storage process boxed in dashed red; (b) processes within the MWBM; (c) the surface-depression storage process within PRMS, modified from Regan and LaFontaine (2017); (d) conceptual map modified from Hay and others (2018) showing HRUs (grey lines), surface-water depression storage (magenta), contributing area to surface-water depressions (light blue), and an outlet streamgage.]*

*[Figure 3. Schematic of water movement into (blue arrows) and out of (red arrows) surface depressions in PRMS.]*

*[Figure 4. CONUS choropleth map of fractional surface-depression storage (`dprst_frac`) at HRU resolution, color ramp 0.0–0.3+.]*

*[Figure 5. Boxplot of the distribution of Spearman's rank coefficient between NHM-PRMS fractional depression storage and normalized NHM-MWBM runoff for each of the 53,007 HRUs, with the four categories bracketed on the right axis.]*

*[Figure 6. Boxplots by Spearman's rank category, monthly (left) and annual (right), for surface-depression seepage rate (upper) and surface-depression ET coefficient (lower). Caption states seepage units as cm/day — see unit note above.]*

*[Figure 7. CONUS choropleth of Spearman's rank correlation between monthly runoff and surface-depression storage.]*

*[Figure 8. CONUS choropleth of Spearman's rank correlation between annual surface-depression storage and runoff.]*

*[Figure 9. CONUS choropleth of the difference between monthly (Fig. 7) and annual (Fig. 8) Spearman's rank correlation categories; blue = annual higher, red = monthly higher, darker = more than one category of difference.]*

*[Figure 10. Ranges of Spearman's rank correlation values for annual (blue) and monthly (red) storage–runoff correlation for all HRUs within the 86 Level III ecoregions of the CONUS, alphabetical left to right, with the five monthly-dominant ecoregions labeled and mapped.]*

---

## Key references for this repo

- Regan, R.S., Markstrom, S.L., Hay, L.E., Viger, R.J., Norton, P.A., Driscoll, J.M., and LaFontaine, J.H., 2018, *Description of the National Hydrologic Model for use with the Precipitation-Runoff Modeling System (PRMS)*: USGS Techniques and Methods 6-B9. https://doi.org/10.3133/tm6B9 — **the parameter-derivation authority**; converted in full at [`NHM_description_Regan_2018_TM6B9.md`](NHM_description_Regan_2018_TM6B9.md).
- Regan, R.S., and LaFontaine, J.H., 2017, *Documentation of the Dynamic Parameter, Water-Use, Stream and Lake Flow Routing, and Two Summary Output Modules and Updates to Surface-Depression Storage Simulation and Initial Conditions Specification Options with PRMS*: USGS Techniques and Methods 6-B8. — the DPRST simulation code documentation.
- Markstrom, S.L., Regan, R.S., Hay, L.E., Viger, R.J., Webb, R.M., Payn, R.A., and LaFontaine, J.H., 2015, *PRMS-IV, the Precipitation-Runoff Modeling System, Version 4*: USGS Techniques and Methods 6-B7. — source of the `smidx` contributing-area equation (eq. 1-98) that TM 6-B9 eq. 1-1 inverts.
- Viger, R.J., Hay, L.E., Jones, J.W., and Buell, G.R., 2010, *Effects of Including Surface Depressions in the Application of PRMS in the Upper Flint River Basin, Georgia*: USGS SIR 2010-5062. — original DPRST parameter derivation, superseded for `sro_to_dprst_*` by TM 6-B9.
- Hay, L.E., Norton, P.A., Viger, R.J., Markstrom, S.L., Regan, R.S., and Vanderhoof, M.K., 2018, "Calibration of PRMS in the Prairie Pothole Region of North Dakota": *Hydrological Processes*. https://doi.org/10.1002/hyp.11416 — the proxy-calibration strategy this paper generalizes to CONUS.
- Bock, A.R., Hay, L.E., McCabe, G.J., Markstrom, S.L., and Atkinson, R.D., 2016, "Parameter Regionalization of a Monthly Water Balance Model for the Conterminous United States": *HESS* 20:2861–2876. — the MWBM runoff used as the calibration proxy.
