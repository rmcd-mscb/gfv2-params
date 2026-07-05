# Spatiotemporal Variability of Snow Depletion Curves Derived from SNODAS for the Conterminous United States, 2004–2013

**Jessica M. Driscoll, Lauren E. Hay, and Andrew R. Bock**

*Journal of the American Water Resources Association (JAWRA)* 53(3):655–666, June 2017.
DOI: [10.1111/1752-1688.12520](https://doi.org/10.1111/1752-1688.12520)

> Converted from `docs/Snow Depletion Curves.pdf` for research reference. Figures
> are described but not reproduced. This is a U.S. Government work in the public
> domain in the USA.

---

## Abstract

Assessment of water resources at a national scale is critical for understanding
their vulnerability to future change in policy and climate. Representation of the
spatiotemporal variability in snowmelt processes in continental-scale hydrologic
models is critical for assessment of water-resource response to continued climate
change. Continental-extent hydrologic models such as the U.S. Geological Survey
National Hydrologic Model (NHM) represent snowmelt processes through the
application of **snow depletion curves (SDCs)**. SDCs relate normalized snow water
equivalent (SWE) to normalized snow covered area (SCA) over a snowmelt season for
a given modeling unit.

SDCs were derived using output from the operational Snow Data Assimilation System
(SNODAS) snow model as daily 1-km gridded SWE over the conterminous United States.
Daily SNODAS output were aggregated to a predefined watershed-scale geospatial
fabric and used to also calculate SCA from October 1, 2004 to September 30, 2013.
The spatiotemporal variability in SNODAS output at the watershed scale was
evaluated through the spatial distribution of the median and standard deviation
for the time period. Representative SDCs for each watershed-scale modeling unit
over the conterminous United States (n = 54,104) were selected using a consistent
methodology and used to create categories of snowmelt based on SDC shape. The
relation of SDC categories to the topographic and climatic variables allows for
national-scale categorization of snowmelt processes.

**Key terms:** snow hydrology; geospatial analysis; watersheds; remote sensing.

---

## Introduction

National-scale assessment of water resources, including water availability, is
critical for understanding the vulnerability of these resources to changes in
policy and climate. Water-resources assessment for the conterminous United States
(CONUS) requires hydrologic information derived using a consistent, standardized
methodology. Small, watershed-scale study-area projects provide critical
information toward understanding the complexity of hydrologic processes; however,
the ability to translate, generalize, and compare hydrologic processes at a
continental scale is a critical step in the field of hydrology (McDonnell et al.,
2007; Archfield et al., 2015). This study focuses on improving the representation
of snowmelt processes at the CONUS scale.

Snow processes are often studied and modeled at the watershed- or field-scale due
to their complexity and local factors (such as wind redistribution and vegetation)
which influence snowpack accumulation and snowmelt. Computationally-expensive
models which account for these factors are useful at watershed or field scales;
however, estimation of snow processes on a continental scale requires
simplification of these complex processes. **Snow depletion curves (SDCs)** are a
simple and dynamic method used in hydrologic models to describe snowmelt processes
(Martinec and Rango, 1981; Liston, 1999; Markstrom et al., 2015). SDCs relate snow
covered area (SCA) to snow water equivalent (SWE) for a given hydrologic response
unit (HRU) over a snowmelt season. Snowmelt is a unidirectional process over time,
so SDCs inherently include a temporal factor. To create comparative SWE and SCA
relations, these data can be normalized to percent of total area and percent of
peak SWE, respectively. Conceptually, the shape of an SDC integrates the
spatiotemporally-complex process of snowmelt, using SWE and SCA (Figure 1). SDCs
allow for a comparable relation over space and time of snowmelt processes.

Investigation of snowmelt processes in the U.S. and their connection to climate
change are concentrated in the Western U.S., where water resources are
snowmelt-dominated. Data sources for snowmelt are often fixed at station locations,
such as the USDA NRCS Snow Telemetry (SNOTEL) locations, which are concentrated in
the Western U.S., neglecting snow processes in the Eastern U.S. — which are not
insignificant and are critical to a national-scale assessment of water
availability.

**Objective:** develop and assess SDCs derived from SNODAS output, using a
nationally consistent methodology. Gridded daily SWE and calculated SCA were
spatially aggregated to **109,951 watershed-scale HRUs** over CONUS for each
snowmelt season within the study period (2004–2013). The interannual variability
in SNODAS output and resulting SDCs were assessed for each HRU. For each HRU, a
representative SDC was selected where appropriate and used to compare snowmelt
processes over the CONUS. Development of SDCs by HRU improves the current
continental-scale representation of snow processes, which is currently very limited
(e.g., two SDCs: one for HRUs above treeline and one for below). Categorization
and relation of SDCs to geographic variables allow for translation of snowmelt
process variability to data-limited snowmelt-dominated areas (such as Alaska).

> **Figure 1 (conceptual model).** Snapshots in time of normalized SWE and SCA for
> two different snowmelt systems (Curve A and Curve B), shown as SDCs. *Curve A*
> loses SCA at a **slower** rate than SWE (consistent, homogeneous snowpack).
> *Curve B* loses SCA at a **faster** rate than SWE (spatially heterogeneous
> snowpack). Time progresses over the snowmelt season from 1 (maximum normalized
> SWE and SCA) to 0.

---

## Study Area and Data Sources

### Study Area and Geospatial Fabric

The spatial extent is the entirety of the CONUS. In addition to gridded SWE and
SCA, geographic variables (elevation and slope) were aggregated to the **109,951
HRUs** of the Geospatial Fabric for the National Hydrologic Model (NHM) (Viger and
Leavesley, 2007; Viger, 2014; Viger and Bock, 2014). The HRUs are derived from the
National Hydrography Dataset, NHDPlusV1. HRU sizes are based on drainage density
and range from **1 km² to over 67,000 km², averaging 74 km²**. HRUs spanning
international borders were excluded because they did not coincide with the spatial
extent of SNODAS output.

> **Figure 2.** Spatial distribution of elevation (A) and slope (B) at the HRU
> scale over CONUS.

### SNODAS Output

The study used publicly available, spatially distributed SNODAS data products from
the National Operational Hydrologic Remote Sensing Center (NOHRSC), available
through the National Snow and Ice Data Center (NSIDC),
<https://nsidc.org/data/g02158>. SNODAS integrates station, airborne, and
satellite data with downscaled Numerical Weather Prediction output to produce SWE
for the CONUS at **1-km spatial** and **daily temporal** resolution.

While field-scale, watershed, and regional studies have found discrepancies
between SNODAS output and observed values, SNODAS is the only currently available
SWE product at the CONUS extent. Other CONUS/global products (e.g., MODIS) provide
SCA but do not model or measure SWE. Deriving SDCs from SNODAS SWE and
SNODAS-calculated SCA (from the same output) allows development of SDCs on a
continental scale.

---

## Methods

### SWE and SCA

Daily 1-km gridded SNODAS output from **October 1, 2004 to September 30, 2013** (the
study period) provided the SWE values.

**Spatial assessment.** Spatial aggregation of gridded SNODAS output to the HRU
scale was completed using the USGS Geo Data Portal (GDP)
(<http://cida.usgs.gov/gdp/>; Blodgett et al., 2011). Because most HRUs cover
multiple SNODAS grid cells, the GDP calculated an **area-weighted mean** of the
daily gridded SWE values within each HRU. SCA is not a base SNODAS parameter, so
an estimate of SCA for each grid cell was calculated using a **binary cell
assignment: 1 if SWE > 0, 0 if SWE = 0**. Gridded SCA was aggregated to the HRU
scale as the number of cells equal to 1 divided by the total number of cells
covered by each HRU, for each day.

> **Figure 3.** Diagram showing how gridded SNODAS output was aggregated to the
> HRU scale.

**Temporal assessment.** Daily time series of SWE and SCA over the study period
were used to develop SDCs for each HRU. The **timing and duration of the snowmelt
season** were calculated for each HRU and season as the *date of peak SWE* and the
*number of days between that date and the day on which SWE = 0*. The standard
deviation of the annual peak-SWE dates and snowmelt-season durations were also
calculated for each HRU over the study period.

> **Figure 4.** Idealized accumulation and melt of a snowpack over time, after
> reversals have been removed. Accumulation period (October → April) → Peak SWE →
> Snowmelt period (→ August).

### Description of Variables

SNODAS-derived variables were derived from SWE and calculated SCA at the HRU scale.
DEM-derived variables were derived as part of the NHM Geospatial Fabric (Viger,
2014). See **Table 1**.

#### Table 1. Variables Used in Analysis of Snow Depletion Curves

| Variable | Source | Definition | Units |
|---|---|---|---|
| Elevation | DEM | Median elevation value within each HRU (Viger, 2014) | Meters |
| Slope | DEM | Area-averaged slope for each HRU (Viger, 2014) | Percent |
| Median SWE | SNODAS | Median value of annual peak SWE over the study period | Millimeters |
| Reversals | SNODAS | Cumulative number of snowmelt-season snowfall events (SCA increases) per HRU, cumulative for the study period | (count) |
| Snowmelt timing | SNODAS | Median day of annual peak SWE per HRU over the study period | Days since Oct 1 |
| Snowmelt duration | SNODAS | Median number of days between snowmelt onset and SCA = 0, per HRU per year | Days |
| SD of snowmelt timing | SNODAS | Standard deviation of annual day of peak SWE per HRU over the study period | Days |
| SD of snowmelt duration | SNODAS | Standard deviation of annual snowmelt-season duration per HRU over the study period | Days |
| Similarity | SNODAS | Sum of vertical distances between annual SCA values and the median, per SWE value, divided by the number of SCA values, per HRU | Dimensionless |

### Snow Depletion Curves

HRU-normalized (0 to 1) SCA and SWE data were plotted to generate **annual SDCs
for each year** of the study period (Figure 1). *Curve A* loses SCA at a slower
rate than SWE (consistent, homogeneous snowpack expected); *Curve B* loses SCA
faster than SWE (spatially heterogeneous snowpack expected).

**Reversals.** Increases in SWE and SCA due to snowfall after peak SWE (during the
snowmelt season) were counted (see *reversals*) and **removed** from each curve.
When SCA returned to the value prior to the increase, data were again included in
the series. This produces SDCs representing an idealized, uninterrupted melt,
allowing comparison over space and time — though it also extends the length of the
snowmelt season.

#### Selection of HRUs (six sequential criteria)

An inclusive CONUS analysis includes many HRUs that should not be part of a
snowmelt analysis (do not receive snow; do not build a snowpack; highly variable
year to year). HRUs were filtered by six criteria, applied in order:

| # | Criterion | HRUs remaining |
|---|---|---|
| — | Start | 109,951 |
| 1 | Remove HRUs without full SNODAS coverage (spanning international borders) | 109,544 |
| 2 | Remove HRUs without snow (no SWE) | 86,520 |
| 3 | Remove small HRUs with too few SNODAS grid cells (**< 25-cell threshold**) | 68,966 |
| 4 | Remove HRUs dominated by water bodies (**> 50% threshold**) | 68,584 |
| 5 | Remove HRUs where only part received snow (**SCA < 50%**) | 65,048 |
| 6 | Remove HRUs only partially snow-covered for the season (**SCA constant for > 80% of SWE values**) | 56,374 |

The following analyses use this subset of **n = 56,374** HRUs.

#### Selection of Representative SDC

A consistent method selects a representative SDC from the available annual SDCs for
each HRU. First, a **similarity metric** measures the temporal variability of the
SDCs across the study period (Equation 1):

```
similarity = Σ (dₙ) / p     for n = 0 … number of snowmelt seasons
```

where *n* is the number of snowmelt seasons with SDCs, *d* is the distance in the
y-axis (SCA) direction from the annual curve to the median curve along the SCA
axis, and *p* is the number of points on the SDC curve. A **zero** similarity value
represents identical SDCs; **higher** values represent more dissimilar SDCs.

HRUs with similarity **> 0.15** were deemed not similar enough to describe with a
representative SDC and were excluded (n = 2,270 removed). The threshold could be
modified to constrain SDCs to only the most similar. A single representative SDC
per HRU was then selected as the annual SDC **closest to the median SDC** of the
nine snowmelt seasons, evaluated independently for each HRU. Representative
HRU-scale SDCs were then compared and aggregated across CONUS (**n = 54,104**).

---

## Results

### Spatial Distribution of HRU-Scale SNODAS-Derived Variables

The spatial distribution of six SNODAS-derived variables (median SWE, reversals,
snowmelt timing, snowmelt duration, SD of snowmelt timing, SD of snowmelt duration)
shows the CONUS-scale spatial distribution of temporal variability in snowmelt
processes (Figure 5):

- **Median SWE** is higher at higher-elevation HRUs in the West and higher-latitude
  HRUs in the Midwest and Northeast.
- **Reversals** (late-season snowfall) are generally greatest at higher-elevation
  Western HRUs, coincident with more snowmelt-duration variability.
- **Median day of peak SWE** is later at higher elevations in the West and high
  latitudes in the Midwest and Northeast.
- **Median snowmelt duration** is higher in continental than coastal Western
  snowpacks.
- **SD of peak-SWE date** is greater in the interior continental Western snowpacks
  and the Appalachians.
- **SD of snowmelt duration** is greatest in the Sierra Nevada and interior western
  snowpacks. The Great Plains east of the Rockies has high SD of both day of peak
  SWE and snowmelt-season length.

> **Figure 5.** Spatial distribution of the six HRU-scale SNODAS-derived variables
> over CONUS.

### Spatial Distribution of SDC Similarity

Temporal variability in SDCs was assessed through the similarity value (Figure 6).
Low similarity = less inter-annual variability; high = more. The most dissimilar
areas are at the fringes where SDCs were calculated (the South and coastal areas
that do not consistently build a snowpack). There is less inter-annual variability
in the Northeast, northern Midwest, and high-elevation large-snowpack areas.

> **Figure 6.** Spatial distribution of SDC similarity over CONUS.

### Spatial Distribution of SDCs

The representative SDC per HRU was used to generate clustered SDCs (Figure 7). SDCs
were classified into three types by the value of **SCA at 50% normalized maximum
SWE (SWE = 0.5)**:

| Class | SCA at SWE = 0.5 | Count | Conceptual curve |
|---|---|---|---|
| Low SCA | < 45% | 1,285 | Curve B |
| Mid SCA | 45–55% | 1,485 | — |
| High SCA | > 55% | 51,334 | Curve A |

(Total n = 54,104.) In Figure 8, low SCA = blue, high SCA = red, mid = white.

> **Figure 7.** Progression of calculated SCA for normalized SWE = 1 → 0 in 10%
> increments, for the representative SDC of each study watershed in CONUS.

### Relation of Topographic and Climatic Data to SDC Type

- **Topography (Figure 8):** low and mid-SCA HRUs have substantially **greater
  slopes** than high-SCA HRUs and are at higher elevations (e.g., Mountain West),
  though the elevation difference from the high-SCA group was not substantial. Low
  and mid-SCA groups have similar distributions with no substantial distinction.
  Aspect showed no substantial between-group difference.
- **Climate (Figure 9):** using average monthly climate data (Daymet; Thornton
  et al., 2016), the **variance of both minimum and maximum temperature in March
  and April** is substantially greater in the high-SCA group than in the low/mid
  groups. May shows much less between-group variability.

> **Figure 8.** Distribution of topographic data (elevation, slope) for each SDC
> group.
> **Figure 9.** Distribution of climate variables (min/max temperature variance)
> for March, April, and May.

---

## Discussion

Snow processes are often studied in the western U.S., but continental- and
national-scale models require analysis of SDCs for their entire spatial extents.
SNODAS provides a methodologically consistent CONUS-extent distribution of SCA and
SWE. Within PRMS (in the USGS NHM), snowmelt is applied at the HRU scale using SDCs
through the Snow Dynamics module (Markstrom et al., 2015). Historically the NHM
used only two default SDCs (above/below treeline); per-HRU SDCs improve NHM
performance at CONUS and local extents.

The relation of SDC groups to topographic and climatic variables shows that broad
categorization of SDCs could be extended beyond the SNODAS spatial extent. The
variance of elevation, slope, and temperature in the High-SCA category may allow
classification of snowmelt processes independent of SNODAS/snow data — potentially
improving watershed-scale models in regions such as Alaska (no SNODAS coverage).

Climate change is driving earlier snowmelt onset, which lengthens the overall
snowmelt season. As an integration of spatial and temporal snowmelt processes,
SDCs are expected to be affected by climate change, but not uniformly. Areas where
SWE declines faster than SCA (Curve A) may be less altered than areas where SWE
declines at the same or slower rate than SCA (Curve B). Ecosystems and water users
relying on streamflow from Curve-A headwaters could be more vulnerable to changes
in snowmelt timing and duration.

---

## Conclusions

Broad-scale hydrologic models are critical tools for water allocation, water
security, national assessments, flood policy, water-quality/ecological directives,
and climate-extreme planning. Evaluating the spatiotemporal variability in SDCs is
essential to national-scale water-availability assessment. Relating SDC groups to
topographic and climatic variables allows improved representation of snowmelt
processes in physically-based hydrologic models in data-scarce regions.
Quantification and categorization of snowmelt processes on a broad scale identifies
areas of possible increased vulnerability to climate change — ideal candidates for
more detailed, watershed-scale assessment.

---

## Selected Literature Cited

- Blodgett, D.L., et al., 2011. *Description and Testing of the Geo Data Portal.*
  USGS Open-File Report 2011–1157.
- Markstrom, S.L., et al., 2015. *PRMS-IV, The Precipitation-Runoff Modeling
  System, Version 4.* USGS Techniques and Methods, Book 6, Chap. B7.
  DOI: 10.3133/tm6B7.
- Martinec, J. and A. Rango, 1981. *Areal Distribution of Snow Water Equivalent
  Evaluated by Snow Cover Modeling.* Water Resources Research 17:1480–1488.
- National Operational Hydrologic Remote Sensing Center, 2004. *Snow Data
  Assimilation System (SNODAS) Data Products at NSIDC, Version 1.*
  DOI: 10.7265/N5TB14TC.
- Thornton, P.E., et al., 2016. *Daymet: Daily Surface Weather Data on a 1-km Grid
  for North America, Version 2.* ORNL DAAC. DOI: 10.3334/ORNLDAAC/1328.
- Viger, R.J., 2014. *Preliminary Spatial Parameters for PRMS Based on the
  Geospatial Fabric, NLCD2001 and SSURGO.* USGS. DOI: 10.5066/F7WM1BF7.
- Viger, R.J. and A. Bock, 2014. *GIS Features of the Geospatial Fabric for
  National Hydrologic Modeling.* USGS. DOI: 10.5066/F7542KMD.
- Viger, R.J. and G.H. Leavesley, 2007. *The GIS Weasel User's Manual.* USGS
  Techniques and Methods, Book 6, Chap. B4.

*(Full reference list is in the source PDF, pages 11–12.)*
