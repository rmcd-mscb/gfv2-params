# Description of the National Hydrologic Model for use with the Precipitation-Runoff Modeling System (PRMS)

**R. Steve Regan, Steven L. Markstrom, Lauren E. Hay, Roland J. Viger, Parker A. Norton, Jessica M. Driscoll, and Jacob H. LaFontaine**

*U.S. Geological Survey Techniques and Methods, book 6, chapter B9, 38 p., 2018. Chapter 9 of Section B, Surface Water; Book 6, Modeling Techniques.*
DOI: [10.3133/tm6B9](https://doi.org/10.3133/tm6B9). ISSN 2328-7055 (online).

Suggested citation: Regan, R.S., Markstrom, S.L., Hay, L.E., Viger, R.J., Norton, P.A., Driscoll, J.M., LaFontaine, J.H., 2018, Description of the National Hydrologic Model for use with the Precipitation-Runoff Modeling System (PRMS): U.S. Geological Survey Techniques and Methods, book 6, chap. B9, 38 p., https://doi.org/10.3133/tm6B9.

> Converted from `docs/tm6b9.pdf` for research reference. Figures are described
> but not reproduced; parameter tables are transcribed in full. This is a
> U.S. Government work in the public domain in the USA.

---

## Font Styles

- Modules, file names, and user input are identified by using Courier New font (rendered here as `monospace`).
- Input parameters and dimensions are identified by using bold, Times New Roman font (rendered here as **bold**).
- State and flux variables are identified by using italic, Times New Roman font (rendered here as *italic*).

## Abbreviations and Acronyms

| Abbreviation | Meaning |
|---|---|
| CBH | Climate-by-HRU |
| CFSR | Climate Forecast System Reanalysis |
| CONUS | Conterminous United States |
| CSV | Comma-Separated Values |
| DAYMET | Daily Surface Weather and Climatological Summaries |
| DEM | Digital Elevation Model |
| DPRST | Surface-Depression Storage |
| ESRI | Environmental Systems Research Institute |
| ET | Evapotranspiration |
| GDP | GeoData Portal |
| GF | Geospatial Fabric for National Hydrologic Modeling |
| GIS | Geographic Information System |
| HRU | Hydrologic Response Unit |
| MAVELU | Mean Annual VELocity U (toward east direction) |
| MOD16 | Moderate Resolution Imaging Spectrometer Global Evapotranspiration Project |
| MODIS | Moderate Resolution Imaging Spectrometer |
| MoWS | U.S. Geological Survey National Research Program Modeling of Watershed Systems Project |
| MWBM | Monthly Water Balance Model |
| NAWQA | National Water Quality Assessment Program |
| NHD | National Hydrography Dataset |
| NHDPlus | National Hydrography Dataset Plus, version 1 |
| NHM | U.S. Geological Survey National Hydrologic Model |
| NHM-PRMS | U.S. Geological Survey National Hydrologic Model application of the Precipitation-Runoff Modeling System |
| NLCD | National Land Cover Database |
| NLCD2001 | National Land Cover Database, year 2001 |
| NhmParamDb | National Hydrologic Model Parameter Database |
| NOAA | National Oceanic and Atmospheric Administration |
| NREL | National Renewable Energy Laboratory |
| NWIS | National Water Information System |
| PET | Potential evapotranspiration |
| POI | Point of Interest |
| PRISM | Parameter-elevation Relationships on Independent Slopes Model |
| PRMS | Precipitation-Runoff Modeling System |
| SCA | Snow-Covered Area |
| SNODAS | Snow Data Assimilation System |
| SPARROW | SPAtially Referenced Regressions On Watershed |
| SSURGO | Soil Survey Geographic Database |
| STATSGO | State Soil Geographic Database |
| SWE | Snow water equivalent |
| TWI | Topographic Wetness Index |
| USGS | U.S. Geological Survey |
| XML | Extensible Markup Language |

---

## Preface

This report documents methods to derive the Precipitation-Runoff Modeling System (PRMS) parameter values as used in the U.S. Geological Survey (USGS) National Hydrologic Model (NHM). These parameter values are available from the NHM Parameter Database. Spatial subsets of the NHM Parameter Database, that is individual watersheds derived on the basis of stream segment or USGS streamgage identification numbers, can be retrieved using the NHM Bandit software tool. This report relies heavily upon (1) USGS Techniques and Methods, book 6, chapter B7 (Markstrom and others, 2015, https://dx.doi.org/10.3133/tm6b7) that documents PRMS-IV and (2) USGS Techniques and Methods, book 6, chapter B4 that documents the GIS Weasel (Viger and Leavesley, 2007, https://pubs.usgs.gov/tm/2007/06B04/). Summaries of changes to input and output values for the PRMS are available on the PRMS software distribution page (https://wwwbrr.cr.usgs.gov/projects/SW_MoWS/PRMS.html, accessed September 30, 2017). This report provides brief descriptions of the PRMS, the NHM, and the Geospatial Fabric for National Hydrologic Modeling (https://wwwbrr.cr.usgs.gov/projects/SW_MoWS/GeospatialFabric.html, accessed September 30, 2017) upon which the NHM is discretized.

The performance of this software has been tested on several different computer systems and configurations. Future use, however, might reveal errors that were not detected during testing. Users are requested to notify the USGS of any errors found in this report or in the code, and submit questions to the Modeling of Watershed Systems (MoWS) group by using the "Help" link at the MoWS web page at https://wwwbrr.cr.usgs.gov/projects/SW_MoWS/. Additionally, users can contact the USGS at:

Integrated Modeling and Prediction Division, Mail Stop 415, 12201 Sunrise Valley Drive, Reston, VA 20192.

Although the PRMS parameter values and related software have been developed successfully on a computer system at the USGS, no warranty expressed or implied is made regarding the display or utility of the values and software for other purposes, nor on all computer systems, nor shall the act of distribution constitute any such warranty. The USGS or the U.S. Government shall not be held liable for improper or incorrect use of the parameter values and software described and/or contained herein. This report is not intended as instruction for application or interpretation of PRMS simulations.

---

## Abstract

This report documents several components of the U.S. Geological Survey National Hydrologic Model of the conterminous United States for use with the Precipitation-Runoff Modeling System (PRMS). It provides descriptions of the (1) National Hydrologic Model, (2) Geospatial Fabric for National Hydrologic Modeling, (3) PRMS hydrologic simulation code, (4) parameters and estimation methods used to compute spatially and temporally distributed default values as required by PRMS, (5) National Hydrologic Model Parameter Database, and (6) model extraction tool named Bandit. The National Hydrologic Model Parameter Database contains values for all PRMS parameters used in the National Hydrologic Model. The methods and national datasets used to estimate all the PRMS parameters are described. Some parameter values are derived from characteristics of topography, land cover, soils, geology, and hydrography using traditional Geographic Information System methods. Other parameters are set to long-established default values and computation of initial values. Additionally, methods (statistical, sensitivity, calibration, and algebraic) were developed to compute parameter values on the basis of a variety of nationally-consistent datasets. Values in the National Hydrologic Model Parameter Database can periodically be updated on the basis of new parameter estimation methods and as additional national datasets become available. A companion ScienceBase resource provides a set of static parameter values as well as images of spatially-distributed parameters associated with PRMS states and fluxes for each Hydrologic Response Unit across the conterminous United States.

---

## Introduction

Assessment of the effects of historical, current, and projected climate and land cover on water resources at various scales is needed by policymakers, natural resource managers, and the public for management and planning purposes. Management needs include flood forecasting, water availability, and planning for changing land use and climate for the timing and source of streamflow. The capability to expand hydrologic simulation applications to the continental scale has become feasible with the increasing availability of national and global climate, digital elevation model (DEM), soils, land cover and use, snowpack, and hydrography datasets. Development of continental-scale models using these datasets can be realized using nationally-consistent, physical and statistical methods for discretization, characterization, parameterization, calibration, and evaluation.

To address continental-scale modeling needs, the USGS is developing a National Hydrologic Model (NHM) and associated tools to support coordinated, comprehensive, and consistent hydrologic model development and applications across the conterminous United States (CONUS). The NHM can be used to improve understanding of watershed hydrologic processes and continental-scale water resource management and planning and assessment of hydrologic processes on the basis of historical and projected climate data. A primary goal of this project is to facilitate hydrologic model parameterization on the basis of datasets that characterize geology, soil, vegetation, contributing area, topography, growing season, snow dynamics, climate, solar radiation, and evapotranspiration across the CONUS.

Datasets that span the CONUS are typically available as gridded products based on aggregated and disaggregated information. For example, Daily Surface Weather and Climatological Summaries (DAYMET, https://daymet.ornl.gov/), State Soil Geographic (STATSGO), Moderate Resolution Imaging Spectrometer (MODIS) Global Evapotranspiration Project (MOD16) gridded products are available at a 1-square kilometer (km²) resolution. Parameter-elevation Relationships on Independent Slopes Model climate data (PRISM), National Renewable Energy Lab (NREL) solar radiation (NSRDB), and University of Idaho Gridded Surface Meteorological Data gridded products are available at a 16-km² resolution. National Land Cover Database (NLCD) and Soil Survey Geographic Database (SSURGO) gridded products are available at 10-, 30-, 90-, and 100-meter resolutions. All URLs accessed on November 1, 2017.

Deterministic hydrologic models, such as the Precipitation-Runoff Modeling System (PRMS) simulation code (Markstrom and others, 2015; Leavesley and others, 1983), provide information that can benefit management of water resources decisions and improve understanding of hydrologic processes at various spatial and temporal scales. Traditionally, PRMS model applications have been developed for relatively small, gaged watersheds. Collections of PRMS models have been used to evaluate effects of climate change across the CONUS, for example see Markstrom and others (2012). However, use of collections of models developed for different purposes, with underlying input from disparate sources and various levels of calibration and evaluation, may not provide consistent, national-scale information. See Archfield and others (2015) for a discussion of considerations for hydrologic modeling across the CONUS.

A PRMS application has been developed as a component of the NHM. This model is referred to as the NHM-PRMS. The flexibility to subset and extract models of one or more watersheds from the NHM-PRMS has been built into the underlying structure of the NHM. These subset models include realistic default parameter values that are retrieved from the NHM Parameter Database (NhmParamDb). A software tool has been developed, called Bandit, to automate the extraction of model subsets from the NHM-PRMS. The subset models include all required input files. See Markstrom and others (2015) for descriptions of the PRMS input file structures. While values in the input files were determined on the basis of national datasets, it is recommended they be examined prior to calibration and evaluation of results. Default parameter values can be enhanced, new parameters added, or different PRMS simulation options used on the basis of local knowledge and additional datasets. Additionally, models extracted from the NHM can be enhanced on the basis of information that accounts for dynamic and evolving watershed characteristics and water use (Regan and LaFontaine, 2017).

The purpose of this report is to describe the NHM for use with the PRMS. Descriptions also are provided of the Geospatial Fabric for National Hydrologic Modeling, the PRMS hydrologic simulation code, the parameters and estimation methods used to compute spatially and temporally distributed default values as required by the PRMS, the NhmParamDb, and the model extraction tool named Bandit.

---

## Description of the National Hydrologic Model (NHM)

The NHM was developed to support coordinated, comprehensive, and consistent hydrologic model development, application, and evaluation of hydrologic processes across the CONUS. It can be used to provide nationally consistent estimates of total water availability, changes in the timing and source of streamflow, and measures of the uncertainty of these estimates. The NHM includes: (1) a modeling platform for model distribution, comparability, and interoperability; (2) a consistent geospatial structure for modeling; and (3) default parameter values. While the NHM infrastructure could be adapted for use with many hydrologic simulation codes, this report describes the NHM as applied to the PRMS (referred to as NHM-PRMS).

The NHM-PRMS includes default values for numerous PRMS parameters that are estimated on the basis of national, physically-based datasets and consistent derivation methods. The NHM-PRMS can be used to compute internally-consistent simulation results of the temporal and spatial distribution of water availability and storage across the CONUS. National-scale datasets include hydrography, solar radiation, potential evapotranspiration, geology, soils, land cover, topography, snow-covered area, and snow-water equivalent. NHM-PRMS models can be driven by historical, current, and projected climate and is currently configured with the DAYMET dataset (DAYMET, Thornton and others, 2016). The default parameter values are maintained in the NhmParamDb. Appendix 1 describes the derivation methods of the default NHM-PRMS parameter values in the NhmParamDb.

---

## Description of the Geospatial Fabric for National Hydrologic Modeling (GF)

The NHM spatial infrastructure and associated geodatabase is named the USGS Geospatial Fabric for National Hydrologic Modeling (GF), which includes two main products: (1) Geographic Information System (GIS) of spatial features (Viger and Bock, 2014); and (2) tables of attributes about those features organized in Environmental Systems Research Institute (Esri) Geodatabase (version 10.2) files (Viger, 2014). The GF data features include a stream network consisting of 56,460 segments and associated spatial units consisting of 109,951 Hydrologic Response Units (HRUs). The mean area of the HRUs is approximately 75 km² with a maximum area of approximately 2,000 km². In addition to their use in hydrologic modeling, these features can be used for creating maps to illustrate spatial patterns of landscape characteristics and for other spatial analyses (including derivation of new attribute tables).

The set of values located in the tables of attributes specified by Viger (2014) are the first version for most of the spatial parameters used in the NHM-PRMS. These parameters are maintained in the NhmParamDb described in this report. The parameter values in the NhmParamDb are being continuously evaluated and improved, so users seeking the currently best available parameter values at the time of their application should work from the NhmParamDb as opposed to Viger (2014).

The data in both Viger and Bock (2014) and Viger (2014) are tiled into NHDPlus regions (fig. 1), using Esri file geodatabases as the file format. The tables from any number of file geodatabases can be joined to the GIS features using techniques available in the Esri GIS platform. For a more detailed description of the GF see the web page: http://wwwbrr.cr.usgs.gov/projects/SW_MoWS/GeospatialFabric.html (accessed September 30, 2017).

The GF provides a consistent infrastructure for building hydrologic models and contains information that can be used for organization, visualization, and statistical analysis of model input and results for the CONUS. Any modeling application that uses the GF will share common feature geometries, identification scheme, and flow connectivity. This means, for example, that streamflow computations by several different models could be compared with ease because they would all be indexed to the same segments on the drainage network. Using this type of shared indexing scheme for publishing simulation results lends itself to improved discoverability and sharing of flow information across not just NHM results produced by the original authors but also of derivative applications made by NHM users.

The GF features were derived based on NHDPlus version 1.0 flowlines and each flowline's associated catchment, using a simplification process that identified the minimally sufficient stream network needed to connect a set of points of interest (POIs) to the ocean or legitimate sinks (for example playas and swales). The POIs were identified based on discussion with the hydrologic modeling community. The POIs used to delineate the GF stream network are:

- USGS streamgages in the GAGES-II dataset (Falcone, 2011),
- National Weather Service River Forecast Center Nodes (http://water.weather.gov/ahps/download.php, accessed February 2017),
- USGS National Water-Quality Assessment Program (NAWQA) SPAtially Referenced Regressions On Watershed (SPARROW) nodes,
- outlets from and inlets to NHD-defined waterbodies exceeding a minimum area of 10 km² in surface area,
- confluences of NHDPlus flowlines exceeding a Strahler stream order (Strahler, 1952, 1957) of four,
- addition of segments to ensure that all features are connected,
- splits of segments to ensure travel times are less than or equal to 24 hours based on segment length and the NHDPlus mean annual velocity (MAVELU, which uses a unit runoff-based mean annual flow, Jobson [1996]), and
- splits of segments to ensure each has an elevation range less than 500 meters from upstream end to downstream end.

A segment in the GF network is the aggregation of flowlines needed from an upstream POI to the next downstream POI, or to the ocean or a sink. The resultant drainage network of the GF consists of 56,460 stream segments. Drainage density varies as a function of the numbers of POIs per unit area. There is generally a single drainage segment per POI.

GF HRUs are defined in a three-step process:

1. For each POI, all of the inflowing NHDPlus flowlines are identified.
2. The catchments associated with these flowlines are merged into a single contributing area for the POI.
3. The segment associated with the POI is used to split the contributing area into left-bank and right-bank polygon HRUs.

In the case of headwater catchments, the segment does not reach the upper divide. To compensate, the NHDPlus catchment associated with the headwater flowline in the segment is added to either the left- or right-bank HRU, whichever is smaller. The GF delineation process resulted in 109,951 HRUs that have a mean area of approximately 75 km² with a maximum area of approximately 2,000 km² from the original 2,900,900 NHDPlus catchments. Parameter estimation for nonCONUS drainage areas are based on sparse and often missing information, thus it is recommended that parameter values be refined based on local datasets prior to building models that include these areas.

*[Figure 1. Map of the National Hydrography Plus Dataset (NHDPlus) regions. Two-digit numbers represent watershed boundary dataset hydrologic regions (modified from U.S. Geological Survey, 2016): 01 Northeast; 02 Mid-Atlantic; 03 South Atlantic; 04 Great Lakes; 05 Ohio; 06 Tennessee; 07 Upper Mississippi; 08 Lower Mississippi; 09 Lower Missouri; 10U Upper Missouri; 10L Arkansas-Red-White; 11 Souris-Red-Rainy; 12 Texas; 13 Rio Grande; 14 Upper Colorado; 15 Lower Colorado; 16 Great Basin; 17 Pacific Northwest; 18 California. Albers Equal Area Conic projection, NAD 1983.]*

*[Figure 2. Geospatial Fabric Hydrologic Response Units (HRU) clipped to the conterminous United States. The inset shows HRUs (colored polygons) and stream segments (blue lines), outlined with a headwater watershed in Colorado (black-lined polygon).]*

The stream network connectivity attributes, **hru_segment**, **tosegment**, and **poi_gage_segment**, were originally derived exclusively by automated means and are indicated by a "_orig" suffix attached to their respective names. These values were manually checked and updated into attribute fields named without the "_orig" suffix. Note that even the manually updated values presented in the Viger and Bock (2014) dataset have been superseded in the NhmParamDb. The domain of region identifiers is '01', '02', '03', '04', '05', '06', '07', '08', '09', '10L', '10U', '11', '12', '13', '14', '15', '16', '17', '18', '20', '21' (fig. 1).

---

## Description of the Precipitation-Runoff Modeling System (PRMS)

The PRMS (Markstrom and others, 2015) is a modular, deterministic, distributed-parameter, physical process-based hydrologic simulation code. It was developed to evaluate effects of various combinations of climate, physical characteristics, and simulation options on hydrologic response and water distribution at the watershed scale. The PRMS computes water flow and storage from and to the atmosphere, plant canopy, land surface, snowpack, surface depressions, shallow subsurface zone, deep aquifers, stream segments, and lakes. Physical characteristics including topography, soils, vegetation, geology, and land use are used to characterize and derive parameters required in simulation algorithms, spatial discretization, and topological connectivity. Computations of the hydrologic processes are driven (or forced) by historical, current, and (or) projections of climate. Daily precipitation and minimum and maximum air temperature data are required for a PRMS simulation. Other datasets, such as potential evapotranspiration, solar radiation, streamflow, plant transpiration period, wind speed, and humidity, are optional. Simulations operate on a daily time step with time periods from days to centuries.

A model domain is discretized spatially into HRUs and stream segments of any area and length, respectively. Individual HRUs and stream segments are assumed to be homogenous with respect to physical characteristics and hydrologic response for a specified simulation time period. The goal of the discretization is to define a flow network that routes water from each HRU and stream segment to its downslope neighbors on the basis of flow direction, contributing area, and physical, anthropogenic, and biologic characteristics. Flows to HRUs and stream segments are considered instantaneously mixed with any existing water storage. Discretization methods include existing watershed delineations, such as the NHDPlus spatial units, derived polygons representing a collection of watersheds, land use, and (or) climate zones, and regular and variably-spaced grids. Any combination of discretization methods can be used.

Hydrologic processes computed include evapotranspiration (ET), snowpack dynamics, overland flow (surface runoff), infiltration, soil-moisture relations, vertical (recharge) and lateral (interflow) subsurface flow, surface depression flow and storage, and stream and lake flow and storage. These processes are simulated using methods based on physical laws and (or) empirical relations and are encoded as modules. Figure 3 illustrates the hydrologic cycle as simulated by PRMS for an HRU.

The PRMS simulates some hydrologic processes using a single module—potential solar radiation, interception, snow, soil zone, and groundwater flow. Various algorithm options are available to compute potential evapotranspiration, solar radiation, precipitation and temperature distribution, surface runoff, plant transpiration period (or growing season), and streamflow and lake routing. The modules available in the PRMS are described in table 1, with a column identifying the ones that are used for the NHM. Computation options can be active or inactive on the basis of flags specified in the Control File: surface-depression storage, subbasin, and use of initial conditions, cascading flow, water use, dynamic parameters, and various output options. See Markstrom and others (2015) and Regan and LaFontaine (2017) for descriptions of the modules, parameters, computation methods and options, and input and output file formats.

An NHM-PRMS model consists of four input file types: (1) Control; (2) Data; (3) Parameter; and (4) Climate-By-HRU (CBH). The Control File includes parameters to specify the active modules and input, computation, and output options. Table 2 describes the parameters specified in the Control File used in the NHM-PRMS. For a complete list of Control File parameters, see table 1–2 in Markstrom and others (2015). The Data File specifies measured time-series data. The NHM-PRMS Data File contains observations of daily streamflow for each USGS streamgage within the model domain and were retrieved from the USGS National Water Information System (NWIS) database. The Parameter File specifies the dimensions of the arrays for parameters and computed variables and the parameter values required for each active module and simulation and output options. Each CBH File specifies a daily time-series of distributed values for one climate variable, with the number of values for each day equal to the number of HRUs. Three CBH Files are input for NHM-PRMS models: (1) minimum air temperature, (2) maximum air temperature, and (3) precipitation. The values in the CBH Files are spatially interpolated from the gridded DAYMET dataset (Thornton and others, 2016) using the USGS GeoData Portal (GDP, http://cida.usgs.gov/gdp/, accessed March 17, 2016; Blodgett and others, 2011). Users can generate CBH Files of preprocessed distributed climate from other sources. Use of such climate data is an example of updating NHM default models with local knowledge. Similarly, local information can be used to update any parameter value.

*[Figure 3. Hydrologic processes as conceptualized in the Precipitation-Runoff Modeling System; dashed lines represent internal states, solid lines represent inflows and outflows (modified from Markstrom and others, 2015). Flow paths shown: Precipitation, Solar radiation, and Air temperature into Plant canopy interception; Rain/Snow to Snowpack and Snowmelt; Depression storage and Impervious surfaces producing Hortonian runoff and Dunnian runoff; Soil zone receiving upslope surface runoff and interflow and producing Interflow; Recharge to Groundwater; Groundwater flow and Groundwater sink; all contributing to Streamflow and lake routing. Sublimation, Evaporation, and transpiration are loss fluxes.]*

### Table 1. Description of modules implemented in the Precipitation-Runoff Modeling System, version 5 (PRMS-V).

[NHM, National Hydrologic Model; HRU, Hydrologic Response Unit; CBH, Climate-by-HRU; CSV, Comma-Separated Values]

| Module name | Description | Used in NHM |
|---|---|---|
| **Basin definition process** | | |
| `basin` | Defines shared watershed-wide and hydrologic response unit (HRU) physical parameters and variables | yes |
| **Cascading flow process** | | |
| `cascade` | Determines computational order of the HRUs and groundwater reservoirs for routing flow downslope | no |
| **Solar table process** | | |
| `soltab` | Compute potential solar radiation and sunlight hours for each HRU for each day of year | yes |
| **Time series data process** | | |
| `obs` | Reads and stores observed data from all specified measurement stations | yes |
| `dynamic_param_read` | Read and makes available dynamic parameters by HRU from pre-processed files | no |
| `water_use_read` | Read and makes available water-use data (diversions and gains) from pre-processed files | no |
| **Temperature distribution process** | | |
| `temp_1sta` | Distributes maximum and minimum temperatures to each HRU by using temperature data measured at one station and an estimated monthly lapse rate | no |
| `temp_laps` | Distributes maximum and minimum temperatures to each HRU by computing a daily lapse rate with temperature data measured at two stations | no |
| `temp_dist2` | Distributes maximum and minimum temperatures to each HRU by using a basin-wide lapse rate applied to the temperature data, adjusted for distance, measured at each station | no |
| `climate_hru` | Reads distributed temperature values directly from files | yes |
| **Precipitation distribution process** | | |
| `precip_1sta` | Determines the form of precipitation and distributes it from one or more stations to each HRU by using monthly correction factors to account for differences in spatial variation, topography, and measurement gage efficiency | no |
| `precip_laps` | Determines the form of precipitation and distributes it to each HRU by using monthly lapse rates | no |
| `precip_dist2` | Determines the form of precipitation and distributes it to each HRU by using an inverse distance weighting scheme | no |
| `climate_hru` | Reads distributed precipitation values directly from files | yes |
| **Combined climate distribution process** | | |
| `ide_dist` | Determines the form of precipitation and distributes precipitation and temperatures to each HRU on the basis of measurements at stations with closest elevation or shortest distance to the respective HRU | no |
| `xyz_dist` | Determines the form of precipitation and distributes precipitation and temperatures to each HRU by using a multiple linear regression of measured data from a group of measurement stations or from atmospheric model simulation | no |
| **Solar radiation distribution process** | | |
| `ddsolrad` | Distributes solar radiation to each HRU and estimates missing solar radiation data using a maximum temperature per degree-day relation | yes |
| `ccsolrad` | Distributes solar radiation to each HRU and estimates missing solar radiation data using a relation between solar radiation and cloud cover | no |
| `climate_hru` | Reads distributed solar radiation values directly from files | no |
| **Transpiration period process** | | |
| `transp_frost` | Determines whether the current time step is in a period of active transpiration by the killing frost method | no |
| `transp_tindex` | Determines whether the current time step is in a period of active transpiration by the temperature index method | yes |
| `climate_hru` | Reads the state of transpiration directly from files | no |
| **Potential evapotranspiration process** | | |
| `potet_hamon` | Computes the potential evapotranspiration by using the Hamon formulation (Hamon, 1961) | no |
| `potet_jh` | Computes the potential evapotranspiration by using the Jensen-Haise formulation (Jensen and Haise, 1963) | yes |
| `potet_hs` | Computes the potential evapotranspiration by using the Hargreaves-Samani formulation (Hargreaves and Samani, 1985) | no |
| `potet_pt` | Computes the potential evapotranspiration by using the Priestley-Taylor formulation (Priestley and Taylor, 1972) | no |
| `potet_pm` | Computes the potential evapotranspiration by using the Penman-Monteith formulation (Penman, 1948; Monteith, 1965) using specified windspeed and humidity in CBH Files | no |
| `potet_pm_sta` | Computes the potential evapotranspiration by using the Penman-Monteith formulation (Penman, 1948; Monteith, 1965) using specified windspeed and humidity in the Data File | no |
| `potet_pan` | Computes the potential evapotranspiration for each HRU by using pan-evaporation data | no |
| `climate_hru` | Reads distributed potential evapotranspiration values directly from files | no |
| **Canopy Interception process** | | |
| `intcp` | Computes volume of intercepted precipitation, evaporation from intercepted precipitation, and throughfall that reaches the soil or snowpack | yes |
| **Snow process** | | |
| `snowcomp` | Initiates development of a snowpack and simulates snow accumulation and depletion processes by using an energy-budget approach | yes |
| **Surface runoff process** | | |
| `srunoff_smidx` | Computes surface runoff and infiltration for each HRU by using a nonlinear variable-source-area method allowing for cascading flow | yes |
| `srunoff_carea` | Computes surface runoff and infiltration for each HRU by using a linear variable-source-area method allowing for cascading flow | no |
| **Soil-zone process** | | |
| `soilzone` | Computes inflows to and outflows from soil zone of each HRU and includes inflows from infiltration, groundwater, and upslope HRUs, and outflows to gravity drainage, interflow, and surface runoff to downslope HRUs | yes |
| **Groundwater process** | | |
| `gwflow` | Sums inflow to and outflow from groundwater reservoirs; outflow can be routed to downslope groundwater reservoirs and stream segments | yes |
| **Streamflow process** | | |
| `strmflow` | Computes daily streamflow as the sum of surface runoff, shallow-subsurface flow, detention reservoir flow, and groundwater flow | no |
| `muskingum` | Routes water between segments in the system using Muskingum routing | no |
| `muskingum_lake` | Routes water between segments in the system using Muskingum routing and on-channel water body storage and flow routing | no |
| `strmflow_in_out` | Routes water between segments in the system by setting the outflow to the inflow | yes |
| **Summary process** | | |
| `basin_sum` | Computes daily, monthly, yearly, and total flow summaries of volumes and flows for all HRUs | yes |
| `subbasin` | Computes streamflow at internal basin nodes and variables by subbasin | no |
| `map_results` | Writes HRU summaries to a user specified target map, such as a grid or set of HRUs, at weekly, monthly, yearly, and total time steps | no |
| `prms_summary` | Writes selected basin area-weighted results to a CSV File when control parameter `csvON_OFF` is specified equal to 1 | yes |
| `nhru_summary` | Write selected results dimensioned by the value of dimension `nhru` to separate CSV Files at daily, monthly, mean monthly, mean yearly, and yearly total time steps | no |

### Table 2. Parameters specified in the Control File for the National Hydrologic Model application of the Precipitation-Runoff Modeling System (PRMS).

[Data type: 1=integer, 2=single precision floating point (real), 4=character string; HRU, hydrologic response unit; CBH, climate-by-HRU; CSV, comma separated values; temp_units, 0=degrees Fahrenheit; 1=degrees Celsius]

| Parameter name | Description | Option | Number of values | Data type | NHM value |
|---|---|---|---|---|---|
| **Simulation execution and required input and output files** | | | | | |
| **data_file** ² | Pathname(s) for measured input Data File(s), typically a single Data File is specified | measured input | number of Data Files | 4 | user defined |
| **end_time** | Simulation end date and time specified in order in the control item as: year, month, day, hour, minute, second | time period | 6 | 1 | user defined |
| **model_mode** | Flag to indicate the simulation mode (PRMS=PRMS; DOCUMENTATION=write files of all declared parameters and variables in the executable) | simulation mode selection | 1 | 4 | PRMS |
| **model_output_file** ² | Pathname for Water-Budget File for results module `basin_sum` | simulation output | 1 | 4 | user defined |
| **param_file** ² | Pathname(s) for Parameter File(s) | parameter input | number of Parameter Files | 4 | user defined |
| **start_time** | Simulation start date and time specified in order in the control item as: year, month, day, hour, minute, second | time period | 6 | 1 | user defined |
| **Module selection and simulation options** | | | | | |
| **dprst_flag** | Flag to indicate if depression-storage simulation is computed (0=no; 1=yes) | surface-depression storage | 1 | 1 | 1 |
| **et_module** | Module name for potential evapotranspiration method (`climate_hru`, `potet_jh`, `potet_hamon`, `potet_hs`, `potet_pt`, `potet_pm`, `potet_pm_sta`, or `potet_pan`) | module selection | 1 | 4 | `potet_jh` |
| **precip_module** | Module name for precipitation-distribution method (`climate_hru`, `ide_dist`, `precip_1sta`, `precip_dist2`, `precip_laps`, or `xyz_dist`) | module selection | 1 | 4 | `climate_hru` |
| **solrad_module** | Module name for solar-radiation-distribution method (`ccsolrad` or `ddsolrad`) | module selection | 1 | 4 | `ddsolrad` |
| **srunoff_module** | Module name for surface-runoff/infiltration computation method (`srunoff_carea` or `srunoff_smidx`) | module selection | 1 | 4 | `srunoff_smidx` |
| **strmflow_module** | Module name for streamflow routing simulation method (`strmflow`, `muskingum`, `strmflow_in_out`, or `muskingum_lake`) | module selection | 1 | 4 | `strmflow_in_out` |
| **temp_module** | Module name for temperature-distribution method (`climate_hru`, `temp_1sta`, `temp_dist2`, `temp_laps`, `ide_dist`, or `xyz_dist`) | module selection | 1 | 4 | `climate_hru` |
| **transp_module** | Module name for transpiration simulation method (`climate_hru`, `transp_frost`, or `transp_tindex`) | module selection | 1 | 4 | `transp_tindex` |
| **Climate by HRU files** | | | | | |
| **precip_day** ¹ | Pathname of the CBH file of pre-processed precipitation input data for each HRU to specify variable precip_units | precip_module = climate_hru | 1 | 4 | user defined |
| **tmax_day** ¹ | Pathname of the CBH file of pre-processed maximum air temperature input data for each HRU to specify variable tmax-temp_units | temp_module = climate_hru | 1 | 4 | user defined |
| **tmin_day** ¹ | Pathname of the CBH file of pre-processed minimum air temperature input data for each HRU to specify variable tminf-temp_units | temp_module = climate_hru | 1 | 4 | user defined |
| **Debug options** | | | | | |
| **cbh_check_flag** | Flag to indicate if CBH values are validated each time step (0=no; 1=yes) | CBH input | 1 | 1 | 1 |
| **parameter_check_flag** | Flag to indicate if selected parameter values validation checks are treated as warnings or errors (0=no; 1=yes; 2=check parameters and then stop) | parameter validation check | 1 | 1 | 1 |
| **print_debug** ² | Flag to indicate type of debug output (-1=minimize screen output; 0=none; 1=water balances) | debug output | 1 | 1 | 0 |
| **Statistic variables (statvar) files** | | | | | |
| **nstatVars** | Number of variables to include in Statistics Variables File and names specified in statVar_names | statsON_OFF = 1 | 1 | 1 | 0 |
| **stat_var_file** ¹ | Pathname for Statistics Variables File | statsON_OFF = 1 | 1 | 4 | user defined |
| **statsON_OFF** | Switch to specify whether or not the Statistics Variables File is generated (0=no; 1=statvar text format; 2=CSV format) | statsON_OFF = 1 | 1 | 1 | 0 |
| **statVar_element** | List of identification numbers corresponding to variables specified in statVar_names list (1 to variable's dimension size) | statsON_OFF = 1 | nstatVars | 4 | user defined |
| **statVar_names** | List of variable names for which output is written to Statistics Variables File | statsON_OFF = 1 | nstatVars | 4 | user defined |
| **PRMS summary results files** | | | | | |
| **csvON_OFF** | Switch to specify whether or not CSV summary output files are generated (0=no; 1=yes) | PRMS summary results | 1 | 1 | 0 |
| **csv_output_file** ¹ | Pathname of CSV output file | csvON_OFF = 1 | 1 | 4 | user defined |

¹ Pathnames for all files can have a maximum of 256 characters.
² File and screen output options: 1=water balance output files written in current directory, for `intcp` module file `intcp.wbal`; for `snowcomp` module `snowcomp.wbal`; for `srunoff` module `srunoff_smidx.wbal` or `srunoff_carea.wbal`; for `soilzone` module `soilzone.wbal`; for `gwflow` module `gwflow.wbal`.

---

## National Hydrologic Model Parameter Database (NhmParamDb)

An NHM Parameter Database (NhmParamDb) has been developed as a system of files and directories for storing all of the NHM-PRMS parameter values. This system features an online version control system implemented in the Git software (Chacon and Straub, 2014) that keeps track of revisions of the parameter values. This system provides two advantages: (1) it provides an easy way for users to archive their parameter values; and (2) it keeps their NHM applications up to date with the currently best available parameter values by synchronizing with a central online NhmParamDb.

### Design of the NhmParamDb

The NhmParamDb is a set of subdirectories and files on a computer file system (fig. 4). The top level directory is named "nhmParamDb/". This directory contains three types of content: (1) the .git subdirectory, (2) a series of subdirectories, each named for a parameter or NHDPlus region identifier, and (3) the "parameters.xml" file.

The ".git/" subdirectory is part of the Git version control system. This subdirectory is hidden on Unix-like systems. Modifying this subdirectory is not recommended and may break the repository. Next, there is a subdirectory named for each parameter (for example, "adjmix_rain/") in the nhmParamDb/ directory. There are currently more than 120 parameter subdirectories in the NhmParamDb and this number may change as parameters are added or removed from PRMS over time. Each of these parameter subdirectories contain 19 subdirectories for each of the NHDPlus regions that cover the CONUS.

Each of the regional subdirectories contains two files. The first is the .csv file, written in Comma-Separated-Value format (CSV). This file has a tabular format, with the first column containing the index into the value array, numbered from one to the total number of values. The second column contains the corresponding parameter value for that index. After the first header row, there is one row for each element in the parameter array. The Extensible Markup Language (XML) file provides information about the version, and the dimension and size of the arrays in the .csv file. Regardless of the array dimension(s), the values in the .csv are written as a single vector, and mapped in Fortran-like index order, with the first index changing fastest, thereby written conterminously, and the last index changing slowest. The adjmix_rain.xml file example indicates that the values in the adjmix_rain.csv file are identified with version 0.8.0, and that the parameter is encoded as a two-dimensional array, with the first dimension being the number of Hydrologic Response Units (nhru = 109,951) and the second being the number of months in the year (nmonths = 12). The total number of values in the .csv file is 109,951 times 12, or 1,319,412. As the .csv and .xml files are edited by the user, different versions of the file can be assigned a number or name (called a "tag" in Git) and checked into the repository. Thus, any previous version of the parameter values can be recalled directly by tag or time stamp. The "parameters.xml" file is the last member of the "nhmParamDb/" directory. This file, written in XML, provides general information about the content of the parameter values. There is one line in this file for each of the parameter subdirectories. The adjmix_rain example shows the units, numerical type, model, description (desc), and help string for the parameter.

*[Figure 4. Major components of the National Hydrologic Model Parameter Database. Illustrates the top-level `nhmParamDb/` directory containing `.git/`, version-tag subdirectories (0.2.0/ through 0.8.0/), per-parameter subdirectories (adjmix_rain/, albset_rna/, albset_rnm/, ...), regional subdirectories (r01/ through r18/), the `parameters.xml` file, and example `adjmix_rain.csv` / `adjmix_rain.xml` file contents showing the two-dimensional nhru × nmonths structure.]*

### Accessing the NhmParamDb

The central NhmParamDb (called the "Origin" in Git) can be accessed through the USGS Bitbucket web page (https://my.usgs.gov/bitbucket/projects/MOWS/repos/nhmparamdb/browse, accessed January 2017). Public access to USGS Bitbucket requires a myUSGS account and the Git software. To download a copy of the Origin from USGS Bitbucket to a local computer, open a command window, move to the target directory, and execute the Git command:

```
git clone https://my.usgs.gov/bitbucket/scm/mows/nhmparamdb.git nhmParamDb
```

This creates a local copy of the NhmParamDb repository that has all previous versions of all parameters that have ever been checked into the Origin into the directory nhmParamDb. A few examples of useful Git functions and requisite commands are described:

1. To check if changes have been made to the local NhmParamDb, type the following from the command prompt, while located in the target directory: `git status`
2. To get new parameter values that have been updated in the Origin since the clone or the last pull, type: `git pull`
3. To commit changes to the users local NhmParamDb repository, type: `git add` then, `git commit`
4. To view the commit history of a directory or file in the local NhmParamDb, type: `git log`

Users that have developed new parameter values that they wish to share with other NHM users can either "push" a tagged version of their parameters to the Origin (requires permission from the repository administrator) or create their own new Origin which would be hosted on a Git server (USGS Bitbucket, github, etc.) of their choice.

A static and stand-alone download of the entire NhmParamDb repository is available on ScienceBase (Driscoll and others, 2017). This resource provides a snapshot in time of parameter values as the NhmParamDb repository may continue to be updated as methods and parameter values are improved. Releases include the date and the commit code in the release to allow for tracking back to the original repository.

---

## Extracting Subsets of the NHM-PRMS

A software tool called Bandit has been developed to support hydrologic modeling for any area-of-interest (model domain) within the NHM. Bandit automates the generation of a complete set of PRMS input files that contain the data and parameters required for a NHM-PRMS model. Users can identify one or more streamgages or stream segments to identify the outlet points of a model domain. Bandit generates parameter values for the upstream HRUs and segments from the NhmParamDb. This tool is written in the Python language and is designed to be run from the command line. Users do not need to download this software and instead can request a model subset as described below.

The Bandit software tool depends on a number of parameters that describe the feature connectivity of the GF network (table 3). These parameters are included in the generated Parameter File. To request a model subset from the NHM:

1. Go to the web page https://wwwbrr.cr.usgs.gov/projects/SW_MoWS (accessed September 30, 2017).
2. Click the 'Help' link, which will go to the page titled, "Modeling of Watershed Systems Request for Support or National Hydrologic Model Extraction."
3. Fill out the required information at the top of the page (Name, Email address, Organization, and optionally, Phone).
4. At the bottom of the page fill out the information for "Basin Outlet ID(s)" and optionally the "Upstream cutoff GeoSpatial Fabric National segment ID(s)" along with a short descriptive summary of your modeling application. At a minimum one or more national segment IDs (nhm_seg) should be entered for the outlet segment. To truncate the model subset at some point upstream from the outlet segments, enter one or more upstream cutoff national segment IDs. The national segment IDs are available as part of the GF file geodatabase which is available at http://dx.doi.org/doi:10.5066/F7542KMD.
5. Click 'Submit'; a member of the Modeling of Watershed Systems group will extract the model subset and email a zip file containing the model subset files to the user.

### Table 3. NHM Parameter Database (NhmParamDb) parameters used by the NHM Bandit software for model extraction.

[NHM, National Hydrologic Model; HRU, Hydrologic Response Unit; POI, Point of Interest; NWS, National Weather Service; NHDPlus, National Hydrography Plus Dataset; nhru, number of HRUs; nsegment, number of stream segments; npoigages, number of POI streamgages; USGS, U.S. Geological Survey; >, greater than]

| Parameter name | Description | Dimension | Type | Units |
|---|---|---|---|---|
| **hru_segment_nhm** | NHM segment index to which an HRU contributes lateral flows | nhru | integer | none |
| **nhm_id** | NHM HRU identifier | nhru | integer | none |
| **nhm_seg** | NHM segment identifier | nhru | integer | none |
| **poi_gage_id** | USGS streamgage identification number for each POI | npoigages | string | none |
| **poi_gage_segment** | Segment index for each POI gage | npoigages | integer | none |
| **poi_type** | Type code for each POI (1=USGS streamgage; 2=NWS River Forecast Center Node (http://water.weather.gov/ahps/download.php); 3=USGS SPARROW node (https://water.usgs.gov/nawqa/sparrow/); 4=outlet from or inlet to NHD-defined waterbody; 5=confluence of NHDPlus flowlines exceeding a Strahler stream order (Strahler, 1952, 1957) of four; 6=connectivity segment not in NHDPlus; 7=split segment from NHDPlus flowline because of travel time > 24 hours; 8=split segment from NHMPlus flowline because of change in elevation > 500 meters) | npoigages | integer | none |
| **tosegment_nhm** | NHM downstream segment identifier to which the segment streamflow flows; set to 0 for segments that do not flow to another segment. | nsegment | integer | none |

---

## Summary

Assessment of water resources for the conterminous United States requires hydrologic information derived using consistent, standardized methodologies. Small, watershed-scale model applications provide critical information towards understanding hydrologic processes at local scales. These models can be calibrated, evaluated, and validated on the basis of site-specific measurements that can reduce simulation uncertainty, and give good estimates of associated components of the hydrologic cycle. However, the ability to translate and compare the results of these local studies to regional and continental scales or even to adjacent watersheds can be difficult both in practicality and in the estimation of model uncertainty.

Derivation of default parameter values assigned to the National Hydrologic Model spatial units (Hydrologic Response Units and stream segments) for the U.S. Geological Survey National Hydrologic Model application of the Precipitation-Runoff Modeling System (NHM-PRMS) are computed on the basis of consistent, national datasets and methodologies. The default NHM-PRMS parameter values are maintained in the National Hydrologic Model Parameter Database. These values have been and will continue to be updated as additional methods are developed and as data become available. Earlier versions of parameter values in the National Hydrologic Model Parameter Database can be accessed using a version control system implemented in the Git software. Upon user request, subset models of the NHM-PRMS can be extracted using the National Hydrologic Model Bandit software tool. These subset models can be refined using locally informed approaches for parameterization and process simulation to address local-scale modeling needs.

---

## References Cited

Archfield, S.A., Clark, M., Arheimer, B., Hay, L.E., McMillan, H., Kiang, J.E., Seibert, J., Hakala, K., Bock, A., Wagener, T., Farmer, W.H., Anderassian, V., Attinger, S., Viglione, A., Knight, R., Markstrom, S.L., 2015, Accelerating advances in continental domain hydrologic modeling: Water Resources Research, v. 51, p. 10078–10091, http://dx.doi.org/10.1002/2015WR017498.

Blodgett, D.L., Booth, N.L., Kunicki, T.C., Walker, J.L., and Viger, R.J., 2011, Description and testing of the geo data portal—Data integration framework and Web processing services for environmental science collaboration: U.S. Geological Survey Open-File Report 2011–1157, 9 p., https://pubs.usgs.gov/of/2011/1157/.

Chacon, Scott, and Straub, Ben, 2014, Pro Git, (2d ed.): Apress, Berkeley, Calif., https://git-scm.com/book/en/v2.

Driscoll, J.M., Markstrom, S.L., Regan, R.S., Hay, L.E., Viger, R.J., 2017, National hydrologic model parameter database: U.S. Geological Survey, accessed May 5, 2017, at http://dx.doi.org/10.5066/F7NS0SCW.

Falcone, J.A., 2011, GAGES–II, Geospatial attributes of gages for evaluating streamflow: U.S. Geological Survey dataset, http://water.usgs.gov/GIS/metadata/usgswrd/XML/gagesII_Sept2011.xml.

Hamon, W.R., 1961, Estimating potential evapotranspiration: Proceedings of the American Society of Civil Engineers, Journal of the Hydraulic Division, v. 87, no. HY3, p. 107–120.

Hargreaves, G.H., and Samani, Z.A., 1985, Reference crop evapotranspiration from temperature: Applied Engineering in Agriculture, v. 1, no. 2, p. 96–99.

Homer, C., Dewitz, J., Fry, J., Coan, M., Hossain, N., Larson, C., Herold, N., McKerrow, A., VanDriel, J.N., and Wickham, J., 2007, Completion of the 2001 national land cover database for the conterminous United States: Photogrammetric Engineering and Remote Sensing, v. 73, no. 4, p. 337–341, http://www.mrlc.gov/nlcd01_data.php.

Jensen, M.E., and Haise, H.R., 1963, Estimating evapotranspiration from solar radiation: New York, American Society of Civil Engineers, Journal of Irrigation and Drainage, v. 89, p. 15–41.

Jobson, H.E., 1996, Prediction of traveltime and longitudinal dispersion in rivers and streams: U.S. Geological Survey Water Resources Investigations Report 96-4013, 69 p.

Leavesley, G.H., Lichty, R.W., Troutman, B.M., and Saindon, L.G., 1983, Precipitation-runoff modeling system—User's manual: U.S. Geological Survey Water-Resources Investigations Report 83–4238, 207 p.

Markstrom, S.L., Hay, L.E., Ward-Garrison, C.D., Risley, J.C., Battaglin, W.A., Bjerklie, D.M., Chase, K.J., Christiansen, D.E., Dudley, R.W., Hunt, R.J., Koczot, K.M., Mastin, M.C., Regan, R.S., Viger, R.J., Vining, K.C., and Walker, J.F., 2012, Integrated watershed-scale response to climate change for selected basins across the United States: U.S. Geological Survey Scientific Investigations Report 2011–5077, 143 p., https://pubs.usgs.gov/sir/2011/5077/SIR11-5077_508.pdf.

Markstrom, S.L., Regan, R.S., Hay, L.E., Viger, R.J., Webb, R.M.T., Payn, R.A., and LaFontaine, J.H., 2015, PRMS-IV, the precipitation-runoff modeling system, version 4: U.S. Geological Survey Techniques and Methods 6–B7, 158 p., https://dx.doi.org/10.3133/tm6B7.

Monteith, J.L., 1965, Evaporation and environment, in Fogg., B.D., ed., The state and movement of water in living organisms: Symposia for the Society of Experimental Biology, v. 19, p. 205–234.

Penman, H.L., 1948, Natural evaporation from open water, bare soil and grass: Proceedings of the Royal Society of London, England, series A, v. 193, p. 120–145.

Priestley, C.H.B., and Taylor, R.J., 1972, On the assessment of the surface heat flux and evaporation using large-scale parameters: Monthly Weather Review, v. 100, no. 2, p. 81–92.

Regan, R.S. and LaFontaine, J.H., 2017, Documentation of the dynamic parameter, water-use, stream and lake flow routing, and two summary output modules and updates to surface-depression storage simulation and initial conditions specification options with the precipitation-runoff modeling system (PRMS): U.S. Geological Survey Techniques and Methods, 6–B8, 60 p., https://pubs.er.usgs.gov/publication/tm6B8.

Strahler, A.N., 1952, Hypsometric (area-altitude) analysis of erosional topology: Geological Society of America Bulletin, v. 63, no. 11, p. 1117–1142.

Strahler, A.N., 1957, "Quantitative analysis of watershed geomorphology": Transactions of the American Geophysical Union, v. 38, no. 6, p. 913–920.

Thornton, P.E., Running, S.W., and White, M.A., 1997, Generating surfaces of daily meteorological variables over large regions of complex terrain: Journal of Hydrology, v. 190, no. 3, p. 214–251.

Thornton, P.E., Thornton, M.M., Mayer, B.W., Wei, Y., Devarakonda, R., Vose, R.S., and Cook, R.B., 2016, DAYMET—daily surface weather data on a 1-km grid for North America, Version 3: Oak Ridge, Tenn., Oak Ridge National Laboratory, Distributed Active Archive Center dataset, https://dx.doi.org/10.3334/ORNLDAAC/1328.

U.S. Geological Survey, 2016, National Water Information System (NWISWeb)—USGS surface-water data for the Nation: U.S. Geological Survey database, http://waterdata.usgs.gov/usa/nwis/sw.

U.S. Geological Survey, 2016, The National Map, NHDPlus High Resolution, https://nhd.usgs.gov/NHDPlus_HR.html.

Viger, R.J., 2014, Preliminary spatial parameters for PRMS based on the geospatial fabric, NLCD2001 and SSURGO: U.S. Geological Survey data release, http://dx.doi.org/doi:10.5066/F7WM1BF7.

Viger, R.J., and Bock, Andrew, 2014, GIS features of the geospatial fabric for national hydrologic modeling: U.S. Geological Survey data release, http://dx.doi.org/doi:10.5066/F7542KMD.

Viger, R.J., and Leavesley, G.H., 2007, The GIS Weasel user's manual: U.S. Geological Survey Techniques and Methods, 6–B4, 201 p.

World Wide Web Consortium, 2008, Extensible markup language (XML) 1.0 (Fifth Edition): W3C, Recommendation 26 November 2008, https://www.w3.org/TR/REC-xml/.

World Wide Web Consortium, 2016, CSV on the web—A primer: W3C CSV on the Web Working Group, Note 25, https://www.w3.org/TR/tabular-data-primer/.

---

## Glossary

The following terms are used to reference components of the Precipitation-Runoff Modeling System (PRMS). See appendix 1 of U.S. Geological Survey Techniques and Methods, book 6, chapter B7 (https://dx.doi.org/10.3133/tm6b7) for descriptions of modules, parameters, simulation algorithms, and computed variables for PRMS, version 4.

- **Flux variables** are calculated flow rates.
- **HRU** refers to Hydrologic Response Unit, the primary spatial unit for which a PRMS application is discretized.
- **Hydrologic response** refers to the computed water storage and flow from and to the atmosphere, plant canopy, land surface, snowpack, surface depressions, shallow subsurface zone, deep aquifers, stream segments, and lakes.
- **Initial conditions** refer to all states and fluxes required to initiate a PRMS simulation.
- **Parameter** refers to preprocessed input values that characterize physical and topological attributes of the application domain and spatial and temporal computation coefficients of simulation algorithms.
- **PRMS** is referred to as a hydrologic simulation code, whereas the associated input and output files, discretization, and executable are referred to as models or applications.
- **Reservoir** refers to the conceptual water-storage capacity of each zone within HRUs, such as the capillary reservoir, groundwater reservoir, gravity reservoir, and preferential-flow reservoir, and not a surface-water body used for storage and regulation.
- **State variables** are calculated water-content storages.

---

# Appendix 1. Derivation of Parameter Values for the National Hydrologic Model (NHM) Precipitation-Runoff Modeling System (PRMS) Application

## Introduction

This Appendix describes all parameters related to the modules and options used for the U.S. Geological Survey National Hydrologic Model application of the Precipitation-Runoff Modeling System (NHM-PRMS). Sensitivity analysis using the Fourier Amplitude Sensitivity Test algorithm demonstrated variance of the importance of parameters across the conterminous United States (CONUS) (Markstrom and others, 2016). Based on this analysis, estimation methods were developed to compute spatially and temporally distributed values across the CONUS. NHM-PRMS parameter values were set using the following methods:

- assigned long-established defaults (see Markstrom and others, 2015; Leavesley and others, 1996; and Leavesley and others, 1983) on the basis of hydrologic and geographic intelligence garnered from more than 30 years of research and operation activities,
- estimated using methods described in Viger and Leavesley (2007) and Viger (2014) that characterize topography, land cover, soils, geology, and hydrography,
- direct solution of the Precipitation-Runoff Modeling System (PRMS) algorithms for unknown variables on the basis of national datasets,
- set to averaged values of the last 20 years of a 34-year PRMS simulation, and
- computation of values based on other hydrologic simulation results.

The set of active PRMS modules and computation options dictate the required set of input parameters. The NHM-PRMS set of modules and computation options are:

- air temperature and precipitation distribution — (temp_module and precip_module = `climate_hru`),
- solar radiation — degree-day method (solrad_module = `ddsolrad`),
- potential evapotranspiration — Jensen-Haise method (et_module = `potet_jh`),
- transpiration period — temperature index method (transp_module = `transp_tindex`),
- Hortonian surface runoff — soil-moisture index method (srunoff_module = `srunoff_smidx`),
- surface-depression storage — dprst_flag = 1, and
- streamflow routing — basin summary method (strmflow_module = `strmflow`).

PRMS parameters can be dimensioned as spatial, temporal, spatial and temporal, or nonspatial and nontemporal. Each parameter has a default value and can be specified as the default, another constant value, or a computed value. For descriptions of the array dimension(s) associated with each parameter, the values specified in the Control File to select modules and computation options, and all the PRMS parameters see table 1–1, table 1–2, and table 1–3, respectively in Markstrom and others (2015).

The NHM-PRMS parameters described in this report are maintained in the National Hydrologic Model Parameter Database (NhmParamDb). Priority to update parameters is given to those that are shown to have a high degree of sensitivity. Some PRMS parameters are specified as a single or constant value even though they may have spatial and temporal dimensions. Single value parameters are flags that specify model units and output options (for example, temp_units, precip_units, and print_type). Constant-value parameters are assigned long-established defaults (for example, adjmix_rain, ppt_rad_adj, epan_coef, albset_rnm, melt_force, imperv_stor_max, and ssr2gw_exp). As new methods are developed for estimation the constant-value parameters, the NhmParamDb will be updated. The remaining NHM-PRMS parameters are described in table 1–1. Computed parameters are described in table 1–2. Note, that some constant-value and computed parameters can have a temporal component; that is, they are specified using a spatial and a temporal dimension (for example, a parameter may be dimensioned by the number of Hydrologic Response Units [HRUs] and the number of months in a year [nhru, nmonths]).

The NhmParamDb includes some parameters that are not used by the NHM-PRMS based on the active modules and computation options. These parameters are available for use with other PRMS modules and software. They primarily provide mapping between National Hydrography Dataset Plus (NHDPlus) regional HRUs and stream segments to the full set of identification numbers in the NHM (for example, nhm_id, nhm_seg, and tosegment_nhm). Other parameters are for use with inactive PRMS modules, for example, `muskingum` (hru_segment, K_coef, tosegment, and x_coef) and `transp_frost` (fall_frost and spring_frost) and mapping between points of interest and the NHM stream network (poi_gage_id, poi_gage_segment, and poi_type). Table 1–3 describes these auxiliary parameters.

### Table 1–1. Description of constant-value parameters in the National Hydrologic Model (NHM) application of the Precipitation-Runoff Modeling System (PRMS).

[HRU, Hydrologic Response Unit; nhru, number of HRUs; nmonths, constant equal to 12; one, constant equal to 1; dday, degree days; temp_units, temperature units flag; nsegment, number of stream segments; ngw, number of groundwater reservoirs, equal to nhru; nssr, number of gravity reservoirs, equal to nhru; cfs, cubic feet per second; cms, cubic meters per second; gm/cm³, grams per cubic centimeter; GWR, groundwater reservoir; CBH, Climate-by-HRU]

| Name | Description | Dimension | Units | NHM Value |
|---|---|---|---|---|
| **Precipitation and temperature** | | | | |
| **adjmix_rain** | Monthly (January to December) factor to adjust rain proportion in a mixed rain/snow event | nhru, nmonths | decimal fraction | 1.0 |
| **rain_cbh_adj** | Monthly (January to December) adjustment factor to measured precipitation determined to be rain on each HRU to account for differences in elevation, and so forth | nhru, nmonths | decimal fraction | 1.0 |
| **snow_cbh_adj** | Monthly (January to December) adjustment factor to measured precipitation determined to be snow on each HRU to account for differences in elevation, and so forth | nhru, nmonths | decimal fraction | 1.0 |
| **tmax_cbh_adj** | Monthly (January to December) adjustment factor to maximum air temperature for each HRU, estimated on the basis of slope and aspect | nhru, nmonths | temp_units | 0.0 |
| **tmin_cbh_adj** | Monthly (January to December) adjustment factor to minimum air temperature for each HRU, estimated on the basis of slope and aspect | nhru, nmonths | temp_units | 0.0 |
| **Solar radiation** | | | | |
| **ppt_rad_adj** | Monthly minimum precipitation, if HRU precipitation exceeds this value, radiation is multiplied by radj_sppt or radj_wppt adjustment factor | nhru, nmonths | inches | 0.02 |
| **radadj_intcp** | Monthly (January to December) intercept in air temperature range adjustment to degree-day equation for each HRU | nhru, nmonths | dday | 1.0 |
| **radadj_slope** | Monthly (January to December) slope in air temperature range adjustment to degree-day equation for each HRU | nhru, nmonths | dday/temp_units | 0.02 |
| **radj_sppt** | Adjustment factor for computed solar radiation for summer day with greater than ppt_rad_adj inches of precipitation for each HRU | nhru | decimal fraction | 0.44 |
| **radj_wppt** | Adjustment factor for computed solar radiation for winter day with greater than ppt_rad_adj inches of precipitation for each HRU | nhru | decimal fraction | 0.5 |
| **radmax** | Monthly (January to December) maximum fraction of the potential solar radiation that may reach the ground due to haze, dust, smog, and so forth, for each HRU | nhru, nmonths | decimal fraction | 0.8 |
| **tmax_index** | Monthly (January to December) index temperature used to determine precipitation adjustments to solar radiation for each HRU | nhru, nmonths | temp_units | 50.0 |
| **Transpiration** | | | | |
| **transp_beg** | Month to begin summing the maximum air temperature for each HRU; when sum is greater than or equal to transp_tmax, transpiration begins | nhru | month | 1 |
| **transp_end** | Month to stop transpiration computations; transpiration is computed thru end of previous month | nhru | month | 13 |
| **transp_tmax** | Temperature index to determine the specific date of the start of the transpiration period; the maximum air temperature for each HRU is summed starting with the first day of month transp_beg; when the sum exceeds this index, transpiration begins | nhru | temp_units | 1.0 |
| **Evaporation** | | | | |
| **epan_coef** | Monthly (January to December) evaporation pan coefficient for each HRU | nhru, nmonths | decimal fraction | 1.0 |
| **Snow computations** | | | | |
| **albset_rna** | Fraction of rain in a mixed precipitation event above which the snow albedo is not reset; applied during the snowpack accumulation stage | one | decimal fraction | 0.8 |
| **albset_rnm** | Fraction of rain in a mixed precipitation event above which the snow albedo is not reset; applied during the snowpack melt stage | one | decimal fraction | 0.6 |
| **albset_sna** | Minimum snowfall, in water equivalent, needed to reset snow albedo during the snowpack accumulation stage | one | inches | 0.05 |
| **albset_snm** | Minimum snowfall, in water equivalent, needed to reset snow albedo during the snowpack melt stage | one | inches | 0.2 |
| **cecn_coef** | Monthly (January to December) convection condensation energy coefficient for each HRU | nhru, nmonths | calories per degree Celsius above 0 | 5.0 |
| **den_init** | Initial density of new-fallen snow | one | gm/cm³ | 0.1 |
| **den_max** | Average maximum snowpack density | one | gm/cm³ | 0.6 |
| **emis_noppt** | Average emissivity of air on days without precipitation for each HRU | nhru | decimal fraction | 0.757 |
| **freeh2o_cap** | Free-water holding capacity of snowpack for each HRU, expressed as a decimal fraction of the frozen water content of the snowpack (*pk_ice*) | nhru | decimal fraction | 0.05 |
| **melt_force** | Julian date to force snowpack to spring snowmelt stage for each HRU; varies with region depending on length of time that permanent snowpack exists | nhru | Julian day | 140 |
| **melt_look** | Julian date to start looking for spring snowmelt stage for each HRU; varies with region depending on length of time that permanent snowpack exists | nhru | Julian day | 90 |
| **potet_sublim** | Fraction of potential evapotranspiration that is sublimated from snow in the canopy and snowpack for each HRU | nhru | decimal fraction | 0.1 |
| **settle_const** | Snowpack settlement time constant | one | decimal fraction | 0.1 |
| **snowinfil_max** | Maximum snow infiltration per day for each HRU | nhru | inches | 2.0 |
| **Hortonian surface runoff and impervious surfaces** | | | | |
| **imperv_stor_max** | Maximum impervious area retention storage for each HRU | nhru | inches | 0.05 |
| **Surface depression storage** | | | | |
| **dprst_depth_avg** | Average depth of surface depressions at maximum storage capacity | nhru | inches | 132.0 |
| **dprst_frac_clos** | Fraction of closed surface-depression storage area within an HRU; this storage does not produce lateral flows | nhru | decimal fraction | 1.0 |
| **dprst_frac_open** | Fraction of open surface-depression storage area within an HRU that can generate surface runoff as a function of storage volume | nhru | decimal fraction | 1.0 |
| **va_clos_exp** | Coefficient in the exponential equation relating maximum surface area to the fraction that closed depressions are full to compute current surface area for each HRU; 0.001 is an approximate rectangle; 1.0 is a triangle | nhru | none | 0.001 |
| **va_open_exp** | Coefficient in the exponential equation relating maximum surface area to the fraction that open depressions are full to compute current surface area for each HRU; 0.001 is an approximate rectangle; 1.0 is a triangle | nhru | none | 0.001 |
| **Soil zone storage, interflow, recharge, Dunnian surface runoff** | | | | |
| **fastcoef_sq** | Non-linear coefficient in equation used to route preferential-flow storage downslope for each HRU | nhru | none | 0.8 |
| **pref_flow_den** | Fraction of the soil zone in which preferential flow occurs for each HRU | nhru | decimal fraction | 0.0 |
| **sat_threshold** | Water holding capacity of the gravity and preferential-flow reservoirs; difference between field capacity and total soil saturation for each HRU | nhru | inches | (none listed) |
| **slowcoef_sq** | Non-linear coefficient in equation to route gravity-reservoir storage downslope for each HRU | nhru | none | 0.1 |
| **ssr2gw_exp** | Non-linear coefficient in equation used to route water from the gravity reservoir to the GWR for each HRU | nssr | none | 1.2 |
| **Groundwater Flow** | | | | |
| **gwsink_coef** | Linear coefficient in the equation to compute outflow to the groundwater sink for each GWR | ngw | fraction/day | 0.0 |
| **gwstor_min** | Minimum storage in each GWR to ensure storage is greater than specified value to account for inflow from deep aquifers or injection wells with the water source outside the basin | ngw | inches | 0.0 |
| **Streamflow** | | | | |
| **obsin_segment** | Index of measured streamflow station that replaces inflow to a segment | nsegment | none | 0 |
| **segment_flow_init** | Initial flow in each stream segment | nsegment | cfs | 0.0 |
| **segment_type** | Segment type (0=segment; 1=diversion; 2=lake; 3=replace inflow) | nsegment | none | 0 |
| **Flags** | | | | |
| **elev_units** | Flag to indicate the units of the elevation values (0=feet; 1=meters) | one | none | 1 |
| **outlet_sta** | Index of measured streamflow station corresponding to the basin outlet | one | none | 1 |
| **precip_units** | Flag to indicate the units of the precipitation values (0=inches; 1=millimeter) | one | none | 0 |
| **print_freq** | Flag to select the output frequency; for combinations, add index numbers, for example, daily plus yearly = 10; yearly plus total=3 (0=none; 1=run totals; 2=yearly; 4=monthly; 8=daily; or additive combinations) | one | none | 3 |
| **print_type** | Flag to select the type of results written to the output file (0=measured and simulated flow only; 1=water balance table; 2=detailed output) | one | none | 1 |
| **runoff_units** | Measured streamflow units (0=cfs; 1=cms) | one | none | 0 |
| **temp_units** | Flag to indicate the units of measured air-temperature values (0=Fahrenheit; 1=Celsius) | one | none | 0 |

### Table 1–2. Description of computed parameters in the National Hydrologic Model (NHM) application of the Precipitation-Runoff Modeling System (PRMS).

[HRU, Hydrologic Response Unit; nhru, number of HRUs; nmonths, constant equal to 12; ngw, number of groundwater reservoirs, equal to nhru]

| Name | Description | Dimension | Units |
|---|---|---|---|
| **Topographic** | | | |
| **hru_area** | Area of each HRU | nhru | acres |
| **hru_aspect** | Aspect of each HRU | nhru | angular degrees |
| **hru_elev** | Mean elevation for each HRU | nhru | meters |
| **hru_lat** | Latitude of each HRU | nhru | degrees North |
| **hru_slope** | Slope of each HRU, specified as change in vertical length divided by change in horizontal length | nhru | decimal fraction |
| **hru_type** | Type of each HRU (0=inactive; 1=land; 2=lake; 3=swale) | nhru | none |
| **Air Temperature** | | | |
| **tmax_allrain_offset** | Monthly (January to December) maximum air temperature when precipitation is assumed to be rain; if HRU air temperature is greater than or equal to tmax_allsnow plus this value, precipitation is rain | nhru, nmonths | degrees Fahrenheit |
| **tmax_allsnow** | Maximum air temperature when precipitation is assumed to be snow; if HRU air temperature is less than or equal to this value, precipitation is snow | nhru, nmonths | degrees Fahrenheit |
| **Solar Radiation** | | | |
| **dday_intcp** | Monthly (January to December) intercept in degree-day equation for each HRU | nhru, nmonths | dday |
| **dday_slope** | Monthly (January to December) slope in degree-day equation for each HRU | nhru, nmonths | dday/degrees Fahrenheit |
| **Potential Evapotranspiration** | | | |
| **jh_coef** | Monthly (January to December) air temperature coefficient used in Jensen-Haise potential evapotranspiration computations for each HRU | nhru, nmonths | per degrees Fahrenheit |
| **jh_coef_hru** | Air temperature coefficient used in Jensen-Haise potential evapotranspiration computations for each HRU | nhru | (none listed) |
| **Interception** | | | |
| **cov_type** | Vegetation cover type for each HRU (0=bare soil; 1=grasses; 2=shrubs; 3=trees; 4=coniferous) | nhru | none |
| **covden_sum** | Summer vegetation cover density for the major vegetation type in each HRU | nhru | decimal fraction |
| **covden_win** | Winter vegetation cover density for the major vegetation type in each HRU | nhru | decimal fraction |
| **snow_intcp** | Snow interception storage capacity for the major vegetation type in each HRU | nhru | inches |
| **srain_intcp** | Summer rain interception storage capacity for the major vegetation type in each HRU | nhru | inches |
| **wrain_intcp** | Winter rain interception storage capacity for the major vegetation type in each HRU | nhru | inches |
| **Snow Computations** | | | |
| **hru_deplcrv** | Index number for the snowpack areal depletion curve associated with each HRU | nhru | none |
| **rad_trncf** | Transmission coefficient for short-wave radiation through the winter vegetation canopy | nhru | decimal fraction |
| **snarea_curve** | Snow area depletion curve values, 11 values for each curve (0.0 to 1.0 in 0.1 increments) | nhru, 11 | decimal fraction |
| **snarea_thresh** | Maximum threshold snowpack water equivalent below which the snow-covered-area curve is applied | nhru | inches |
| **snowpack_init** | Storage of snowpack in each HRU at the beginning of a simulation | nhru | inches |
| **tstorm_mo** | Monthly flag (January to December) for prevalent storm type for each HRU (0=frontal storms; 1=convective storms) | nhru, nmonths | none |
| **Hortonian Surface Runoff and Impervious Surfaces** | | | |
| **carea_max** | Maximum possible area contributing to surface runoff expressed as a portion of the HRU area | nhru | decimal fraction |
| **hru_percent_imperv** | Fraction of each HRU area that is impervious | nhru | decimal fraction |
| **smidx_coef** | Coefficient in non-linear contributing area algorithm for each HRU | nhru | decimal fraction |
| **smidx_exp** | Exponent in non-linear contributing area algorithm for each HRU | nhru | 1.0/inch |
| **Surface-Depression Storage** | | | |
| **dprst_et_coef** | Fraction of unsatisfied potential evapotranspiration to apply to surface-depression storage | nhru | decimal fraction |
| **dprst_flow_coef** | Coefficient in linear flow routing equation for open surface depressions for each HRU | nhru | fraction/day |
| **dprst_frac** | Fraction of each HRU area that has surface depressions | nhru | decimal fraction |
| **dprst_frac_init** | Fraction of maximum surface-depression storage that contains water at the start of a simulation | nhru | decimal fraction |
| **dprst_seep_rate_clos** | Coefficient used in linear seepage flow equation for closed surface depressions for each HRU | nhru | fraction/day |
| **dprst_seep_rate_open** | Coefficient used in linear seepage flow equation for open surface depressions for each HRU | nhru | fraction/day |
| **op_flow_thres** | Fraction of open depression storage above which surface runoff occurs; any water above maximum open storage capacity spills as surface runoff | nhru | decimal fraction |
| **sro_to_dprst_imperv** | Fraction of impervious surface runoff that flows into surface-depression storage; the remainder flows to a stream network for each HRU | nhru | decimal fraction |
| **sro_to_dprst_perv** | Fraction of pervious surface runoff that flows into surface-depression storage; the remainder flows to a stream network for each HRU | nhru | decimal fraction |
| **Soil Zone Storage, Interflow, Recharge, Dunnian Surface Runoff** | | | |
| **fastcoef_lin** | Linear coefficient in equation to route preferential-flow storage downslope for each HRU | nhru | fraction/day |
| **sat_threshold** | Water holding capacity of the gravity and preferential-flow reservoirs; difference between field capacity and total soil saturation for each HRU | nhru | inches |
| **slowcoef_lin** | Linear coefficient in equation to route gravity-reservoir storage downslope for each HRU | nhru | fraction/day |
| **soil2gw_max** | Maximum amount of the capillary reservoir excess that is routed directly to the GWR for each HRU | nhru | inches |
| **soil_moist_init_frac** | Initial fraction of the capillary reservoir maximum water content for each HRU | nhru | decimal fraction |
| **soil_moist_max** | Maximum available water holding capacity of capillary reservoir from land surface to rooting depth of the major vegetation type of each HRU | nhru | inches |
| **soil_rechr_frac** | Initial fraction of the capillary reservoir maximum water content for each HRU | nhru | decimal fraction |
| **soil_rechr_init_frac** | Initial fraction of the soil recharge zone maximum water content for each HRU | nhru | decimal fraction |
| **soil_rechr_max_frac** | Maximum storage for soil recharge zone (upper portion of capillary reservoir where losses occur as both evaporation and transpiration) as a fraction of soil_moist_max | nhru | inches |
| **soil_type** | Soil type of each HRU (1=sand; 2=loam; 3=clay) | nhru | none |
| **ssr2gw_rate** | Linear coefficient in equation used to route water from the gravity reservoir to the GWR for each HRU | nhru | fraction/day |
| **ssstor_init_frac** | Initial fraction of the gravity and preferential-flow reservoirs maximum water content for each HRU | nhru | decimal fraction |
| **Groundwater Flow** | | | |
| **gwflow_coef** | Linear coefficient in the equation to compute groundwater discharge for each GWR | ngw | fraction/day |
| **gwstor_init** | Storage in each GWR at the beginning of a simulation | ngw | inches |

## Geospatial Fabric Attribute Tables

The set of attribute tables for the U.S. Geological Survey (USGS) Geospatial Fabric for National Hydrologic Modeling version 1.0 (GF; Viger, 2014) is referred to in this report as the preliminary spatial parameters. The term preliminary is used here to indicate that the GF attribute values can be updated as additional parameter estimation methods are developed and may need to be evaluated for each modeling application. These parameter were named, and values were derived, according to methodologies originally encoded within the GIS Weasel (Viger and Leavesley, 2007). These GF parameter values were the first version of many of the parameters in the NhmParamDb. The tables can be accessed from http://dx.doi.org/doi:10.5066/F7WM1BF7. Although most attributes associated with the HRUs have been published as part of the tables in Viger (2014), a few have been included in the features of Viger and Bock (2014). These attributes rely exclusively on spatial information, such as geographic or relative location, or the geometry of the features.

The soils attribute values in the GF were derived on the basis of the Soil Survey Geographic Database (SSURGO, U.S. Department of Agriculture, Natural Resources Conservation Service, 2013). The land-cover attribute values were derived on the basis of the National Land Cover Database from the year 2001 (NLCD2001; Homer and others, 2007). The subsurface flow attribute values were derived on the basis of a map of hydrogeology and permeability information (Gleeson and others, 2011). The surface-depression storage attribute values were derived on the basis of the high-resolution version of the National Hydrography Data (NHD; McDonald and others, 2012) as the aggregate sum of waterbodies within each HRU. See Viger and others (2010) and Regan and LaFontaine (2017) for a description of surface-depression storage derivation for use with PRMS.

## Topographic and Geographic Parameters

A number of parameters were derived based on the basic geometry or location of each HRU and stream segment. These include area (in acres), latitude and longitude (in degrees North and East, respectively), and the projected coordinates of each HRU centroid (in Albers meters, with the centroid forced to fall within each HRU polygon; parameters **hru_area**, **hru_lat**, **hru_lon**, **hru_x**, **hru_y**, respectively. Mean HRU elevation (**hru_elev**) was derived using the outline of the GF-defined HRU and the Digital Elevation Model (DEM) supplied with NHDPlus version 1.0.

The mean orientation (**hru_aspect**) of the predominant down slope direction of each HRU is expressed as (0–360) degrees clockwise from north. For each cell in the DEM, aspect is calculated as described at http://resources.arcgis.com/en/help/main/10.1/index.html#/Aspect/009z000000tr000000/. Then the trigonometric sine and cosine of each cell's aspect are derived to create two new rasters of values. The average value for both of these raster values is determined for each HRU. The hru_aspect value is then set to the inverse tangent of these two values `atan2[sin(aspect), cos(aspect)]`.

The mean slope (**hru_slope**) of each HRU, expressed as decimal fraction rise (over run), is derived as the mean DEM cell slope within each HRU. Slope for the individual cells was determined as described at http://resources.arcgis.com/en/help/main/10.1/index.html#//009z000000v2000000.

## Initial Water Content Parameters

Hydrologic conditions (model states such as soil zone, snowpack, and saturated-zone water content at a point in time) can take years to equilibrate with variable climatic conditions. Typically, surface-water hydrologic simulation codes set initial values for most states to 0.0 with the option to specify some values, such as water content of subsurface reservoirs. Simulation start times are generally set to a time at which water content is at a minimum, such as the first day of a water year, to minimize the effect of unknown antecedent conditions. A water year is the period from October 1 to September 30 and is identified by the year in which the period ends.

Initial values for the water content fraction of surface-depression storage (**dprst_frac_init**), capillary reservoir storage (**soil_moist_init_frac**), recharge zone storage of the capillary reservoir (**soil_rechr_init_frac**), gravity reservoir storage (**ssstor_init_frac**), the water content of groundwater reservoir storage (**gwstor_init**), and the snowpack (**snowpack_init**) of each HRU were computed based on a 34-year simulation (water years 1980–2014). The first 14 years of the simulation were used as a warm-up period. These values for those water-holding reservoirs were set to the average September mean monthly value for the last 20 years of the simulation. Initial water content of the 34-year simulation of the canopy and impervious areas are set to zero in the PRMS code. Other values were set to PRMS default values (dprst_frac_init, snowpack_init, gwstor_init, and segment_flow_init) and others (soil_moist_init_frac, soil_rechr_init_frac, and ssstor_init_frac) set to the fraction of the associated water-holding reservoirs estimated using methods described in Viger and Leavesley (2007). Users can set these initial values by using the PRMS restart option that provides a method to execute and spin-up simulation that saves the states and fluxes, which then can be used as the antecedent conditions of a restart simulation. See Regan and LaFontaine (2017) for a description of the restart option.

### Table 1–3. Description of parameters in the NHM Parameter Database (NhmParamDb) not used by the active modules for NHM-PRMS simulations.

[NHM, National Hydrologic Model; HRU, Hydrologic Response Unit; nhru, number of HRUs; one, constant equal to 1; nsegment, number of stream segments; npoigages, number of points of interest; ID, identification number; USGS, U.S. Geological Survey; POI, point of interest]

| Name | Description | Dimension | Units |
|---|---|---|---|
| **Topographic** | | | |
| **basin_fall_frost** | The basin average solar date of the first killing frost of the fall | one | Solar day |
| **basin_spring_frost** | The basin average solar date of the last killing frost of the fall | one | Solar day |
| **fall_frost** | The solar date (number of days after winter solstice) of the first killing frost of the fall | nhru | Solar day |
| **hru_lon** | Longitude of each HRU | nhru | degrees East |
| **hru_segment_nhm** | National Hydrologic Model Segment index to which an HRU contributes lateral flows (surface runoff, interflow, and groundwater discharge) | nhru | none |
| **hru_segment** | Local model segment index to which an HRU contributes lateral flows (surface runoff, interflow, and groundwater discharge) | nhru | none |
| **hru_x** | Longitude (X) for each HRU in albers projection | nhru | meters |
| **hru_y** | Latitude (Y) for each HRU in albers projection | nhru | meters |
| **K_coef** | Travel time of flood wave from one segment to the next downstream segment, called the Muskingum storage coefficient; enter 1.0 for reservoirs, diversions, and segment(s) flowing out of the basin | nsegment | hours |
| **nhm_id** | National Hydrologic Model HRU ID | nhru | none |
| **nhm_seg** | National Hydrologic Model segment ID | nsegment | none |
| **poi_gage_id** | USGS stream gage for each POI gage | npoigages | string |
| **poi_gage_segment** | Local model segment index for each POI gage | npoigages | none |
| **poi_type** | Type code for each point-of-interest (POI) gage (0=not used for calibration; 1=used for calibration; 2=used for flow replacement) | npoigages | none |
| **spring_frost** | The solar date (number of days after winter solstice) of the last killing frost of the fall | nhru | Solar day |
| **tosegment** | Index of downstream segment to which the segment streamflow flows; enter 0 for segments that do not flow to another segment | nsegment | none |
| **tosegment_nhm** | National Hydrologic Model downstream segment ID | nsegment | none |
| **x_coef** | The amount of attenuation of the flow wave, called the Muskingum routing weighting factor; enter 0.0 for reservoirs, diversions, and segment(s) flowing out of the basin | nsegment | decimal fraction |

## Climate-Based Parameters

The **tmax_allsnow** and **tmax_allrain_offset** parameters are used to partition precipitation into rain, mixed rain and snow, or snow. The parameter values were computed on the basis of precipitation-phase data sets and the monthly average of the daily maximum air temperature for each HRU. The tmax_allsnow parameter establishes the monthly average of the daily maximum air temperature threshold below which occurrences of daily precipitation are considered entirely snow. The tmax_allrain_offset is combined with tmax_allsnow to provide the air temperature threshold above which daily precipitation occurs as rain. Precipitation that occurs when daily maximum air temperature is between tmax_allsnow and tmax_allrain_offset is partitioned as a mix of rain and snow precipitation phases.

Values for tmax_allsnow and tmax_allrain_offset in the NHM were derived on the basis of relations between precipitation occurrence and phase and the daily maximum air temperature using two gridded products. The Snow Data Assimilation System (SNODAS; National Operational Hydrologic Remote Sensing Center, 2004) is designed to ingest satellite, airborne, and ground-based observations of snow cover and snow-water equivalent (SWE) and produce simulations of snow cover at a 1-kilometer grid spacing for the CONUS. Model output from SNODAS includes precipitation amounts of rain and snow precipitation phases as separate variables at a daily time step. Estimates of daily maximum air temperature and precipitation were obtained from Daily Surface Weather and Climatological Summaries (DAYMET), which is a gridded product with a 1-kilometer grid spacing created from daily meteorological observations (Thornton and others, 1997). Precipitation and air temperature for the CONUS for 2004–2014 were obtained from the SNODAS and DAYMET products for computing values for the tmax_allsnow and tmax_allrain_offset parameters.

Daily occurrences of rain and snow from SNODAS were used to create three masks based on precipitation phase: (1) rain, (2) snow, and (3) mixed rain and snow. The GeoData Portal (GDP; https://cida.usgs.gov/gdp; Blodgett and others, 2011) was used to create daily masks of rain, snow, and mixed rain and snow events by Hydrologic Response Unit (HRU). The GDP was also used to create area weighted averages by HRU from DAYMET precipitation and daily maximum air temperature variables. The mean monthly values for tmax_allsnow and tmax_allrain_offset are computed using DAYMET daily maximum air temperatures masked by: (1) SNODAS rain-snow mixed events and (2) DAYMET precipitation events; any remaining missing values are then set to a default air temperature depending on the type of precipitation event. This multistep process is used to ensure there are no missing values for the tmax_allsnow or tmax_allrain_offset parameters.

Daily maximum air temperatures from DAYMET were restricted between 28.2 and 34.24 °Fahrenheit (F) as a starting point for computing values for the tmax_allsnow parameter. Air temperature values for non-snow days (for example, rain or rain-snow mix) were then filtered using the SNODAS snow event mask. The monthly mean of daily maximum air temperatures then was computed for each HRU. Missing air temperature values were filled with the monthly mean of daily maximum air temperature values filtered by DAYMET precipitation days, with any remaining missing values assigned a default air temperature of 32.0 °F. The tmax_allsnow parameter was defined by the resulting dataset of monthly mean of daily maximum air temperatures where snow precipitation events occurred.

The process for tmax_allrain_offset is similar to tmax_allsnow. Daily maximum air temperatures were restricted between 34.7 °F and 38.0 °F for the tmax_allrain_offset parameter. Air temperature values for rain events were further filtered using the SNODAS rain event mask. The monthly mean of daily maximum air temperatures then was computed by HRU. Missing air temperature values were filled from monthly mean air temperature values filtered by DAYMET precipitation days, with any remaining missing values assigned a default air temperature of 38.0 °F. The tmax_allrain_offset parameter was defined by the resulting dataset of monthly mean daily maximum air temperatures minus the tmax_allsnow parameter.

## Solar Radiation Parameters

Computed daily shortwave radiation for each HRU is estimated by using the `ddsolrad` module (Markstrom and others, 2015), a modification of the degree-day method described by Leaf and Brink (1973). This method was developed for the Rocky Mountain region of the United States. It is most applicable to regions where predominantly clear skies prevail on days without precipitation. However, this module has been applied to the entire CONUS, using the NHM, with good results.

This method uses a graphical approach where two parameters (**dday_intcp** and **dday_slope**) are used to define a function that relates daily maximum air temperature to the daily ratio between the actual and clear sky solar radiation (see Markstrom and others, 2015, fig. 3–1). As daily maximum air temperature and clear sky solar radiation have been previously computed by other PRMS modules, the daily horizontal surface solar radiation value, for each HRU, is computed by the ddsolrad module.

Mean-monthly values of solar radiation have been compiled for the CONUS by the National Renewable Energy Laboratory (NREL, Wilcox, 2012, National Renewable Energy Laboratory 1992, accessed December 2016) and were summarized to the resolution of the NHM GF through the USGS GDP (Blodgett and others, 2011; accessed December 2016). These values were used to compute the parameter values used by the ddsolrad module for a 35-year PRMS NHM simulation (calendar years 1980–2014) forced with DAYMET gridded station data (Thornton and others, 2016; accessed December 2016). The parameter dday_intcp is assigned values between the minimum and maximum values based on the intensity of the solar radiation (Markstrom and others, 2015, table 1–3) for the corresponding month of the year. Along with precipitation-day information, the degree-day shortwave radiation equations (Markstrom and others, 2015, eqs. 1–45 and 1–46) can be solved for the ratio of actual to potential solar radiation (PRMS variable *solf*). This value, with the monthly mean HRU daily maximum air temperature from DAYMET, are used to compute the dday_slope parameter (Markstrom and others, 2015, fig. 1–3).

The parameter dday_intcp is used to define the Y-axis intercept in the functional relation between daily maximum air temperature on the HRU and the ratio between actual and clear sky solar radiation (Markstrom and others, 2015, fig. 1–3). Because there are two parameters (dday_intcp and dday_slope) that both must be fit to a single source of solar radiation information simultaneously, there are an infinite number of combinations of these parameters that will satisfy the fit. Consequently the values of the dday_intcp parameter have been distributed over the months of the year, independent of the NREL solar radiation information. To do this, a sinusoid function was fit to the suggested range of values for this parameter (Markstrom and others, 2015, table 1–3). These values do not vary by HRU, but rather by month (table 1–4).

The parameter dday_slope is used to define the slope of the line in the functional relation between daily maximum air temperature on the HRU and the ratio between actual and clear sky solar radiation (Markstrom and others, 2015, fig. 1–3). Computer code was developed that solved for the unknown dday_slope parameter using the values of dday_intcp (described previously), monthly mean daily maximum air temperature, clear sky solar radiation, and NREL estimated solar radiation at the HRUs. The simulated solar radiation for this 35-year period, summarized to monthly means, when using the estimated parameter values is shown in figure 1–1.

### Table 1–4. Final computed monthly values of the dday_intcp parameter.

[Units are in degrees Fahrenheit–days.]

| Month | Parameter value |
|---|---|
| January | –10.0 |
| February | –11.0 |
| March | –13.0 |
| April | –16.0 |
| May | –20.0 |
| June | –25.0 |
| July | –30.0 |
| August | –25.0 |
| September | –20.0 |
| October | –16.0 |
| November | –13.0 |
| December | –11.0 |

*[Figure 1–1. Maps showing daily solar radiation by month (January–December) as simulated by the National Hydrologic Model for the 35-year period from 1980 through 2014. Explanation scale: 0 to 800 Langley of daily solar radiation.]*

## Potential Evapotranspiration Parameters

Computed daily potential evapotranspiration (PET) for each HRU is estimated by using the `potet_jh` module (Markstrom and others, 2015), which implements a modified Jensen-Haise formulation (Jensen and Haise, 1963; Jensen and others, 1969). This method computes PET as a function of air temperature and solar radiation. Markstrom and others (2015) describes a methodology for estimating these parameters; however, an alternative approach was developed for the NHM using air temperature, solar radiation, and monthly reference evaporation values.

The Jensen-Haise method uses an empirical relation where the two parameters (**jh_coef_hru** and **jh_coef**) are used to define a function that relates the energy associated with the daily average air temperature and incoming solar radiation to the daily PET rate (Markstrom and others, 2015, eq. 1–50). The daily PET rate, for each HRU, is computed by the potet_jh module using daily average air temperature and solar radiation values that have been previously computed by other PRMS modules.

Mean monthly values of PET have been compiled for the CONUS by the National Oceanic and Atmospheric Administration (NOAA) in Farnsworth and others (1982) and Farnsworth and Thompson (1982). These PET values were summarized to the resolution of the NHM GF through the USGS GDP (Blodgett and others, 2011; accessed December 2016). The NOAA-based values were used to compute the values of the parameters used by the potet_jh module for a 35-year NHM run (calendar years 1980–2014) forced with DAYMET gridded station data (Thornton and others, 2016; accessed December 2016).

The parameter jh_coef_hru is used to define the reference air temperature in the Jensen-Haise PET calculation (Markstrom and others, 2015, eq. 1–50); daily average air temperatures above this value contribute energy to the PET process. To determine this parameter value for each HRU, the lowest daily average air temperature over the period of record (1980–2014) was established.

The parameter jh_coef is used to scale the computed PET to the measured values (Markstrom and others, 2015, eq. 1–50). Computer code was developed that solves for the unknown jh_coef parameter using the values of jh_coef_hru (described previously), the monthly mean of daily air temperature, monthly solar radiation, and the NOAA estimated PET rate for each HRU. The simulated PET rate for this 35-year period, summarized to monthly means, when using the estimated parameter values are shown in figure 1–2.

*[Figure 1–2. Maps showing mean monthly potential evapotranspiration by month (January–December) as simulated by the National Hydrologic Model for the 35-year period from 1980 through 2014. Explanation scale: 0 to 400 millimeters total monthly potential evapotranspiration.]*

## Interception Parameters

Parameters describing the land cover of HRUs related to interception of precipitation were derived using the NLCD2001 dataset (Homer and others, 2007). Complete details for interception parameters can be found in Viger (2014) and Viger and Leavesley (2007). The vegetation cover type parameter, **cov_type**, has five categories (0=bare soil; 1=grass; 2=shrub; 3=deciduous tree; 4=coniferous tree). A value is first designated for each cell in the input raster map of land cover data on the basis of a reclassification table between the NLCD cover type categories and cov_type categories. The value of cov_type for each HRU area is assigned on the basis of the following sequence:

1. HRUs with 90 percent of the cells designated as bare soil are assigned a value of 0; else
2. HRUs with greater than 20 percent tree cells are assigned values based on the dominant tree species types, 3 for tree; else
3. HRUs with greater than 20 percent of the cells designated as shrub and less than 20 percent tree cells are assigned a value of 2; else
4. HRUs with greater than 35 percent of a combination of shrub and tree cover are assigned a value of 3 when the number of tree cells is greater than the number of shrub cells with the remainder being assigned a value of 2; else
5. Remaining HRUs are assigned a value on the basis of the dominant type.
6. HRUs with a cov_type value of 2 or 3 are reset to 1 if the associated hru_elev value exceeds 11,500 feet.

Summer cover density (**covden_sum**) is the fraction of the land surface within each HRU that is shaded by vegetation when illuminated from directly above. It is computed as the HRU mean value of all cells in the NLCD2001 vegetation density layer. Winter vegetation density (**covden_win**) is the summer vegetation density reduced by a "leaf loss" factor based on the proportion of grass, shrub, deciduous, and coniferous plant types. The percentage of leaf loss is computed by multiplying the summer vegetation density by a percentage value on the basis of an enhanced cover type scheme (not the cov_type parameter): ([bare, 0], [grass, 80], [shrub, 70], [deciduous tree, 60], [coniferous tree, 100]). More details are provided in Viger and Leavesley (2007) and Viger (2014). Interception capacity of summer rain, winter rain, and snow (**srain_intcp**, **wrain_intcp**, and **snow_intcp**, respectively) are computed on the basis of interception potential, expressed as a rate of inches of interception per inch of precipitation using NLCD2001 vegetation cover type categories.

## Snow Computation Parameters

PRMS simulates the accumulation and depletion of snowpack on each HRU. Snowpack dynamics are simulated through estimates of water and energy balances. The relation of snow-covered area (SCA) to SWE is also known as a snow-depletion curve, which is used to compute a fractional SCA within a HRU to account for heterogeneity of SCA. The water and energy balances conserve mass and energy, such that the difference between inputs and outputs is equal to the change in snowpack storage (fig. 3 of Markstrom and others, 2015). Several snow computation parameters are described in this section.

The **snarea_curve** parameter is the normalized relation of SCA to SWE. Specifically, SCA data were normalized to percent of total area and SWE data were normalized to percent of peak SWE at the HRU scale. Parameter snarea_curve is the normalized value of SCA at each of eleven 0.1 increments of SWE, from 0 to 1. Individual HRU parameter values for snarea_curve were derived on the basis of the CONUS-scale SNODAS (National Operational Hydrologic Remote Sensing Center, 2004) daily SWE and SCA output for 2004–2013. The methods for snarea_curve are further described in Driscoll and others (2017a). With the computation of a snow-depletion curve for each HRU, the values of parameter **hru_deplcrv** are set to the associated HRU identification number.

The **snarea_thresh** parameter is the maximum threshold of SWE below which the snow-covered-area curve is applied. Computer code was developed to determine, for each HRU, the median of the yearly maximum SWE based on the SWE from SNODAS.

The **tstorm_mo** parameter is a monthly indicator of the predominant storm type by HRU where a value of zero indicates frontal storms and a value of one indicates convective storms. This parameter in the NHM is derived from the Climate Forecast System Reanalysis model (CFSR), which provides a global reanalysis dataset with horizontal grid spacings as fine as 0.5° × 0.5° and 64 levels in the vertical at subdaily and monthly intervals (Saha and others, 2010). The CFSR model output was obtained from the Research Data Archive hosted by the National Center for Atmospheric Research (NCAR; http://rda.ucar.edu). Model output from CFSR includes separate convective and frontal precipitation variables in addition to total precipitation. Convective and frontal precipitation values from CFSR for 1979–2010 were used to derive the tstorm_mo parameter for the NHM domain. First, mean monthly values of convective and frontal precipitation were computed for 1979–2010. Next, a convective mask by month was created, where tstorm_mo was set to 1 if the convective precipitation divided by the frontal precipitation was greater than 0.9, indicating a predominance of convective precipitation occurring on average for the month. Otherwise tstorm_mo was set to 0, indicating a predominance of frontal precipitation events. Finally, the GDP (Blodgett and others, 2011) was used to compute values of the tstorm_mo parameter for the NHM based on the areal maximum value of the convective mask for each month by HRU.

A coefficient, **rad_trncf**, of radiation transmission for shortwave radiation through the winter vegetation canopy density (covden_win, described in the "Interception Parameters" section) is derived as a function of NLCD2001 canopy density and a leaf loss factor. A per-cell surface indicating the winter vegetation density where the cov_type value is 3 ("tree") is created.

## Hortonian Surface Runoff Parameters

Four interdependent parameters are used to characterize HRUs for the simulation of Hortonian surface runoff (Horton, 1945) (table 1-2). These include quantifications of the amount of impervious surface (**hru_percent_imperv**) and the maximum (**carea_max**) and minimum (**smidx_coef**) pervious acreage considered capable of runoff generation during a snowmelt or precipitation event. Parameter hru_percent_imperv is the decimal fraction within an HRU where NLCD2001 impervious surface cells have a value greater than 50 percent.

The contributing area from which Hortonian surface runoff is computed for each time step is determined for the pervious fraction of the HRU land surface on the basis of the antecedent water content of the capillary reservoir (*soil_moist*), the water available for infiltration (*srp*), the linear and exponential soil-moisture index parameters (**smidx_coef** and **smidx_exp**, respectively), and the maximum contributing area parameter (**carea_max**). The pervious area is equal to the HRU area (**hru_area**) minus impervious area (`hru_percent_imperv * hru_area`) and surface-depression area (`dprst_frac * hru_area`). See Markstrom and others (2015) pages 103 and 104 for a description of computing the maximum contributing area.

Parameters carea_max and smidx_coef are computed on the basis of a DEM-derived Topographic Wetness Index (TWI; Beven, 1979) maps and on-stream waterbody maps. TWI is computed from the NHDPlus hydrologically conditioned DEM. The on-stream storage is defined as the NHDPlus version 1.0 waterbodies that are within or intersect a 60-meter buffer around the GF stream segments. Parameter **carea_max** is defined as the fraction of HRU pervious area where TWI is greater than 8.0 or on-stream storage exists. Parameter **smidx_coef** uses the same formulation where the value of TWI is greater than 15.6 with the condition that values must be less than or equal to carea_max.

Parameter **smidx_exp** (that is, the exponential term in the PRMS soil moisture index equation; Markstrom and others, 2015, eq. 1–98) is used by PRMS to compute the fractional area of each HRU that generates direct surface runoff on each time step. This equation can be rearranged as:

```
                                          (1 / smidx_max)
smidx_exp = ( log10( carea_max / smidx_coef ) )                (1–1)
```

where *smidx_max* is the maximum value of the soil moisture index, computed as:

```
smidx_max = soil_moist_max + (0.5 × ppt_max)                   (1–2)
```

where *ppt_max* is the maximum daily precipitation amount, for each HRU, from the DAYMET gridded station data (Thornton and others, 2016; accessed December 2016) for the time period from 1980 through 2014.

Equations 1–1 and 1–2 are solved for every HRU, resulting in values of smidx_exp.

> **Editorial note (not in the source).** Equation 1–1 is transcribed above exactly
> as typeset on p. 34 of the report, and as typeset it is almost certainly a typo.
> PRMS-IV eq. 1-98 gives the contributing fraction as
> `carea = smidx_coef × 10^(smidx_exp × smidx)`, capped at `carea_max`. Setting
> `carea = carea_max` at `smidx = smidx_max` and solving for `smidx_exp` yields a
> **division**, not a fractional power:
>
> ```
> smidx_exp = log10( carea_max / smidx_coef ) / smidx_max
> ```
>
> The units confirm it: TM 6-B9 Table 1–2 gives `smidx_exp` units of `1.0/inch`,
> and `smidx_max` is in inches (eq. 1–2), so `log10(ratio) / inches` → `1/inch`.
> The printed form `(log10(...))^(1/smidx_max)` is dimensionless and cannot carry
> the stated units. **Implement the division form.**

## Surface-Depression Storage Parameters

Simulation of surface-depression storage (DPRST) is used to account for the hydrologic effect of numerous, small, unregulated, water bodies within HRUs. Examples of surface depressions are prairie potholes, wetlands, and agricultural, mill, and detention ponds. Note, lakes are not simulated in the NHM. See Steuer and Hunt (2001) for a discussion on including surface-depression storage and flow simulation in hydrologic modeling. See Regan and LaFontaine (2017), Markstrom and others (2015), and Viger and others (2010) for descriptions of the simulation of DPRST in PRMS.

Surface-depression storage area is characterized as the aggregate sum of water body features within HRUs that are neither on-stream nor located where land cover data indicate impervious surfaces exist. On-stream water bodies, such as lakes and reservoirs that are within or intersect with a 60-meter buffer of GF stream segments, are considered flow-through features and are not included in DPRST. For convenience in calibration procedures, the surface area is specified as a decimal fraction (**dprst_frac**) of the HRU area (**hru_area**). PRMS surface depressions can be geographically isolated (closed) from or connected (open) to the stream network. The fraction of surface-depression area within an HRU that is open is specified using parameter **dprst_frac_open** with the amount of closed depressions equal to 1.0 minus dprst_frac_open. For the NHM the values of dprst_frac_open are set to the default, 1.0.

Water is captured by surface depressions from throughfall and snowmelt and from a fraction of surface-runoff generated from the pervious (**sro_to_dprst_perv**) and impervious (**sro_to_dprst_imperv**) parts of each HRU. The sro_to_dprst_imperv and sro_to_dprst_perv parameters are closely related to each other. Both are expressed as decimal fractions. For the NHM these parameters are derived differently than the initial estimates described in Viger (2014). First, the contributing areas to all surface depressions (waterbodies that are not considered on-stream) are delineated. Note that these contributing areas exclude the surface depressions themselves. Parameter sro_to_dprst_imperv is the ratio of impervious surface within all surface depression-contributing areas in an HRU to the total impervious area within the HRU (that is, the total includes both the impervious within and beyond the surface depression-contributing areas). Parameter sro_to_dprst_perv is computed in the same way, except using pervious instead of impervious area. Total impervious area is derived by multiplying hru_area and hru_percent_imperv. Total pervious area is derived by subtracting total impervious area and the area of surface depressions within an HRU from the hru_area value.

Figure 1–3 illustrates the areas associated with the pervious and impervious contributing areas to open DPRST. For this example, the total area is 150 units, 5 units of which are DPRST. The total impervious area is 15 units; 10 units produce surface runoff that flows to the stream network and 5 units to DPRST. The total pervious area is 130 units; 90 units produce surface runoff that flows to the stream network; 30 units to DPRST; and 10 units are included in the on-stream buffer. The value of sro_to_dprst_perv is set to the pervious area that flows to DPRST divided by the total pervious area. For this example, `sro_to_dprst_perv = 30 / 130 = 0.2301`. The value of sro_to_dprst_imperv is set to the impervious area runoff to DPRST divided by the total impervious area. For this example, `sro_to_dprst_imperv = 5 / 15 = 0.333`.

*[Figure 1–3. Diagram of surface-depression storage contributing areas (DPRST, surface-depression storage; units, portion of total area, which is 150 units). Shows: Pervious-area runoff to stream, 90 units; Impervious-area runoff to DPRST, 5 units; Pervious-area runoff to DPRST, 30 units; DPRST, 5 units; Impervious-area runoff to stream, 10 units; Pervious-area runoff that is onstream, 10 units.]*

Water is released from DPRST as surface runoff (spill), interflow, evaporation, and seepage to groundwater storage. Surface runoff (spill) occurs from open surface-depressions when the captured water plus antecedent storage exceeds the maximum water-holding capacity of DPRST. Interflow occurs from open surface-depressions when the captured water plus antecedent storage exceeds a fraction (**op_flow_thres**) of the maximum water-holding capacity at a rate specified by **dprst_flow_coef**. Water evaporates based on the current surface-area multiplied by the product of the potential evapotranspiration rate and an adjustment factor (**dprst_et_coef**). The maximum volume of the depressions for each HRU is the average depth of depressions (**dprst_depth_avg**) multiplied by the area of the HRU (**hru_area**) and **dprst_frac**. When the water content of open depressions exceeds this volume the excess water spills to the stream network. For closed depressions, when the water content exceeds this volume the excess water does not spill; for this case the storage volume can be greater than the specified maximum volume. Open and closed depressions can seep at different rates as specified by **dprst_seep_rate_open** and **dprst_seep_rate_clos**, respectively.

A calibration procedure by HRU was used to determine spatially distributed values for the average DPRST depth within and HRU (**dprst_depth_avg**), **dprst_et_coef**, **dprst_flow_coef**, **op_flow_thres**, and **dprst_seep_rate_open** in areas where dprst_frac was greater than zero. Monthly values of runoff from the Monthly Water Balance Model (MWBM) NHM (Bock and others, 2016) were used as a proxy for change in surface-depression storage. These runoff time series at each HRU were normalized between 0 and 1 so they could be compared by HRU to the PRMS intermediate process variable that contains the fraction (0 to 1) of the depression storage within an HRU that is filled (*dprst_vol_frac*). (Intermediate process variables are not listed in the tables in this report). If the normalized MWBM runoff value was zero, then it was assumed that the HRU surface-depression storage was empty (*dprst_vol_frac*=0). The information is not available to determine when the depressions are empty or at the upper threshold (how full it gets) from MWBM simulation results; they just provide the pattern. The upper threshold may be more critical, because that is what controls whether or not 'spill' occurs. The region-wide calibrated values are meant to produce distributed parameters that reproduce the general behavior of the depressions. For local applications, fine tuning of these parameters is suggested using local information for calibration and evaluation. Parameter **dprst_seep_rate_clos** was set to values of **dprst_seep_rate_open**.

## Soil Zone Parameters

A group of parameters are used to characterize the rate at which water moves through the soil and groundwater parts of an HRU. Because accurately determining these parameter values requires data that are not readily available, the derivation of these NHM parameters attempts to create a realistic spatial pattern of variation in values, even if the absolute values may not be accurate. This provides a much more useful set of information for use in calibration (in place of the assumption of spatially constant values). This is done by linearly interpolating the range of average permeability through soil and porous rocks (Gleeson and others, 2011) for all HRUs in a GF region to the range of acceptable values associated with each parameter (Markstrom and others, 2015 table 1–3), or by deriving one of these flux parameters as a function of another. More specifically, the permeability (*k_perm*) values in the Gleeson and others (2011) dataset were used as a starting point (unless otherwise noted).

Three types of information from the STATSGO soils dataset (U.S. Department of Agriculture, Natural Resources Conservation Service, 2013) are used to derive the parameters **soil_type**, **soil_moist_max**, **soil_rechr_max**; these are soil texture, available water holding capacity in the rooting zone, and available water holding capacity that is available to evaporation, respectively. For additional details about the computation of these parameters see Viger (2014) and Viger and Leavesley (2007). Parameter soil_rechr_max is not used in NHM-PRMS. It is replaced by the parameter **soil_rechr_max_frac**, which is computed as soil_rechr_max divided by soil_moist_max.

The parameter soil_type is based on a reclassification of NLCD2001 into three categories (1=sand; 2=loam; 3=clay) on the basis of the following sequence. First each cell is assigned: 3 if clay content exceeds 40 percent; 1 if less than 40 percent clay but greater than 50 percent sand content; or 2 for the remaining cells. Finally, the value for soil_type is set to the most commonly occurring (per-cell) category for each HRU.

The parameter soil_moist_max also is based on a reclassification of NLCD2001. For every cell in the supplied raster map of soils data, the available water-holding capacity (a rate expressed as inches of water per inch of soil) is multiplied by the rooting depth. The soil_moist_max value is computed as the average of this depth for all cells within the HRU.

Parameter values for **ssr2gw_rate** initially are derived as (*k_perm* cubed) × (1 − hru_slope). The region-wide interpolation adjusts all values to the range 0.3 to 0.7. Parameter **slowcoef_lin** values are derived as (*k_perm* cubed) × (hru_slope) / (hru_area). The region-wide interpolation adjusts all values to the range 0.005 to 0.3. Prior to interpolation, **fastcoef_lin** values are derived as (slowcoef_lin × 2). The region-wide interpolation adjusts all fastcoef_lin values to range from 0.01 to 0.6. Parameter values for **soil2gw_max** initially are derived as *k_perm* cubed. The region-wide interpolation adjusts all values to range from 0.1 to 0.3.

## Groundwater Flow Parameters

The parameter **gwflow_coef** is a linear coefficient that is used to compute the daily groundwater discharge rate from the groundwater reservoir. As such, it defines the recession characteristics of the simulated hydrograph, specifically the base-flow recession rate. Values of gwflow_coef for each HRU were approximated with a best-fit multiple-linear regression equation on the basis of CONUS-wide GIS data, including geology, drainage density, aquifer type, vegetation type, and base flow index information. Estimating base-flow recession rates based on these characteristics is described in Rutledge and Mesko (1996), Brandes and others (2005), and Berhail and others (2012). The base-flow index is based on the hydrographs of a set of high quality streamgages (GAGES-II; Falcone, 2011) and base-flow separation analysis using the USGS HYSEP software (Sloto and Crouse, 1996). The computed gwflow_coef values range from approximately 0.004–0.055.

## Parameter Visualization

All parameter values from the NhmParamDb were pulled and published as Comma-Separated Values (CSV) files in ScienceBase (Driscoll and others, 2017b). Published along with the CSV files are basic visualizations of nhru-dimensioned parameter values that illustrate the spatial variability of values across the CONUS. These CSV files and visualizations provide a static point from which to measure change in the NhmParamDb. Future release of CSV files can be periodically updated from the contents of the NhmParamDb and visualizations regenerated.

## References Cited (Appendix 1)

Beven, K.J., Kirkby, M.J., 1979, A physically based, variable contributing area model of basin hydrology: Hydrological Science Bulletin, v. 24, p. 43–69.

Berhail, Sabri, Lahbassi Ouerdachia, and Hamouda Boutaghanea, 2012, The use of the recession index as indicator for components of flow: Energy Procedia v. 18, p. 741–750, http://www.sciencedirect.com/science/article/pii/S1876610212008600.

Blodgett, D.L., Booth, N.L., Kunicki, T.C., Walker, J.L., and Viger, R.J., 2011, Description and testing of the Geo Data Portal—Data integration framework and Web processing services for environmental science collaboration: U.S. Geological Survey Open-File Report 2011–1157, 9 p., https://pubs.usgs.gov/of/2011/1157/.

Bock, A.R., Hay, L.E., Markstrom, S.L., and Atkinson, R.D., 2016, Monthly water balance model hydrology futures: U.S. Geological Survey data release, http://dx.doi.org/10.5066/F7VD6WJQ.

Brandes, David, Hoffmann, J.G., Mangarillo, J.T., 2005, Base flow recession rates, low flows, and hydrologic features of small watersheds in Pennsylvania, USA: Journal of the American Water Resources Association v. 41, no. 5, p. 1177–1186.

Driscoll, J.M., Hay, L.E., and Bock, A.R., 2017a, Spatiotemporal variability of snow depletion curves derived from SNODAS for the conterminous United States, 2004–2013: Journal of the American Water Resources Association, v. 53, no. 3, p. 655–666, http://dx.doi.org/10.1111/1752-1688.12520.

Driscoll, J.M., Markstrom, S.L., Regan, R.S., Hay, L.E., Viger, R.J., 2017b, National hydrologic model parameter database: U.S. Geological Survey database, accessed May 5, 2017, at http://dx.doi.org/10.5066/F7NS0SCW.

Falcone, J.A., 2011, GAGES–II, Geospatial attributes of gages for evaluating streamflow: U.S. Geological Survey dataset, http://water.usgs.gov/GIS/metadata/usgswrd/XML/gagesII_Sept2011.xml.

Farnsworth, R.K., and Thompson, E.S., 1982, Mean monthly, seasonal, and annual pan evaporation for the United States: Washington, D.C., National Oceanic and Atmospheric Administration Technical Report NWS 34, 82 p.

Farnsworth, R.K., Thompson, E.S., and Peck, E.L., 1982, Evaporation atlas for the contiguous 48 United States: Washington, D.C., National Oceanic and Atmospheric Administration Technical Report NWS 33, 41 p.

Gleeson, T., Smith, L., Moosdorf, N., Hartmann, J., Dürr, H.H., Manning, A.H., Van Beek, L.P.H., and Jellinek, A.M., 2011, Mapping permeability over the surface of the Earth: Geophysical Research Letters, v. 38, L02401, https://dx.doi.org/10.1029/2010GL045565.

Homer, C., Dewitz, J., Fry, J., Coan, M., Hossain, N., Larson, C., Herold, N., McKerrow, A., VanDriel, J.N., and Wickham, J., 2007, Completion of the 2001 National land cover database for the conterminous United States: Photogrammetric Engineering and Remote Sensing, v. 73, no. 4, p. 337–341, http://www.mrlc.gov/nlcd01_data.php.

Horton, R.E., 1945, Erosional development of streams and their drainage basins—hydro-physical approach to quantitative morphology: Geological Society of America Bulletin 56, v. 3, p. 275–370, https://dx.doi.org/10.1130/0016-7606(1945)56[275:EDOSAT]2.0.CO;2.

Jensen, M.E., and Haise, H.R., 1963, Estimating evapotranspiration from solar radiation: New York, American Society of Civil Engineers, Journal of Irrigation and Drainage, v. 89, p. 15–41.

Jensen, M.E., Rob, D.C.N., and Franzoy, C.E., 1969, Scheduling irrigations using climate-crop-soil data, in National Conference on Water Resources Engineering of the American Society of Civil Engineers: New Orleans, La., American Society of Civil Engineers, p. 20.

Leaf, C.F., and Brink, G.E., 1973, Hydrologic simulation model of Colorado subalpine forest: U.S. Department of Agriculture, U.S. Forest Service Research Paper RM–107, 23 p.

Leavesley, G.H., Lichty, R.W., Troutman, B.M., and Saindon, L.G., 1983, Precipitation-runoff modeling system—User's manual: U.S. Geological Survey Water-Resources Investigations Report 83–4238, 207 p.

Leavesley, G.H., Restrepo, P.J., Markstrom, S.L., Dixon, M.J., and Stannard, L.G., 1996, The modular modeling system (MMS)—User's manual: U.S. Geological Survey Open-File Report 96–151, 142 p.

McDonald, C.P., Rover, J.A., Stets, E.G., Striegl, R.G., 2012, The regional abundance and size distribution of lakes and reservoirs in the United States and implications for estimates of global lake extent: Limnology and Oceanography, v. 57, p. 597–606.

Markstrom, S.L, Hay, L.E, and Clark, M.P., 2016, Towards simplification of hydrologic modeling—identification of dominant processes: Hydrology and Earth System Sciences, v. 20, p. 4655–4671, http://dx.doi.org/10.5194/hess-20-4655-2016.

Markstrom, S.L., Niswonger, R.G., Regan, R.S., Prudic, D.E., and Barlow, P.M., 2008, GSFLOW—Coupled ground-water and surface-water flow model based on the integration of the precipitation-runoff modeling system (PRMS) and the modular ground-water flow model (MODFLOW-2005): U.S. Geological Survey Techniques and Methods 6–D1, 240 p.

Markstrom, S.L., Regan, R.S., Hay, L.E., Viger, R.J., Webb, R.M.T., Payn, R.A., and LaFontaine, J.H., 2015, PRMS-IV, the precipitation-runoff modeling system, version 4: U.S. Geological Survey Techniques and Methods 6–B7, 158 p., https://dx.doi.org/10.3133/tm6B7.

National Operational Hydrologic Remote Sensing Center, 2004, Snow data assimilation system (SNODAS) data products at NSIDC, 2004–2014: Boulder, Colorado USA, National Snow and Ice Data Center, http://dx.doi.org/10.7265/N5TB14TC.

National Renewable Energy Laboratory, 1992, User's manual national solar radiation data base (1961–1990), NSRDB-Vol. 1, http://rredc.nrel.gov/solar/pubs/NSRDB/.

Regan, R.S., and LaFontaine, J.H., 2017, Documentation of the dynamic parameter, water-use, stream and lake flow routing, and two summary output modules and updates to surface-depression storage simulation and initial conditions specification options with the precipitation-runoff modeling system (PRMS): U.S. Geological Survey Techniques and Methods, book 6, chap. B8, 72 p., http://dx.doi.org/10.3133/tm6B8.

Rosenberg, N.J., Blad, B.L., and Verma, S.B., 1983, Microclimate: The biological environment, Wiley and Sons, Inc., 170 p.

Rutledge, A.T., and Mesko, T.O., 1996, Estimated hydrologic characteristics of shallow aquifer systems in the Valley and Ridge, the Blue Ridge, and the Piedmont physiographic provinces based on analysis of streamflow recession and base flow: U.S. Geological Survey Professional Paper 1422–B, 37 p., https://pubs.er.usgs.gov/publication/pp1422B.

Saha, S., and others, 2010, NCEP climate forecast system reanalysis (CFSR) 6-hourly products, January 1979 to December 2010: Research Data Archive at the National Center for Atmospheric Research, Computational and Information Systems Laboratory, http://rda.ucar.edu/datasets/ds093.0.

Sloto, R.A., and Crouse, M.Y., 1996, HYSEP—A computer program for streamflow hydrograph separation and analysis: U.S. Geological Survey Water-Resources Investigations Report 96–4040, 46 p., https://pubs.er.usgs.gov/publication/wri964040.

Steuer, J.J., and Hunt, R.J., 2001, Use of a watershed-modeling approach to assess hydrologic effects of urbanization, North Fork Pheasant Branch basin near Middleton, Wisconsin: U.S. Geological Survey Water-Resources Investigations Report 2001–4113, 49 p.

Thornton, P.E., Running, S.W., and White, M.A., 1997, Generating surfaces of daily meteorological variables over large regions of complex terrain: Journal of Hydrology, v. 190, no. 3, p. 214–251.

Thornton, P.E., Thornton, M.M., Mayer, B.W., Wei, Y., Devarakonda, R., Vose, R.S., and Cook, R.B., 2016, Daymet: daily surface weather data on a 1-km grid for North America, version 3: Oak Ridge, Tenn., Oak Ridge National Laboratory Distributed Active Archive Center dataset, https://dx.doi.org/10.3334/ORNLDAAC/1328.

U.S. Department of Agriculture, Natural Resources Conservation Service, 2013, Web soil survey: U.S. Department of Agriculture, Natural Resources Conservation Service database, https://websoilsurvey.nrcs.usda.gov/.

Viger, R.J. 2014, Preliminary spatial parameters for PRMS based on the geospatial fabric, NLCD2001 and SSURGO: U.S. Geological Survey data release, http://dx.doi.org/doi:10.5066/F7WM1BF7.

Viger, R.J., and Bock, Andrew, 2014, GIS features of the geospatial fabric for national hydrologic modeling: U.S. Geological Survey data release, https://dx.doi.org/doi:10.5066/F7542KMD.

Viger, R.J., Hay, L.E., Jones, J.W., and Buell, G.R., 2010, Effects of including surface depressions in the application of the precipitation-runoff modeling system in the Upper Flint River Basin, Georgia: U.S. Geological Survey Scientific Investigations Report 2010–5062, 36 p., https://pubs.er.usgs.gov/publication/sir20105062.

Viger, R.J., and Leavesley, G.H., 2007, The GIS weasel user's manual: U.S. Geological Survey Techniques and Methods, 6–B4, 201 p.

Wilcox, S., 2012, National solar radiation database 1991–2010 update: users's manual, NREL report no. TP-5500-54824, 479 p., https://www.nrel.gov/docs/fy12osti/54824.pdf.
