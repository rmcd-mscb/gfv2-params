---
source_pdf: docs/hyp.13735.pdf
citation: Sexstone, G. A., Driscoll, J. M., Hay, L. E., Hammond, J. C., & Barnhart, T. B. (2020). Runoff sensitivity to snow depletion curve representation within a continental scale hydrologic model. Hydrological Processes, 34(11), 2365-2380. https://doi.org/10.1002/hyp.13735
note: Auto-extracted text (pdfminer.six) for full-text search. Layout/tables approximate; verify against the PDF for quotation.
---

# Runoff sensitivity to snow depletion curve representation within a continental scale hydrologic model

Sexstone, Driscoll, Hay, Hammond & Barnhart (2020) — *Hydrological Processes* 34:2365-2380

> Extracted from `docs/hyp.13735.pdf` for search/reference.

---

Received: 14 July 2019

Accepted: 18 February 2020

DOI: 10.1002/hyp.13735

R E S E A R C H A R T I C L E

Runoff sensitivity to snow depletion curve representation
within a continental scale hydrologic model

Graham A. Sexstone1
John C. Hammond3

|

Jessica M. Driscoll2

| Lauren E. Hay2

|

| Theodore B. Barnhart4

1U.S. Geological Survey, Colorado Water
Science Center, Denver, Colorado

2U.S. Geological Survey, Integrated Modeling
and Prediction Division, Denver, Colorado

3U.S. Geological Survey, Maryland, Delaware,
D.C. Water Science Center, Baltimore,
Maryland

4U.S. Geological Survey, Wyoming-Montana
Water Science Center, Helena, Montana

Correspondence
Graham A. Sexstone, U.S. Geological Survey,
Colorado Water Science Center, Denver,
Colorado, USA.
Email: sexstone@usgs.gov

Abstract

The spatial variability of snow water equivalent (SWE) can exert a strong influence

on the timing and magnitude of snowmelt delivery to a watershed. Therefore, the

representation of sub-grid or sub-watershed snow variability in hydrologic models is

important for accurately simulating snowmelt dynamics and runoff response. The

U.S. Geological Survey National Hydrologic Model

infrastructure with the

precipitation-runoff modelling system (NHM-PRMS) represents the sub-grid variabil-

ity of SWE with snow depletion curves (SDCs), which relate snow-covered area to

watershed-mean SWE during the snowmelt period. The main objective of this

research was to evaluate the sensitivity of simulated runoff to SDC representation

within the NHM-PRMS across the continental United States (CONUS). SDCs for the

model experiment were derived assuming a range of SWE coefficient of variation

values and a lognormal probability distribution function. The NHM-PRMS was simu-

lated at a daily time step for each SDC over a 14-year period. Results highlight that

increasing the sub-grid snow variability (by changing the SDC) resulted in a consis-

tently slower snowmelt rate and longer snowmelt duration when averaged across the

hydrologic response unit scale. Simulated runoff was also found to be sensitive to
SDC representation, as decreases in simulated snowmelt rate by 1 mm day−1

resulted in decreases in runoff ratio by 1.8% on average in snow-dominated regions

of the CONUS. Simulated decreases in runoff associated with slower snowmelt rates

were approximately inversely proportional to increases in simulated evapotranspira-

tion. High snow persistence and peak SWE:annual precipitation combined with a

water-limited dryness index was associated with the greatest runoff sensitivity to

changing snowmelt. Results from this study highlight the importance of carefully

parameterizing SDCs for hydrologic modelling. Furthermore, improving model repre-

sentation of snowmelt input variability and its relation to runoff generation processes

is shown to be an important consideration for future modelling applications.

K E Y W O R D S

continental United States, model sensitivity, precipitation-runoff modelling system, scaling,

snow depletion curve, snowmelt, streamflow, sub-grid variability

Published 2020. This article is a U.S. Government work and is in the public domain in the USA

Hydrological Processes. 2020;34:2365–2380.

wileyonlinelibrary.com/journal/hyp

2365

2366

SEXSTONE ET AL.

1

|

I N T RO DU CT I O N

variability of

snowmelt processes within hydrologic models

(Anderson, 1968; Driscoll, Hay, & Bock, 2017; Liston, 1999; Luce &

Seasonal snow is a critical component of the surface energy balance

Tarboton, 2004; Magand, Ducharne, Le Moine, & Gascoin, 2014;

and hydrologic cycle in mountainous areas and high latitudes; changes

Markstrom et al., 2015; Martinec & Rango, 1981; Yang, Dickinson,

to snowmelt duration, timing and magnitude in these regions may

Robock, & Vinnikov, 1997). Luce and Tarboton (2004) highlight that

substantially alter hydrological processes. Mountains, which function

dimensionless SDCs can show good inter-annual agreement, but also

as essential water towers, generally produce more streamflow per unit

suggest there may be environments or scales where dimensionless

area

than

lower

lying

terrain

(Christensen, Wood, Voisin,

SDCs may change from year to year. Furthermore, SDCs are difficult

Lettenmaier, & Palmer, 2004; Hunsaker, Whitaker, & Bales, 2012;

to derive over large regions (Driscoll et al., 2017; Fassnacht et al.,

Viviroli, Durr, Messerli, Meybeck, & Weingartner, 2007), so hydrologic

2016; Shamir & Georgakakos, 2007), variable in their representation

changes in these areas can have substantial effects both where snow-

between models (Essery & Pomeroy, 2004; Liston, 2004), and often

melt occurs as well as across large areas they feed downstream.

applied consistently over heterogeneous landscapes. Where more

Approximately 2 billion people are expected to experience diminished

realistically, the representation of the SWE-SCA relation should be

water supplies due to seasonal snowpack decline this century

regionally variable (Driscoll et al., 2017), with differences between

(Barnett, Adam, & Lettenmaier, 2005; Mankin, Viviroli, Singh,

landscape type and melt energy accounted for by the HRUs

Hoekstra, & Diffenbaugh, 2015). Snowpacks have declined across the

(DeBeer & Pomeroy, 2009, 2010; Donald, Soulis, Kouwen, &

western United States (U.S.) in recent years (Clow, 2010; Fritze, Stew-

Pietroniro, 1995; Dornes, Pomeroy, Pietroniro, Carey, & Quinton,

art, & Pebesma, 2011; Harpold et al., 2012; Mote, Hamlet, Clark, &

2010), that change based on the scale of interest (Blöschl, 1999;

Lettenmaier, 2005; Mote, Li, Lettenmaier, Xiao, & Engel, 2018;

Seyfried & Wilcox, 1995). Given the importance of scaling and spatial

Regonda, Rajagopalan, Clark, & Pitlick, 2005; Stewart, Cayan, & Det-

representation of SDCs described here, it is important to understand

tinger, 2005), with both empirical and modelled evidence that snow-

the sensitivity of simulated hydrologic fluxes to SDC representation at

pack decline may lead to reduced runoff efficiency (ratio of total

a scale of interest.

runoff to total precipitation; Barnhart et al., 2016; Berghuijs, Woods, &

In a review of past approaches to represent sub-grid SWE vari-

Hrachowitz, 2014; Clow, 2010; Furey, Kampf, Lanini, & Dozier, 2012;

ability in modelling, Clark et al. (2011) suggest that the most physically

Hammond, Harpold, Weiss, & Kampf, 2019; Jefferson, 2011; Regonda

realistic approach is using a probability distribution function (pdf), as it

et al., 2005; Stewart, Cayan, & Dettinger, 2004; Stewart et al., 2005;

only relies on an estimate of the coefficient of variation (CV; ratio of

Xiao, Udall, & Lettenmaier, 2018). However, hydrologic response to

the standard deviation to the mean) of SWE, an observable property

snowpack decline is heterogeneous; in some areas, snowpack reduc-

in the field (e.g., Winstral & Marks, 2014). Various studies have

tions produce declines in streamflow, but this does not occur every-

highlighted that snow distributions can be reasonably described by a

where (McCabe, Wolock, & Valentin, 2018). Sources of this variability

lognormal pdf (DeBeer & Pomeroy, 2009; Donald et al., 1995; Faria,

in hydrologic response are often unknown and likely a result of inter-

Pomeroy, & Essery, 2000; Pomeroy et al., 1998), whereas others have

actions between climatic, vegetative, topographic and edaphic factors

suggested that snow distributions tend to more closely follow a

with the relative importance of each factor changing between climatic

gamma pdf (Egli, Jonas, Grunewald, Schirmer, & Burlando, 2012; Ska-

zones and along elevation gradients within each climatic zone. There-

ugen, 2007; Winstral & Marks, 2014). Despite these differences, Luce

fore, physically based hydrologic models that are run with consistent

and Tarboton (2004) highlighted that the representation of sub-grid

methods across broad extents and can be used to evaluate hydrologic

snow variability is much more sensitive to the distribution parameters

processes and dynamics that vary considerably across spatial and tem-

(e.g., CV of SWE and mean SWE) as opposed to distribution type.

poral scales may provide important insights into the relative sensitivi-

Liston (2004) developed a global classification scheme of the CV of

ties of hydrologic responses to these anticipated snowpack declines.

SWE and showed how a lognormal pdf could be used to simulate the

The seasonal snowpack exhibits marked variability both spatially

sub-grid variability of SWE and associated snowmelt by assuming uni-

and temporally across the landscape (Lopez-Moreno et al., 2015),

form melting conditions, offering an approach to represent the sub-

which exerts a strong influence on the timing and magnitude of snow-

grid snow variability across broad-scale modelling applications.

melt delivery to a watershed (Anderton, White, & Alvera, 2002;

Although the assumption of uniform melting conditions is likely vio-

Liston, 1999; Luce, Tarboton, & Cooley, 1998) and its streamflow

lated in many areas and is dependent on the HRU scale, Luce et al.

response (DeBeer & Pomeroy, 2017; Lundquist & Dettinger, 2005;

(1998) noted that most of the spatial variability in SWE at the water-

Lundquist, Dettinger, & Cayan, 2005). Therefore, the representation

shed scale is due to variations in accumulation, rather than variations

of the sub-grid or sub-watershed snow variability in broad-scale

in melt. Based on these sub-grid SWE variability approaches, this

hydrologic models is particularly important for accurately simulating

study used a range of SDCs derived by a lognormal pdf to perform a

variations in energy fluxes, snowmelt dynamics and runoff response

sensitivity analysis of simulated hydrologic fluxes.

(Clark et al., 2011; He, Ohara, & Miller, 2019; Liston, 1999, 2004).

The U.S. Geological Survey (USGS) Water Availability and Use

Snow depletion curves (SDCs) that relate the snow-covered area

Science

Program (https://www.usgs.gov/water-resources/water-

(SCA) to the mean snow water equivalent (SWE) for a given hydro-

availability-and-use-science-program) aims to quantify the availability

logic response unit (HRU) are often used to represent the sub-grid

of water resources at the watershed scale for the U.S. Towards this

SEXSTONE ET AL.

2367

goal, the national-extent application of the National Hydrologic

The NHM-PRMS was calibrated for each HRU based on five base-

Model infrastructure with the precipitation-runoff modelling sys-

line calibration datasets derived from CONUS-scale remote sensing

tem (NHM-PRMS) (Regan et al., 2018; Regan et al., 2019) has been

and/or modelled products with error bounds at daily, monthly, or annual

applied to simulate water budget components contributing to

time scales (Hay, 2019). These baseline calibration datasets included sim-

streamflow. Although empirical studies have shown location-

ulated runoff (Bock, Farmer, & Hay, 2018; Bock, Hay, McCabe, Mar-

specific results of the importance of the timing and duration of

kstrom, & Atkinson, 2016), simulated and remotely sensed actual

snowmelt to streamflow (e.g., Anderton et al., 2002; DeBeer &

evapotranspiration (Bock et al., 2016; Running, Mu, & Zhao, 2017; Senay

Pomeroy, 2017; Luce et al., 1998), this study investigates the sensi-

et al., 2013), empirical regression estimates and simulated recharge (Doll,

tivity of simulated hydrologic component fluxes to the representa-

Mueller Schmied, Schuh, Portmann, & Eicker, 2014; Reitz, Sanford,

tion of snowmelt rate in the NHM-PRMS across the continental

Senay, & Cazenas, 2017), simulated soil moisture (Kalnay et al., 1996;

United States (CONUS). More specifically, a model experiment was

Reichle et al., 2011; Xia et al., 2012) and remotely sensed SCA (Hall &

completed with the goal of assessing the sensitivity of simulated

Riggs, 2016b). A Fourier amplitude sensitivity test (FAST; Cukier, Fortuin,

runoff to the representation of watershed-scale snowmelt rate, as

Shuler, Petschek, & Schaibly, 1973; Markstrom, Hay, & Clark, 2016; Salt-

represented by SDCs derived by a lognormal pdf, within the model.

elli et al., 2006; Schaibly & Shuler, 1973) was used to assess how the

Model simulations with the same meteorological forcing and model

parameters chosen for calibration were related to overall sensitivity of

parameters but different SDCs allowed for the direct comparison

each baseline calibration dataset and thus was used to determine the

of simulated hydrological fluxes between simulations. The objec-

step-wise order of calibration for each HRU (Hay, 2019). The optimal set

tives of this study were to (a) determine which regions of the

of calibration parameters for each HRU was chosen based on a multiple-

CONUS are relatively more or less sensitive to a change in snow-

objective, step-wise, automated calibration procedure (Hay & Umemoto,

melt representation in the model, and (b) evaluate how the simu-

2007) using the shuffled complex evolution (SCE) global search algorithm

lated changes in hydrologic fluxes and partitioning are related to

(Duan, Gupta, & Sorooshian, 1993; Duan, Sorooshian, & Gupta, 1992;

runoff sensitivity. Results from this study will help to better under-

Duan, Sorooshian, & Gupta, 1994). The objective function for each step

stand the importance and sensitivity of NHM-PRMS SDC represen-

in the by-HRU calibration used the normalized root mean square error.

tation across the CONUS with the goal of

focusing future

Hay (2019) provides additional detail and the baseline HRU calibration of

investigations to further improve the USGS's national modelling

the NHM-PRMS that was used in this study.

and predictive capacity of water availability for the U.S.

PRMS uses a dimensionless SDC to simulate the fraction of SCA

2

| M E T H O D S

2.1

| Model description

associated with simulated SWE normalized by the threshold SWE

value (SWE100; the maximum SWE amount for a given HRU below
which the SDC is applied as further decreases in SWE are associated

with reductions in SCA) within an HRU during the snowmelt period

(Anderson, 1968; Markstrom et al., 2015). Hay (2019) calibrated the

NHM-PRMS SDCs at the HRU level using SCA observations from the

The USGS NHM-PRMS was applied in this study (Regan et al., 2018;

MODIS satellite (MOD10C1; Hall & Riggs, 2016b). In this study, we

Regan et al., 2019). The PRMS is a daily time-step, deterministic,

used the calibrated baseline version of the NHM-PRMS that is pro-

distributed-parameter, process-based hydrologic model used to simu-

vided by Hay (2019) for our model experiment, with the exception of

late hydrologic component fluxes in response to precipitation and cli-

the calibrated SDCs (described in the next section), to evaluate the

mate forcing (Markstrom et al., 2015). The NHM-PRMS was applied

sensitivity of simulated runoff to SDC representation within the

across the extent of the CONUS on the Geospatial Fabric (Viger &

model. An important scaling and model structure consideration to this

Bock, 2014) composed of 109,951 HRUs that represent a given

study is how hydrologic models distribute simulated snowmelt input

watershed contributing area to a given stream segment divided into

across an HRU. In PRMS, snowmelt input from the SCA is uniformly

left and right banks (Regan et al., 2018; Regan et al., 2019; Viger &

broadcasted across the entire HRU rather than being concentrated

Bock, 2014). The mean ± standard deviation (median) HRU size of the
Geospatial Fabric is 75 ± 399 km2 (33 km2). Regan et al. (2018) pro-

within the SCAs only. Therefore, there is potential scaling uncertainty

in relating the watershed-wide surface water input rate to runoff gen-

vide a detailed description of the Geospatial Fabric HRU delineation

eration processes that are spatially dependent on snowmelt variability.

procedures, which were based on points of interest discussed with

Based on this potential uncertainty, we evaluate the importance of

the hydrologic modelling community; optimizing HRU delineations

model structure as it relates to the results of this study in the scaling

specifically based on the representation of the spatial heterogeneity

and model structure considerations section later in this paper.

of snow dynamics was not feasible during the Geospatial Fabric devel-

opment. The NHM-PRMS utilizes nationally consistent hydrologic

model development (i.e., physical and statistical methods for dis-

2.2

| Model experiment

cretization, characterization, parameterization, forcing, calibration and

evaluation)

for simulating watershed-scale hydrologic processes

The model experiment completed for this study consisted of seven

across the CONUS (Regan et al., 2018).

NHM-PRMS model simulations. For each of the model simulations,

2368

SEXSTONE ET AL.

we replaced the HRU-calibrated NHM-PRMS SDCs with a single SDC

where λ and ζ are the distribution parameters related to the mean (μ)

derived by assuming a lognormal pdf and assigning a CV value. Luce,

and CV of the pre-melt SWE (S). Using this technique, we derived

Tarboton, and Cooley (1999) described how a dimensionless SDC

seven dimensionless SDCs assuming a two-parameter lognormal pdf

could be estimated for a given pdf by evaluating the decreases in SCA

with CV values ranging from 0.1 to 2.0 (Figure 1). Because this tech-

with decreasing normalized watershed-mean SWE for several values

nique assumes uniform snowmelt conditions, the derivation of the

of watershed-mean snowmelt amount (assuming uniform snowmelt

SDCs did not change when changing the mean pre-melt SWE or

conditions). As summarized by Donald et al. (1995), Luce et al. (1999),

snowmelt rate and were only dependent on the CV value. Each SDC

Essery and Pomeroy, Essery, and Toth (2004), and Clark et al. (2011),

represents how the aerial SCA of an HRU decreases with the frac-

these values can be evaluated based on the pdf with the following

equations:

SCA =

ð∞

M

f Sð ÞdS

SWE =

ð∞

M

S− Mð

Þf Sð ÞdS,

ð1Þ

ð2Þ

tional decrease of SWE below the threshold SWE value (SWE100) for
the given HRU (Donald et al., 1995; Fassnacht et al., 2016; Markstrom

et al., 2015). As described above, the simulated snowmelt input from

the SCA in PRMS is uniformly broadcasted across the entire HRU.

Additionally, in PRMS, new snowfall that occurs during the snowmelt

period will increase the SCA to its maximum value and a linear inter-

polation is used to between this value and the previous location on

the depletion curve as the new snowfall melts (Figure 1b; Markstrom

et al., 2015).

where SCA is the fractional snow-covered area, SWE is the grid-mean

SWE, M is the total melt since the beginning of the snow season and

The seven NHM-PRMS simulations were completed at a daily
time step over a 14-year period (2003–2016) using daily minimum

S is the SWE (as described by the pdf). Liston (2004) and DeBeer and

and maximum air temperature and daily precipitation forcing data

Pomeroy (2009) described this framework with the use of a two-

from Daily Surface Weather and Climatological Summaries (DAYMET,

parameter lognormal pdf:

(

f Sð Þ =

p exp − 1
1
ﬃﬃﬃﬃﬃﬃ
2π
2

Sζ

(cid:4)
(cid:3)
2
ln Sð Þ− λ
ζ

)

,

with

(A)

λ = ln μð Þ− 1
2

ζ2

(cid:5)
ζ2 = ln 1 + CV2

(cid:6)

,

https://daymet.ornl.gov/) that were spatially aggregated from the

gridded DAYMET dataset (Thornton et al., 2016) using the USGS

GeoData Portal

(GDP, http://cida.usgs.gov/gdp/; Blodgett, Booth,

Kunicki, Walker, & Viger, 2011). Each of the seven model experiment

simulations used the baseline HRU calibrated NHM-PRMS model

parameters from Hay (2019), replacing all SDCs with those derived

from SWE CV values ranging from 0.1 to 2.0 (Figure 1). This model

experiment allowed for nationally consistent methods to evaluate the

sensitivity of simulated runoff to varying SDC representations.

Model output variables from each simulation that were evaluated

in this study are described in Table 1. To evaluate simulation results

ð3Þ

ð4Þ

ð5Þ

(B)

F I G U R E 1

(a) Snow depletion curves (SDCs) derived based on a lognormal probability distribution function and snow water equivalent (SWE)

coefficient of variation (CV) value used for the seven model experiment simulations. The vertical arrows denote the difference between the CV
0.5 and 1.0 simulations. (b) Example illustration of the depletion of snow-covered area (SCA) and SWE during the snowmelt period based on the
CV 0.5 and CV 1.0 SDCs that highlight how the simulated SCA responds to new snowfall during the snowmelt period. SWE100 is the maximum
SWE amount for a given hydrologic response unit below which the SDC is applied, as further decreases in SWE are associated with reductions
in SCA

SEXSTONE ET AL.

2369

over a consistent decadal mean condition, annual summary statistics

(McDonald, 2014) were then used to test for significant differences

were computed from daily model outputs for every HRU, and a mean

(p-values <.01)

in aridity and multiple metrics of runoff efficiency

was computed of annual summary statistics over the 14-year period.

between snow zones.

To directly evaluate the sensitivity of the model simulation results to

the change in SDC, a linear regression between the CV value (explana-

tory variable) and model output summary value (response variable)

3

| RE SU LT S

was computed for each HRU. We define the model output

(e.g., runoff ratio; Table 1) sensitivity as the slope of the linear regres-

sion over a change in CV of 1.0. Model output data that support the

findings of this study are available in Sexstone and Driscoll (2019).

| Differences in variable response to SDC

3.1
representation

The snow persistence (SP) metric derived from MODIS SCA

3.1.1

|

Simulated snowmelt

(MOD10A2; Hall & Riggs, 2016a) and described in detail by Ham-

mond et al. (2018) was used in this study to classify HRUs with similar

Simulated peak SWE was relatively unchanged between the model

snow properties and evaluate runoff sensitivity to the choice of SDC

experiment simulations (Figure 2a), as the varying SDCs only resulted

(Table 1). Annual SP layers were obtained for water years 2001 to

in changes to the simulated snowmelt period. Deviations in simulated

2015 (Hammond, Saavedra, & Kampf, 2017) and averaged across all

peak SWE were only observed in HRUs where the maximum SWE

years to obtain mean annual SP. Mean annual SP was then zonally

occurred following the initiation of snowmelt in a given year. More

averaged for each HRU with full SP grid coverage (Figure S1) and

substantial differences between model simulations were observed in

HRUs were then classified into the following four categories:
snow SP ≤ 0.25,
snow 0.50 < SP ≤ 0.75 and persistent

intermittent snow 0.25 < SP ≤ 0.50,

snow SP > 0.75. Non-

transitional

low

simulated snowmelt rate (Figure 2b) and simulated snowmelt duration

(Figure 2c). With an increase in the SWE CV and its associated SDC

(Figure 1), simulated snowmelt rate decreased and simulated snow-

parametric Kruskal-Wallis

and Wilcoxon rank

sum analyses

melt duration increased monotonically (Figure 2).

In other words,

T A B L E 1 Definitions of model variables and metrics used in this
study

Variable

Definition

Snow water

equivalent (SWE)

Snowpack water equivalent on each
hydrologic response unit (HRU)

Snow-covered area

SCA on each HRU

(SCA)

Snowmelt

Snowmelt from snowpack on each HRU

Runoff ratio

Total flow leaving each HRU normalized by

precipitation

Evaporative index

Actual evapotranspiration (ET) for each HRU

SWE precipitation
ratio (SWE:P)

Dryness index
(PET/P)

Surface runoff
efficiency

Interflow efficiency

Groundwater
efficiency

normalized by precipitation

Maximum SWE on each HRU normalized by

precipitation

Potential ET for each HRU normalized by

precipitation

Surface runoff to the stream network for each

HRU normalized by precipitation

Interflow from gravity and preferential-flow
reservoirs to the stream network for each
HRU normalized by precipitation

reservoirs to the stream network for each
HRU normalized by precipitation

Snow persistence

Fraction of time snow is present on the

(SP)

ground between January and July for each
HRU based on MODIS SCA satellite
observations.

Note: Refer to Markstrom et al. (2015) for further description of
precipitation-runoff modelling system variables and Hammond, Saavedra,
and Kampf (2018) for further description of the snow persistence metric.

increasing the sub-grid snow variability as represented by the SDC

resulted in a consistently slower simulated snowmelt rate and longer

simulated snowmelt season at the HRU scale across the CONUS.

Total simulated snowmelt amount also increased with increasing snow

variability (Figure 2d), as the longer simulated snowmelt duration

yielded a greater period for precipitation as rain or snow to fall onto

the existing snowpack and be counted towards simulated snowmelt

amount. The sensitivity of snowmelt rate to the SDC representation

(i.e., the regression slope between the CV value and snowmelt rate for

each HRU) highlights that changes in snowmelt rate were widespread

across regions of the CONUS that receive snow and were greatest in

snow-dominated areas such as the western U.S. mountains (Figure 3).

3.1.2

|

Simulated runoff

The sensitivity in runoff ratio across all model simulations highlights

widespread decreases in overall runoff with increases in CV because

of slower snowmelt conditions at the HRU scale (Figure 4). Although

the mean decrease in runoff ratio across all CONUS HRUs was equal
to −0.5%, runoff ratio decreased by as much as 12% in highly sensi-

tive regions (Figure 4). Runoff ratio sensitivity to changing snowmelt

U.S. mountains (Figure 4). Additionally, little to no simulated runoff

sensitivity in much of the central and eastern U.S. (except for the most

northern regions) was observed (Figure 4) despite these regions

receiving winter snow accumulation. A comparison between simu-

lated evaporative index (including all evaporation, transpiration and

sublimation processes) and simulated runoff ratio shows that changes

in runoff ratio were approximately inversely proportional to changes

in evaporative index during the model experiment (Figure 4). A linear

Groundwater discharge from groundwater

rate at the HRU scale was greatest in the snow-dominated western

SEXSTONE ET AL.

2370

(a)

(c)

(b)

(d)

F I G U R E 2

Boxplots of (a) peak snow water equivalent, (b) snowmelt rate, (c) snowmelt duration and (d) total snowmelt for each of the

coefficient of variation model experiment simulations showing the median (black horizontal line), 25th and 75th percentiles (box), and the largest
(smallest) value within 1.5 times the interquartile range above (below) the 75th (25th) percentile (whiskers)

regression between the simulated evaporative index sensitivity and

runoff sensitivity to varying snow metrics within this region. First, we

the simulated runoff ratio sensitivity for runoff ratio decreases greater
than 5% (n = 2,458; runoff ratio sensitivity = −0.923 × evaporative
index sensitivity − 0.004; R2 = 0.96; p-value <.001)

indicates that

compared runoff sensitivity to mean SP (%) computed from MODIS

SCA observed during the simulation period (Figure S1;Hammond et al.,

2017; Hammond et al., 2018). A statistically significant difference (p-

approximately 92% of the decreases in runoff ratio may be attributed

value <.001) in simulated runoff ratio sensitivity was observed between

to increases in evaporative index. Based on the hydrologic processes

the low snow,

intermittent, transitional and persistent snow zones

simulated within the NHM-PRMS (Markstrom et al., 2015), this result

using the non-parametric Kruskal-Wallis test (Figure 5). The runoff ratio

indicates that increases in simulated groundwater flow to deep

sensitivity across all model simulations was nonlinearly related to SP,

groundwater storage may account for only approximately 8% of the

with greater decreases in runoff ratio associated with higher SP

simulated runoff ratio decreases. Hydrologic partitioning between sur-

(Figure 5). The spread in the distribution of runoff ratio sensitivities was

face runoff, interflow and groundwater (Table 1) also displayed wide-

greater for transitional and persistent SP zones as compared to the low

spread sensitivity to changing snowmelt rate and duration (Figures S2

snow and intermittent zones (Figure 5). The simulated runoff ratio sen-

and S3); however, these changes did not always translate into overall

sitivity was also compared with snow metrics simulated by the NHM-

runoff sensitivity (Figure 4c).

3.2

| Regional sensitivity

PRMS. The sensitivity in simulated snowmelt rate (Figure 3b) was sig-
nificantly linearly related (R2 = 0.60; p-value <.001) with the simulated

(decreases)

runoff ratio sensitivity (Figure 6a). This relation highlights that increases
in simulated snowmelt rate by 1 mm day−1 resulted in
in runoff ratio 1.8% on average in snow-

increases (decreases)

3.2.1

| Western U.S. snow amount and aridity

dominated regions of the CONUS. Additionally, the ratio of simulated

Given that runoff ratio sensitivity was greatest in snow-dominated

peak SWE to annual precipitation (SWE:P; Figure S4) for the western
U.S. was significantly linearly related (R2 = 0.83; p-value <.001) with the

regions of the western U.S. (Figure 4c), we evaluated the relation of

runoff ratio sensitivity (Figure 6b).

SEXSTONE ET AL.

2371

F I G U R E 3

Spatial variability of the

(a) snowmelt rate simulated for the
coefficient of variation 0.5 model
simulation and (b) the snowmelt rate
sensitivity computed across all model
simulations. Mean value and 10th and
90th percentiles (P) across all
hydrologic response units are shown in
each panel

(a)

(b)

The sensitivity in simulated runoff ratio was additionally found to

the highest water limitation likely receive less snowfall and are thus

be related to aridity in the western U.S. region. Dryness index (PET/P;

less sensitive to changing snowmelt

than regions that

receive

Table 1) is a measure of the relative water limitation (dryness index

more snow.

>1) or energy limitation (dryness index <1) of an HRU (Budyko, 1974;

Knowles et al., 2015). The simulated dryness index was divided into

six quantiles for each of the western U.S. SP zones (e.g., Figure 5) to

evaluate its effect on overall runoff sensitivity (Figure 7). Results high-

light a statistically significant difference (p-value <.001; Kruskal-Wallis

| Regional sensitivity of runoff in

3.2.2
snow-dominated western U.S.

test) between the runoff ratio sensitivities divided by dryness index

We evaluated the regional simulated runoff sensitivity of changing

quantiles for each SP zone (Figure 7). A similar pattern for intermit-

snowmelt conditions in the western U.S. using both Level III ecoregion

tent, transitional and persistent snow zones showed the greatest run-

boundaries (https://www.epa.gov/eco-research/ecoregions) and the

off sensitivity occurring for dryness index values with water limitation

MODIS derived SP zones (Hammond et al., 2017; Hammond et al.,

between approximately 1 and 2. As dryness index increased above

2018). For each of the 10 major mountainous Level III ecoregions of

this range, the overall runoff sensitivity decreased. Likewise, runoff

the western U.S. evaluated by Barnhart et al. (2016), we compiled

sensitivity also decreased for energy limited HRUs with dryness index

HRUs located within either the transitional and persistent snow zones

values <1. This pattern highlights the importance of aridity to the run-

(Figure 8). A statistical comparison between the means of each region

off sensitivity; however, this pattern also highlights that HRUs with

compared to the mean of all regions using the non-parametric

2372

(a)

(c)

SEXSTONE ET AL.

(b)

(d)

F I G U R E 4

Spatial variability of the (a) runoff ratio and (b) evaporative index simulated for the coefficient of variation 0.5 model simulation
and (c) the runoff ratio sensitivity and (d) evaporative index sensitivity across all model simulations. Mean value and 10th and 90th percentiles
(P) across all hydrologic response units are shown in each panel

Wilcoxon rank sum test highlights that the Sierra Nevada, Idaho Bath-

rates resulted in increases in simulated actual evapotranspiration pro-

olith, Southern Rockies and Wasatch and Uintas ecoregions have sig-

moted by an increase in the contact time of snow with the atmo-

nificantly more runoff sensitivity and the Cascades, North Cascades,

sphere as well as snowmelt water within the rooting zone. Our results

Northern Rockies, Canadian Rockies and Eastern Cascades ecoregions

indicate the importance of concentrated input to efficiently generat-

have significantly less runoff sensitivity to changing snowmelt com-

ing streamflow and are consistent with a recent modelling study by

pared to the mean of all regions (Figure 8). Figure 8 highlights this

Barnhart et al. (2016) highlighting that faster snowmelt rates tend to

numbered order of regional sensitivity (from highest sensitivity to

promote greater streamflow generation by bringing the soil to field

lowest sensitivity) to simulated decreases in snowmelt rate across the

capacity, allowing for below-root zone percolation and streamflow

western U.S. mountains.

4

| D I S C U S S I O N

4.1

|

Sensitivity of simulated runoff to SDCs

generation. These results were further evidenced by recent 1-D soil

input partitioning modelling (Hammond et al., 2019). Runoff ratio sen-

sitivity to SDC representation presented in this study highlights the

importance of carefully parameterising and evaluating SDCs for

hydrologic modelling applications in snow-dominated regions.

Simulated runoff by the NHM-PRMS was shown to be widely sensi-

4.2

|

Scaling and model structure considerations

tive to the SDC representation in snow-dominated regions of the

CONUS. Across all model simulations, an increase in the derived SDC

Our results showing that an increased sub-grid snow variability as rep-

CV by 1.0 resulted in a runoff ratio decrease by as much as 12% and

resented by SDCs produces an effectively longer snowmelt duration

by 4% on average in the major mountainous ecoregions in the west-

and slower snowmelt rate at the HRU scale is supported by previous

ern U.S. (Figure 8). These decreases in runoff ratio were driven by

research (e.g., Luce et al., 1998). However, at the point scale, areas

slower simulated snowmelt rates averaged across the HRU, with
decreases in snowmelt rate of 1 mm day−1 yielding decreases in run-
off ratio of 1.8% on average (Figure 6a). Slower simulated snowmelt

that hold snow longer through the snowmelt season, such as wind-

induced snow drifts, are likely to experience faster snowmelt rates

(Trujillo & Molotch, 2014), while also being associated with

SEXSTONE ET AL.

2373

watersheds that have high sub-grid snow variability. Furthermore,

drifts have high runoff efficiency and are a key source of runoff for

many studies evaluating runoff in Reynolds Creek Experimental

the watershed (Chauvin et al., 2011; Flerchinger, Hanson, & Wight,

Watershed,

Idaho, have highlighted that deep wind-induced snow

1996; Luce et al., 1998; Marshall et al., 2019; Stephenson & Freeze,

1974). Therefore, in the context of our results suggesting that runoff

ratio decreases with increased snow variability at the watershed scale,

an important scaling consideration is how hydrologic models distrib-

ute simulated snowmelt input across an HRU. Snowmelt input from

the SCA simulated by the NHM-PRMS in this study was uniformly

broadcasted across the entire HRU rather than being concentrated

within the SCAs only. To test the effect of this model structure on

simulated runoff response, we used PRMS to simulate a given volume

of snow distributed unevenly across two HRU elements, and the same

volume and snow distribution applied to a single HRU using an SDC
(Figure 9). This “distributed” model using two HRUs versus “lumped”

model using an SDC was tested based on a uniform snowpack with

(a) 80% SCA and CV of 0.5 and (b) 50% SCA and CV of 1.1 (Figure 9).

The climate and model parameters from a snow-dominated HRU

within the Southern Rockies ecoregion was used for these test simula-

tions over the same 14-year period as the original model experiment

(Figure 8).

Figure 9 shows the simulated snowmelt rate, total snowmelt

amount and runoff ratio during the snowmelt period for both the dis-

tributed (two HRUs) simulations and the lumped (SDC) simulations.

Results highlight that simulated snowmelt rate and amount are com-

parable between the distributed versus lumped simulations when

averaged across the entire HRU (Figure 9). However, when the snow-

melt rate is averaged over the SCA only for the distributed simula-

tions, the effective snowmelt rate was shown to increase substantially

(Figure 9). Additionally, a clear effect in model structure is highlighted

by the differences in simulated runoff ratio between the distributed

F I G U R E 5

Boxplots of western U.S. runoff ratio sensitivity across

all model simulations grouped by the snow persistence (SP) derived
low snow (LS; SP ≤ 0.25), intermittent (I; 0.25 < SP ≤ 0.50),
transitional (T; 0.50 < SP ≤ 0.75) and persistent (P; SP > 0.75) snow
zones. Boxplots are hereinafter represented by the median (black
horizontal line), 25th and 75th percentiles (box), and the largest
(smallest) value within 1.5 times the interquartile range above (below)
the 75th (25th) percentile (whiskers), and values that are >1.5 times
and <3 times the interquartile range beyond either end of the box
(outliers). The statistical significance (p-value) of difference between
groups computed by the non-parametric Kruskal-Wallis test is shown

(a)

(b)

F I G U R E 6

Scatterplots comparing both (a) the simulated snowmelt rate sensitivity across all model simulations and (b) the simulated peak

snow water equivalent (SWE) over annual precipitation (P) ratio (SWE:P) to the western U.S. runoff ratio sensitivity across all model simulations

2374

(a)

(b)

(c)

(d)

SEXSTONE ET AL.

F I G U R E 7

Boxplots of western U.S. runoff ratio sensitivity across all model simulations divided by six dryness index quantiles (coloured)

computed for each of the (a) low snow, (b) intermittent, (c) transitional and (d) persistent snow persistence zones. The statistical significance (p-
value) of difference between groups computed by the non-parametric Kruskal-Wallis test is shown on each panel

versus lumped simulations (Figure 9). The lumped simulations using an

effect of model structure (with respect to redistribution of snowmelt

SDC to represent the snow variability resulted in a greater percentage

water across the HRU) to both the effective snowmelt rate and the

of snowmelt being partitioned into evapotranspiration because the

associated hydrologic partitioning presented here (Figure 9), we sug-

snowmelt water was uniformly distributed across the entire HRU

gest that additional research is needed in snow-dominated regions

rather than concentrated within the SCA (Figure 9). Furthermore, this

with dense data available for model evaluation to examine model

effect was more pronounced for the CV of 1.1 (50% SCA) simulation

improvements in representing sub-grid snowmelt input associated

compared to the CV of 0.5 (80% SCA) simulation (Figure 9). However,

with the SDC and its relation with associated runoff generation pro-

when taken together, the annual results from both the lumped and

cesses. However, despite this substantial model scaling effect, we

distributed simulations show a robust linear relation between snow-

show that the relation and regional sensitivity between runoff genera-

melt rate and runoff ratio (Figure 9; decreases in snowmelt rate of
1 mm day−1 result in decreases in runoff ratio of 3.6%) that is consis-
tent with key findings from this study (Figure 6a; decreases in snow-
melt rate of 1 mm day−1 result in decreases in runoff ratio of 1.8% on
average in snow dominated regions of the CONUS).

These results indicate that accurately simulating the snowmelt

input averaged across an entire HRU may not necessarily result in

accurate simulation of snowmelt runoff generation. Recent studies

tion and snowmelt rate presented in this study is robust to these

model structure differences and provides important process insights;

thus, we provide further discussion related to these results below.

| Runoff response to changing snowmelt

4.3
dynamics

showing that substantial snowmelt can be localized to specific areas

Observational evidence for slower snowmelt in warmer periods has

of a watershed because of redistribution of meltwater and that

led to modelling that shows meltwater produced at high snowmelt

greater runoff can be generated than if snowmelt were evenly distrib-

rates is greatly reduced in a warmer climate because of an earlier

uted across landscape further highlight the importance of the sub-grid

snowmelt

season during a period of

lower available energy

variability of

snowmelt

input

(Eiriksson et al., 2013; Webb,

(Musselman, Clark, Liu, Ikeda, & Rasmussen, 2017). The results pres-

Fassnacht, & Gooseff, 2018; Webb, Williams, & Erickson, 2018).

ented in this study are important for providing insights to the

Therefore, as suggested by Luce et al. (1998), an important challenge

CONUS-scale spatial variability of runoff sensitivity and response to

for future hydrologic modelling applications is relating the watershed-

changing snowmelt dynamics. However, the model experiment pres-

wide surface water input rate to runoff generation processes that are

ented in this study does not represent any change in the timing of

spatially dependent on snowmelt variability. Given the substantial

snowmelt initiation, which is also an observational result of changing

2375

SEXSTONE ET AL.

(a)

10

6

1

9

8

7

2

5

4

3

(b)

0.00

–0.05

–0.10

(a) The runoff ratio sensitivity across all model simulations shown for the western U.S. Level III ecoregions with the major

F I G U R E 8
mountainous ecoregions numbered for reference. (b) Boxplots of the sensitivity in simulated runoff ratio for the transitional (0.50 < SP ≤ 0.75)
and persistent (SP > 0.75) snow persistence zones within each of the major mountainous ecoregions ordered from most sensitive to least
sensitive (ordered left to right; numbered 1–10). The statistical significance of the difference of means between each region (denoted by the ×)
with all regions (denoted by the horizontal dashed black line) computed by the non-parametric Wilcoxon rank sum test is shown above each
boxplot (**** = p-value <0.0001; *** = p-value <.001; * = p-value <.1)

(a) Conceptual diagram showing the difference between the snowmelt input area across the hydrologic response unit for the

F I G U R E 9
“lumped with snow depletion curve” and “distributed” model configurations, (b) the resulting comparison of mean ± standard deviation snowmelt
rate, total snowmelt, and runoff ratio during the snowmelt period for the distributed versus lumped model, and (c) scatterplot and linear relation
comparing the simulated snowmelt rate to the simulated runoff ratio for both model configurations. Model comparisons are shown for a uniform
snowpack distribution with (1) 80% snow-covered area (SCA) (coefficient of variation [CV] 0.5) and (2) 50% SCA (CV 1.1)

climate (Clow, 2010; Stewart et al., 2004) where the timing of snow-

(Figure 4), which is supported by previous work showing higher

melt is also an important factor influencing the dynamics of evapo-

evapotranspiration in areas where snowmelt rate declines (Hammond

transpiration (Barnhart et al., 2016; Brooks et al., 2015; Harpold &

et al., 2019). Other evidence indicates that decreased annual

Molotch, 2015; Molotch et al., 2009). In this study, higher evaporative

streamflow is also due to increased evapotranspiration in response to

indices consistently coincided with reduced runoff efficiencies

increased temperatures, earlier snowmelt and longer snow-free

2376

SEXSTONE ET AL.

duration (Christensen & Lettenmaier, 2007; Foster, Bearup, Molotch,

efficiency depends primarily on slope, with steeper slopes limiting

Brooks, & Maxwell, 2016; McCabe, Wolock, Pederson, Woodhouse, &

infiltrated input's time in the rooting zone (Voepel et al., 2011). A

McAfee, 2017; Woodhouse, Pederson, Morino, McAfee, & McCabe,

higher snowfall fraction (i.e., SWE:P) has been shown to be associated

2016). The results from the model experiment in this study indicate

with a lower evaporative index (Berghuijs et al., 2014); therefore,

that a decrease in snowmelt rate and increase in snowmelt season

results shown in Figure 7 highlight the combined influence of aridity

duration can result in increases in evapotranspiration that are approxi-

and SP to runoff sensitivity.

mately inversely proportional to decreases in runoff (Figure 4).

Distinct differences in the regional sensitivity of changes to

snowmelt rate in the western U.S. mountains were observed in this

4.4

|

Implications for future modelling applications

study (Figure 8). Overall, the greatest simulated runoff ratio sensitivi-

ties were found in the persistent snow zone (Figure 5) where SWE:P

The model experiment presented in this study was designed specif-

is high (Figure 6). Although the simulated change in snowmelt rate

ically to evaluate the sensitivity of simulated runoff to the repre-

was the driving mechanism resulting in a change in runoff ratio, a key

sentation of SDCs within the model rather than determining the

result of this study is that the SWE:P was able to explain more of the

most accurate SDC representation across the CONUS scale. How-

variability in runoff ratio sensitivity than the change in snowmelt rate

ever, results presented here provide important insights into future

(Figure 6). This result highlights that the regional sensitivity of runoff

development of model representations of snow variability within

to changing snowmelt rate is strongly linked to how much of the

the NHM-PRMS and other models using methods to represent sub-

annual runoff originates as snowmelt (Li, Wrzesien, Durand, Adam, &

grid snow variability (e.g., Liston, 2004; Newman, Clark, Winstral,

Lettenmaier, 2017). For example, the transitional and persistent snow

Marks, & Seyfried, 2014; and references within). Overall, Figure 4

zones of the Sierra Nevada and Cascades ecoregions (Figure 8) both

highlights the HRUs with simulated runoff that are particularly sen-

exhibited substantial decreases in simulated snowmelt rate during the

sitive to SDC representation within the model, which are generally

model experiment (Figure 3b), but the SWE:P in the Sierra Nevada

situated in the persistent and transitional snow zones (Figure 5).

was much greater than the Cascades (Figure S4). Accordingly, the run-

The development and calibration of SDCs for these HRUs may war-

off sensitivity in the Sierra Nevada ecoregion was significantly greater

rant special attention and potentially prioritization. Additionally,

than the Cascades ecoregion during the model experiment (Figure 8).

this study highlights the importance of representing the sub-grid

Furthermore, results from this study highlight how regional aridity

variability of snowmelt input into a watershed (Figure 9). Simulating

is important to the sensitivity of runoff to changing snowmelt rate,

the sub-grid SWE variability and resulting snowmelt input averaged

with water-limited dryness index values between 1.0 and 1.6 in the

across an HRU may not necessarily result in accurate simulation of

transitional and persistent snow zones exhibiting the greatest runoff

snowmelt runoff generation; therefore, model

improvements to

sensitivity (Figure 7). These results are important in the context of

represent the sub-grid snowmelt input associated with the SDC

previous research that has shown that evaporative index varies along

and its relation with associated runoff generation processes are

gradients of input seasonality, the fraction of precipitation falling as

needed.

snow, input intensity and topography among other factors. With sea-

The spatially variable dimensionless SDCs based on the lognormal

sonal input, runoff may be more efficiently generated during the wet

pdf (Figure 1) presented in this study could be implemented in future

season with accompanying lower evapotranspiration efficiency, espe-

applications across the CONUS using the SWE CV classification sys-

cially when subsurface storage is low (Carey et al., 2010). In areas with

tem developed by Liston (2004). Using these SDCs as a starting point,

less seasonal precipitation and greater storage, evapotranspiration

targeted calibration strategies (based on the sensitivity results pres-

efficiency may increase. Similar to the results of our work, Berghuijs

ented in this study) to remotely sensed SCA observations could then

et al. (2014) show the highest sensitivity of hydrologic partitioning in

be completed (Hay, 2019). Calibration could then be evaluated using

the range of aridity index between 1 and 2. This may indicate that

both SWE and SCA observations from both remote sensing platforms

snowmelt dynamics play a greater role in hydrologic partitioning in

and ground-based observations (e.g., Arsenault & Houser, 2018;

this range than in very dry or very wet locations. Using the Budyko

Fassnacht et al., 2016) as well as results from fine-scale snow model-

(1974) framework, Berghuijs et al.

(2014) reveal greater positive

ling applications (e.g., Broxton, van Leeuwen, & Biederman, 2019;

streamflow anomalies with increasing fraction of precipitation falling

Hedrick et al., 2018; Sexstone et al., 2018). Additionally, known rela-

as snow as well as corresponding decreases in evapotranspiration effi-

tions between SDCs and topography and climatic variables (Driscoll

ciency with increases in the fraction of precipitation falling as snow.

et al., 2017) could be utilized for evaluation. Potential scaling chal-

Hammond et al. (2018) show that the effect of snow dominance on

lenges and uncertainties of these methods remain in that Pomeroy

runoff efficiency is greater in cold/dry watersheds than warm/wet

et al. (2004) and DeBeer and Pomeroy (2010) show that a lognormal

watersheds. Input intensity also strongly controls input partitioning to

distribution can fail to accurately represent snow distributions when

evapotranspiration, where efficiency declines with higher input rates

aggregated over varying landscape classes. This indicates there may

(Barnhart et al., 2016; Wang & Tang, 2014) and greater input concen-

be a need to further disaggregate HRUs in the NHM-PRMS to more

tration in time (Hammond et al., 2019) in both arid and humid water-

accurately represent sub-grid snow processes; further investigation is

sheds. The role of topography in controlling evapotranspiration

needed to evaluate the trade-offs between computational efficiency,

SEXSTONE ET AL.

2377

data availability and improved process representation within this

OR CID

national-scale modelling system (e.g., Archfield et al., 2015).

Graham A. Sexstone

https://orcid.org/0000-0001-8913-0546

5

| C O N CL U S I O N S

Jessica M. Driscoll

https://orcid.org/0000-0003-3097-9603

Lauren E. Hay

https://orcid.org/0000-0003-3763-4595

John C. Hammond

https://orcid.org/0000-0002-4935-0736

Theodore B. Barnhart

https://orcid.org/0000-0002-9682-3217

In this study, we applied a daily time-step, deterministic, distributed-

parameter, process-based hydrologic model across the extent of the

RE FE RE NCE S

CONUS to investigate the sensitivity of simulated hydrologic compo-

nent fluxes to the representation of SDCs within the NHM-PRMS.

Increases in the sub-grid variability of SWE as represented by SDCs in

the model resulted in a consistently slower snowmelt rate and longer

snowmelt duration when averaged across the HRU scale. As a result

of slower snowmelt rates, simulated runoff ratios were shown to

decrease in snow-dominated regions by 1.8% on average with associ-
ated decreases in snowmelt rate by 1 mm day−1. Increases in evapora-
tive index were approximately inversely proportional to decreases in

runoff ratio, indicating decreases in runoff at small scales may primar-

ily be attributed to increasing evapotranspiration because of an

increase in the contact time of snow with the atmosphere as well as

snowmelt water with the rooting zone. However, across the large

HRUs in this model application, hydrologic partitioning was shown to

be sensitive to fast snowmelt from small SCAs being effectively

slowed by being redistributed across the entire HRU area. Regions of

the CONUS with high SP and SWE:P exhibited the greatest runoff

sensitivity to changes in snowmelt

rate. Within these snow-

dominated regions, runoff sensitivity was greatest in semi-arid areas

with dryness index values between approximately 1 and 2 (water limi-

tation). These results provide important insights into the CONUS-

scale spatial variability of runoff sensitivity to changes in snowmelt

dynamics and highlight the importance of carefully parameterizing

SDCs within hydrologic modelling. Improving model representation of

snowmelt input variability and its relation to runoff generation pro-

cesses is shown to be an important consideration for future modelling

applications. Furthermore, this study provides guidance for future

development of the NHM-PRMS to improve modelling and predictive

capacity of water availability across the CONUS.

ACKNOWLEDGEMEN TS

This research was funded by the U.S. Geological Survey (USGS) Water

Availability and Use Science Program as part of the USGS Water Bud-

get Estimation & Evaluation Project (WBEEP). Computing resources

were provided by USGS Core Science Systems (CSS) Advanced

Research Computing (ARC) Center. Thanks to Steve Regan (USGS) for

assistance with the NHM-PRMS. We would also like to thank Mat-

thew Miller (USGS), Charles Luce and two anonymous reviewers for

their insightful comments that improved this manuscript. Any use of

trade, firm or product names is for descriptive purposes only and does

not imply endorsement by the U.S. Government.

DATA AVAI LAB ILITY S TATEMENT

The data that support the findings of this study are available in USGS

ScienceBase at https://doi.org/10.5066/P9OEIRJF.

Anderson, E. A. (1968). Development and testing of snow pack energy bal-
ance equations. Water Resources Research, 4(1), 19–37. https://doi.
org/10.1029/WR004i001p00019

Anderton, S. P., White, S. M., & Alvera, B. (2002). Micro-scale spatial vari-
ability and the timing of snow melt runoff in a high mountain catch-
ment. Journal of Hydrology, 268(1-4), 158–176. https://doi.org/10.
1016/S0022-1694(02)00179-8

Archfield, S. A., Clark, M., Arheimer, B., Hay, L. E., McMillan, H.,
Kiang, J. E., … Over, T. (2015). Accelerating advances in continental
domain hydrologic modeling. Water Resources Research, 51(12),
10078–10091. https://doi.org/10.1002/2015wr017498

Arsenault, K., & Houser, P. (2018). Generating observation-based snow
depletion curves for use in snow cover data assimilation. Geosciences,
8(12), 484. https://doi.org/10.3390/geosciences8120484

Barnett, T. P., Adam, J. C., & Lettenmaier, D. P. (2005). Potential impacts
of a warming climate on water availability in snow-dominated regions.
Nature, 438(7066), 303–309. https://doi.org/10.1038/nature04141
Barnhart, T. B., Molotch, N. P., Livneh, B., Harpold, A. A., Knowles, J. F., &
Schneider, D. (2016). Snowmelt rate dictates streamflow. Geophysical
Research
https://doi.org/10.1002/
2016gl069690

Letters, 43(15), 8006–8016.

Berghuijs, W. R., Woods, R. A., & Hrachowitz, M. (2014). A precipitation shift
from snow towards rain leads to a decrease in streamflow. Nature Climate
Change, 4(7), 583–586. https://doi.org/10.1038/Nclimate2246

Blodgett, D. L., Booth, N. L., Kunicki, T. C., Walker, J. L., & Viger, R. J.
(2011). Description and testing of the Geo Data Portal—Data integra-
tion framework and Web processing services for environmental sci-
ence collaboration. U.S. Geological Survey Open-File Report
2011–1157. https://pubs.usgs.gov/of/2011/1157/

Blöschl, G. (1999). Scaling issues in snow hydrology. Hydrological Processes,

13(14-15), 2149–2175.

Bock, A. R., Farmer, W. H., & Hay, L. E. (2018). Quantifying uncertainty in
simulated streamflow and runoff from a continental-scale monthly
water balance model. Advances in Water Resources, 122, 166–175.
https://doi.org/10.1016/j.advwatres.2018.10.005

Bock, A. R., Hay, L. E., McCabe, G. J., Markstrom, S. L., & Atkinson, R. D.
(2016). Parameter regionalization of a monthly water balance model for
the conterminous United States. Hydrology and Earth System Sciences, 20
(7), 2861–2876. https://doi.org/10.5194/hess-20-2861-2016

Brooks, P. D., Chorover, J., Fan, Y., Godsey, S. E., Maxwell, R. M.,
McNamara, J. P., & Tague, C. (2015). Hydrological partitioning in the
critical zone: Recent advances and opportunities for developing trans-
ferable understanding of water cycle dynamics. Water Resources
Research, 51(9), 6973–6987. https://doi.org/10.1002/2015wr017039
Broxton, P. D., van Leeuwen, W. J. D., & Biederman, J. A. (2019). Improv-
ing snow water equivalent maps with machine learning of snow survey
and lidar measurements. Water Resources Research, 55, 3739–3757.
https://doi.org/10.1029/2018wr024146

Budyko, M. I. (1974). Climate and life. New York: Academic Press.
Carey, S. K., Tetzlaff, D., Seibert, J., Soulsby, C., Buttle, J., Laudon, H., …
Pomeroy, J. W. (2010). Inter-comparison of hydro-climatic regimes
across northern catchments: Synchronicity, resistance and resilience.
Hydrological Processes, 24(24), 3591–3602.

Chauvin, G. M., Flerchinger, G. N., Link, T. E., Marks, D., Winstral, A. H., &
Seyfried, M. S. (2011). Long-term water balance and conceptual model

2378

SEXSTONE ET AL.

of a semi-arid mountainous catchment. Journal of Hydrology, 400(1-2),
133–143. https://doi.org/10.1016/j.jhydrol.2011.01.031

Christensen, N. S., & Lettenmaier, D. P. (2007). A multimodel ensemble
approach to assessment of climate change impacts on the hydrology
and water resources of the Colorado River basin. Hydrology and Earth
System Sciences, 11(4), 1417–1434. https://doi.org/10.5194/hess-11-
1417-2007

Christensen, N. S., Wood, A. W., Voisin, N., Lettenmaier, D. P., &
Palmer, R. N. (2004). The effects of climate change on the hydrology and
water resources of the Colorado River basin. Climatic Change, 62(1-3),
337–363. https://doi.org/10.1023/B:CLIM.0000013684.13621.1f
Clark, M. P., Hendrikx, J., Slater, A. G., Kavetski, D., Anderson, B.,
Cullen, N. J., … Woods, R. A. (2011). Representing spatial variability of
snow water equivalent in hydrologic and land-surface models: A
review. Water Resources Research, 47, W07539. https://doi.org/10.
1029/2011wr010745

Clow, D. W. (2010). Changes in the timing of snowmelt and streamflow in
Colorado: A response to recent warming. Journal of Climate, 23(9),
2293–2306. https://doi.org/10.1175/2009jcli2951.1

Cukier, R. I., Fortuin, C. M., Shuler, K. E., Petschek, A. G., & Schaibly, J. H.
(1973). Study of the sensitivity of coupled reaction systems to uncer-
tainties in rate coefficients. I Theory. The Journal of Chemical Physics,
59(8), 3873–3878. https://doi.org/10.1063/1.1680571

DeBeer, C. M., & Pomeroy, J. W. (2009). Modelling snow melt and snow-
cover depletion in a small alpine cirque, Canadian Rocky Mountains.
Hydrological Processes, 23(18), 2584–2599. https://doi.org/10.1002/
hyp.7346

DeBeer, C. M., & Pomeroy, J. W. (2010). Simulation of the snowmelt run-
off contributing area in a small alpine basin. Hydrology and Earth Sys-
tem Sciences, 14(7), 1205–1219. https://doi.org/10.5194/hess-14-
1205-2010

DeBeer, C. M., & Pomeroy, J. W. (2017). Influence of snowpack and melt
energy heterogeneity on snow cover depletion and snowmelt runoff
simulation in a cold mountain environment. Journal of Hydrology, 553,
199–213. https://doi.org/10.1016/j.jhydrol.2017.07.051

Doll, P., Mueller Schmied, H., Schuh, C., Portmann, F. T., & Eicker, A.
(2014). Global-scale assessment of groundwater depletion and related
groundwater abstractions: Combining hydrological modeling with
information from well observations and GRACE satellites. Water
Resources Research, 50(7), 5698–5720. https://doi.org/10.1002/
2014wr015595

Donald, J. R., Soulis, E. D., Kouwen, N., & Pietroniro, A. (1995). A land
cover-based snow cover representation for distributed hydrologic-
models. Water Resources Research, 31(4), 995–1009. https://doi.org/
10.1029/94wr02973

Dornes, P. F., Pomeroy, J. W., Pietroniro, A., Carey, S. K., & Quinton, W. L.
(2010). Influence of landscape aggregation in modelling snow-cover
ablation and snowmelt runoff in a sub-arctic mountainous environ-
ment. Hydrological Sciences Journal, 53(4), 725–740. https://doi.org/
10.1623/hysj.53.4.725

Driscoll, J. M., Hay, L. E., & Bock, A. R. (2017). Spatiotemporal variability of
snow depletion curves derived from SNODAS for the conterminous
United States, 2004-2013. Journal of the American Water Resources Asso-
ciation, 53(3), 655–666. https://doi.org/10.1111/1752-1688.12520
Duan, Q. Y., Gupta, V. K., & Sorooshian, S. (1993). Shuffled complex evolu-
tion approach for effective and efficient global minimization. Journal of
Optimization Theory and Applications, 76(3), 501–521. https://doi.org/
10.1007/Bf00939380

Duan, Q. Y., Sorooshian, S., & Gupta, V. (1992). Effective and efficient global
optimization for conceptual rainfall-runoff models. Water Resources
Research, 28(4), 1015–1031. https://doi.org/10.1029/91wr02985

Duan, Q. Y., Sorooshian, S., & Gupta, V. K. (1994). Optimal use of the SCE-
UA global optimization method for calibrating watershed models. Jour-
nal of Hydrology, 158(3-4), 265–284. https://doi.org/10.1016/0022-
1694(94)90057-4

Egli, L., Jonas, T., Grunewald, T., Schirmer, M., & Burlando, P.

(2012).
Dynamics of snow ablation in a small Alpine catchment observed by
scans. Hydrological Processes, 26(10),
repeated terrestrial
laser
1574–1585. https://doi.org/10.1002/hyp.8244

Eiriksson, D., Whitson, M., Luce, C. H., Marshall, H. P., Bradford, J.,
Benner, S. G., … McNamara, J. P. (2013). An evaluation of the hydrologic
relevance of lateral flow in snow at hillslope and catchment scales. Hydro-
logical Processes, 27(5), 640–654. https://doi.org/10.1002/hyp.9666
Essery, R., & Pomeroy, J. (2004). Implications of spatial distributions of
snow mass and melt rate for snow-cover depletion: Theoretical con-
siderations. Annals of Glaciology, 38, 261–265. https://doi.org/10.
3189/172756404781815275

Faria, D. A., Pomeroy, J. W., & Essery, R. L. H. (2000). Effect of covariance
between ablation and snow water equivalent on depletion of snow-
covered area in a forest. Hydrological Processes, 14(15), 2683–2695.
Fassnacht, S. R., Sexstone, G. A., Kashipazha, A. H., Lopez-Moreno, J. I.,
Jasinski, M. F., Kampf, S. K., & Von Thaden, B. C. (2016). Deriving
snow-cover depletion curves for different spatial scales from remote
sensing and snow telemetry data. Hydrological Processes, 30(11),
1708–1717. https://doi.org/10.1002/hyp.10730

Flerchinger, G. N., Hanson, C. L., & Wight, J. R. (1996). Modeling evapo-
transpiration and surface energy budgets across a watershed. Water
Resources Research, 32(8), 2539–2548. https://doi.org/10.1029/
96wr01240

Foster, L. M., Bearup, L. A., Molotch, N. P., Brooks, P. D., & Maxwell, R. M.
(2016). Energy budget increases reduce mean streamflow more than
snow–rain transitions: Using integrated modeling to isolate climate change
impacts on Rocky Mountain hydrology. Environmental Research Letters, 11
(4), 044015. https://doi.org/10.1088/1748-9326/11/4/044015

Fritze, H., Stewart, I. T., & Pebesma, E. (2011). Shifts in western North
American snowmelt runoff regimes for the recent warm decades. Jour-
nal of Hydrometeorology, 12(5), 989–1006. https://doi.org/10.1175/
2011jhm1360.1

Furey, P. R., Kampf, S. K., Lanini, J. S., & Dozier, A. Q. (2012). A stochastic
conceptual modeling approach for examining the effects of climate
change on streamflows in mountain basins. Journal of Hydrometeorol-
ogy, 13(3), 837–855. https://doi.org/10.1175/Jhm-D-11-037.1

Hall, D. K., & Riggs, G. A. (2016a). MODIS/Terra snow cover 8-day L3 global
500m grid, version 6, MOD10A2. Boulder, CO: National Snow and Ice
Data Center.

Hall, D. K., & Riggs, G. A. (2016b). MODIS/Terra snow cover daily L3 global
0.05Deg CMG, version 6, MOD10C1. Boulder, CO: National Snow and
Ice Data Center.

Hammond, J. C., Harpold, A. A., Weiss, S., & Kampf, S. K. (2019). Par-
titioning snowmelt and rainfall in the critical zone: Effects of climate
type and soil properties. Hydrology and Earth System Sciences Discus-
sions, 2019, 1–29. https://doi.org/10.5194/hess-2019-98

Hammond, J. C., Saavedra, F. A., & Kampf, S. K. (2017). MODIS MOD10A2
derived snow persistence and no data index for the western U.S. Cam-
bridge, MA: Hydroshare. https://doi.org/10.4211/hs.1c62269aa80
2467688d25540caf2467e

Hammond, J. C., Saavedra, F. A., & Kampf, S. K. (2018). How does snow
persistence relate to annual streamflow in mountain watersheds of
the western U.S. with wet maritime and dry continental climates?
Water Resources Research, 54(4), 2605–2623. https://doi.org/10.
1002/2017wr021899

Harpold, A., Brooks, P., Rajagopal, S., Heidbuchel,

I., Jardine, A., &
Stielstra, C. (2012). Changes in snowpack accumulation and ablation in
the intermountain west. Water Resources Research, 48(11), W11501.
https://doi.org/10.1029/2012wr011949

Harpold, A. A., & Molotch, N. P. (2015). Sensitivity of soil water availability
to changing snowmelt timing in the western U.S. Geophysical Research
Letters, 42(19), 8011–8020. https://doi.org/10.1002/2015gl065855
Hay, L. E. (2019). Application of the National Hydrologic Model infrastruc-
ture with the precipitation-runoff modeling system (NHM-PRMS), by

SEXSTONE ET AL.

2379

HRU calibrated version. U.S. Geological Survey data release. https://
doi.org/10.5066/P9NM8K8W

Hay, L. E., & Umemoto, M. (2007). Multiple-objective stepwise calibration
using Luca. U.S. Geological Survey Open-File Report 2006–1323.
https://pubs.usgs.gov/of/2006/1323/

He, S. W., Ohara, N., & Miller, S. N. (2019). Understanding subgrid variabil-
ity of snow depth at 1-km scale using Lidar measurements. Hydrologi-
cal Processes, 33(11), 1525–1537. https://doi.org/10.1002/hyp.13415
Hedrick, A. R., Marks, D., Havens, S., Robertson, M., Johnson, M.,
Sandusky, M., … Painter, T. H. (2018). Direct insertion of NASA air-
borne snow observatory-derived snow depth time series into the
isnobal energy balance snow model. Water Resources Research, 54(10),
8045–8063. https://doi.org/10.1029/2018wr023190

Hunsaker, C. T., Whitaker, T. W., & Bales, R. C. (2012). Snowmelt runoff
and water yield along elevation and temperature gradients in Cali-
fornia's southern sierra Nevada. JAWRA Journal of the American Water
Resources Association, 48(4), 667–678. https://doi.org/10.1111/j.
1752-1688.2012.00641.x

Jefferson, A. J. (2011). Seasonal versus transient snow and the elevation
dependence of climate sensitivity in maritime mountainous regions.
Geophysical Research Letters, 38(16), L16402. https://doi.org/10.
1029/2011gl048346

Kalnay, E., Kanamitsu, M., Kistler, R., Collins, W., Deaven, D., Gandin, L., …
Joseph, D. (1996). The NCEP/NCAR 40-year reanalysis project. Bulle-
tin of the American Meteorological Society, 77(3), 437–471. https://doi.
org/10.1175/1520-0477(1996)077<0437:Tnyrp>2.0.Co;2

Knowles, J. F., Harpold, A. A., Cowie, R., Zeliff, M., Barnard, H. R.,
Burns, S. P., … Williams, M. W. (2015). The relative contributions of
alpine and subalpine ecosystems to the water balance of a mountain-
ous, headwater catchment. Hydrological Processes, 29(22), 4794–4808.
https://doi.org/10.1002/hyp.10526

Li, D., Wrzesien, M. L., Durand, M., Adam, J., & Lettenmaier, D. P. (2017).
How much runoff originates as snow in the western United States,
and how will that change in the future? Geophysical Research Letters,
44(12), 6163–6172. https://doi.org/10.1002/2017gl073551

Liston, G. E. (1999). Interrelationships among snow distribution, snowmelt,
and snow cover depletion: Implications for atmospheric, hydrologic, and
ecologic modeling. Journal of Applied Meteorology, 38(10), 1474–1487.
Liston, G. E. (2004). Representing subgrid snow cover heterogeneities in
regional and global models. Journal of Climate, 17(6), 1381–1397.
Lopez-Moreno, J. I., Revuelto, J., Fassnacht, S. R., Azorin-Molina, C., Vicente-
Serrano, S. M., Moran-Tejeda, E., & Sexstone, G. A. (2015). Snowpack var-
iability across various spatio-temporal resolutions. Hydrological Processes,
29(6), 1213–1224. https://doi.org/10.1002/hyp.10245

Luce, C. H., & Tarboton, D. G. (2004). The application of depletion curves
for parameterization of subgrid variability of snow. Hydrological Pro-
cesses, 18(8), 1409–1422. https://doi.org/10.1002/hyp.1420

Luce, C. H., Tarboton, D. G., & Cooley, K. R. (1999). Sub-grid parameteriza-
tion of snow distribution for an energy and mass balance snow cover
model. Hydrological Processes, 13(12–13), 1921–1933.

Luce, C. H., Tarboton, D. G., & Cooley, R. R. (1998). The influence of the
spatial distribution of snow on basin-averaged snowmelt. Hydrological
Processes, 12(10-11), 1671–1683.

Lundquist, J. D., & Dettinger, M. D. (2005). How snowpack heterogeneity
affects diurnal streamflow timing. Water Resources Research, 41(5),
W05007. https://doi.org/10.1029/2004wr003649

Lundquist, J. D., Dettinger, M. D., & Cayan, D. R. (2005). Snow-fed
streamflow timing at different basin scales: Case study of the Tuol-
umne River above Hetch Hetchy, Yosemite, California. Water
Resources Research, 41(7), W07005. https://doi.org/10.1029/
2004wr003933

Magand, C., Ducharne, A., Le Moine, N., & Gascoin, S. (2014). Introducing
hysteresis in snow depletion curves to improve the water budget of a
land surface model in an alpine catchment. Journal of Hydrometeorol-
ogy, 15(2), 631–649. https://doi.org/10.1175/Jhm-D-13-091.1

Mankin, J. S., Viviroli, D., Singh, D., Hoekstra, A. Y., & Diffenbaugh, N. S.
(2015). The potential for snow to supply human water demand in the
present and future. Environmental Research Letters, 10(11), 114016.
https://doi.org/10.1088/1748-9326/10/11/114016

Markstrom, S. L., Hay, L. E., & Clark, M. P. (2016). Towards simplification
of hydrologic modeling: Identification of dominant processes. Hydrol-
ogy and Earth System Sciences, 20(11), 4655–4671. https://doi.org/10.
5194/hess-20-4655-2016

Markstrom, S. L., Regan, R. S., Hay, L. E., Viger, R. J., Webb, R. M.,
Payn, R. A., & LaFontaine, J. H. (2015). PRMS-IV, the precipitation-
runoff modeling system, version 4. U.S. Geological Survey Techniques
and Methods. Book 6. Chap. B7. p. 158. https://dx.doi.org/10.3133/
tm6B7

Marshall, A. M., Link, T. E., Abatzoglou, J. T., Flerchinger, G. N.,
Marks, D. G., & Tedrow, L. (2019). Warming alters hydrologic hetero-
geneity: Simulated climate sensitivity of hydrology-based Microrefugia
in the snow-to-rain transition zone. Water Resources Research, 55(3),
2122–2141. https://doi.org/10.1029/2018wr023063

Martinec, J., & Rango, A. (1981). Areal distribution of snow water equiva-
lent evaluated by snow cover monitoring. Water Resources Research,
17(5), 1480–1488. https://doi.org/10.1029/WR017i005p01480
McCabe, G. J., Wolock, D. M., Pederson, G. T., Woodhouse, C. A., &
McAfee, S. (2017). Evidence that recent warming is reducing upper
Colorado river flows. Earth Interactions, 21(10), 1–14. https://doi.org/
10.1175/Ei-D-17-0007.1

McCabe, G. J., Wolock, D. M., & Valentin, M. (2018). Warming is driving
decreases in snow fractions while runoff efficiency remains mostly
unchanged in snow-covered areas of the western United States. Jour-
nal of Hydrometeorology, 19(5), 803–814. https://doi.org/10.1175/
Jhm-D-17-0227.1

McDonald, J. H. (2014). Handbook of biological statistics (3rd ed.). Balti-

more, Maryland: Sparky House Publishing.

Molotch, N. P., Brooks, P. D., Burns, S. P., Litvak, M., Monson, R. K.,
McConnell, J. R., & Musselman, K. (2009). Ecohydrological controls on
snowmelt
forests.
sub-alpine
Ecohydrology, 2(2), 129–142. https://doi.org/10.1002/eco.48

in mixed-conifer

partitioning

Mote, P. W., Hamlet, A. F., Clark, M. P., & Lettenmaier, D. P. (2005).
Declining mountain snowpack in western North America. Bulletin of
the American Meteorological Society, 86(1), 39–50. https://doi.org/10.
1175/Bams-86-1-39

Mote, P. W., Li, S., Lettenmaier, D. P., Xiao, M., & Engel, R. (2018). Dramatic
declines in snowpack in the western US. npj Climate and Atmospheric Sci-
ence, 1, 2. https://doi.org/10.1038/s41612-018-0012-1

Musselman, K. N., Clark, M. P., Liu, C., Ikeda, K., & Rasmussen, R. (2017).
Slower snowmelt in a warmer world. Nature Climate Change, 7(3),
214–219. https://doi.org/10.1038/nclimate3225

Newman, A. J., Clark, M. P., Winstral, A., Marks, D., & Seyfried, M. (2014).
The use of similarity concepts to represent subgrid variability in land
surface models: Case study in a snowmelt-dominated watershed. Jour-
nal of Hydrometeorology, 15(5), 1717–1738. https://doi.org/10.1175/
Jhm-D-13-038.1

Pomeroy, J., Essery, R., & Toth, B. (2004). Implications of spatial distri-
butions of snow mass and melt rate for snow-cover depletion:
Observations in a subarctic mountain catchment. Annals of Glaciol-
ogy, 38, 195–201. https://doi.org/10.3189/172756404781814744
Pomeroy, J. W., Gray, D. M., Shook, K. R., Toth, B., Essery, R. L. H.,
Pietroniro, A., & Hedstrom, N. (1998). An evaluation of snow accumu-
lation and ablation processes for land surface modelling. Hydrological
Processes, 12(15), 2339–2367.

Regan, R. S., Juracek, K. E., Hay, L. E., Markstrom, S. L., Viger, R. J.,
Driscoll, J. M., … Norton, P. A. (2019). The U. S. Geological survey
National Hydrologic Model infrastructure: Rationale, description, and
application of a watershed-scale model for the conterminous United
States. Environmental Modelling & Software, 111, 192–203. https://doi.
org/10.1016/j.envsoft.2018.09.023

2380

SEXSTONE ET AL.

Regan, R. S., Markstrom, S. L., Hay, L. E., Viger, R. J., Norton, P. A.,
Driscoll, J. M., & LaFontaine, J. H. (2018). Description of the National
Hydrologic Model for use with the Precipitation-Runoff Modeling Sys-
tem (PRMS). U.S. Geological Survey Techniques and Methods. Book
6. Chap B9. p. 38. https://doi.org/10.3133/tm6B9

Regonda, S. K., Rajagopalan, B., Clark, M., & Pitlick, J. (2005). Seasonal
cycle shifts in hydroclimatology over the western United States. Jour-
nal of Climate, 18(2), 372–384. https://doi.org/10.1175/Jcli-3272.1
Reichle, R. H., Koster, R. D., De Lannoy, G. J. M., Forman, B. A., Liu, Q.,
Mahanama, S. P. P., & Toure, A. (2011). Assessment and enhancement
of MERRA land surface hydrology estimates. Journal of Climate, 24
(24), 6322–6338. https://doi.org/10.1175/Jcli-D-10-05033.1

Reitz, M., Sanford, W. E., Senay, G. B., & Cazenas, J. (2017). Annual esti-
mates of recharge, quick-flow runoff, and evapotranspiration for the
contiguous U.S. using empirical regression equations. JAWRA Journal
of the American Water Resources Association, 53(4), 961–983. https://
doi.org/10.1111/1752-1688.12546

Running, S., Mu, Q., & Zhao, M. (2017). MOD16A2 MODIS/Terra Net
Evapotranspiration 8-Day L4 Global 500m SIN Grid V006. NASA
EOSDIS Land Processes DAAC. https://doi.org/10.5067/MODIS/
MOD16A2.006

Saltelli, A., Ratto, M., Tarantola, S., Campolongo, F., Commission, E, &
Ispra, J. R. C.
(2006). Sensitivity analysis practices: Strategies for
model-based inference. Reliability Engineering & System Safety, 91
(10–11), 1109–1125. https://doi.org/10.1016/j.ress.2005.11.014
Schaibly, J. H., & Shuler, K. E. (1973). Study of the sensitivity of coupled
reaction systems to uncertainties in rate coefficients. II applications.
The Journal of Chemical Physics, 59(8), 3879–3888. https://doi.org/10.
1063/1.1680572

Senay, G. B., Bohms, S., Singh, R. K., Gowda, P. H., Velpuri, N. M.,
Alemu, H., & Verdin, J. P. (2013). Operational evapotranspiration map-
ping using remote sensing and weather datasets: A new parameteriza-
tion for the SSEB approach. Journal of the American Water Resources
Association, 49(3), 577–591. https://doi.org/10.1111/jawr.12057
Sexstone, G. A., Clow, D. W., Fassnacht, S. R., Liston, G. E., Hiemstra, C. A.,
Knowles, J. F., & Penn, C. A. (2018). Snow sublimation in mountain
environments and its sensitivity to forest disturbance and climate
warming. Water Resources Research, 54(2), 1191–1211. https://doi.
org/10.1002/2017wr021172

Sexstone, G.A. Driscoll, J.M. (2019), Data release in support of runoff sen-
sitivity to snow depletion curve representation within a continental
scale hydrologic model: U.S. Geological Survey data release. https://
doi.org/10.5066/P9OEIRJF

Seyfried, M. S., & Wilcox, B. P. (1995). Scale and the nature of spatial variabil-
ity - field examples having implications for hydrologic modeling. Water
Resources Research, 31(1), 173–184. https://doi.org/10.1029/94wr02025
Shamir, E., & Georgakakos, K. P. (2007). Estimating snow depletion curves for
American River basins using distributed snow modeling. Journal of Hydrol-
ogy, 334(1–2), 162–173. https://doi.org/10.1016/j.jhydrol.2006.10.007
Skaugen, T. (2007). Modelling the spatial variability of snow water equiva-
lent at the catchment scale. Hydrology and Earth System Sciences, 11
(5), 1543–1550. https://doi.org/10.5194/hess-11-1543-2007

Stephenson, G. R., & Freeze, R. A. (1974). Mathematical simulation of sub-
surface flow contributions to snowmelt runoff, Reynolds Creek Water-
shed, Idaho. Water Resources Research, 10(2), 284–294. https://doi.
org/10.1029/WR010i002p00284

Stewart, I. T., Cayan, D. R., & Dettinger, M. D. (2004). Changes in snow-
melt runoff timing in western North America under a ‘business as
usual’ climate change scenario. Climatic Change, 62(1-3), 217–232.
https://doi.org/10.1023/B:CLIM.0000013702.22656.e8

Stewart, I. T., Cayan, D. R., & Dettinger, M. D. (2005). Changes toward ear-
lier streamflow timing across western North America. Journal of Cli-
mate, 18(8), 1136–1155. https://doi.org/10.1175/Jcli3321.1

Thornton, P. E., Thornton, M. M., Mayer, B. W., Wei, Y., Devarakonda, R.,
Vose, R. S., & Cook, R. B. (2016). Daymet: Daily surface weather data on

a 1-km grid for North America, version 3. Oak Ridge, Tenn: Oak Ridge
National Laboratory, Distributed Active Archive Center dataset.
https://doi.org/10.3334/ORNLDAAC/1328

Trujillo, E., & Molotch, N. P. (2014). Snowpack regimes of the western
United States. Water Resources Research, 50(7), 5611–5623. https://
doi.org/10.1002/2013wr014753

Viger, R. J., & Bock, A. (2014). GIS features of the geospatial fabric for
National Hydrologic Modeling: U.S. Geological Survey data release.
https://doi.org/10.5066/F7542KMD

Viviroli, D., Durr, H. H., Messerli, B., Meybeck, M., & Weingartner, R.
(2007). Mountains of the world, water towers for humanity: Typology,
mapping, and global significance. Water Resources Research, 43(7),
W07447. https://doi.org/10.1029/2006wr005653

Voepel, H., Ruddell, B., Schumer, R., Troch, P. A., Brooks, P. D., Neal, A., …
Sivapalan, M. (2011). Quantifying the role of climate and landscape
characteristics on hydrologic partitioning and vegetation response.
Water Resources Research, 47(10), W00J09. https://doi.org/10.1029/
2010WR009944

Wang, D., & Tang, Y. (2014). A one-parameter Budyko model for water
balance captures emergent behavior in Darwinian hydrologic models.
Geophysical Research Letters, 41(13), 4569–4577.

Webb, R. W., Fassnacht, S. R., & Gooseff, M. N. (2018). Hydrologic flow
path development varies by aspect during spring snowmelt in complex
subalpine terrain. The Cryosphere, 12(1), 287–300. https://doi.org/10.
5194/tc-12-287-2018

Webb, R. W., Williams, M. W., & Erickson, T. A. (2018). The spatial and
temporal variability of Meltwater flow paths: Insights from a grid of
Over 100 snow Lysimeters. Water Resources Research, 54(2),
1146–1160. https://doi.org/10.1002/2017wr020866

Winstral, A., & Marks, D. (2014). Long-term snow distribution observations
in a mountain catchment: Assessing variability, time stability, and the
representativeness of an index site. Water Resources Research, 50(1),
293–305. https://doi.org/10.1002/2012wr013038

Woodhouse, C. A., Pederson, G. T., Morino, K., McAfee, S. A., &
McCabe, G. J. (2016). Increasing influence of air temperature on upper
Colorado River streamflow. Geophysical Research Letters, 43(5),
2174–2181. https://doi.org/10.1002/2015gl067613

Xia, Y. L., Mitchell, K., Ek, M., Sheffield, J., Cosgrove, B., Wood, E., …
Mocko, D. (2012). Continental-scale water and energy flux analysis
and validation for the North American land data assimilation system
Intercomparison and application of
project phase 2 (NLDAS-2): 1.
model products. Journal of Geophysical Research-Atmospheres, 117(D3),
D03109. https://doi.org/10.1029/2011jd016048

Xiao, M., Udall, B., & Lettenmaier, D. P. (2018). On the causes of declining
streamflows. Water Resources Research, 54(9),

Colorado River
6739–6756. https://doi.org/10.1029/2018wr023153

Yang, Z.-L., Dickinson, R. E., Robock, A., & Vinnikov, K. Y. (1997). Valida-
tion of the snow submodel of the biosphere–atmosphere transfer
scheme with Russian snow cover and meteorological observational
data. Journal of Climate, 10(2), 353–373.

SUPPORTING INF ORMATION

Additional supporting information may be found online in the

Supporting Information section at the end of this article.

How to cite this article: Sexstone GA, Driscoll JM, Hay LE,

Hammond JC, Barnhart TB. Runoff sensitivity to snow

depletion curve representation within a continental scale

hydrologic model. Hydrological Processes. 2020;34:
2365–2380. https://doi.org/10.1002/hyp.13735

