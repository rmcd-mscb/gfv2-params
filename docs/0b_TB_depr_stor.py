#-------------------------------------------------------------------------------
# Name: 0b_TB_depr_stor.py
# Purpose: calcualtes parameters related to depression storage
#
# Author:      abock@usgs.gov, mewieczo@usgs.gov
#
# Latest Update:  1/25/2021
#
# Data Source: Shuttle Radar Topography Mission DEMs, NHDPlus data
#
# Run Environments:
#   ArcPro 2.52 with Python/IDLE 3.6.9
#   ArcGis 10.7.1 with Python 2.7.16 in PyScripter 3.6.1.0 x64
#
# Inputs:
#   Topo, data Layers from:
#        https://www.sciencebase.gov/catalog/item/5ebb17d082ce25b5136181cb
#        https://www.sciencebase.gov/catalog/item/5ebb182b82ce25b5136181cf
#
# Spatial params calculated:
#   hru_imperv, hruSro_to_dprst, dprst_frac, sro_to_dprst_imperv,
#   sro_to_dprst_perv, smidx_coef, carea_max, hru_percent_imperv
#
# Default params calculated:
#   dprst_depth_avg, dprst_et_coef, dprst_frac_init, dprst_frac_open,
#   dprst_seep_rate_close, imperv_stor_max, op_flow_thres, va_clos_exp,
#   va_open_exp
#-------------------------------------------------------------------------------

import arcpy
import pandas as pd
import numpy, os
import tbFunc
arcpy.CheckOutExtension("spatial")

def getSro_to_dprst_perv(hru, datadir, gdb):
    """
    Surface runoff to depression storage for perviouse surfaces. Returns proportion of perviouse area draining to depression storage.

    Parameters
    ----------
    hru : str
        Path to HRU ID grid
    datadir : directory
        where suite.gdb is written
    gdb : geodatabase
        geodatabase containing features being parameterized

    Returns
    -------
    sro_to_dprst_perv : dataframe
        Dataframe with fractional values of pervious draining to depressions.
    """

    hruSro_to_dprst = getHruSro_to_dprst(hru, datadir, gdb) # returns grid
    pervAreaTotal = getPervAreaTotal(hru, datadir,gdb)

    hruSro_to_dprst_perv = "hruSro_to_dprst_perv"
    if not arcpy.Exists(hruSro_to_dprst_perv):
        r = arcpy.sa.Con(hruSro_to_dprst, pervAreaTotal)
        r.save(hruSro_to_dprst_perv)

    # probably not necessary, since there will (hopefully) always be at
    # least some "pervious" surface. Really parallels the _imperv structure.
    try:
        hruSro_to_dprst_pervCount = getZoneCount("hruSro_to_dprst_pervCount",
                                                  hruSro_to_dprst_perv)
    except:
        hruSro_to_dprst_pervCount = getZoneCount(
                                                 "hruSro_to_dprst_pervCount",
                                                  hru)
        hruSro_to_dprst_pervCount["hruSro_to_dprst_pervCount"] = 0

    # denominator
    try:
        pervAreaTotalCount = getZoneCount("pervAreaTotalCount", pervAreaTotal)
    except:
        pervAreaTotalCount = getZoneCount("pervAreaTotalCount", hru)
        pervAreaTotalCount["pervAreaTotalCount"] = 0

    # sro_to_dprst_perv = ( dprstPervArea ) / ( pervAreaTotal )
    hruCount = getZoneCount("hruCount", hru) # returns table
    sro_to_dprst_perv = hruCount.merge(pervAreaTotalCount,
                                       how="left",
                                       on="VALUE")
    sro_to_dprst_perv = sro_to_dprst_perv.merge(hruSro_to_dprst_pervCount,
                                                    how="left", on="VALUE")

    sro_to_dprst_perv["sro_to_dprst_perv"] = \
            sro_to_dprst_perv["hruSro_to_dprst_pervCount"] / \
            sro_to_dprst_perv["pervAreaTotalCount"]

    for c in sro_to_dprst_perv.columns[1:-1]:
        del sro_to_dprst_perv[c]

    sro_to_dprst_perv.ix[sro_to_dprst_perv["sro_to_dprst_perv"].isnull(),
                                               "sro_to_dprst_perv"] = 0

    return sro_to_dprst_perv

def getSro_to_dprst_imperv(hru,datadir,gdb):
    """
    Surface runoff to depression storage for imperviouse surfaces. Returns proportion of imperviouse area draining to depression storage.

    Parameters
    ----------
    hru : str
        Path to HRU ID grid
    datadir : directory
        where suite.gdb is written
    gdb : geodatabase
        geodatabase containing features being parameterized

    Returns
    -------
    sro_to_dprst_imperv : dataframe
        Dataframe with fractional values of impervious draining to depressions.
    """
    # sro_to_dprst_imperv = dprstImpervArea / impervAreaTotal
    # ( impervious area w/in HRU that is within the dprst contributing areas ) /
    # ( hru_percent_imperv * hru_area )

    hruImperv = getHruImperv(hru, datadir) # returns grid
    hruSro_to_dprst = getHruSro_to_dprst(hru, datadir, gdb) # returns grid

    hruSro_to_dprst_imperv = "hruSro_to_dprst_imperv"
    if not arcpy.Exists(hruSro_to_dprst_imperv):
        # r = arcpy.sa.Con(hruSro_to_dprst, arcpy.sa.Con(hruImperv, hru))
        r = arcpy.sa.Con(hruSro_to_dprst, hruImperv, hru)
        r.save(hruSro_to_dprst_imperv)

    try:
        hruSro_to_dprst_impervCount = getZoneCount("hruSro_to_dprst_impervCount",hruSro_to_dprst_imperv)
    except:
        hruSro_to_dprst_impervCount = getZoneCount("hruSro_to_dprst_impervCount", hru)
        hruSro_to_dprst_impervCount["hruSro_to_dprst_impervCount"] = 0

    # denominator
    hru_percent_imperv = getHru_percent_imperv(hru, datadir) # returns table
    hruCount = getZoneCount("hruCount", hru) # returns table

    sro_to_dprst_imperv = hruCount.merge(hru_percent_imperv, how="left", on="VALUE")
    sro_to_dprst_imperv = sro_to_dprst_imperv.merge(hruSro_to_dprst_impervCount,
                                                    how="left", on="VALUE")

    sro_to_dprst_imperv["sro_to_dprst_imperv"] = \
            sro_to_dprst_imperv["hruSro_to_dprst_impervCount"] / \
            (sro_to_dprst_imperv["hru_percent_imperv"] *
            sro_to_dprst_imperv["hruCount"])

    for c in sro_to_dprst_imperv.columns[1:-1]: # clean up columns
        del sro_to_dprst_imperv[c]

    sro_to_dprst_imperv.ix[sro_to_dprst_imperv["sro_to_dprst_imperv"].isnull(),"sro_to_dprst_imperv"] = 0 # handle no data values

    return sro_to_dprst_imperv


def getHruSro_to_dprst(hru, datadir, gdb):
    """
    Get surface runoff to depressions

    Generates a raster from the areas upstream of surface depressions (water bodies).

    Parameters
    ----------
    hru : str
        Path to HRU ID raster
    datadir : directory
        where suite.gdb is written
    gdb : geodatabase
        geodatabase containing features being parameterized

    Returns
    -------
    r : Raster
        Raster object with Depression storage values in the areas contributing to each depression.
    """

    hruSro_to_dprst = "hruSro_to_dprst"
    if arcpy.Exists(hruSro_to_dprst):
        return arcpy.sa.Raster(hruSro_to_dprst)


    # implement this function
    dprst = getDprst(hru, datadir, gdb)

    sro_to_dprst ="sro_to_dprst"
    base = hru.split("_")[-1]
    fdr = os.path.join(datadir, "fdr.tif")
    if not arcpy.Exists(sro_to_dprst):
        # compute the area upstream of all the depressions and use it to fill in areas not labeled as water bodies.
        if not arcpy.Exists("res1"):
            res1 = arcpy.sa.Watershed(fdr, dprst, "VALUE")
            res1.save("res1")
        else:
            res1 = arcpy.sa.Raster("res1")

        # compute the area upstream of all the depressions and use it to fill in areas not labeled as water bodies.
        if not arcpy.Exists("res2"):
            res2 = arcpy.sa.IsNull(dprst)
            res2.save("res2")
        else:
            res2 = arcpy.sa.Raster("res2")

        #rSro_to_dprst = arcpy.sa.Con(arcpy.sa.IsNull(dprst),
        #                             arcpy.sa.Watershed(fdr, dprst, "VALUE"))

        rSro_to_dprst = arcpy.sa.Con(res2, res1)

        rSro_to_dprst.save(sro_to_dprst)

        print("getHruSro_to_dprst: Limiting results to source HRUs. ")
        r = arcpy.sa.Con(rSro_to_dprst == arcpy.sa.Raster(hru), rSro_to_dprst)
        r.save(hruSro_to_dprst)
        return r
    else:
        return arcpy.sa.Raster(hruSro_to_dprst)

def getSmidx_coef(hru, datadir, gdb):
    """
    Computes Soil Moisture Index Coefficient with a TWI threshold of 15.6.

    Parameters
    ----------
    hru : str
        Path to HRU ID raster.
    datadir : directory
        where suite.gdb is written
    gdb : geodatabase
        geodatabase containing features being parameterized

    Returns
    -------
    smidx_coef : raster
        smidx_coef Raster object.
    """

    return getCarea(hru, "smidx_coef", 15.6, datadir, gdb)


def getCarea_max(hru, datadir, gdb):
    """
    Computes contributing area with a threshold of 8.0.

    Parameters
    ----------
    hru : str
        Path to HRU ID raster.
    datadir : directory
        where suite.gdb is written
    gdb : geodatabase
        geodatabase containing features being parameterized

    Returns
    -------
    carea_max : raster
        Maximum contributing area raster object.
    """

    return getCarea(hru, "carea_max", 8.0, datadir, gdb)


def getCarea(hru, name, threshold, datadir, gdb):
    """
    Computes proportion of pervious area that contributes to the stream.

    Parameters
    ----------
    hru : str
        Path to HRU ID raster
    name : str
        Output/input raster name, used to check if raster exists and to compute it if it doesnt.
    threshold : float
        TWI threshold value passed to getCareaMap
    datadir : directory
        where suite.gdb is written
    gdb : geodatabase
        geodatabase containing features being parameterized

    Returns
    -------
    carea : dataframe
        Pervious contributing area dataframe.
    """

    gridName = name
    if not arcpy.Exists(gridName):
        careaMap = getCareaMap(hru, gridName, threshold, datadir, gdb)
    else:
        careaMap = arcpy.sa.Raster(gridName)

    #hruPervious = getHruPervious(hru)
    hruPervious = getPervAreaTotal(hru, datadir, gdb)

    careaCount = getZoneCount(name + "Count", gridName)
    perviousCount = getZoneCount("perviousCount",
                                  arcpy.Describe(hruPervious).path + "/" +
                                  hruPervious.name)

    carea = careaCount.merge(
                          perviousCount, how="left",
                          left_on="VALUE", right_on="VALUE")

    carea[name] = carea[name + "Count"] / \
                             (carea["perviousCount"] * 1.0)
    carea.ix[carea[name] > 1.0, name] = 1.0

    for c in carea.columns[1:-1]:
        del carea[c]

    return carea


def getCareaMap(hru, gridName, threshold, datadir, gdb):
    """
    Returns a raster of HRU IDs where there is pervAreaTotal and TWI is above a certain value, otherwise it returns null. For areas less than the TWI threshold that are also classified as onstream storage the HRU ID is also returned.

    Parameters
    ----------
    hru : str
        Path to hru raster
    gridName : str
        Output grid name
    threshold : float
        TWI threshold
    datadir : directory
        where suite.gdb is written
    gdb : geodatabase
        geodatabase containing features being parameterized

    Returns
    -------
    grid : Raster
        Raster object of contributing area with HRU IDs.
    """

    if arcpy.Exists(gridName):
        return arcpy.sa.Raster(gridName)

    pervAreaTotal = getPervAreaTotal(hru, datadir,gdb)
    twi = getTwi(hru, datadir)
    onStreamStor = getOnStreamStor(hru, datadir,gdb)

    grid = arcpy.sa.Con(pervAreaTotal,
                        arcpy.sa.Con(twi > threshold, hru,
                        arcpy.sa.Con(onStreamStor, hru)))

    grid.save(gridName)
    return grid

def getOnStreamStor(hru, datadir, gdb):
    """
     Parameters
    ----------
    hru : str
        Path to hru raster
    datadir : directory
        where suite.gdb is written
    gdb : geodatabase
        geodatabase containing features being parameterized

    Returns
    -------
    gridName : Raster
    """

    gridName = "onStreamStor"

    if not arcpy.Exists(gridName):
        waterbodiesInHRUsGrid = getwaterbodiesInHRUsGrid(hru, datadir)
        dprst = getDprst(hru,datadir, gdb)
        imperv = getImpervBin(hru, datadir)
        # below execution is funcitonally identical to
        #   arcpy.sa.Con(waterbodiesInHRUsGrid,
        #   arcpy.sa.Con(arcpy.sa.IsNull(dprst),
        #   arcpy.sa.Con(arcpy.sa.IsNull(imperv),
        #   waterbodiesInHRUsGrid)))
        nondprstWbodies = arcpy.sa.Con(waterbodiesInHRUsGrid &
                                       arcpy.sa.IsNull(dprst) &
                                       arcpy.sa.IsNull(imperv),
                                       waterbodiesInHRUsGrid)
        nondprstWbodies.save(gridName)
        return nondprstWbodies
    else:
        return arcpy.sa.Raster(gridName)


def getNondprstWbodies(hru, datadir):
    """
    Identify non-depression storage water bodies.

    Parameters
    ----------
    hru : str
        Path to HRU ID raster.
    datadir : directory
        where suite.gdb is written


    Returns
    -------
    nondprstWbodies : Raster
        Non-depression stroage water bodies raster object.
    """

    hruBase = os.path.basename(hru)
    punit = hruBase.split("_")[1]
    gridName = "nondprstWbd_"

    if not arcpy.Exists(gridName):
        waterbodiesInHRUsGrid = getwaterbodiesInHRUsGrid(hru, datadir)
        dprst = getDprst(hru, datadir, gdb)
        imperv = getImpervBin(hru, datadir)
        # below execution is funcitonally identical to
        #   arcpy.sa.Con(waterbodiesInHRUsGrid,
        #   arcpy.sa.Con(arcpy.sa.IsNull(dprst),
        #   arcpy.sa.Con(arcpy.sa.IsNull(imperv),
        #   waterbodiesInHRUsGrid)))
        nondprstWbodies = arcpy.sa.Con(waterbodiesInHRUsGrid &
                                       arcpy.sa.IsNull(dprst) &
                                       arcpy.sa.IsNull(imperv),
                                       waterbodiesInHRUsGrid)
        nondprstWbodies.save(gridName)
        return nondprstWbodies
    else:
        return arcpy.sa.Raster(gridName)


def getTwi(hru, datadir):
    """
    Load the topographic wetness index raster

    Parameters
    ----------
    hru : str
        Path to the hru raster
    datadir : directory
        where suite.gdb is written

    Returns
    -------
    twi : Raster
        Topographic Wetness index raster object.
    """
    setRasterEnv(hru)
    arcpy.env.mask = hru
    intwi = arcpy.sa.Raster(os.path.join(datadir, "twiX100.tif"))
    twi = intwi * .01

    return twi

def getDprst_frac(hru, datadir,gdb):
    """
    Compute the proportion of the HRU is depression storage. Accounts Does not acount for different cell sizes.

    Parameters
    ----------
    hru : str
        Path to the hru raster
    datadir : directory
        where suite.gdb is written
    gdb : geodatabase
        geodatabase containing features being parameterized

    Returns
    -------
    dprst_frac : Raster
        depression storage fraction raster object.
    """

    print(hru)
    dprst = getDprst(hru, datadir, gdb)

    hruCount = getZoneCount("hruCount", hru)
    dprstName = os.path.join(dprst.path, dprst.name)
    dprstCount = getZoneCount("dprstCount", dprstName)

    dprst_frac = hruCount.merge(dprstCount, how = "left", on = "VALUE")
    dprst_frac["dprst_frac"] = dprst_frac["dprstCount"] / dprst_frac["hruCount"]

    for c in dprst_frac.columns[1:-1]:
        del dprst_frac[c]

    dprst_frac.ix[dprst_frac["dprst_frac"].isnull(), "dprst_frac"] = 0

    return dprst_frac

def getOnStreamStor(hru, datadir,gdb):
    """
    Parameters
    ----------
    hru : str
        Path to the hru raster
    datadir : directory
        where suite.gdb is written
    gdb : geodatabase
        geodatabase containing features being parameterized

    Returns
    -------
    dprst_frac : Raster
        depression storage fraction raster object.

    Returns
    -------
    onStreamStor : Raster
        On Stream Depression storage raster(?) labeled with HRU ID.
    """

    hruBase = os.path.basename(hru)
##    punit = hruBase.split("_")[1]
    gridName = "onStreamStor"
    try:
        return arcpy.sa.Raster(gridName)
    except:
        pass

    hruSegBuf = getSegsBuf(hru,datadir,gdb)
    waterbodiesInHRUsGrid = getwaterbodiesInHRUsGrid(hru, datadir)
    dprst = getDprst(hru, datadir, gdb)

    onStreamStor = arcpy.sa.Con(arcpy.sa.IsNull(hruSegBuf), arcpy.sa.Con((waterbodiesInHRUsGrid & arcpy.sa.IsNull(dprst)), hru), hru)

    onStreamStor.save(gridName)
    return onStreamStor

def getDprst(hru, datadir,gdb):
    """
    Gets depression storage raster with depressions labeled with HRU IDs and other areas as null. Depressions are only areas outside of imperviouse zones and outside the stream buffer.

    Parameters
    ----------
    hru : str
        Path to the HRU ID raster.
    datadir : directory
        where suite.gdb is written
    gdb : geodatabase
        geodatabase containing features being parameterized


    Returns
    -------
    dprst : Raster
        Depression storage raster object where depression storage is given the same ID as the HRU that it is in.
    """
    setRasterEnv(hru)
    dprstName = "dprst"

    if arcpy.Exists(dprstName):
       return arcpy.sa.Raster(dprstName)

    hruImperv = getHruImperv(hru, datadir)
    hruSegBuf = getSegsBuf(hru, datadir, gdb)

    #junkList = ["junk1", "junk2"]
    #cleanUp(junkList)

    waterbodiesInHRUsGrid = getwaterbodiesInHRUsGrid(hru, datadir) # loads and reclassifies water body raster
    print (hruImperv)
    print (waterbodiesInHRUsGrid)
    print (hruSegBuf)

    if not arcpy.Exists(dprstName):
        if not arcpy.Exists("junk1"):
            r1 = arcpy.sa.Con(arcpy.sa.IsNull(hruImperv), waterbodiesInHRUsGrid) # only include non-imperviouse areas
            r1.save("junk1")
        else:
            r1 = rcpy.sa.Raster("junk1")

        if not arcpy.Exists("junk2"):
            r2 = arcpy.sa.Con(hruSegBuf, r1) # exclude the stream buffer
            r2.save("junk2")
        else:
            r2 = rcpy.sa.Raster("junk2")

        #print ("the moment of truth")
        ids = arcpy.da.TableToNumPyArray("junk2", ["VALUE"]) # AN changed to LINK
        print(ids[0])
        ids = str(list(set([i[0] for i in ids.tolist()]))).replace("[", "(").replace("]", ")") # get a list of water body IDs?

        r = arcpy.sa.Con(r1, hru, "", "VALUE not in " + ids) # label the areas outside of r2 with HRU IDs
        r.save(dprstName)
        return r
    else:
        return arcpy.sa.Raster(dprstName)

def getwaterbodiesInHRUsGrid(hru, datadir):
    """
    Generates a HRU water body grid labeled with HRU IDs(?) that is then re-labeled using the RegionGroup function.

    Parameters
    ----------
    hru : str
        Path to the HRU ID raster.
    datadir : directory
        where suite.gdb is written


    Returns
    -------
    wbodsHruGrid : Raster
        Gridded waterbodies with HRUs
    """

    arcpy.CheckOutExtension("Spatial")
    wbodsHruGrid = "wbodsHruGrid"
    setRasterEnv(hru)
    wbg = "wbg_nhru"

    if not arcpy.Exists(wbodsHruGrid):
        w = arcpy.sa.RegionGroup(wbg, "EIGHT")
        w.save("preReg")
        w2 = arcpy.sa.Lookup(w, "LINK")
        w2.save(wbodsHruGrid)
        return w2
    else:
        return arcpy.sa.Raster(wbodsHruGrid)

def getHru_percent_imperv(hru, datadir):
    """
    Compute the imperviouse fracton of the HRUs.

     Parameters
    ----------
    hru : str
        Path to the HRU ID raster.
    datadir : directory
        where suite.gdb is written


    Returns
    -------
    hru_percent_imperv : Raster
        Percent impervious per HRU
    """

    hruImperv = getHruImperv(hru, datadir)
    hiDx = arcpy.Describe(hruImperv).children[0].meanCellHeight

    arcpy.MakeTableView_management(hruImperv, "hruImpervTv")
    impervCount = pd.DataFrame(arcpy.da.TableToNumPyArray("hruImpervTv",
                               ["VALUE", "COUNT"]))

    arcpy.Delete_management("hruImpervTv")

    hruCount = getZoneCount("hruCount", hru)
    hruDx = arcpy.Describe(hru).children[0].meanCellHeight

    hru_percent_imperv = impervCount.merge(hruCount, how = "left", on = "VALUE")
    # compute proportion of HRU that is impervious and account for differences in cell sizes
    hru_percent_imperv["hru_percent_imperv"] = \
        (hiDx * hru_percent_imperv["COUNT"]) / (hruDx * hru_percent_imperv["hruCount"])

    del hru_percent_imperv["COUNT"]
    del hru_percent_imperv["hruCount"]
    del hruImperv

    return hru_percent_imperv


def getHruImperv(hru, datadir):
    """
    Get impervious raster labeled with HRU IDs.

    Parameters
    ----------
    hru : str
        Path to the HRU ID raster.
    datadir : directory
        where suite.gdb is written


    Returns
    -------
    hruImperv : Raster
        Binary impervious surface raster imprinted with HRU IDs
    """

    import arcpy
    import pandas as pd
    import numpy
    import pickle
    import os

    gridName =  "hruImperv"

    if arcpy.Exists(gridName):
        return arcpy.sa.Raster(gridName)

    impervBin = getImpervBin(hru, datadir)

    hruImperv = arcpy.sa.Con(impervBin, hru)
    hruImperv.save(gridName)

    return hruImperv

def getPervAreaTotal(hru, datadir,gdb):
    """
    Get raster with HRU IDs in areas that are not impervious or depressions.

    Parameters
    ----------
    hru : str
        Path to HRU ID raster.
    datadir : directory
        where suite.gdb is written
    gdb : geodatabase
        geodatabase containing features being parameterized


    Returns
    -------
    pervAreaTotal : Raster
        Perviouse raster with HRU IDs for perviouse areas.
    """

    gridName = "pervAreaTotal"

    if arcpy.Exists(gridName):
        return arcpy.sa.Raster(gridName)

    imperv = getHruImperv(hru, datadir)
    dprst = getDprst(hru, datadir, gdb)

    pervAreaTotal = arcpy.sa.Con(arcpy.sa.IsNull(imperv) &
                                 arcpy.sa.IsNull(dprst),
                                 hru)
    pervAreaTotal.save(gridName)

    return pervAreaTotal


def getImpervBin(hru,datadir):
    """
    Generate a binary impervious raster.

    Parameters
    ----------
    hru : str
        Path to the HRU ID raster.
    datadir : directory
        where suite.gdb is written
    gdb : geodatabase
        geodatabase containing features being parameterized


    Returns
    -------
    impervBin : Raster
        Binary impervious surface raster
    """

    setRasterEnv(hru)
    arcpy.env.mask = hru
    imperv = os.path.join(datadir, "imperv.tif")
    impervBinName = "impervBin"

    # this is done dataset wide
    if not arcpy.Exists(impervBinName):
        print("Creating binary imperv surface (> 50%).")
        setRasterEnv(imperv)
        impervBin = arcpy.sa.Con(arcpy.sa.Raster(imperv), 1, "", "VALUE > 50")
        impervBin.save(impervBinName)
    else:
        impervBin = arcpy.sa.Raster(impervBinName)

    return impervBin

def setRasterEnv(hru):
    """
    Generate raster HRU

    Parameters
    ----------
    hru : str
        Name of vector HRU feature layer
    """
    arcpy.env.snapRaster = hru
    arcpy.env.cellsize = hru
    print (hru)
    arcpy.env.extent = hru
    arcpy.env.mask = hru

def getZoneCount(varName, grid):
    """
    Get Zone Counts from a grid

    Parameters
    ----------
    varName : str
        The variable name used in the output dataframe.
    grid : str
        Path to the raster grid.

    Returns
    -------
    zoneCounts : dataframe
        Pandas dataframe of the values and the counts.
    """
    print(grid)
    arcpy.MakeTableView_management(grid, "zoneTv")
    zoneCounts = pd.DataFrame(arcpy.da.TableToNumPyArray("zoneTv",
                            ["VALUE", "COUNT"]))
    arcpy.Delete_management("zoneTv")

    zoneCounts.columns = ["VALUE", varName]

    return zoneCounts


def getSegsBuf(hru, datadir, gdb):
    """
    Buffer the stream segments, save the buffered grid, return the stream segments buffered raster.

    Parameters
    ----------
    hru : str
        Name of vector HRU feature layer
    datadir : directory
        where suite.gdb is written
    gdb : geodatabase
        geodatabase containing features being parameterized

    Returns
    -------
    outEucDistance : Raster
        Raster with HRU values within the buffer and nulls outside.
    """

    gridSegBufName = "segsBufs"
    try:
        return arcpy.sa.Raster(gridSegBufName)
    except:
        pass

    # eucdistance < 60 m around raster segments
    if not arcpy.Exists(gridSegBufName):
        print("Buffering segments by 60 meters on a side.")
        outBuff = arcpy.sa.EucDistance(os.path.join(gdb, "nsegment"), 60, 30)

        outEucDistance = arcpy.sa.Con(outBuff, hru, "", "VALUE GE 0")
        outEucDistance.save(gridSegBufName)

    return outEucDistance

def cleanUp(inFls):
    """
    Delete files in inFls if they exist.

    Parameters
    ----------
    inFls : list
        List of paths to delete.
    """

    for junk in inFls:
        if arcpy.Exists(junk):
            arcpy.Delete_management(junk)

def main(datadir, paramDB, gdb, hru_layer, defaults, idfield, rastdir):
    """
    Runs the parameter function

    Parameters
    ----------
    datadir : directory
        where suite.gdb is written
    paramDB : folder location
        location that holds the parameter csv files
    gdb : geodatabase
        geodatabase containing features being parameterized
    hru_layer : ESRI feature layer
        Name of feature layer within ESRI GDB that holds HRUs
    defaults : binary
        Flag for whether to calculate default parameters or not
    rastdir : folder location
        Location of raster dirs

    Returns
    -------
    Calculated depression storage paramters
    """
    # Set environments
    os.chdir(datadir)
    arcpy.env.overwriteOutput = True
    arcpy.CheckOutExtension("Spatial")

    arcpy.env.cellSize = os.path.join(rastdir, "twiX100.tif")
    arcpy.env.snapRaster =  os.path.join(rastdir, "twiX100.tif")

    if not os.path.exists(paramDB):
        os.makedirs(paramDB)

    gdbOut = os.path.join(rastdir, "suite.gdb")
    if os.path.exists(gdbOut) == False:
        arcpy.CreateFileGDB_management(rastdir, "suite.gdb")

    arcpy.env.workspace = gdbOut
    arcpy.env.scratchWorkspace = arcpy.env.workspace

    # Features to be used for param calculations
    # Ensure features are sorted based on key field

    nhru = os.path.join(gdb, hru_layer)
    nhruSort = os.path.join(gdb, "nhru_sort")

    if not arcpy.Exists(nhruSort):
        arcpy.MakeFeatureLayer_management(nhru, "nhru")
        arcpy.Sort_management("nhru", nhruSort, "hru_id")
    arcpy.MakeFeatureLayer_management(nhruSort, "featuresFL")

    nhrug = "nhrug"
    if not arcpy.Exists(nhrug):
        arcpy.FeatureToRaster_conversion(nhruSort, "hru_id", nhrug, arcpy.env.cellSize)

    wbg_nhru = "wbg_nhru"
    if not arcpy.Exists(wbg_nhru):
        wbd_hru = arcpy.sa.Con(os.path.join(rastdir, "wbg.tif"), nhrug) # only include non-imperviouse areas
        wbd_hru.save(wbg_nhru)

    setRasterEnv(nhrug)

    hruSro_to_dprst = getHruSro_to_dprst(nhrug, rastdir, gdb)
    dprst_frac = getDprst_frac(nhrug, rastdir, gdb)
    sro_to_dprst_imperv = getSro_to_dprst_imperv(nhrug, rastdir, gdb)
    sro_to_dprst_perv = getSro_to_dprst_perv(nhrug, rastdir, gdb)
    smidx_coef = getSmidx_coef(nhrug, rastdir, gdb)
    carea_max = getCarea_max(nhrug, rastdir,gdb)
    hru_percent_imperv = getHru_percent_imperv(nhrug, rastdir)

    hruParams = getZoneCount("Count", "nhrug")
    hruParams = hruParams.merge(smidx_coef, how = "left", on = "VALUE")
    hruParams = hruParams.merge(carea_max, how = "left", on = "VALUE")
    hruParams = hruParams.merge(hru_percent_imperv, how = "left", on = "VALUE")
    hruParams = hruParams.merge(dprst_frac, how = "left", on = "VALUE")
    hruParams = hruParams.merge(sro_to_dprst_imperv, how = "left", on = "VALUE")
    hruParams = hruParams.merge(sro_to_dprst_perv, how = "left", on = "VALUE")
    hruParams = hruParams.fillna(0)

    # sequetially write out parameters to a text file
    outfile = os.path.join(datadir, "suite.params.txt")

    if not os.path.isfile(outfile): # first iteration
        with open(outfile, "w") as f:
            hruParams.to_csv(f, sep = "\t", header = True, index = False,
                             float_format = "%2.12f")
    else: # subsequent iterations.
        with open(outfile, "a") as f:
            hruParams.to_csv(f, sep = "\t", header = False, index = False,
                             float_format = "%2.12f")

    # try to make a master parameter data frame
    try: # subsequent iterations
        hruParamsAllUnits = hruParamsAllUnits.append(hruParams)
    except: # first iteration
        import copy
        hruParamsAllUnits = copy.deepcopy(hruParams)

    print("Pushing back into gdb.")
    if arcpy.Exists(os.path.join(gdbOut, "results")):
        print("Deleting pre-existing results table.")
        arcpy.Delete_management(os.path.join(gdbOut, "results"))
    else:
        print("No pre-existing results table.")

    # save the results to table in the output geodatabase
    arcpy.da.NumPyArrayToTable(hruParamsAllUnits.to_records(), os.path.join(gdbOut, "results"))

    if not arcpy.Exists("nhru"):
        arcpy.Copy_management(nhru, "/nhru")

    arcpy.MakeFeatureLayer_management("nhru", "nhruFL")

    flds = ["Field1", "VALUE", "hruCount",
            "hru_percent_imperv", "smidx_coef", "carea_max",
            "dprst_frac", "sro_to_dprst_imperv", "sro_to_dprst_perv",
            "index", ] # parameter field names
    for fld in flds: # clean up the output feature class
        if arcpy.ListFields("nhru", fld):
            arcpy.DeleteField_management("nhru", fld)

    print("Merging results table into nhru map.")
    arcpy.JoinField_management("nhru", "hru_id", os.path.join(gdbOut, "results"), "VALUE") # join the results table to the feature class.

    # Derive soil zone parameters per HRU with default values
    nhru = int(arcpy.GetCount_management("featuresFL")[0])
    dprst_depth_avg = [132] * nhru
    dprst_et_coef = [1] * nhru
    dprst_frac_init = [0.5] * nhru
    dprst_frac_open = [1] * nhru
    dprst_seep_rate_close = [0.2] * nhru
    imperv_stor_max = [0.05] * nhru
    op_flow_thres = [1] * nhru
    va_clos_exp = [.001] * nhru
    va_open_exp = [.001] * nhru

    # List of parameters and names for easy iteration
    # Precision list for params
    precList = [1, 1, 1, 1, 1, 2, 1, 3, 3]
    # Names of list vector objects
    paramList = [dprst_depth_avg, dprst_et_coef, dprst_frac_init, dprst_frac_open, dprst_seep_rate_close, imperv_stor_max, op_flow_thres, va_clos_exp, va_open_exp]
    # Strings for file names
    paramNames = ["dprst_depth_avg", "dprst_et_coef", "dprst_frac_init", "dprst_frac_open", "dprst_seep_rate_close",
    "imperv_stor_max", "op_flow_thres", "va_clos_exp", "va_open_exp"]
    # iterate over each parameter and write file
    [tbFunc.vec2csv(param, paramDB, name, prec) for param, name, prec in zip(paramList, paramNames, precList)]
    #paramWork.extend(paramNames)

    # Write out model inputs, run Date, and outputs
    paramList = [elem for elem in flds if elem not in ["Field1", "VALUE", "hruCount", "index"]]
    prec = 3
    features = "nhru"
    print(paramList)
    [tbFunc.att2csv(features, paramDB, name, prec) for name in paramList]
    inArgs['gbd'] = gdbOut
    tbFunc.breadCrumbs("depr_stor", inArgs, paramList, paramDB)

#******************************************************************************
#******************************************************************************
# User-defined arguments
thisdir = os.getcwd()

# Open Configuration file and read parameters as python dictionary
inArgs = pd.read_csv("config_depstor.txt", sep = " = ", index_col = 0, header = None, squeeze = True, engine = "python").to_dict()
# Go into main function
main(inArgs["datadir"], inArgs["paramDB"], inArgs["gdb"], inArgs["hru_layer"], inArgs["defaults"], inArgs["hru_id_key"], inArgs["DL_dir"])
