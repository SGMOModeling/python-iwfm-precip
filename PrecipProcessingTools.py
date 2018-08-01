#############################################################
# Import Modules
#############################################################
import arcpy, os, sys, re, datetime, glob, time
import numpy as np
import pandas as pd
import multiprocessing as mp

#############################################################
# Define Functions to use in the main program
#############################################################
def IsGeodatabase(inWorkspace):
    ''' checks if the workspace provided is a geodatabase '''
    workspaceDescription = arcpy.Describe(inWorkspace)
    if workspaceDescription.dataType == "Workspace":
        return True
    else:
        return False

def IsFolder(inWorkspace):
    ''' checks if the workspace provided is a Folder '''
    workspaceDescription = arcpy.Describe(inWorkspace)
    if workspaceDescription.dataType == "Folder":
        return True
    else:
        return False

def IsTextFile(inWorkspace):
    ''' checks if the workspace provided is a text file '''
    workspaceDescription = arcpy.Describe(inWorkspace)
    if workspaceDescription.dataType == "TextFile":
        return True
    else:
        return False

def GetAllRastersFromFolders(inWorkspace):
    ''' generates a list of all rasters in subfolders of the parent directory '''
    listDir = os.listdir(inWorkspace)
    listRasters = []
    
    # add rasters to list from parent directory
    rasterList = glob.glob(os.path.join(inWorkspace, '*.bil'))
    if len(rasterList) > 0:
        listRasters = [os.path.join(inWorkspace, raster) for raster in rasterList]
        
    # only do if the parent directory has subdirectories within it
    if len(listDir) > 0:
        dirPaths = [os.path.join(inWorkspace, dir) for dir in listDir]
        for dirPath in dirPaths:
            rasterList = glob.glob(os.path.join(dirPath, '*.bil'))
            if len(listRasters) > 0:
                listRasters.extend([os.path.join(dirPath, raster) for raster in rasterList])
            else:
                listRasters = [os.path.join(dirPath, raster) for raster in rasterList]
    
    return sorted(listRasters)

def GetAllRastersFromGeodatabase(inWorkspace):
    ''' generates a list of all rasters in geodatabase '''
    arcpy.env.workspace = inWorkspace
    listRasters = arcpy.ListRasters('*', 'All')

    return sorted(listRasters)

def GetAllRastersFromFile(inTextFile):
    ''' generates a list of rasters from a text file '''

    # open text file in read mode and read contents ignoring lines beginning with '#'
    with open(inTextFile, 'r') as textFile:
        listRasters = [str(line.strip()) for line in textFile if line[0] != "#"]

    return sorted(listRasters)

def GetListOfFeatureClasses(inWorkspace):
    ''' returns a list of feature classes in the provided location '''
    listFeatureClasses = glob.glob(os.path.join(inWorkspace, '*.shp'))
    return sorted(listFeatureClasses)

def LastDayOfMonth(inDate):
    ''' generates a date for the last day of a given month '''
    nextMonth = inDate.replace(day=28) + datetime.timedelta(days=4)
    return nextMonth - datetime.timedelta(days=nextMonth.day)

def LengthUnitConversionFactor(inUnits, outUnits):
    ''' conversion factor between two different length units '''
    if inUnits == 'feet' and outUnits == 'feet':
        return 1.0
    if inUnits == 'feet' and outUnits == 'inches':
        return 12.0
    if inUnits == 'feet' and outUnits == 'meters':
        return 12.0/39.37
    if inUnits == 'feet' and outUnits == 'millimeters':
        return 12.0/39.37 * 1000.0
    if inUnits == 'inches' and outUnits == 'feet':
        return 1.0/12.0
    if inUnits == 'inches' and outUnits == 'inches':
        return 1.0
    if inUnits == 'inches' and outUnits == 'meters':
        return 1.0/39.37
    if inUnits == 'inches' and outUnits == 'millimeters':
        return 1.0/39.37 * 1000.0
    if inUnits == 'meters' and outUnits == 'feet':
        return 39.37/12
    if inUnits == 'meters' and outUnits == 'inches':
        return 39.37
    if inUnits == 'meters' and outUnits == 'meters':
        return 1.0
    if inUnits == 'meters' and outUnits == 'millimeters':
        return 1000.0
    if inUnits == 'millimeters' and outUnits == 'feet':
        return 0.001 * 39.37/12.0
    if inUnits == 'millimeters' and outUnits == 'inches':
        return 0.001 * 39.37
    if inUnits == 'millimeters' and outUnits == 'meters':
        return 0.001
    if inUnits == 'millimeters' and outUnits == 'millimeters':
        return 1.0

def FACTRN(outUnits):
    if outUnits == 'feet':
        return 1.0
    if outUnits == 'inches':
        return 1.0/12.0
    if outUnits == 'meters':
        return 12.0/39.37
    if outUnits == 'millimeters':
        return 0.001 * 39.37/12.0

def WriteRastersToFile(inRastersList, outWorkspace, outFileName):
    ''' writes rasters from a list to a text file '''
    outFile = os.path.join(outWorkspace, outFileName)
    with open(outFile, 'w') as f:
        f.write("# Rasters To Process\n")
        for raster in inRastersList:
                    f.write("{}\n".format(raster))

def MakeDirectory(dirLocation, dirName):
    ''' creates a folder of the desired name if it does not exist '''
    dirPath = os.path.join(dirLocation, dirName)
    if not os.path.isdir(dirPath):
        os.mkdir(dirPath)
        print("Folder '{0}' Created: {1}.".format(dirName, dirPath))

    return dirPath

def GetPropertiesFromRaster(inRaster):
    ''' returns raster properties from raster object '''
    # Instantiate a raster object for the raster to access its properties
    rasterDataset = arcpy.sa.Raster(inRaster)
    
    # Get properties from raster object
    inRasterName = rasterDataset.name
    numRows = rasterDataset.height
    numCols = rasterDataset.width
    rastExtent = rasterDataset.extent
    xMin = rastExtent.XMin
    yMin = rastExtent.YMin
    xMax = rastExtent.XMax
    yMax = rastExtent.YMax

    del rasterDataset
    
    return inRasterName, numRows, numCols, xMin, yMin, xMax, yMax

def ClipRaster(inRaster, clipFeature, outWorkspace):
    ''' clips a raster to the geometry of the boundary of the feature class
        provided and returns the name of the clipped raster '''
    
    # Use the input raster name to generate a default output name
    inRasterName, inRasterExt = os.path.splitext(os.path.basename(inRaster))
    outRasterName = "{0}_clip{1}".format(inRasterName, inRasterExt)
    outClipRaster = os.path.join(outWorkspace, outRasterName)
    
    # Clip raster to geometry of the feature class specified by the user
    #arcpy.Clip_management(inRaster, "#", outClipRaster, clipFeature, "#", "ClippingGeometry")
    arcpy.Clip_management(inRaster, "#", outClipRaster, clipFeature, "#", "NONE")
    
    return outClipRaster

def ClipRasterMulti(inputList):
    ''' clips a raster to the geometry of the boundary of the feature class
        provided and returns the name of the clipped raster '''

    # unpack list from function input
    inRaster, clipFeature, outWorkspace = inputList
    
    # Use the input raster name to generate a default output name
    inRasterName, inRasterExt = os.path.splitext(os.path.basename(inRaster))
    outRasterName = "{0}_clip{1}".format(inRasterName, inRasterExt)
    outClipRaster = os.path.join(outWorkspace, outRasterName)
    
    # Clip raster to geometry of the feature class specified by the user
    #arcpy.Clip_management(inRaster, "#", outClipRaster, clipFeature, "#", "ClippingGeometry")
    arcpy.Clip_management(inRaster, "#", outClipRaster, clipFeature, "#", "NONE")
    
    return outClipRaster

def ConvertRasterToPoints(inRaster, outWorkspace):
    ''' converts a raster to points and returns the name of the point feature class '''
    
    # Use the input raster name to generate a default output name
    inRasterName = os.path.splitext(os.path.basename(inRaster))[0]
    outPointFeatureName = "{0}.shp".format(inRasterName)
    outPointFeature = os.path.join(outWorkspace, outPointFeatureName)
    
    # Convert raster to point feature class
    arcpy.RasterToPoint_conversion(inRaster, outPointFeature, "Value")
    
    return outPointFeature

def ConvertRasterToPointsMulti(inputDataList):
    ''' converts a raster to points and returns the name of the point feature class '''
    # Unpack list from function input
    inRaster, outWorkspace = inputDataList
    
    # Use the input raster name to generate a default output name
    inRasterName = os.path.splitext(os.path.basename(inRaster))[0]
    outPointFeatureName = "{0}.shp".format(inRasterName)
    outPointFeature = os.path.join(outWorkspace, outPointFeatureName)
    
    # Convert raster to point feature class
    arcpy.RasterToPoint_conversion(inRaster, outPointFeature, "Value")
    
    return outPointFeature

def CreateFishnetFeature(inRaster, outWorkspace):
    ''' creates a polyline fishnet feature class and returns it's name '''
    
    # Get properties from raster dataset for use in generating fishnet
    inRasterName, numRows, numCols, xMin, yMin, xMax, yMax = GetPropertiesFromRaster(inRaster)
    
    # Use the input raster name to generate a default output name for the fishnet feature class
    inRasterBaseName = os.path.splitext(inRasterName)[0]
    if "clip" in inRasterBaseName:
        outFishnetFeatureName = "{0}.shp".format(inRasterBaseName.replace("clip", "fishnet"))
    else:
        outFishnetFeatureName = "{0}_fishnet.shp".format(inRasterBaseName)
    
    outFishnetFeature = os.path.join(outWorkspace, outFishnetFeatureName)
    
    # Set origin and rotation of fishnet using extent properties
    # rotation is set by the difference in the x-values in the origin coordinate and the y-axis coordinate
    originCoordinate = "{0} {1}".format(xMin, yMin)
    yAxisCoordinate = "{0} {1}".format(xMin, yMax)
    
    # Create fishnet using clipped rasters
    arcpy.CreateFishnet_management(outFishnetFeature,
                                   originCoordinate,
                                   yAxisCoordinate,
                                   0,
                                   0,
                                   numRows,
                                   numCols,
                                   "#",
                                   "NO_LABELS",
                                   inRaster,
                                   "POLYLINE")
    
    return outFishnetFeature

def CreateFishnetFeatureMulti(inputDataList):
    ''' creates a polyline fishnet feature class and returns it's name '''
    
    # unpack list from function input
    inRaster, outWorkspace = inputDataList
    
    # Get properties from raster dataset for use in generating fishnet
    inRasterName, numRows, numCols, xMin, yMin, xMax, yMax = GetPropertiesFromRaster(inRaster)
    
    # Use the input raster name to generate a default output name for the fishnet feature class
    inRasterBaseName = os.path.splitext(inRasterName)[0]
    if "clip" in inRasterBaseName:
        outFishnetFeatureName = "{0}.shp".format(inRasterBaseName.replace("clip", "fishnet"))
    else:
        outFishnetFeatureName = "{0}_fishnet.shp".format(inRasterBaseName)
    
    outFishnetFeature = os.path.join(outWorkspace, outFishnetFeatureName)
    
    # Set origin and rotation of fishnet using extent properties
    # rotation is set by the difference in the x-values in the origin coordinate and the y-axis coordinate
    originCoordinate = "{0} {1}".format(xMin, yMin)
    yAxisCoordinate = "{0} {1}".format(xMin, yMax)
    
    # Create fishnet using clipped rasters
    arcpy.CreateFishnet_management(outFishnetFeature,
                                   originCoordinate,
                                   yAxisCoordinate,
                                   0,
                                   0,
                                   numRows,
                                   numCols,
                                   "#",
                                   "NO_LABELS",
                                   inRaster,
                                   "POLYLINE")
    
    return outFishnetFeature

def ConvertFishnetToPolygon(inFishnetFeature, inPointFeature, outWorkspace):
    ''' converts the fishnet polyline features to polygons and assigns
        values from the point feature class to the polygons as attributes '''
    
    # Define name and location of output polygon feature class
    inFishnetName = os.path.basename(inFishnetFeature)
    outFeatureName = inFishnetName.replace("_fishnet", "")
    outPolygonFeature = os.path.join(outWorkspace, outFeatureName)

    # Convert Features to Polygons
    arcpy.FeatureToPolygon_management(inFishnetFeature,
                                      outPolygonFeature,
                                      "#",
                                      "ATTRIBUTES",
                                      inPointFeature)

    return outPolygonFeature

def ConvertFishnetToPolygonMulti(inputList):
    ''' converts the fishnet polyline features to polygons and assigns
        values from the point feature class to the polygons as attributes '''
    # unpack list of input variables
    inFishnetFeature, inPointFeature, outWorkspace = inputList
    
    # Define name and location of output polygon feature class
    inFishnetName = os.path.basename(inFishnetFeature)
    outFeatureName = inFishnetName.replace("_fishnet", "")
    outPolygonFeature = os.path.join(outWorkspace, outFeatureName)

    # Convert Features to Polygons
    arcpy.FeatureToPolygon_management(inFishnetFeature,
                                      outPolygonFeature,
                                      "#",
                                      "ATTRIBUTES",
                                      inPointFeature)

    return outPolygonFeature

def IntersectFeatures(referenceFeatureClass, targetFeatureClass, outWorkspace):
    ''' generates a new feature class that is the result of intersecting two input feature classes '''

    # Define name and location of output intersection feature class
    inFeatureClassName, inFeatureClassExt = os.path.splitext(os.path.basename(targetFeatureClass))
    outIntersectFeatureName = "{0}_intersect{1}".format(inFeatureClassName, inFeatureClassExt)
    outIntersectFeature = os.path.join(outWorkspace, outIntersectFeatureName)

    # list features to intersect for input to Intersect tool
    intersectFeatures = [referenceFeatureClass, targetFeatureClass]

    # Intersect Features
    arcpy.Intersect_analysis(intersectFeatures, outIntersectFeature)

    return outIntersectFeature

def IntersectFeaturesMulti(inputList):
    ''' generates a new feature class that is the result of intersecting two input feature classes '''
    # unpack list of input variables
    referenceFeatureClass, targetFeatureClass, outWorkspace = inputList
    
    # Define name and location of output intersection feature class
    inFeatureClassName, inFeatureClassExt = os.path.splitext(os.path.basename(targetFeatureClass))
    outIntersectFeatureName = "{0}_intersect{1}".format(inFeatureClassName, inFeatureClassExt)
    outIntersectFeature = os.path.join(outWorkspace, outIntersectFeatureName)

    # list features to intersect for input to Intersect tool
    intersectFeatures = [referenceFeatureClass, targetFeatureClass]

    # Intersect Features
    arcpy.Intersect_analysis(intersectFeatures, outIntersectFeature)

    return outIntersectFeature

def MultiProcess(func, funcArgList):
    ''' wrapper function to create a processing pool and map a function to it '''
    pool = mp.Pool(processes=mp.cpu_count() - 1)
    resultList = pool.map(func, funcArgList)
    pool.close()
    return resultList

def ParseDateFromFileName(inFileName, fmt="%m/%d/%Y_24:00"):
    ''' use regex to parse dates in YYYYMM, YYYYM, YYYY_MM, or YYYY_M format and return the date as a string in the desired format '''

    # find date between 1900 and 2099 within basename of provided file
    expr = re.findall(r"19\d{2}0?[1-9](?!\d+)|19\d{2}1[0-2](?!\d+)|20\d{2}0?[1-9](?!\d+)|20\d{2}1[0-2](?!\d+)|19\d{2}_0?[1-9](?!\d+)|19\d{2}_1[0-2](?!\d+)|20\d{2}_0?[1-9](?!\d+)|20\d{2}_1[0-2](?!\d+)", os.path.basename(inFileName))[0]

    # determine the format of the expression and parse accordingly
    if "_" not in expr:
        fileDate = datetime.datetime.strptime(expr, "%Y%m")
    else:
        fileDate = datetime.datetime.strptime(expr, "%Y_%m")
    
    modelDate = datetime.datetime.strftime(LastDayOfMonth(fileDate), fmt)

    return modelDate

def AreaWeightValuesFromFeatureClass(inFeature, inValueUnits, outValueUnits, inIDField, inValueField="grid_code", inAreaField="SHAPE@AREA"):
    ''' performs area weighting on value field and groups to a unique Identifier '''
    arr = arcpy.da.FeatureClassToNumPyArray(inFeature, [inIDField, inValueField, inAreaField])
    df = pd.DataFrame(arr)
    df2 = df.join(df.groupby(inIDField)[inAreaField].sum(), on=inIDField, rsuffix="_total")
    df2["WeightedGridCode"] = df2[inAreaField]/df2["SHAPE@AREA_total"]*df2[inValueField]*LengthUnitConversionFactor(inValueUnits, outValueUnits)
    df3 = df2.groupby(inIDField)["WeightedGridCode"].sum()
    weightedValues = df3.tolist()

    return weightedValues

def PrecipHeader():
    string = """C*******************************************************************************
C
C                  INTEGRATED WATER FLOW MODEL (IWFM)
C                         *** Version 2015 ***
C
C*******************************************************************************
C
C                         PRECIPITATION DATA FILE
C               Precipitation and Evapotranspiration Component
C
C             Project:  C2VSim Fine Grid (C2VSimFG)
C                       California Central Valley Groundwater-Surface Water Simulation Model
C             Filename: C2VSimFG_Precip.dat
C             Version:  C2VSimFG_BETA     2018-05-01
C
C*******************************************************************************
C
C                        ***** Beta Model Disclaimer *****
C
C          This is a beta version of C2VSimFG and is subject to change.  Users 
C          of this version should be aware that this model is undergoing active
C          development and adjustment. Users of this model do so at their own
C          risk subject to the GNU General Public License below. The Department 
C          does not guarantee the accuracy, completeness, or timeliness of the 
C          information provided. Neither the Department of Water Resources nor 
C          any of the sources of the information used by the Department in the
C          development of this model shall be responsible for any errors or 
C          omissions, for the use, or results obtained from the use of this model.
C
C*******************************************************************************
C
C  California Central Valley Groundwater-Surface Water Flow Model (C2VSim)
C  Copyright (C) 2012-2018
C  State of California, Department of Water Resources
C
C  This model is free. You can redistribute it and/or modify it
C  under the terms of the GNU General Public License as published
C  by the Free Software Foundation; either version 2 of the License,
C  or (at your option) any later version.
C
C  This model is distributed WITHOUT ANY WARRANTY; without even
C  the implied warranty of MERCHANTABILITY or FITNESS FOR A
C  PARTICULAR PURPOSE.  See the GNU General Public License for
C  more details. (http://www.gnu.org/licenses/gpl.html)
C
C  The GNU General Public License is available from the Free Software
C  Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
C
C  For technical support, e-mail: IWFMtechsupport@water.ca.gov
C
C
C    Principal Contact:
C          Tariq N. Kadir, PE, Senior Engineer, DWR
C          (916) 653-3513, kadir@water.ca.gov
C
C   SGMA Contact:
C          Tyler Hatch, PhD, PE, Senior Engineer, DWR
C          (916) 651-7014, tyler.hatch@water.ca.gov
C
C*******************************************************************************
C                             File Description:
C
C   This data file contains the time-series rainfall at each rainfall station used
C   in the model.
C """
    return string

def PrecipSpecs(NRAIN, FACTRN, NSPRN, NFQRN):
    string = """
C*******************************************************************************
C                         Rainfall Data Specifications
C
C   NRAIN ;  Number of rainfall stations (or pathnames if DSS files are used)
C             used in the model 
C   FACTRN;  Conversion factor for rainfall rate 
C             It is used to convert only the spatial component of the unit; 
C             DO NOT include the conversion factor for time component of the unit.
C             * e.g. Unit of rainfall rate listed in this file = INCHES/MONTH
C                    Consistent unit used in simulation        = FEET/DAY 
C                    Enter FACTRN (INCHES/MONTH -> FEET/MONTH) = 8.33333E-02 
C                     (conversion of MONTH -> DAY is performed automatically) 
C   NSPRN ;  Number of time steps to update the precipitation data
C             * Enter any number if time-tracking option is on
C   NFQRN ;  Repetition frequency of the precipitation data 
C             * Enter 0 if full time series data is supplied
C             * Enter any number if time-tracking option is on
C   DSSFL ;  The name of the DSS file for data input (maximum 50 characters); 
C             * Leave blank if DSS file is not used for data input
C 
C-------------------------------------------------------------------------------
C         VALUE                                      DESCRIPTION
C-------------------------------------------------------------------------------
          {:<43}/ NRAIN 
          {:<43.5}/ FACTRN  (in/month -> ft/month)         
          {:<43}/ NSPRN
          {:<43}/ NFQRN
                                                     / DSSFL"""
    return string.format(NRAIN, FACTRN, NSPRN, NFQRN)

def PrecipData(NRAIN):
    string = """
C-------------------------------------------------------------------------------
C                             Rainfall Data 
C                         (READ FROM THIS FILE)
C
C   List the rainfall rates for each of the rainfall station below, if it will 
C   not be read from a DSS file (i.e. DSSFL is left blank above).
C
C   ITRN ;   Time 
C   ARAIN;   Rainfall rate at the corresponding rainfall station; [L/T]
C
C-------------------------------------------------------------------------------     
C   ITRN          ARAIN(1)  ARAIN(2)  ARAIN(3) ...
C   TIME        {}
C-------------------------------------------------------------------------------
"""
    return string.format('{:>10}'*NRAIN).format(*[i+1 for i in range(NRAIN)])

if __name__ == '__main__':

    # store time processing begins
    startTime = time.time()

    ##############################################################
    # Define explicit variables
    ##############################################################
    # rasterList is for testing purposes only
    rasterList = [r'D:\ppt\2015\prism_ppt_us_30s_201509.bil', r'D:\ppt\2015\prism_ppt_us_30s_201510.bil', r'D:\ppt\2015\prism_ppt_us_30s_201511.bil', r'D:\ppt\2015\prism_ppt_us_30s_201512.bil']
    ##############################################################
    # inWorkspace could be a geodatabase, folder, list of folders, or a text file
    inWorkspace = r'D:\ppt'
    writeToFileFlag = True
    outRasterListFileName = 'RastersToProcess.txt'
    writeToFileOnly = False
    inUnits = 'millimeters'
    outUnits = 'inches'
    aoiFeature = r'C:\Temp\C2VSimFG-BETA_GIS\Shapefiles\C2VSimFG_Elements_SmallWatersheds_GCS.shp'
    aoiIDField = 'ModelID'
    outWorkspace = r'C:\Temp\C2VSimFG'
    outFileName = 'C2VSimFG_Precip.dat'
    mode = 'test'
    ##############################################################
    # Define derived variables
    ##############################################################
    if mode == 'test':
        inRastersList = rasterList
    else:
        # Generate a list of rasters from inWorkspace
        inRastersList = []
        for wkspace in inWorkspace:
            if IsGeodatabase(wkspace):
                if inRastersList == None:
                    inRastersList = GetAllRastersFromGeodatabase(wkspace)
                else:
                    inRastersList.extend(GetAllRastersFromGeodatabase(wkspace))
            elif IsFolder(inWorkspace):
                if inRastersList == None:
                    inRastersList = GetAllRastersFromFolders(wkspace)
                else:
                    inRastersList.extend(GetAllRastersFromFolders(wkspace))
            elif IsTextFile(inWorkspace):
                if inRastersList == None:
                    inRastersList = GetAllRastersFromFile(wkspace)
                else:
                    inRastersList.extend(GetAllRastersFromFile(wkspace))
            else:
                print('format of inWorkspace was not compatible. Must contain one or more folders, geodatabases, or text files')
                sys.exit(1)

    # Determine the length of the list of rasters
    lenRasterList = len(inRastersList)

    # Check to see that there is at least one raster to process
    if lenRasterList <= 0:
        print('There are no rasters to process in the locations provided.')
        sys.exit(0)
            
    print('There are {0} rasters to process.'.format(lenRasterList))
    
    if writeToFileFlag:
        if writeToFileOnly:
            print("Writing rasters to {0} only.".format(os.path.join(outWorkspace, outRasterListFileName)))
            WriteRastersToFile(inRastersList, outWorkspace, outRasterListFileName)
        else:
            print("Writing rasters to {0}.".format(os.path.join(outWorkspace, outRasterListFileName)))
            WriteRastersToFile(inRastersList, outWorkspace, outRasterListFileName)
            
            # Count number of polygons in aoi feature class
            featureCount = int(arcpy.GetCount_management(aoiFeature)[0])

            # Make a directory called Clipped to hold the clipped rasters for later processing
            clippedDir = MakeDirectory(outWorkspace, "Clipped")

            # Make a directory called Points to hold the point features for later processing
            pointsDir = MakeDirectory(outWorkspace, "Points")

            # Make a directory called Fishnet to hold the fishnet line features for later processing
            fishnetDir = MakeDirectory(outWorkspace, "Fishnet")

            # Make a directory called Polygons to hold the polygon features for later processing
            polygonDir = MakeDirectory(outWorkspace, "Polygon")

            # Make a directory called Polygons to hold the polygon features for later processing
            intersectDir = MakeDirectory(outWorkspace, "Intersect")

            #################################################################
            # multiprocessing
            #################################################################

            print("Clipping Rasters")

            # zip input data for Clip function into a tuple of tuples for each raster being processed
            aoiFeatureList = [aoiFeature for i in range(lenRasterList)]
            clippedDirList = [clippedDir for i in range(lenRasterList)]
            inClipRasterData = tuple(zip(inRastersList, aoiFeatureList, clippedDirList))

            # clip rasters using multiprocessing
            clipRasterList = MultiProcess(ClipRasterMulti, inClipRasterData)

            print("Converting Clipped Rasters to Points")
            
            # zip input data for Raster to Points function into a tuple of tuples for each raster being processed
            pointsDirList = [pointsDir for i in range(len(clipRasterList))]    
            inRasterToPointsData = tuple(zip(clipRasterList, pointsDirList))

            # convert rasters to points using multiprocessing
            pointFeatureList = MultiProcess(ConvertRasterToPointsMulti, inRasterToPointsData)

            print("Creating Fishnets for Rasters")

            # zip input data for fishnet conversion into a tuple of tuples for each raster being processed
            fishnetDirList = [fishnetDir for i in range(len(clipRasterList))]
            inFishnetData = tuple(zip(clipRasterList, fishnetDirList))

            # create fishnet using multiprocessing
            fishnetList = MultiProcess(CreateFishnetFeatureMulti, inFishnetData)

            print("Converting Fishnets to Polygon Features")
            
            # zip input data for fishnet to polygon into a tuple of tuples for each fishnet being converted to a polygon  
            polygonDirList = [polygonDir for i in range(len(fishnetList))]
            inFishnetToPolygonData = tuple(zip(sorted(fishnetList), sorted(pointFeatureList), polygonDirList))

            # convert fishnets to polygons using multiprocessing
            polygonFeatures = MultiProcess(ConvertFishnetToPolygonMulti, inFishnetToPolygonData)

            print("Intersecting AOI with Vectorized Raster Polygons")
            
            # zip input data for intersecting vectorized rasters and the area of interest polygon feature class
            intersectDirList = [intersectDir for i in range(len(polygonFeatures))]
            intersectData = tuple(zip(aoiFeatureList, polygonFeatures, intersectDirList))

            # intersect features using multiprocessing
            intersectFeatureList = MultiProcess(IntersectFeaturesMulti, intersectData)

            #################################################################
            # single processing
            #################################################################

            # Create output file
            outFile = os.path.join(outWorkspace, outFileName)

            # Write output file    
            with open(outFile, 'w') as f:
                f.write(PrecipHeader())
                f.write(PrecipSpecs(featureCount, FACTRN(outUnits), 1, 0))
                f.write(PrecipData(featureCount))
                for fc in sorted(intersectFeatureList):

                    # parse and format date from table name for printing to output file
                    try:
                        modelDate = ParseDateFromFileName(inFileName=fc, fmt='%m/%d/%Y_24:00')
                    except IndexError:
                        exc = sys.exc_info()
                        print("{0}: date could not be read from filename: {1}".format(exc[0].__name__, fc))
                        print("files should have dates of the following format:\n" + \
                              "    1. Years should be between 1900 and 2099\n" + \
                              "    2. Format:\n" + \
                              "        a. YYYYMM\n" + \
                              "        b. YYYYM\n" + \
                              "        c. YYYY_MM\n" + \
                              "        d. YYYY_M\n")
                        sys.exit(1)

                    # convert feature class table to array for processing
                    values = AreaWeightValuesFromFeatureClass(fc, inUnits, outUnits, aoiIDField, 'grid_code', 'SHAPE@AREA')
                    
                    if len(values) == featureCount:
                        f.write(modelDate)
                        f.write(('{:>10.3}'*len(values)).format(*values))
                        f.write('\n')
                    else:
                        raise arcpy.ExecuteError
    else:
        # Count number of polygons in aoi feature class
        featureCount = int(arcpy.GetCount_management(aoiFeature)[0])

        # Make a directory called Clipped to hold the clipped rasters for later processing
        clippedDir = MakeDirectory(outWorkspace, "Clipped")

        # Make a directory called Points to hold the point features for later processing
        pointsDir = MakeDirectory(outWorkspace, "Points")

        # Make a directory called Fishnet to hold the fishnet line features for later processing
        fishnetDir = MakeDirectory(outWorkspace, "Fishnet")

        # Make a directory called Polygons to hold the polygon features for later processing
        polygonDir = MakeDirectory(outWorkspace, "Polygon")

        # Make a directory called Polygons to hold the polygon features for later processing
        intersectDir = MakeDirectory(outWorkspace, "Intersect")

        #################################################################
        # multiprocessing
        #################################################################

        print("Clipping Rasters")

        # zip input data for Clip function into a tuple of tuples for each raster being processed
        aoiFeatureList = [aoiFeature for i in range(lenRasterList)]
        clippedDirList = [clippedDir for i in range(lenRasterList)]
        inClipRasterData = tuple(zip(inRastersList, aoiFeatureList, clippedDirList))

        # clip rasters using multiprocessing
        clipRasterList = MultiProcess(ClipRasterMulti, inClipRasterData)

        print("Converting Clipped Rasters to Points")
        
        # zip input data for Raster to Points function into a tuple of tuples for each raster being processed
        pointsDirList = [pointsDir for i in range(len(clipRasterList))]    
        inRasterToPointsData = tuple(zip(clipRasterList, pointsDirList))

        # convert rasters to points using multiprocessing
        pointFeatureList = MultiProcess(ConvertRasterToPointsMulti, inRasterToPointsData)

        print("Creating Fishnets for Rasters")

        # zip input data for fishnet conversion into a tuple of tuples for each raster being processed
        fishnetDirList = [fishnetDir for i in range(len(clipRasterList))]
        inFishnetData = tuple(zip(clipRasterList, fishnetDirList))

        # create fishnet using multiprocessing
        fishnetList = MultiProcess(CreateFishnetFeatureMulti, inFishnetData)

        print("Converting Fishnets to Polygon Features")
        
        # zip input data for fishnet to polygon into a tuple of tuples for each fishnet being converted to a polygon  
        polygonDirList = [polygonDir for i in range(len(fishnetList))]
        inFishnetToPolygonData = tuple(zip(sorted(fishnetList), sorted(pointFeatureList), polygonDirList))

        # convert fishnets to polygons using multiprocessing
        polygonFeatures = MultiProcess(ConvertFishnetToPolygonMulti, inFishnetToPolygonData)

        print("Intersecting AOI with Vectorized Raster Polygons")
        
        # zip input data for intersecting vectorized rasters and the area of interest polygon feature class
        intersectDirList = [intersectDir for i in range(len(polygonFeatures))]
        intersectData = tuple(zip(aoiFeatureList, polygonFeatures, intersectDirList))

        # intersect features using multiprocessing
        intersectFeatureList = MultiProcess(IntersectFeaturesMulti, intersectData)

        #################################################################
        # single processing
        #################################################################

        # Create output file
        outFile = os.path.join(outWorkspace, outFileName)

        # Write output file    
        with open(outFile, 'w') as f:
            f.write(PrecipHeader())
            f.write(PrecipSpecs(featureCount, FACTRN(outUnits), 1, 0))
            f.write(PrecipData(featureCount))
            for fc in sorted(intersectFeatureList):

                # parse and format date from table name for printing to output file
                try:
                    modelDate = ParseDateFromFileName(inFileName=fc, fmt='%m/%d/%Y_24:00')
                except IndexError:
                    exc = sys.exc_info()
                    print("{0}: date could not be read from filename: {1}".format(exc[0].__name__, fc))
                    print("files should have dates of the following format:\n" + \
                          "    1. Years should be between 1900 and 2099\n" + \
                          "    2. Format:\n" + \
                          "        a. YYYYMM\n" + \
                          "        b. YYYYM\n" + \
                          "        c. YYYY_MM\n" + \
                          "        d. YYYY_M\n")
                    sys.exit(1)

                # convert feature class table to array for processing
                values = AreaWeightValuesFromFeatureClass(fc, inUnits, outUnits, aoiIDField, 'grid_code', 'SHAPE@AREA')
                
                if len(values) == featureCount:
                    f.write(modelDate)
                    f.write(('{:>10.3}'*len(values)).format(*values))
                    f.write('\n')
                else:
                    raise arcpy.ExecuteError


    print("Processing Complete!")

    # store time processing ends
    endTime = time.time()

    print("The program took {0}".format(endTime - startTime))
