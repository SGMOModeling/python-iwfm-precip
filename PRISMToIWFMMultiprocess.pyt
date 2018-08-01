import arcpy, os, sys, re, datetime, glob
import numpy as np
import pandas as pd
import multiprocessing as mp

from PrecipProcessingTools import *

class Toolbox(object):
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "PRISM to IWFM Tools"
        self.alias = ""

        # List of tool classes associated with this toolbox
        self.tools = [ConvertRasterToIWFMInput]

class ConvertRasterToIWFMInput(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Convert Raster to IWFM Input"
        self.description = "Takes input rasters clips them to an area of interest " + \
                           "and then converts the pixels to a polygon feature class." + \
                           "The features are then processed to the input file format for IWFM"
        self.canRunInBackground = False

    def getParameterInfo(self):
        """Define parameter definitions"""
        param0 = arcpy.Parameter(
            displayName="Input workspace",
            name="inWorkspace",
            datatype=["DEWorkspace","DETextfile"],
            parameterType="Required",
            direction="Input",
            multiValue=True)

        param1 = arcpy.Parameter(
            displayName="Select Rasters To Process",
            name="inRasters",
            datatype="GPString",
            parameterType="Required",
            direction="Input",
            multiValue=True)

        param1.parameterDependencies = [param0.name]

        param2 = arcpy.Parameter(
            displayName="Write List of Rasters to File",
            name="writeToFile",
            datatype="GPBoolean",
            parameterType="Required",
            direction="Input")

        param2.parameterDependencies = [param0.name]
        param2.value = False

        param3 = arcpy.Parameter(
            displayName="Name of Text File to Write Raster List",
            name="rasterTextFile",
            datatype="GPString",
            parameterType="Required",
            direction="Input")

        param3.parameterDependencies = [param0.name, param2.name]
        param3.value = "rasterlist.txt"
        param3.enabled = False

        param4 = arcpy.Parameter(
            displayName="Only Write Rasters to File",
            name="writeToFileOnly",
            datatype="GPBoolean",
            parameterType="Required",
            direction="Input")

        param4.parameterDependencies = [param0.name, param2.name]
        param4.value = False

        param5 = arcpy.Parameter(
            displayName="Select the Units for the Raster Datasets",
            name="inUnits",
            datatype="GPString",
            parameterType="Required",
            direction="Input",
            multiValue=False)

        #param5.parameterDependencies = [param2.name]
        param5.filter.list = ["feet", "inches", "meters", "millimeters"]
        param5.value = "feet"

        param6 = arcpy.Parameter(
            displayName="Select the Units for the Output File Name",
            name="outUnits",
            datatype="GPString",
            parameterType="Required",
            direction="Input",
            multiValue=False)

        #param6.parameterDependencies = [param2.name]
        param6.filter.list = ["feet", "inches", "meters", "millimeters"]
        param6.value = "feet"

        param7 = arcpy.Parameter(
            displayName="Select Polygon Feature Class For Area of Interest",
            name="aoiFeature",
            datatype="DEFeatureClass",
            parameterType="Optional",
            direction="Input",
            multiValue=False)

        #param7.parameterDependencies = [param2.name]

        param8 = arcpy.Parameter(
            displayName="Select ID Field from Feature Class",
            name="aoiIDField",
            datatype="Field",
            parameterType="Optional",
            direction="Input",
            multiValue=False)

        param8.parameterDependencies = [param7.name]

        param9 = arcpy.Parameter(
            displayName="Select Output Folder",
            name="outWorkspace",
            datatype="DEWorkspace",
            parameterType="Required",
            direction="Input",
            multiValue=False)

        param10 = arcpy.Parameter(
            displayName="Provide Output Filename",
            name="outFileName",
            datatype="GPString",
            parameterType="Required",
            direction="Input",
            multiValue=False)

        param10.value = "C2VSimFG_Input.dat"

        params = [param0, param1, param2, param3, param4, param5, param6, param7, param8, param9, param10]

        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""

        # if parameters[0].value:
            # inRasterWorkspaces = parameters[0].valueAsText
            # rasterWorkspaces = inRasterWorkspaces.split(";")
            # listDir = []
            # for wkspace in rasterWorkspaces:
                # if not IsGeodatabase(wkspace):
                    # if listDir == None:
                        # listDir = GetAllRastersFromFolders(wkspace)
                    # else:
                        # listDir.extend(GetAllRastersFromFolders(wkspace))
                # else:
                    # if listDir == None:
                        # listDir = GetAllRastersFromGeodatabase(wkspace)
                    # else:
                        # listDir.extend(GetAllRastersFromGeodatabase(wkspace))
            # parameters[1].filter.list = listDir

        if parameters[0].value:
            inRasterWorkspaces = parameters[0].valueAsText
            rasterWorkspaces = inRasterWorkspaces.split(";")
            listDir = []
            for wkspace in rasterWorkspaces:
                if IsFolder(wkspace):
                    if parameters[2].altered:
                        if parameters[2].value:
                            parameters[3].enabled = True
                            parameters[4].enabled = True
                        else:
                            parameters[3].enabled = False
                            parameters[4].enabled = False
                    if listDir == None:
                        listDir = GetAllRastersFromFolders(wkspace)
                    else:
                        listDir.extend(GetAllRastersFromFolders(wkspace))
                elif IsGeodatabase(wkspace):
                    if parameters[2].altered:
                        if parameters[2].value:
                            parameters[3].enabled = True
                            parameters[4].enabled = True
                        else:
                            parameters[3].enabled = False
                            parameters[4].enabled = False
                    if listDir == None:
                        listDir = GetAllRastersFromGeodatabase(wkspace)
                    else:
                        listDir.extend(GetAllRastersFromGeodatabase(wkspace))
                elif IsTextFile(wkspace):
                    parameters[2].enabled = False
                    parameters[3].enabled = False
                    parameters[4].enabled = False
                    if listDir == None:
                        listDir = GetAllRastersFromFile(wkspace)
                    else:
                        listDir.extend(GetAllRastersFromFile(wkspace))
            parameters[1].filter.list = listDir

        if parameters[4].altered:
            if parameters[4].value:
                parameters[5].enabled = False
                parameters[6].enabled = False
                parameters[7].enabled = False
                parameters[8].enabled = False
                parameters[10].enabled = False
            else:
                parameters[5].enabled = True
                parameters[6].enabled = True
                parameters[7].enabled = True
                parameters[8].enabled = True
                parameters[10].enabled = True

        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        """The source code of the tool."""

        # Check out ArcGIS Spatial Analyst Extension License
        arcpy.CheckOutExtension("Spatial")

        # Get User-defined parameter values from interface
        inWorkspace = parameters[0].valueAsText
        inRasters = parameters[1].valueAsText
        writeToFileFlag = parameters[2].value
        outRasterListFileName = parameters[3].valueAsText
        writeToFileOnly = parameters[4].value
        inUnits = parameters[5].valueAsText
        outUnits = parameters[6].valueAsText
        aoiFeature = parameters[7].valueAsText
        aoiIDField = parameters[8].valueAsText
        outWorkspace = parameters[9].valueAsText
        outFileName = parameters[10].valueAsText

        # convert the User-specified input raster string to a list
        inRastersList = inRasters.split(";")
        lenRasterList = len(inRastersList)
        arcpy.AddMessage("List of {0} Rasters generated.".format(lenRasterList))

        if writeToFileFlag:
            if writeToFileOnly:
                arcpy.AddMessage("Writing rasters to {0} only.".format(os.path.join(outWorkspace, outRasterListFileName)))
                WriteRastersToFile(inRastersList, outWorkspace, outRasterListFileName)
            else:
                arcpy.AddMessage("Writing rasters to {0}.".format(os.path.join(outWorkspace, outRasterListFileName)))
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

                arcpy.AddMessage("Clipping Rasters")

                # zip input data for Clip function into a tuple of tuples for each raster to process
                aoiFeatureList = [aoiFeature for i in range(lenRasterList)]
                clippedDirList = [clippedDir for i in range(lenRasterList)]
                inClipRasterData = tuple(zip(inRastersList, aoiFeatureList, clippedDirList))

                # Clip Rasters using multiprocessing
                clipRasterList = MultiProcess(ClipRasterMulti, inClipRasterData)

                arcpy.AddMessage("Converting Rasters to points and fishnets")

                pointsDirList = [pointsDir for i in range(len(clipRasterList))]
                fishnetDirList = [fishnetDir for i in range(len(clipRasterList))]
                
                inRasterToPointsData = tuple(zip(clipRasterList, pointsDirList))
                inFishnetData = tuple(zip(clipRasterList, fishnetDirList))

                pointFeatureList = MultiProcess(ConvertRasterToPointsMulti, inRasterToPointsData)
                fishnetList = MultiProcess(CreateFishnetFeatureMulti, inFishnetData)

                arcpy.AddMessage("Converting Fishnets to Polygon Features")

                polygonDirList = [polygonDir for i in range(len(fishnetList))]
                inFishnetToPolygonData = tuple(zip(sorted(fishnetList), sorted(pointFeatureList), polygonDirList))

                polygonFeatures = MultiProcess(ConvertFishnetToPolygonMulti, inFishnetToPolygonData)

                arcpy.AddMessage("Intersecting Data with {0}.".format(aoiFeature))

                intersectDirList = [intersectDir for i in range(len(polygonFeatures))]
                intersectData = tuple(zip(aoiFeatureList, polygonFeatures, intersectDirList))

                intersectFeatureList = MultiProcess(IntersectFeaturesMulti, intersectData)
                  
                # Create output file
                outFile = os.path.join(outWorkspace, outFileName)

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
                            arcpy.AddMessage("{0}: date could not be read from filename: {1}".format(exc[0].__name__, fc))
                            arcpy.AddMessage("files should have dates of the following format:\n" + \
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

            arcpy.AddMessage("Clipping Rasters")

            # zip input data for Clip function into a tuple of tuples for each raster to process
            aoiFeatureList = [aoiFeature for i in range(lenRasterList)]
            clippedDirList = [clippedDir for i in range(lenRasterList)]
            inClipRasterData = tuple(zip(inRastersList, aoiFeatureList, clippedDirList))

            # Clip Rasters using multiprocessing
            clipRasterList = MultiProcess(ClipRasterMulti, inClipRasterData)

            arcpy.AddMessage("Converting Rasters to points and fishnets")

            pointsDirList = [pointsDir for i in range(len(clipRasterList))]
            fishnetDirList = [fishnetDir for i in range(len(clipRasterList))]
            
            inRasterToPointsData = tuple(zip(clipRasterList, pointsDirList))
            inFishnetData = tuple(zip(clipRasterList, fishnetDirList))

            pointFeatureList = MultiProcess(ConvertRasterToPointsMulti, inRasterToPointsData)
            fishnetList = MultiProcess(CreateFishnetFeatureMulti, inFishnetData)

            arcpy.AddMessage("Converting Fishnets to Polygon Features")

            polygonDirList = [polygonDir for i in range(len(fishnetList))]
            inFishnetToPolygonData = tuple(zip(sorted(fishnetList), sorted(pointFeatureList), polygonDirList))

            polygonFeatures = MultiProcess(ConvertFishnetToPolygonMulti, inFishnetToPolygonData)

            arcpy.AddMessage("Intersecting Data with {0}.".format(aoiFeature))

            intersectDirList = [intersectDir for i in range(len(polygonFeatures))]
            intersectData = tuple(zip(aoiFeatureList, polygonFeatures, intersectDirList))

            intersectFeatureList = MultiProcess(IntersectFeaturesMulti, intersectData)
              
            # Create output file
            outFile = os.path.join(outWorkspace, outFileName)

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
                        arcpy.AddMessage("{0}: date could not be read from filename: {1}".format(exc[0].__name__, fc))
                        arcpy.AddMessage("files should have dates of the following format:\n" + \
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

        arcpy.AddMessage("Processing Complete!")

        return