# IWFM Precipitation File Generator

## **Description:**
The scripts in this repository were developed for processing gridded precipitation data to an IWFM input file

Gridded precipitation data is available from several sources:

1. PRISM
2. USGS Basin Characterization Model (BCM)
3. daymet
4. NLDAS

This script requires that the user download and have the gridded data available locally or on network attached storage.

## **How-To**
From the command line using a python environment that supports arcpy, run the following:

```bash
python iwfm_precip_processing.py input.txt
```

## **Modes**
The program has two primary modes of operation.

The first 4 input fields in input.txt dictate which mode is followed.

1. Generate a file listing the raster datasets to process
   provide the parent folder path (absolute or relative to the location of iwfm_precip_processing.py) to the location of the raster data
   set the flag telling program to write names of rasters to process to a file to True
   set the file name to output list of rasters (default RastersToProcess.txt)
   set the write to file only flag to True

2. Process the raster data to an area-weighted value for the IWFM model elements and small watersheds and organize into the IWFM model input file structure
    provide the parent folder path to the location of the raster data or the file produced from option 1.
    set the flag telling program to write names of rasters to process to a file to False
    set the write to file only flag to False.
    provide the path to the combined model elements and small watersheds feature class
    provide the field name in the model elements and small watersheds feature class containing the id
    provide the output path name for the model input file
    provide the model input file comment information

   