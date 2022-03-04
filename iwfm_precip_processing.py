# Import Modules
import os
import sys
import re
import time
import datetime
import glob
import arcpy

import numpy as np
import pandas as pd
import multiprocessing as mp

from iwfm_file_headers import (
    write_precip_header,
    write_precip_specs
)

from geoprocessing import (
    is_geodatabase,
    is_folder,
    is_text_file,
    get_all_rasters_from_folders,
    get_all_rasters_from_geodatabase,
    get_all_rasters_from_file,
    get_list_of_feature_classes,
    last_day_of_month,
    length_unit_conversion_factor,
    write_rasters_to_file,
    make_directory,
    get_properties_from_raster,
    clip_raster,
    clip_raster_multi,
    convert_raster_to_points,
    convert_raster_to_points_multi,
    create_fishnet_feature,
    create_fishnet_feature_multi,
    convert_fishnet_to_polygon,
    convert_fishnet_to_polygon_multi,
    intersect_features,
    intersect_features_multi,
    multi_process,
    parse_date_from_file_name,
    format_IWFM_date,
    order_files_by_date,
    area_weight_values_from_feature_class
)

if __name__ == '__main__':

    # store time processing begins
    start_time = time.time()

    ##############################################################
    # Define explicit variables
    ##############################################################
    # raster_list is for testing purposes only
    raster_list = [r'F:\Tyler\DWR\SGMP\Modeling\ppt\2015\prism_ppt_us_30s_201509.bil', 
                   r'F:\Tyler\DWR\SGMP\Modeling\ppt\2015\prism_ppt_us_30s_201510.bil', 
                   r'F:\Tyler\DWR\SGMP\Modeling\ppt\2015\prism_ppt_us_30s_201511.bil', 
                   r'F:\Tyler\DWR\SGMP\Modeling\ppt\2015\prism_ppt_us_30s_201512.bil']
    ##############################################################
    # in_workspace could be a geodatabase, folder, list of folders, or a text file
    in_workspace = r'F:\Tyler\DWR\SGMP\Modeling\ppt'
    write_to_file_flag = False
    out_raster_list_file_name = 'RastersToProcess.txt'
    write_to_file_only = False
    in_units = 'millimeters'
    out_units = 'inches'
    aoi_feature = r'F:\Tyler\DWR\SGMP\Modeling\C2VSimFG\PRISMPrecip\C2VSimFG_Elements_SmallWatersheds_GCS.shp'
    aoi_id_field = 'ModelID'
    out_workspace = r'C:\Users\hatch\Desktop\raster\PRISMPrecip'
    project_short_name = 'C2VSim Fine Grid (C2VSimFG)'
    project_long_name = 'California Central Valley Groundwater-Surface Water Simulation Model'
    model_name = 'California Central Valley Groundwater-Surface Water Flow Model (C2VSim)'
    model_version = 'C2VSimFG_v1.01'
    organization = 'State of California, Department of Water Resources'
    tech_support_email = 'c2vsimfgtechsupport@water.ca.gov'
    contact_name = 'Tyler Hatch, PhD, PE, Supervising Engineer, DWR'
    contact_email = 'tyler.hatch@water.ca.gov'

    d = 'This is Version 1.01 of C2VSimFG and is subject to change.  Users ' + \
        'of this version should be aware that this model is undergoing active ' + \
        'development and adjustment. Users of this model do so at their own ' + \
        'risk subject to the GNU General Public License below. The Department ' + \
        'does not guarantee the accuracy, completeness, or timeliness of the ' + \
        'information provided. Neither the Department of Water Resources nor ' + \
        'any of the sources of the information used by the Department in the ' + \
        'development of this model shall be responsible for any errors or ' + \
        'omissions, for the use, or results obtained from the use of this model.'
    
    n_ts_update = 1
    ts_frequency = 0
    dss_file = ''
    out_file_name = 'C2VSimFG_Precip_test.dat'
    mode = 'test'
    ##############################################################
    # Define derived variables
    ##############################################################
    if mode == 'test':
        in_rasters_list = raster_list
    else:
        # Generate a list of rasters from in_workspace
        in_rasters_list = []
        for wkspace in [in_workspace]:
            print(wkspace)
            if is_geodatabase(wkspace):
                if in_rasters_list is None:
                    in_rasters_list = get_all_rasters_from_geodatabase(wkspace)
                else:
                    in_rasters_list.extend(get_all_rasters_from_geodatabase(wkspace))
            elif is_folder(in_workspace):
                if in_rasters_list is None:
                    in_rasters_list = get_all_rasters_from_folders(wkspace)
                else:
                    in_rasters_list.extend(get_all_rasters_from_folders(wkspace))
            elif is_text_file(in_workspace):
                if in_rasters_list is None:
                    in_rasters_list = get_all_rasters_from_file(wkspace)
                else:
                    in_rasters_list.extend(get_all_rasters_from_file(wkspace))
            else:
                print('format of in_workspace was not compatible. Must contain one or more folders, geodatabases, or text files')
                sys.exit(1)

    # Determine the length of the list of rasters
    len_raster_list = len(in_rasters_list)

    # Check to see that there is at least one raster to process
    if len_raster_list <= 0:
        print('There are no rasters to process in the locations provided.')
        sys.exit(0)
            
    print('There are {0} rasters to process.'.format(len_raster_list))
    
    if write_to_file_flag:
        print("Writing rasters to {0} only.".format(os.path.join(out_workspace, out_raster_list_file_name)))
        write_rasters_to_file(in_rasters_list, out_workspace, out_raster_list_file_name)

    if not write_to_file_only:
        
        # Count number of polygons in aoi feature class
        feature_count = int(arcpy.GetCount_management(aoi_feature)[0])

        # Make a directory called Clipped to hold the clipped rasters for later processing
        clipped_dir = make_directory(out_workspace, "Clipped")

        # Make a directory called Points to hold the point features for later processing
        points_dir = make_directory(out_workspace, "Points")

        # Make a directory called Fishnet to hold the fishnet line features for later processing
        fishnet_dir = make_directory(out_workspace, "Fishnet")

        # Make a directory called Polygons to hold the polygon features for later processing
        polygon_dir = make_directory(out_workspace, "Polygon")

        # Make a directory called Polygons to hold the polygon features for later processing
        intersect_dir = make_directory(out_workspace, "Intersect")

        #################################################################
        # multiprocessing
        #################################################################

        print("Clipping Rasters")

        # zip input data for Clip function into a tuple of tuples for each raster being processed
        aoi_feature_list = [aoi_feature for i in range(len_raster_list)]
        clipped_dir_list = [clipped_dir for i in range(len_raster_list)]
        in_clip_raster_data = tuple(zip(in_rasters_list, aoi_feature_list, clipped_dir_list))

        # clip rasters using multiprocessing
        clip_raster_list = multi_process(clip_raster_multi, in_clip_raster_data)

        print("Converting Clipped Rasters to Points")
        
        # zip input data for Raster to Points function into a tuple of tuples for each raster being processed
        points_dir_list = [points_dir for i in range(len(clip_raster_list))]    
        in_raster_to_points_data = tuple(zip(clip_raster_list, points_dir_list))

        # convert rasters to points using multiprocessing
        point_feature_list = multi_process(convert_raster_to_points_multi, in_raster_to_points_data)

        print("Creating Fishnets for Rasters")

        # zip input data for fishnet conversion into a tuple of tuples for each raster being processed
        fishnet_dir_list = [fishnet_dir for i in range(len(clip_raster_list))]
        in_fishnet_data = tuple(zip(clip_raster_list, fishnet_dir_list))

        # create fishnet using multiprocessing
        fishnet_list = multi_process(create_fishnet_feature_multi, in_fishnet_data)

        print("Converting Fishnets to Polygon Features")
        
        # zip input data for fishnet to polygon into a tuple of tuples for each fishnet being converted to a polygon  
        polygon_dir_list = [polygon_dir for i in range(len(fishnet_list))]
        in_fishnet_to_polygon_data = tuple(zip(sorted(fishnet_list), sorted(point_feature_list), polygon_dir_list))

        # convert fishnets to polygons using multiprocessing
        polygon_features = multi_process(convert_fishnet_to_polygon_multi, in_fishnet_to_polygon_data)

        print("Intersecting AOI with Vectorized Raster Polygons")
        
        # zip input data for intersecting vectorized rasters and the area of interest polygon feature class
        intersect_dir_list = [intersect_dir for i in range(len(polygon_features))]
        intersect_data = tuple(zip(aoi_feature_list, polygon_features, intersect_dir_list))

        # intersect features using multiprocessing
        intersect_feature_list = multi_process(intersect_features_multi, intersect_data)

        #################################################################
        # single processing
        #################################################################

        # prepare features for writing to file
        output_features = order_files_by_date(intersect_feature_list)

        # Create output file
        out_file = os.path.join(out_workspace, out_file_name)

        version = '{} - {}'.format(model_version, datetime.date.today())

        # Write output file    
        with open(out_file, 'w') as f:
            write_precip_header(
                f,
                project_short_name,
                project_long_name,
                version,
                organization,
                tech_support_email,
                contact_name,
                contact_email,
                model_name,
                d
            )
            write_precip_specs(
                f,
                feature_count, 
                length_unit_conversion_factor(out_units, 'feet'), 
                n_ts_update, 
                ts_frequency, 
                dss_file
                )
            for dt in output_features['TextDate'].to_list():
                # get name of feature class
                fc = output_features[output_features['TextDate'] == dt]['FileNames'].to_numpy()[0]

                # convert feature class table to array for processing
                values = area_weight_values_from_feature_class(fc, in_units, out_units, aoi_id_field, 'grid_code', 'SHAPE@AREA')
                    
                if len(values) == feature_count:
                    f.write(dt)
                    f.write(('{:>10.3}'*len(values)).format(*values))
                    f.write('\n')
                else:
                    raise arcpy.ExecuteError


    print("Processing Complete!")

    # store time processing ends
    end_time = time.time()

    run_time = end_time - start_time

    # convert duration to hours, minutes, seconds
    minutes, seconds = divmod(run_time, 60)
    hours, minutes = divmod(minutes, 60)
        
    if hours > 0:
        print("{}{} HOURS {} MINUTES {:6.3f} SECONDS".format('TOTAL RUN TIME: ', hours, minutes, seconds))
    elif minutes > 0:
        print("{}{} MINUTES {:6.3f} SECONDS".format('TOTAL RUN TIME: ', minutes, seconds))
    else:
        print("{}{:6.3f} SECONDS".format('TOTAL RUN TIME: ', seconds))
