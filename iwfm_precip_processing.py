# Import Modules
import os
import sys
import re
import datetime
import arcpy

from timer import Timer

from iwfm_file_headers import write_precip_header, write_precip_specs

from geoprocessing import (
    generate_raster_list,
    length_unit_conversion_factor,
    write_rasters_to_file,
    make_directory,
    clip_raster_multi,
    convert_raster_to_points_multi,
    create_fishnet_feature_multi,
    convert_fishnet_to_polygon_multi,
    intersect_features_multi,
    multi_process,
    order_files_by_date,
    area_weight_values_from_feature_class,
)


def read_from_command_line(args):
    """
    Return a list of inputs provided in a text file for running a program
    """
    if len(args) == 2:
        with open(args[-1], "r") as f:
            input_data = f.read()

    elif len(args) == 1:
        file_name = input("Please specify the name of the input file:\n")
        with open(file_name, "r") as f:
            input_data = f.read()

    else:
        raise TypeError("Too many arguments were provided.")

    input_list = input_data.split("\n")

    clean_list = [item for item in input_list if len(item) != 0 and item[0] != "#"]

    return clean_list


def filter_monthly(raster_list):
    """
    Filter raster list by date
    """
    expr = re.compile(
        r"19\d{2}0?[1-9](?!\d+)"
        r"|19\d{2}1?[0-2](?!\d+)"
        r"|20\d{2}0?[1-9](?!\d+)"
        r"|20\d{2}1[0-2](?!\d+)"
    )

    monthly_rasters = []
    for raster in raster_list:
        match = re.search(expr, raster)

        if match:
            monthly_rasters.append(raster)

    return monthly_rasters


if __name__ == "__main__":

    # store time processing begins
    timer = Timer()
    timer.start()

    ##############################################################
    # input variables
    ##############################################################
    # raster_list is for testing purposes only
    raster_list = [
        r"E:\Tyler\DWR\SGMP\Modeling\ppt\2015\prism_ppt_us_30s_201509.bil",
        r"E:\Tyler\DWR\SGMP\Modeling\ppt\2015\prism_ppt_us_30s_201510.bil",
        r"E:\Tyler\DWR\SGMP\Modeling\ppt\2015\prism_ppt_us_30s_201511.bil",
        r"E:\Tyler\DWR\SGMP\Modeling\ppt\2015\prism_ppt_us_30s_201512.bil",
    ]
    ##############################################################
    inputs_list = read_from_command_line(sys.argv)

    in_workspace = inputs_list[0]

    if inputs_list[1] == "True":
        write_to_file_flag = True
    elif inputs_list[1] == "False":
        write_to_file_flag = False

    out_raster_list_file_name = inputs_list[2]

    if inputs_list[3] == "True":
        write_to_file_only = True
    elif inputs_list[3] == "False":
        write_to_file_only = False

    in_units = inputs_list[4]
    out_units = inputs_list[5]

    aoi_feature = inputs_list[6]
    aoi_id_field = inputs_list[7]

    out_workspace = inputs_list[8]

    project_short_name = inputs_list[9]
    project_long_name = inputs_list[10]
    model_name = inputs_list[11]
    model_version = inputs_list[12]
    organization = inputs_list[13]
    tech_support_email = inputs_list[14]
    contact_name = inputs_list[15]
    contact_email = inputs_list[16]

    disclaimer_file = inputs_list[17]
    with open(disclaimer_file, "r") as f:
        d = f.read().replace("\n", " ")

    n_ts_update = int(inputs_list[18])
    ts_frequency = int(inputs_list[19])

    if inputs_list[20] == "''":
        dss_file = ""
    else:
        dss_file = inputs_list[20]

    out_file_name = inputs_list[21]

    mode = inputs_list[22]

    ##############################################################
    # Define derived variables
    ##############################################################
    if mode == "test":
        in_rasters_list = raster_list
    else:
        rasters_list = generate_raster_list(in_workspace)
        in_rasters_list = filter_monthly(rasters_list)

    # Determine the length of the list of rasters
    len_raster_list = len(in_rasters_list)

    # Check to see that there is at least one raster to process
    if len_raster_list <= 0:
        print("There are no rasters to process in the locations provided.")
        sys.exit(0)

    print("There are {0} rasters to process.".format(len_raster_list))

    if write_to_file_flag:

        message = "Writing rasters to {0} only.".format(
            os.path.join(out_workspace, out_raster_list_file_name)
        )

        print(message)

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
        # Process Raster Data (multiprocessing)
        #################################################################

        print("Clipping Rasters")

        # zip input data for Clip function into a tuple of tuples for each raster being processed
        aoi_feature_list = [aoi_feature for i in range(len_raster_list)]
        clipped_dir_list = [clipped_dir for i in range(len_raster_list)]
        in_clip_raster_data = tuple(
            zip(in_rasters_list, aoi_feature_list, clipped_dir_list)
        )

        # clip rasters using multiprocessing
        clip_raster_list = multi_process(clip_raster_multi, in_clip_raster_data)

        print("Converting Clipped Rasters to Points")

        # zip input data for Raster to Points function into a tuple of tuples for each raster being processed
        points_dir_list = [points_dir for i in range(len(clip_raster_list))]
        in_raster_to_points_data = tuple(zip(clip_raster_list, points_dir_list))

        # convert rasters to points using multiprocessing
        point_feature_list = multi_process(
            convert_raster_to_points_multi, in_raster_to_points_data
        )

        print("Creating Fishnets for Rasters")

        # zip input data for fishnet conversion into a tuple of tuples for each raster being processed
        fishnet_dir_list = [fishnet_dir for i in range(len(clip_raster_list))]
        in_fishnet_data = tuple(zip(clip_raster_list, fishnet_dir_list))

        # create fishnet using multiprocessing
        fishnet_list = multi_process(create_fishnet_feature_multi, in_fishnet_data)

        print("Converting Fishnets to Polygon Features")

        # zip input data for fishnet to polygon into a tuple of tuples for each fishnet being converted to a polygon
        polygon_dir_list = [polygon_dir for i in range(len(fishnet_list))]
        in_fishnet_to_polygon_data = tuple(
            zip(sorted(fishnet_list), sorted(point_feature_list), polygon_dir_list)
        )

        # convert fishnets to polygons using multiprocessing
        polygon_features = multi_process(
            convert_fishnet_to_polygon_multi, in_fishnet_to_polygon_data
        )

        print("Intersecting AOI with Vectorized Raster Polygons")

        # zip input data for intersecting vectorized rasters and the area of interest polygon feature class
        intersect_dir_list = [intersect_dir for i in range(len(polygon_features))]
        intersect_data = tuple(
            zip(aoi_feature_list, polygon_features, intersect_dir_list)
        )

        # intersect features using multiprocessing
        intersect_feature_list = multi_process(intersect_features_multi, intersect_data)

        #################################################################
        # Generate IWFM Input File (sequential, not multiprocessing)
        #################################################################

        # prepare features for writing to file
        output_features = order_files_by_date(intersect_feature_list)

        # Create output file
        out_file = os.path.join(out_workspace, out_file_name)

        version = "{} - {}".format(model_version, datetime.date.today())

        # Write output file
        with open(out_file, "w") as f:
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
                d,
            )
            write_precip_specs(
                f,
                feature_count,
                length_unit_conversion_factor(out_units, "feet"),
                n_ts_update,
                ts_frequency,
                dss_file,
            )
            for dt in output_features["TextDate"].to_list():
                # get name of feature class
                fc = output_features[output_features["TextDate"] == dt][
                    "FileNames"
                ].to_numpy()[0]

                # convert feature class table to array for processing
                values = area_weight_values_from_feature_class(
                    fc, in_units, out_units, aoi_id_field, "grid_code", "SHAPE@AREA"
                )

                if len(values) == feature_count:
                    f.write(dt)
                    f.write(("{:>10.3}" * len(values)).format(*values))
                    f.write("\n")
                else:
                    raise arcpy.ExecuteError

    print("Processing Complete!")

    timer.stop()
    timer.print_run_time()
