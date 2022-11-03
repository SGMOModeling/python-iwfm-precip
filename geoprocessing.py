import arcpy
import os
import glob
import datetime
import re

import pandas as pd
import multiprocessing as mp

def generate_raster_list(in_workspace):
    """
    Generate a list of rasters from workspace

    A workspace can be one or more of the following:
    - geodatabase
    - directory (folder)
    - text file

    Parameters
    ----------
    in_workspace : str
        workspace containing raster files

    Returns
    -------
    list
        list of rasters in workspace
    """
    print(in_workspace)
    workspace_desc = arcpy.Describe(in_workspace)
    
    if workspace_desc.dataType == "Workspace":
        rasters_list = get_all_rasters_from_geodatabase(in_workspace)

    elif workspace_desc.dataType == "Folder":
        rasters_list = get_all_rasters_from_folders(in_workspace)
    
    elif workspace_desc.dataType == "TextFile":
        rasters_list = get_all_rasters_from_file(in_workspace)

    else:
        raise TypeError("in_workspace is not currently supported")

    return rasters_list
    

def get_all_rasters_from_folders(in_workspace, raster_format='.bil'):
    """
    Generate a list of all rasters in subfolders of the parent directory.
    """
    list_raster = []
    for root, dirs, files in os.walk(in_workspace, topdown=False):
        for name in files:
            if name.endswith(raster_format):
                list_raster.append(os.path.join(root, name))

    return sorted(list_raster)


def get_all_rasters_from_geodatabase(in_workspace):
    """
    Generate a list of all rasters in geodatabase.
    """
    arcpy.env.workspace = in_workspace
    list_rasters = arcpy.ListRasters("*", "All")

    return sorted(list_rasters)


def get_all_rasters_from_file(in_text_file):
    """
    Generate a list of rasters from a text file.
    """
    # open text file in read mode and read contents ignoring lines beginning with '#'
    with open(in_text_file, "r") as text_file:
        list_rasters = [str(line.strip()) for line in text_file if line[0] != "#"]

    return sorted(list_rasters)


def get_list_of_feature_classes(in_workspace):
    """
    Return a list of feature classes in the provided location.
    """
    list_feature_classes = glob.glob(os.path.join(in_workspace, "*.shp"))
    return sorted(list_feature_classes)


def last_day_of_month(in_date):
    """
    Generate a date for the last day of a given month.
    """
    next_month = in_date.replace(day=28) + datetime.timedelta(days=4)
    return next_month - datetime.timedelta(days=next_month.day)


def length_unit_conversion_factor(in_units, out_units):
    """
    Conversion factor between two different length units.
    """
    if in_units == "feet" and out_units == "feet":
        return 1.0
    if in_units == "feet" and out_units == "inches":
        return 12.0
    if in_units == "feet" and out_units == "meters":
        return 12.0 / 39.37
    if in_units == "feet" and out_units == "millimeters":
        return 12.0 / 39.37 * 1000.0
    if in_units == "inches" and out_units == "feet":
        return 1.0 / 12.0
    if in_units == "inches" and out_units == "inches":
        return 1.0
    if in_units == "inches" and out_units == "meters":
        return 1.0 / 39.37
    if in_units == "inches" and out_units == "millimeters":
        return 1.0 / 39.37 * 1000.0
    if in_units == "meters" and out_units == "feet":
        return 39.37 / 12
    if in_units == "meters" and out_units == "inches":
        return 39.37
    if in_units == "meters" and out_units == "meters":
        return 1.0
    if in_units == "meters" and out_units == "millimeters":
        return 1000.0
    if in_units == "millimeters" and out_units == "feet":
        return 0.001 * 39.37 / 12.0
    if in_units == "millimeters" and out_units == "inches":
        return 0.001 * 39.37
    if in_units == "millimeters" and out_units == "meters":
        return 0.001
    if in_units == "millimeters" and out_units == "millimeters":
        return 1.0


def FACTRN(out_units):
    if out_units == "feet":
        return 1.0
    if out_units == "inches":
        return 1.0 / 12.0
    if out_units == "meters":
        return 12.0 / 39.37
    if out_units == "millimeters":
        return 0.001 * 39.37 / 12.0


def write_rasters_to_file(in_rasters_list, out_workspace, out_file_name):
    """
    Write rasters from a list to a text file.
    """
    out_file = os.path.join(out_workspace, out_file_name)
    with open(out_file, "w") as f:
        f.write("# Rasters To Process\n")
        for raster in in_rasters_list:
            f.write("{}\n".format(raster))


def make_directory(dir_location, dir_name):
    """
    Create a folder of the desired name if it does not exist.
    """
    dir_path = os.path.join(dir_location, dir_name)

    if not os.path.isdir(dir_path):
        os.mkdir(dir_path)
        print("Folder '{0}' Created: {1}.".format(dir_name, dir_path))

    return dir_path


def get_properties_from_raster(in_raster):
    """
    Return raster properties from raster object.
    """
    # Instantiate a raster object for the raster to access its properties
    raster_dataset = arcpy.sa.Raster(in_raster)

    # Get properties from raster object
    in_raster_name = raster_dataset.name
    num_rows = raster_dataset.height
    num_cols = raster_dataset.width
    raster_extent = raster_dataset.extent
    x_min = raster_extent.XMin
    y_min = raster_extent.YMin
    x_max = raster_extent.XMax
    y_max = raster_extent.YMax

    del raster_dataset

    return in_raster_name, num_rows, num_cols, x_min, y_min, x_max, y_max


def clip_raster(in_raster, clip_feature, out_workspace):
    """
    Clip a raster to the geometry of the boundary of the feature class
    provided and return the name of the clipped raster.
    """

    # Use the input raster name to generate a default output name
    in_raster_name, in_raster_ext = os.path.splitext(os.path.basename(in_raster))

    out_raster_name = "{}_clip{}".format(
        in_raster_name, ".bil" if in_raster_ext == "" else in_raster_ext
    )
    out_clip_raster = os.path.join(out_workspace, out_raster_name)

    if not os.path.exists(out_clip_raster):
        # Clip raster to geometry of the feature class specified by the user
        arcpy.Clip_management(
            in_raster, "#", out_clip_raster, clip_feature, "#", "NONE"
        )

    return out_clip_raster


def clip_raster_multi(input_list):
    """
    Clip a raster to the geometry of the boundary of the feature class
    provided and return the name of the clipped raster.
    """
    # unpack list from function input
    in_raster, clip_feature, out_workspace = input_list

    return clip_raster(in_raster, clip_feature, out_workspace)


def convert_raster_to_points(in_raster, out_workspace):
    """
    Convert a raster to points and return the name of the point feature class.
    """
    # Use the input raster name to generate a default output name
    in_raster_name = os.path.splitext(os.path.basename(in_raster))[0]
    out_point_feature_name = "{0}.shp".format(in_raster_name)
    out_point_feature = os.path.join(out_workspace, out_point_feature_name)

    if not os.path.exists(out_point_feature):
        # Convert raster to point feature class
        arcpy.RasterToPoint_conversion(in_raster, out_point_feature, "Value")

    return out_point_feature


def convert_raster_to_points_multi(input_data_list):
    """
    Convert a raster to points and return the name of the point feature class.
    """
    # Unpack list from function input
    in_raster, out_workspace = input_data_list

    return convert_raster_to_points(in_raster, out_workspace)


def create_fishnet_feature(in_raster, out_workspace):
    """
    Create a polyline fishnet feature class and returns it's name
    """
    # Get properties from raster dataset for use in generating fishnet
    (
        in_raster_name,
        num_rows,
        num_cols,
        x_min,
        y_min,
        x_max,
        y_max,
    ) = get_properties_from_raster(in_raster)

    # Use the input raster name to generate a default output name for the fishnet feature class
    in_raster_base_name = os.path.splitext(in_raster_name)[0]
    if "clip" in in_raster_base_name:
        out_fishnet_feature_name = "{0}.shp".format(
            in_raster_base_name.replace("clip", "fishnet")
        )
    else:
        out_fishnet_feature_name = "{0}_fishnet.shp".format(in_raster_base_name)

    out_fishnet_feature = os.path.join(out_workspace, out_fishnet_feature_name)

    # Set origin and rotation of fishnet using extent properties
    # rotation is set by the difference in the x-values in the origin
    # coordinate and the y-axis coordinate
    origin_coordinate = "{0} {1}".format(x_min, y_min)
    y_axis_coordinate = "{0} {1}".format(x_min, y_max)

    if not os.path.exists(out_fishnet_feature):
        # Create fishnet using clipped rasters
        arcpy.CreateFishnet_management(
            out_fishnet_feature,
            origin_coordinate,
            y_axis_coordinate,
            0,
            0,
            num_rows,
            num_cols,
            "#",
            "NO_LABELS",
            in_raster,
            "POLYLINE",
        )

    return out_fishnet_feature


def create_fishnet_feature_multi(input_data_list):
    """
    Create a polyline fishnet feature class and returns it's name.
    """
    # unpack list from function input
    in_raster, out_workspace = input_data_list

    return create_fishnet_feature(in_raster, out_workspace)


def convert_fishnet_to_polygon(in_fishnet_feature, in_point_feature, out_workspace):
    """
    Convert the fishnet polyline features to polygons and assign
    values from the point feature class to the polygons as attributes.
    """

    # Define name and location of output polygon feature class
    in_fishnet_name = os.path.basename(in_fishnet_feature)
    out_feature_name = in_fishnet_name.replace("_fishnet", "")
    out_polygon_feature = os.path.join(out_workspace, out_feature_name)

    if not os.path.exists(out_polygon_feature):
        # Convert Features to Polygons
        arcpy.FeatureToPolygon_management(
            in_fishnet_feature, out_polygon_feature, "#", "ATTRIBUTES", in_point_feature
        )

    return out_polygon_feature


def convert_fishnet_to_polygon_multi(input_list):
    """
    Convert the fishnet polyline features to polygons and assign
    values from the point feature class to the polygons as attributes.
    """
    # unpack list of input variables
    in_fishnet_feature, in_point_feature, out_workspace = input_list

    return convert_fishnet_to_polygon(
        in_fishnet_feature, in_point_feature, out_workspace
    )


def intersect_features(reference_feature_class, target_feature_class, out_workspace):
    """
    Generate a new feature class that is the result of intersecting two input feature classes.
    """

    # Define name and location of output intersection feature class
    in_feature_class_name, in_feature_class_ext = os.path.splitext(
        os.path.basename(target_feature_class)
    )
    out_intersect_feature_name = "{0}_intersect{1}".format(
        in_feature_class_name, in_feature_class_ext
    )
    out_intersect_feature = os.path.join(out_workspace, out_intersect_feature_name)

    # list features to intersect for input to Intersect tool
    intersect_features = [reference_feature_class, target_feature_class]

    if not os.path.exists(out_intersect_feature):
        # Intersect Features
        arcpy.Intersect_analysis(intersect_features, out_intersect_feature)

    return out_intersect_feature


def intersect_features_multi(input_list):
    """
    Generate a new feature class that is the result of intersecting two input feature classes.
    """
    # unpack list of input variables
    reference_feature_class, target_feature_class, out_workspace = input_list

    return intersect_features(
        reference_feature_class, target_feature_class, out_workspace
    )

def process_raster(in_raster, aoi_feature, clip_dir, point_dir, fishnet_dir, polygon_dir, intersect_dir):
    """
    Processes a raster dataset to a vector polygon and intersects it with the area of interest feature class

    Parameters
    ----------
    in_raster : str
        path and name of raster dataset

    aoi_feature : str
        path and name of area of interest feature class

    clip_dir : str
        output directory for clipped rasters
    
    point_dir : str
        output directory for raster centroid points feature class

    fishnet_dir : str
        output directory for fishnet feature class

    polygon_dir : str
        output directory for polygon feature class

    intersect_dir : str
        output directory for intersect feature class

    Returns
    -------
    str
        path and name of intersect feature class
    """
    # clip raster
    print("Clipping {}".format(in_raster))
    clipped_raster = clip_raster(in_raster, aoi_feature, clip_dir)

    # raster to points
    print("Converting {} to points".format(clipped_raster))
    points_feature = convert_raster_to_points(clipped_raster, point_dir)

    # raster to fishnet
    print("Converting {} to fishnet".format(clipped_raster))
    fishnet_feature = create_fishnet_feature(clipped_raster, fishnet_dir)
        
    # fishnet to polygons
    print("Converting {} to Polygon".format(fishnet_feature))
    polygon_feature = convert_fishnet_to_polygon(fishnet_feature, points_feature, polygon_dir)
      
    # intersect features
    print("Intersecting {} with {}".format(polygon_feature, aoi_feature))
    intersect_feature = intersect_features(polygon_feature, aoi_feature, intersect_dir)

    return intersect_feature


def multi_process(func, func_arg_list):
    """
    Wrapper function to create a processing pool and map a function to it.
    """
    pool = mp.Pool(processes=mp.cpu_count() - 1)
    result_list = pool.map(func, func_arg_list)
    pool.close()
    return result_list


def parse_date_from_file_name(in_file_name):
    """
    Parse dates of various types from a file name.

    Parameters
    ----------
    inFileName : str
        file name containing a date to parse

    Returns
    -------
    datetime.datetime
        datetime object
    """
    # regex expression for YYYYMM
    expr1 = re.compile(
        r"19\d{2}0?[1-9](?!\d+)"
        r"|19\d{2}1?[0-2](?!\d+)"
        r"|20\d{2}0?[1-9](?!\d+)"
        r"|20\d{2}1[0-2](?!\d+)"
    )

    match1 = re.search(expr1, in_file_name)

    # regex expression for YYYY_MM
    expr2 = re.compile(
        r"19\d{2}_0?[1-9](?!\d+)"
        r"|19\d{2}_1[0-2](?!\d+)"
        r"|20\d{2}_0?[1-9](?!\d+)"
        r"|20\d{2}_1[0-2](?!\d+)"
    )

    match2 = re.search(expr2, in_file_name)

    # regex expression for YYYYmon
    expr3 = re.compile(r"19\d{2}[a-zA-Z]{3}" r"|20\d{2}[a-zA-Z]{3}")

    match3 = re.search(expr3, in_file_name)

    if match1:
        date_string = match1.group()
        file_date = datetime.datetime.strptime(date_string, "%Y%m")

    elif match2:
        date_string = match2.group()
        file_date = datetime.datetime.strptime(date_string, "%Y_%m")

    elif match3:
        date_string = match3.group()
        file_date = datetime.datetime.strptime(date_string, "%Y%b")

    return file_date


def format_IWFM_date(file_date, fmt="%m/%d/%Y_24:00"):
    """
    Format a datetime object to an end of month string.

    Parameters
    ----------
    file_date : datetime.datetime
        date to format

    fmt : str
        python datetime format

    Returns
    -------
    str
        string formatted date following the format given by fmt
    """
    model_date = datetime.datetime.strftime(last_day_of_month(file_date), fmt)

    return model_date


def order_files_by_date(features):
    """
    Organize and sort filenames by date.

    Parameters
    ----------
    features : list
        list of filenames needing to be sorted by date

    Returns
    -------
    pd.DataFrame
        pandas DataFrame object containing filenames, dates, and formatted text dates
    """
    df = pd.DataFrame(data=features, columns=["FileNames"])
    df["Date"] = df.apply(
        lambda row: parse_date_from_file_name(row["FileNames"]), axis=1
    )
    df.sort_values(by="Date", inplace=True)
    df["TextDate"] = df.apply(
        lambda row: last_day_of_month(row["Date"]).strftime("%m/%d/%Y_24:00"), axis=1
    )

    return df


def area_weight_values_from_feature_class(
    in_feature,
    in_value_units,
    out_value_units,
    id_field,
    value_field="grid_code",
    area_field="SHAPE@AREA",
):
    """
    Perform area weighting on value field and groups to a unique Identifier.

    Parameters
    ----------
    in_feature : str
        path and name of feature class

    in_value_units : str
        units of value field in feature class

    out_value_units : str
        output units for area-weighted values

    id_field : str
        field name in feature class containing IDs

    value_field : str default="grid_code"
        field name in feature class containing values to area-weight

    area_field : str default="SHAPE@AREA"
        field name in feature class containing area

    Returns
    -------
    list
        list of area-weighted values by the id_field
    """
    # retrieve array of data from feature class
    arr = arcpy.da.FeatureClassToNumPyArray(
        in_feature, [id_field, value_field, area_field]
    )

    # convert feature class array to DataFrame
    df = pd.DataFrame(arr)

    # add the total area of each field ID
    df = df.join(
        df.groupby(id_field)[area_field].sum(), on=id_field, rsuffix="_total"
    )

    # calculate the area-weighted value of each part in the output units
    df["WeightedGridCode"] = (
        df[area_field]
        / df["SHAPE@AREA_total"]
        * df[value_field]
        * length_unit_conversion_factor(in_value_units, out_value_units)
    )

    # sum all parts by the ID
    area_weighted_values = df.groupby(id_field)["WeightedGridCode"].sum()
    
    # convert resulting series to a list
    weighted_values = area_weighted_values.tolist()

    return weighted_values