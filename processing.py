import os
import re
import glob
import datetime

import pandas as pd
import multiprocessing as mp


def get_all_rasters_from_folders(in_workspace, raster_format=".bil"):
    """
    Generate a list of all rasters in subfolders of the parent directory.
    """
    list_raster = []
    for root, dirs, files in os.walk(in_workspace, topdown=False):
        for name in files:
            if name.endswith(raster_format):
                list_raster.append(os.path.join(root, name))

    return sorted(list_raster)


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


def multi_process(func, func_arg_list):
    """
    Wrapper function to create a processing pool and map a function to it.
    """
    pool = mp.Pool(processes=mp.cpu_count() - 1)
    result_list = pool.map(func, func_arg_list)
    pool.close()
    return result_list
