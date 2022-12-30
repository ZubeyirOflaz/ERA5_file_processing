''' This script provides the helper functions used by converter and API access point.'''
from config import system_config
import s3fs
import xarray
import h3.api.numpy_int as h3
from datetime import datetime, timedelta


def logging():
    # TODO: implement logging for the system
    pass


def retrieve_netcdf(file_path):
    fs_s3 = s3fs.S3FileSystem(anon=True)
    remote_file_obj = fs_s3.open(file_path, mode="rb")
    return xarray.open_dataset(remote_file_obj, engine='h5netcdf')


def pair_coordinates(lat, long):
    paired_list = [(elem1, elem2) for elem1 in lat for elem2 in long]
    return paired_list


def apply_h3_array(latitude, longitude):
    coordinates = [(lat, long) for lat in latitude for long in longitude]
    coarse_resolution = system_config.h3_coarse_resolution
    h3_coarse_indexes = [h3.geo_to_h3(i[0], i[1], coarse_resolution) for i in coordinates]
    fine_resolution = system_config.h3_fine_resolution
    h3_fine_indexes = [h3.geo_to_h3(i[0], i[1], fine_resolution) for i in coordinates]
    return h3_coarse_indexes, h3_fine_indexes


def return_h3_cells(h3_index):
    # This function gets a h3 cell and returns the associated h3 cells in the database
    # It gets an h3 index as an input and returns the h3 reolution level and associated cells from that level
    # as the output.

    # Check if resolution lower than saved coarse resolution
    if h3.h3_get_resolution(h3_index) <= system_config.h3_coarse_resolution:
        # if h3 index requested is bigger than saved resolution format, retun the associated children
        children_cells = h3.h3_to_children(h3_index, system_config.h3_coarse_resolution)
        return 'h3_coarse', children_cells
    # Check if resolution is between coarse and fine resolution levels
    elif h3.h3_get_resolution(h3_index) <= system_config.h3_fine_resolution:
        # if h3 index requested is bigger than saved resolution format, retun the associated children
        children_cells = h3.h3_to_children(h3_index, system_config.h3_fine_resolution)
        return 'h3_fine', children_cells
    else:
        # if h3 index requested is smaller than the saved fine resolution, return the parent cell
        parent_cell = h3.h3_to_parent(h3_index, system_config.h3_save_resolution)
        return 'h3_fine', parent_cell


def divide_time_period(start, end, interval):
    interval = timedelta(hours=interval)
    current_time = start
    while current_time < end:
        if current_time + interval < end:
            yield [current_time, current_time +interval]
        else:
            yield [current_time, end]
        current_time += interval


def filter_by_time(date_from, date_to):
    # TODO: Add functionality to enable filtering by timestamp
    pass
