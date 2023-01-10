''' This script provides the helper functions used by converter and API access point.'''
from config import system_config
import s3fs
import xarray
import h3.api.numpy_int as h3
from datetime import datetime, timedelta


def main(bucket_path, time_interval=(None, None), latitude_range=None, longitude_range=None):
    # Parquet file name (same name with original netCDF file)
    file_name = bucket_path.split('/')[-1].split('.')[0]
    ncdf = retrieve_netcdf(bucket_path)
    times = ncdf['time1'].values
    # Check if filters for time or location has been set
    filter_provided = time_interval.count(None) < 2 or latitude_range != None < 2 or longitude_range != None

    if system_config.save_only_filtered_data and filter_provided:
        filter = {}
        # Add coordinate filters if exists
        if latitude_range is not None:
            filter.update({'lat': slice(max(latitude_range), min(latitude_range))})
        if longitude_range is not None:
            filter.update({'lon': slice(min(longitude_range), max(longitude_range))})
        ncdf = ncdf.sel(filter)
        # Break time interval into processing chunks which can be set from config file
        start = time_interval[0] if time_interval[0] else times[0]
        end = time_interval[1] if time_interval[1] else times[-1]
        processing_intervals = divide_time_period(start, end, system_config.processing_interval)
    else:
        processing_intervals = divide_time_period(times[0], times[-1], system_config.processing_interval)

    latitudes = ncdf['lat'].values
    longitudes = ncdf['lon'].values
    num_coordinate_points = len(latitudes) * len(longitudes)
    coarse_h3, fine_h3 = apply_h3_array(latitudes, longitudes)

    for interval in processing_intervals:
        pass


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


