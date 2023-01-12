''' This script provides the helper functions used by converter and API access point.'''
from config import system_config
import s3fs
import xarray
import h3.api.numpy_int as h3
from datetime import datetime, timedelta
import numpy as np
import pyarrow
import pyarrow.parquet as pq


def main(bucket_path, time_interval=(None, None), latitude_range=None, longitude_range=None):
    # Parquet file name (same name with original netCDF file)
    file_name = bucket_path.split('/')[-1].split('.')[0]
    ncdf = retrieve_netcdf(bucket_path)
    times = ncdf['time1'].values
    # Check if filters for time or location has been set
    filter_provided = time_interval.count(None) < 2 or latitude_range != None or longitude_range != None

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
        processing_intervals = list(divide_time_period(start, end, system_config.processing_interval))
    else:
        processing_intervals = list(divide_time_period(times[0], times[-1], system_config.processing_interval))

    # Get the relevant h3 coordinate values, and some helpful variables

    latitudes = ncdf['lat'].values
    longitudes = ncdf['lon'].values
    num_coordinate_points = len(latitudes) * len(longitudes)
    coarse_h3, fine_h3 = apply_h3_array(latitudes, longitudes)
    prev_num_observations = 0
    var_name = list(ncdf.keys())[1]

    for index, interval in enumerate(processing_intervals):
        # Create current time filter, apply to the netcdf reader
        filter.update({'time1': slice(interval[0], interval[1])})
        ncdf_time_filtered = ncdf.sel(filter)

        # Create the columnar arrays for the time and h3 values
        time_values = ncdf_time_filtered['time1'].values
        num_observations = len(time_values)
        if num_observations != prev_num_observations:
            coarse_h3_vals, fine_h3_vals = coarse_h3 * num_observations, fine_h3 * num_observations
        time_values = np.repeat(time_values, num_coordinate_points)

        # Retrieve the data, create the pyarrow table
        observation_values = ncdf_time_filtered[var_name].values.flatten()
        table = pyarrow.Table.from_arrays([coarse_h3_vals, fine_h3_vals, time_values, observation_values],
                                          names=['h3_coarse_resolution', 'h3_fine_resolution', 'time', 'precipitation'])

        # Create the Parquet writer, save the current batch of data
        if index == 0:
            pywriter = pq.ParquetWriter(f'{file_name}.parquet', table.schema, compression=system_config.compression)
        pywriter.write(table)
    if pywriter:
        pywriter.close()
    return ncdf_time_filtered


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

# Upload parquet file to an S3 bucket, the credentials have to be provided to boto3 for this function to execute using
# one of the methods provided here https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html


# Read the parquet file either from the local instance or from an S3 bucket
# Inputs either a filepath for the file or S3 file link, prioritizes the local instance if both are provided

# Query local file with time and/or h3 index
# Inputs a list of h3 cells for location filtering, and from/to datetime objects
# Outputs the query result

# Query S3 file with time and/or h3 index using S3 SELECT
# Inputs a list of h3 cells for location filtering, and from/to datetime objects. Optionally a custom query can be
# provided as a string the credentials have to be provided to boto3 for this function to execute using
# one of the methods provided here https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html
# Outputs the query result





