''' This script provides the helper functions used by converter and API access point.'''
from config import system_config
import s3fs
import xarray
import h3.api.numpy_int as h3
from datetime import datetime, timedelta
import numpy as np
import pyarrow
import pyarrow.parquet as pq
import boto3
from boto3.s3.transfer import TransferConfig
import json
from typing import List
from io import StringIO
import pandas as pd
import logging
import os

logging.basicConfig(filename='system_logs.log', level=logging.DEBUG)


def convert_store_data(bucket_path, time_interval=(None, None), latitude_range=None,
                       longitude_range=None, verbose=False, upload_bucket_name: str = None):
    # Parquet file name (currently set to the same name with original netCDF file)
    file_name = bucket_path.split('/')[-1].split('.')[0]
    file_name += '.parquet'
    # Connecting to the NetCDF file
    retries = 0
    if verbose:
        print('Connecting to the NetCDF file')
    while retries < system_config.max_retries:
        logging.debug('Connecting to the NetCDF file')
        try:
            netcdf = retrieve_netcdf(bucket_path)
            break
        except Exception as e:
            if retries < system_config.max_retries:
                logging.warning(f'Error: {e} occured while connecting to the system. Will retry connecting '
                                f'in {system_config.wait_time} seconds')
            else:
                logging.error(f'Connection establishment failed with error: {e}. Please check for any issues in '
                              f'internet connection or the access to the S3 file')
                raise ConnectionError
    retries = 0
    times = netcdf['time1'].values
    # Check if filters for time or location has been set
    filter_provided = time_interval.count(None) < 2 or latitude_range is not None or longitude_range is not None
    netcdf_filter = {}
    if verbose:
        print('Filtering data')
    if system_config.save_only_filtered_data and filter_provided:
        # Add coordinate filters if exists
        if latitude_range is not None:
            netcdf_filter.update({'lat': slice(max(latitude_range), min(latitude_range))})
        if longitude_range is not None:
            netcdf_filter.update({'lon': slice(min(longitude_range), max(longitude_range))})
        netcdf = netcdf.sel(netcdf_filter)

        # Break time interval into processing chunks which can be set from config file
        start = time_interval[0] if time_interval[0] else times[0]
        end = time_interval[1] if time_interval[1] else times[-1]
        processing_intervals = list(divide_time_period(start, end, system_config.processing_interval))
    else:
        processing_intervals = list(divide_time_period(times[0], times[-1], system_config.processing_interval))

    # Get the relevant h3 coordinate values, and some helpful variables
    if verbose:
        'Creating h3 cell indices for the data'
    latitudes = netcdf['lat'].values
    longitudes = netcdf['lon'].values
    num_coordinate_points = len(latitudes) * len(longitudes)
    coarse_h3, fine_h3 = apply_h3_array(latitudes, longitudes)
    prev_num_observations = 0
    var_name = list(netcdf.keys())[1]

    for index, interval in enumerate(processing_intervals):
        if verbose:
            print(f'Retrieving data: {round((index / len(processing_intervals)) * 100, 2)}% complete')
        # Create current time filter, apply to the netcdf reader
        netcdf_filter.update({'time1': slice(interval[0], interval[1])})
        netcdf_time_filtered = netcdf.sel(netcdf_filter)
        while retries < system_config.max_retries:
            logging.debug('Accessing the partial data to be read')
            try:
                time_values = netcdf_time_filtered['time1'].values
                break
            except Exception as e:
                if retries < system_config.max_retries:
                    logging.warning(f'Error: {e} occured while retrieving data. Will retry connecting '
                                    f'in {system_config.wait_time} seconds')
                else:
                    logging.error(f'Connection establishment failed with error: {e}. Please check for any issues in '
                                  f'internet connection or the access to the S3 file')
                    raise ConnectionError
        retries = 0
        # Create the columnar arrays for the time and h3 values
        if verbose:
            print('Saving the latest downloaded partial data')
        num_observations = len(time_values)
        if num_observations != prev_num_observations:
            coarse_h3_values, fine_h3_values = coarse_h3 * num_observations, fine_h3 * num_observations
        time_values = np.repeat(time_values, num_coordinate_points)

        # Retrieve the data, create the pyarrow table
        observation_values = netcdf_time_filtered[var_name].values.flatten()
        table = pyarrow.Table.from_arrays([coarse_h3_values, fine_h3_values, time_values, observation_values],
                                          names=['h3_coarse_resolution', 'h3_fine_resolution',
                                                 'observation_time', 'precipitation'])

        # Create the Parquet writer, save the current batch of data
        if index == 0:
            py_writer = pq.ParquetWriter(f'{file_name}', table.schema, compression=system_config.compression)
        py_writer.write(table)
    if verbose:
        print('Download and conversion completed, closing the file')
    if py_writer:
        py_writer.close()
    if system_config.save_location in ['both', 's3']:
        upload_success = upload_to_bucket(upload_bucket_name, file_name)
        if not upload_success:
            logging.error('The system was unable to upload the file to the specified S3 bucket, Please ensure '
                          'stable internet connection and required access to the specified S3 bucket')
        else:
            logging.info('The upload of the file is complete')
        if system_config.save_location == 's3' and upload_success:
            os.remove(file_name)
            logging.info('The local file instance is removed')


    return file_name



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
    if not h3.h3_is_valid(h3_index):
        raise TypeError('h3 index is not valid, please make sure to provide a valid h3 index')
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
        parent_cell = h3.h3_to_parent(h3_index, system_config.h3_fine_resolution)
        return 'h3_fine', parent_cell


def divide_time_period(start, end, interval):
    interval = timedelta(hours=interval)
    current_time = start
    while current_time < end:
        if current_time + interval < end:
            yield [current_time, current_time + interval]
        else:
            yield [current_time, end]
        current_time += interval


# Upload parquet file to an S3 bucket, the credentials have to be provided to boto3 for this function to execute using
# one of the methods provided here https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html

def upload_to_bucket(bucket_name, filename):
    s3_session = boto3.client('s3')
    config = TransferConfig(multipart_threshold=system_config.concurrent_transfer_threshold,
                            max_concurrency=system_config.max_concurrency)

    try:
        s3_session.upload_file(filename, bucket_name, filename, Config=config)
    except Exception as e:
        print('system encountered an error uploading the file')
        print(e)
        raise ConnectionError
    return True


# Read the parquet file either from the local instance or from an S3 bucket
# Inputs either a filepath for the file or S3 file link, prioritizes the local instance if both are provided

# Query S3 file with time and/or h3 index using S3 SELECT
# Inputs a list of h3 cells for location filtering, and from/to datetime objects. Optionally a custom query can be
# provided as a string. The credentials have to be provided to boto3 for this function to execute using
# one of the methods provided here https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html
# Outputs the query result

def query_s3(bucket_name, filename, custom_query: str = None, from_date: datetime = None,
             to_date: datetime = None, h3_cell_filter: int = None, limit: int = 100):
    date_format = "%Y-%m-%dT%H:%M:%S.000Z"
    if custom_query is not None:
        s3_filter = custom_query
    else:
        s3_filter = ''
        # Appending time filters for the query if they are provided
        if from_date is not None and to_date is not None:
            s3_filter += f"WHERE CAST(s.observation_time as TIMESTAMP) BETWEEN CAST('{from_date.strftime(date_format)}'" \
                         f" as TIMESTAMP) AND CAST('{to_date.strftime(date_format)}' as TIMESTAMP)"
        elif from_date is not None:
            s3_filter += f"WHERE CAST(s.observation_time as TIMESTAMP) > CAST('{from_date.strftime(date_format)}'" \
                         f" as TIMESTAMP)"
        elif to_date is not None:
            s3_filter += f"WHERE CAST(s.observation_time as TIMESTAMP) < CAST('{to_date.strftime(date_format)}'" \
                         f" as TIMESTAMP)"
        # Appending location filter if it is provided
        if h3_cell_filter is not None:
            h3_resolution, relevant_h3_cells = return_h3_cells(h3_cell_filter)
            col_name = h3_resolution + '_resolution'
            h3_string = str(tuple(relevant_h3_cells))
            if len(s3_filter) == 0:
                s3_filter += ' WHERE'
            else:
                s3_filter += ' AND'
            s3_filter += f' s.{col_name} IN {h3_string} LIMIT {limit}'

    query = f'SELECT * FROM s3object s {s3_filter}'
    s3_query = boto3.client('s3')
    try:
        response = s3_query.select_object_content(
            Bucket=bucket_name,
            Key=filename,
            ExpressionType='SQL',
            Expression=query,
            InputSerialization={'Parquet': {}},
            OutputSerialization={'JSON': {}}, )
    except Exception as e:
        print('system encountered the following error while querying the results')
        print(e)
        raise ConnectionError
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        for event in response['Payload']:
            if 'Records' in event:
                data = event['Records']['Payload'].decode('utf-8')
                json_obj = json.loads('[{}]'.format(data.replace('\n', ',')[:-1]))
                # dataframe = pd.read_json(data, lines=True)
    else:
        status_code = response['ResponseMetadata']['HTTPStatusCode']
        raise ConnectionError(f'S3 Select returned with the following error code: {status_code}')

    return json.dumps(json_obj)


# Query local file with time and/or h3 index
# Inputs a list of h3 cells for location filtering, and from/to datetime objects
# Outputs the query result
def query_local(filename, from_date: datetime = None,
                to_date: datetime = None, h3_cell_filter: int = None):

    filter = []
    if from_date is not None:
        filter.append(('observation_time', '>', from_date))
    if to_date is not None:
        filter.append(('observation_time', '<', to_date))
    if h3_cell_filter is not None:
        h3_resolution, relevant_h3_cells = return_h3_cells(h3_cell_filter)
        col_name = h3_resolution + '_resolution'
        relevant_h3_cells = set(relevant_h3_cells)
        filter.append((col_name, 'in', relevant_h3_cells))
    table = pq.read_table(filename, filters=filter)
    return table
