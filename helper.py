''' This script provides the helper functions used by converter and API access point.'''
from config import system_config
import s3fs
import xarray
import netCDF4
import h3


def logging():
    # TODO: implement logging for the system
    pass


def retrieve_netcdf(file_path):
    fs_s3 = s3fs.S3FileSystem(anon=True)
    remote_file_obj = fs_s3.open(file_path, mode="rb")
    # If the file is not going to be recorded, return only the file read from the system
    if system_config.save_location is None:
        return xarray.open_dataset(remote_file_obj, engine='h5netcdf')
    elif system_config.save_location == 'local':
        file = xarray.open_dataset(remote_file_obj, engine='h5netcdf')
        file.to_netcdf('netcdf_file.nc', 'w')
        return file
    else:
        print('unrecognized save_location parameter, returning None')
        return None

    pass


def apply_h3_array(latitude, longitude, resolution):
    h3_indexes = [h3.geo_to_h3(lat, lon, resolution) for lat, lon in zip(latitude, longitude)]
    # TODO: Add function to enable filtering by the hierarchical geospatial index
    pass


def filter_by_time():
    # TODO: Add functionality to enable filtering by timestamp
    pass



