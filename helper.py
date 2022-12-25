''' This script provides the helper functions used by converter and API access point.'''
from config import system_config
import s3fs
import xarray
import h3.api.numpy_int as h3


def logging():
    # TODO: implement logging for the system
    pass


def retrieve_netcdf(file_path):
    fs_s3 = s3fs.S3FileSystem(anon=True)
    remote_file_obj = fs_s3.open(file_path, mode="rb")
    return xarray.open_dataset(remote_file_obj, engine='h5netcdf')

def apply_h3_array(latitude, longitude):
    resolution = system_config.h3_save_resolution
    h3_indexes = [h3.geo_to_h3(lat, lon, resolution) for lat, lon in zip(latitude, longitude)]
    return h3_indexes


def return_h3_cells(h3_index):
    # This function gets a h3 cell index and returns the associated h3 cells in the database
    if h3.h3_get_resolution(h3_index) < system_config.h3_save_resolution:
        # if h3 index requested is bigger than saved resolution format, retun the associated children
        children_cells = h3.h3_to_children(h3_index, system_config.h3_save_resolution)
        return children_cells
    else:
        # if h3 index requested is smaller than the saved resolution, return the parent cell
        parent_cell = h3.h3_to_parent(h3_index, system_config.h3_save_resolution)
        return parent_cell


def filter_by_time():
    # TODO: Add functionality to enable filtering by timestamp
    pass



