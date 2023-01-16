'''Master script that can be used in order to retrieve the NetCDF files, and filter the data using both local filtering
as well as AWS S3 Select filtering
'''

import helper as hl

from datetime import datetime
from h3.api.numpy_int import geo_to_h3
# NetCDF file url
file_url = 's3://era5-pds/2022/05/data/precipitation_amount_1hour_Accumulation.nc'

# Time interval that will be used for filtering
time_interval = (datetime(2022, 5, 1), datetime(2022, 5, 30))

# Coordinate range that will be used for filtering. Currently set to cover Europe
latitude_range = (35, 72)
longitude_range = (-25, 45)

# Query filters that will be used on data after it is downloaded
query_from_date = datetime(2022,5,1)
query_to_date = datetime(2022,5,25)

query_location = geo_to_h3(49.946199, 11.581078, 4)



if __name__ == "__main__":
    # Function to convert the NetCDF file to a Parquet file and store the contents locally and/or on an S3 bucket
    file_name = hl.convert_store_data(file_url, time_interval, latitude_range, longitude_range, verbose=True)
    print(f'The parquet file creation is completed. The file is saved with the following name: {file_name}')
    # Query to local parquet file
    filtered_table = hl.query_local(file_name,
                                    query_from_date, query_to_date, query_location)
    # Query the S3 bucket. Please provide bucket name and required credentials before testing
    credentials_provided = False
    if credentials_provided:
        bucket_name = ''
        filter_s3_select = hl.query_s3(bucket_name=bucket_name,filename=file_name,from_date=query_from_date,
                                       to_date=query_to_date,h3_cell_filter=query_location)
