'''Master script that can be used in order to retrieve the NetCDF files, and filter the data using both local filtering
as well as AWS S3 Select filtering
'''

import helper as hl


# TODO: Create the main fuction to retrieve the data and convert it




if __name__ == "__main__" :
    test = hl.retrieve_netcdf('s3://era5-pds/2022/05/data/precipitation_amount_1hour_Accumulation.nc')
