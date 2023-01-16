''' Script used to provide API access point to the converter to make the system scalable via Docker.
The script also provides additional checks and logging functionalities to monitoring.'''

from fastapi import FastAPI
from datetime import datetime
import helper as hl
import logging
app = FastAPI()


@app.get("/status")
async def check_server_status():
    return 200


@app.post("/ingest_era5_file/{s3_url}")
def convert_load_data(s3_url: str, from_date: str = None, to_date: str = None, latitude_min: float = None,
                      latitude_max: float = None, longitude_min: float = None, longitude_max: float = None):
    time_string_format = '%Y_%m_%d_%H'
    s3_url = s3_url.split('_')
    year = s3_url[0]
    month = s3_url[1]
    s3_url = f's3://era5-pds/{year}/{month}/data/precipitation_amount_1hour_Accumulation.nc'
    if from_date is not None:
        from_date = datetime.strptime(from_date, time_string_format)
    if to_date is not None:
        to_date = datetime.strptime(to_date, time_string_format)
    time_interval = (from_date, to_date)
    latitude = (latitude_min, latitude_max)
    longitude = (longitude_min, longitude_max)
    file = hl.convert_store_data(s3_url, time_interval=time_interval, latitude_range=latitude,
                                 longitude_range=longitude)
    return 200


@app.get("/query_local_data/{filename}")
def local_query(filename, from_date: str = None, to_date: str = None, h3_cell_filter: int = None):
    time_string_format = '%Y_%m_%d_%H'
    if from_date is not None:
        from_date = datetime.strptime(from_date, time_string_format)
    if to_date is not None:
        to_date = datetime.strptime(to_date, time_string_format)
    query_result = hl.query_local(filename=filename, from_date=from_date, to_date=to_date,
                                  h3_cell_filter=h3_cell_filter)
    query_result = query_result.to_pandas('query_result')
    query_result['observation_time'] = query_result['observation_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
    return query_result.to_json()


@app.get("/query_s3_bucket/{bucketname_filename}")
def query_s3_file(bucketname_filename:str, from_date: str = None, to_date: str = None, h3_cell_filter: int = None):
    time_string_format = '%Y_%m_%d_%H'
    bucketname_filename = bucketname_filename.split('-')
    bucketname = bucketname_filename[0]
    filename = bucketname_filename[1]

    if from_date is not None:
        from_date = datetime.strptime(from_date, time_string_format)
    if to_date is not None:
        to_date = datetime.strptime(to_date, time_string_format)
    logging.error('Works until here')
    query_result = hl.query_s3(bucket_name=bucketname,filename=filename, from_date=from_date,
                               to_date=to_date, h3_cell_filter=h3_cell_filter)

    return query_result

