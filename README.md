<p align="center">
<img src="./title_image.png" alt="image" width="600">
</p>

<h1 align="center">
  ERA5 File Conversion and Query
  <br>
</h1>

ERA5 is the fifth generation of ECMWF atmospheric 
reanalyses of the global climate, and the first reanalysis produced as an operational service. 
It utilizes the best available observation data from satellites and in-situ stations, 
which are assimilated and processed using ECMWF's Integrated Forecast System (IFS) Cycle 41r2.

This project provides tools needed to filter provided in ERA5 repository and convert the NetCDF files to Apache Parquet files with h3 indexes as location markers, and query the data locally or using AWS S3 Select. It is designed to be potentially used as both a standalone tool and a worker node in a cluster. Because of this, the functions can be accessed directly, or via API calls.

The project can be used in two different ways: The conversion and filtering functions can be directly accessed via master.py, or the API interface can be started using the

``` uvicorn app:app --reload ```

command and API calls can be made to the IP address port mentioned in the terminal during startup (usually: http://127.0.0.1:8000). The available APIs and their parameters can be accessed with /redoc page on the server. Example API calls are also supplied in NetCDF to Parquet Requests.postman_collection.json Postman API call collection. The important configuration parameters of the project can be configured from config.py file

Saving the parquet file and the S3 Select Querying functionality requires the user to provide credentials for an IAM user that has required access to the S3 bucket that is attempted to be written to or queried. This [link](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html) provides information on how authentication can be done.

The environment for the project can be created by both using the requirements.txt file manually or creating a container using the Dockerfile in the project

