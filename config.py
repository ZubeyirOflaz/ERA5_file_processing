from typing import NamedTuple


''' Configuration script that provides the variables that is used by the script in order to make refactoring and
reconfiguration easier.
'''

class system_config(NamedTuple):
    #Compression method for the final parquet file-
    compression = 'snappy'

    #Configures whether the data will be saved locally ('local') or to a AWS S3 bucket ('s_3')
    save_location = 'local'

    # Different h3 resolutions that will be appended
    h3_coarse_resolution = 5
    h3_fine_resolution = 10

    # Maximum time chunk that is processed at a time (in hours)
    processing_interval = 48

    # Configures whether the data will be saved as a whole or filtered
    save_only_filtered_data = True

    # Configuration variables for file upload to S3 that enable concurrent upload if the file is larger than specified
    # size

    concurrent_transfer_threshold = 0.1 * (1024 ** 3) # Concurrent transfer enabled for files larger than 100MBs
    max_concurrency = 2
