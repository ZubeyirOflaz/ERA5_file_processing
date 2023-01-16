from typing import NamedTuple


''' Configuration script that provides the variables that is used by the script in order to make refactoring and
reconfiguration easier.
'''

class system_config(NamedTuple):
    #Compression method for the final parquet file-
    compression = 'snappy'

    # Configures whether the data will be saved locally ('local'), to both AWS S3 bucket and local ('both') or
    # only to an s3 bucket ('s3')
    save_location = 'local'

    # Maximum number of retries and wait time between retries during the operations when an error is encountered
    max_retries = 5
    wait_time = 15

    # Different h3 resolutions that will be appended
    h3_coarse_resolution = 5
    h3_fine_resolution = 10

    # Maximum time chunk that is processed during partial download a time (in hours)
    processing_interval = 12

    # Configures whether the data will be saved as a whole or filtered
    save_only_filtered_data = True

    # Configuration variables for file upload to S3 that enable concurrent upload if the file is larger than specified
    # size

    concurrent_transfer_threshold = 0.1 * (1024 ** 3) # Concurrent transfer enabled for files larger than 100MBs
    max_concurrency = 2
