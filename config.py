from typing import NamedTuple


''' Configuration script that provides the variables that is used by the script in order to make refactoring and
reconfiguration easier.
'''

class system_config(NamedTuple):
    compression = False
    save_location = None
    h3_coarse_resolution = 5
    h3_fine_resolution = 10