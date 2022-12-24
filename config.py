from typing import NamedTuple


''' Configuration script that provides the variables that is used by the script in order to make refactoring and
reconfiguration easier.
'''

class system_config(NamedTuple):
    compression = False
    save_location = None