"""Utilities module.

The utils_global module contains all global variables used in edna_utils, scada_utils,
 ami_utils, tickets_utils and utils modules.
"""

# Copyright (c) 2011-2015 AutoGrid Systems
# Author(s): 'Shailesh Birari' <shailesh.birari@auto-grid.com>

from pytz import timezone
import pytz

# Define PAM system
PAM = 'PAM'

# Define timezone
EASTERN = timezone('US/Eastern')
UTC = pytz.utc

# Define database types
REDIS_DB = 'REDIS'
MYSQL_DB = 'MySQL'

# Redis - number of values to extract from Redis Sets
VALUE_COUNT = 1000

# Define Redis namespace for SCADA, AMI, EDNA and TICKETS
SCADA_NS = 'feeders:scada'
AMI_NS = 'feeders:ami'
EDNA_NS = 'feeders:edna'
TICKETS_NS = 'feeders:tickets'
NS_SEPARATOR = ':'

# Seconds in an hour
SECONDS_IN_HOUR = 60 * 60.
