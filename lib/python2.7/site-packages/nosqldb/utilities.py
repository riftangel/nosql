#
#  This file is part of Oracle NoSQL Database
#  Copyright (C) 2014, 2018 Oracle and/or its affiliates.  All rights reserved.
#
# If you have received this file as part of Oracle NoSQL Database the
# following applies to the work as a whole:
#
#   Oracle NoSQL Database server software is free software: you can
#   redistribute it and/or modify it under the terms of the GNU Affero
#   General Public License as published by the Free Software Foundation,
#   version 3.
#
#   Oracle NoSQL Database is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#   Affero General Public License for more details.
#
# If you have received this file as part of Oracle NoSQL Database Client or
# distributed separately the following applies:
#
#   Oracle NoSQL Database client software is free software: you can
#   redistribute it and/or modify it under the terms of the Apache License
#   as published by the Apache Software Foundation, version 2.0.
#
# You should have received a copy of the GNU Affero General Public License
# and/or the Apache License in the LICENSE file along with Oracle NoSQL
# Database client or server distribution.  If not, see
# <http://www.gnu.org/licenses/>
# or
# <http://www.apache.org/licenses/LICENSE-2.0>.
#
# An active Oracle commercial licensing agreement for this product supersedes
# these licenses and in such case the license notices, but not the copyright
# notice, may be removed by you in connection with your distribution that is
# in accordance with the commercial licensing terms.
#
# For more information please contact:
#
# berkeleydb-info_us@oracle.com
#
# This file contains a class needed to verify the existance of a couple of
# environment variables that contains location for two Jar files needed to
# start a new Proxy instance.
import os
# Exception moved here to be able to be used by nosqldb.py and config.py


class IllegalArgumentException(Exception):
    """
    Exception class that is used when an invalid argument was passed, this
    could mean that the type is not the expected or the value is not valid
    for the especific case.
    """
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)

class IllegalStateException(Exception):
    """
    Exception that is thrown when a method has been invoked at an illegal or
    inappropriate time.
    """
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)

# Set default log location as the location of the library
LOG_LOCATION = os.path.realpath(
    "{0}/../logs/nosqldb.log".format(os.path.dirname(__file__)))
_directory = os.path.dirname(LOG_LOCATION)
if (not os.path.exists(_directory)):
    try:
        os.makedirs(_directory)
    except Exception:
        # probably we have no permission to write here
        if (not os.path.isdir(_directory)):
            # if default is not possible use working directory
            LOG_LOCATION = os.path.realpath(
                "{0}/nosqldb.log".format(os.environ["PWD"]))
            _directory = os.path.dirname(LOG_LOCATION)
            if (not os.path.exists(_directory)):
                try:
                    os.makedirs(_directory)
                except Exception:
                    # if current directory is also forbiden
                    LOG_LOCATION = os.path.realpath(
                        "{0}/.nosqldb/logs/nosqldb.log".format(
                            os.environ["HOME"]))
                    _directory = os.path.dirname(LOG_LOCATION)
                    if (not os.path.exists(_directory)):
                        try:
                            os.makedirs(_directory)
                        except Exception:
                            # this was last option so rethrow the execption
                            raise

# Predefined constants for valid keys
ONDB_MASTER_SYNC = 'master_sync'
ONDB_REPLICA_SYNC = 'replica_sync'
ONDB_REPLICA_ACK = 'replica_ack'
ONDB_DURABILITY = 'durability'
ONDB_CONSISTENCY = 'consistency'
ONDB_SIMPLE_CONSISTENCY = 'simple'
ONDB_TIME_CONSISTENCY = 'time'
ONDB_VERSION_CONSISTENCY = 'version'
ONDB_PERMISSIBLE_LAG = 'lag'
ONDB_TIMEOUT = 'timeout'
ONDB_RETURN_CHOICE = 'return_choice'
ONDB_IF_ABSENT = 'if_absent'
ONDB_IF_PRESENT = 'if_present'
ONDB_IF_VERSION = 'if_version'
ONDB_FIELD = 'field'
ONDB_START_VALUE = 'start_value'
ONDB_END_VALUE = 'end_value'
ONDB_START_INCLUSIVE = 'start_inclusive'
ONDB_END_INCLUSIVE = 'end_inclusive'
ONDB_FIELD_RANGE = 'field_range'
ONDB_INCLUDED_TABLES = 'included_tables'
ONDB_DIRECTION = 'direction'
ONDB_MAX_RESULTS = 'max_results'
ONDB_READ_OPTIONS = 'read_options'
ONDB_OPERATION_TYPE = 'operation_type'
ONDB_TABLE_NAME = 'table_name'
ONDB_OPERATION = 'operation'
ONDB_ABORT_IF_UNSUCCESSFUL = 'abort_if_unsuccessful'
ONDB_ROW = 'row'
ONDB_VERSION = 'version'
ONDB_FORWARD = 'FORWARD'
ONDB_REVERSE = 'REVERSE'
ONDB_UNORDERED = 'UNORDERED'
ONDB_ABSOLUTE = 'ABSOLUTE'
ONDB_NONE_REQUIRED = 'NONE_REQUIRED'
ONDB_NONE_REQUIRED_NO_MASTER = 'NONE_REQUIRED_NO_MASTER'
ONDB_RC_NONE = 'NONE'
ONDB_RC_ALL = 'ALL'
ONDB_RC_VERSION = 'VERSION'
ONDB_RC_VALUE = 'VALUE'
ONDB_SP_NO_SYNC = 'NO_SYNC'
ONDB_SP_SYNC = 'SYNC'
ONDB_SP_WRITE_NO_SYNC = 'WRITE_NO_SYNC'
ONDB_AP_ALL = 'ALL'
ONDB_AP_NONE = 'NONE'
ONDB_AP_SIMPLE_MAJORITY = 'SIMPLE_MAJORITY'
ONDB_INFO = 'info'
ONDB_INFO_AS_JSON = 'info_as_json'
ONDB_ERROR_MESSAGE = 'error_message'
ONDB_IS_CANCELLED = 'is_cancelled'
ONDB_IS_DONE = 'is_done'
ONDB_IS_SUCCESSFUL = 'is_successful'
ONDB_STATEMENT = 'statement'
ONDB_RESULT = 'result'
ONDB_EXECUTION_ID = 'execution_id'
ONDB_CURRENT_ROW_VERSION = 'current_row_version'
ONDB_PREVIOUS_ROW = 'previous_row'
ONDB_PREVIOUS_ROW_VERSION = 'previous_row_version'
ONDB_WAS_SUCCESSFUL = 'was_successful'
ONDB_PUT = 'PUT'
ONDB_PUT_IF_ABSENT = 'PUT_IF_ABSENT'
ONDB_PUT_IF_PRESENT = 'PUT_IF_PRESENT'
ONDB_PUT_IF_VERSION = 'PUT_IF_VERSION'
ONDB_DELETE = 'DELETE'
ONDB_DELETE_IF_VERSION = 'DELETE_IF_VERSION'
ONDB_TYPE_STRING = 'STRING'
ONDB_PRIMARY_FIELD = 'primary'
ONDB_SECONDARY_FIELD = 'secondary'
ONDB_TIMEUNIT = 'timeunit'
ONDB_HOURS = 'HOURS'
ONDB_DAYS = 'DAYS'
ONDB_TTL_VALUE = 'ttl_value'
ONDB_TTL_TIMEUNIT = 'ttl_timeunit'
ONDB_UPDATE_TTL = 'update_ttl'
# needed for logging
MAX_SIZE_BEFORE_ROTATION = 1073741824
MAX_AMOUNT_OF_LOG_FILES = 50
# needed for long number checking
MAX_LONG_VALUE = 9223372036854775807
