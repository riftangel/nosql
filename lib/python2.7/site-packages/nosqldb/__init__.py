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

"""
All of the classes, constants, and exceptions that are part of the public
interface of the Python driver for Oracle NoSQL Database are included here so
they can be imported by applications without knowledg of the internal modules
implementing the interfaces.
"""
import logging
import os

from config import ProxyConfig
from config import StoreConfig
from nosqldb import Consistency
from nosqldb import Direction
from nosqldb import Durability
from nosqldb import ExecutionFuture
from nosqldb import Factory
from nosqldb import FieldRange
from nosqldb import MultiRowOptions
from nosqldb import Operation
from nosqldb import OperationResult
from nosqldb import OperationType
from nosqldb import ReadOptions
from nosqldb import Result
from nosqldb import ResultIterator
from nosqldb import Row
from nosqldb import SimpleConsistency
from nosqldb import StatementResult
from nosqldb import Store
from nosqldb import TableIteratorOptions
from nosqldb import TimeConsistency
from nosqldb import TimeToLive
from nosqldb import TimeUnit
from nosqldb import VersionConsistency
from nosqldb import WriteOptions

# Exceptions
from nosqldb import CancellationException
from nosqldb import ConnectionException
from nosqldb import ConsistencyException
from nosqldb import DurabilityException
from nosqldb import ExecutionException
from nosqldb import FaultException
from nosqldb import IllegalArgumentException
from nosqldb import InterruptionException
from nosqldb import OperationExecutionException
from nosqldb import ProxyException
from nosqldb import RequestTimeoutException
from nosqldb import TimeoutException
from nosqldb import UnsupportedOperationException

# Constants used as part of the API
from nosqldb import ABSOLUTE
from nosqldb import COMMIT_NO_SYNC
from nosqldb import COMMIT_SYNC
from nosqldb import COMMIT_WRITE_NO_SYNC
from nosqldb import NONE_REQUIRED
from nosqldb import NONE_REQUIRED_NO_MASTER
from nosqldb import ONDB_JAVA_CLIENT_ID
from nosqldb import ONDB_PROXY_SERVER_ID
from nosqldb import ONDB_TTL_DO_NOT_EXPIRE
from utilities import LOG_LOCATION
from utilities import MAX_AMOUNT_OF_LOG_FILES
from utilities import MAX_SIZE_BEFORE_ROTATION
from utilities import ONDB_ABORT_IF_UNSUCCESSFUL
from utilities import ONDB_ABSOLUTE
from utilities import ONDB_AP_ALL
from utilities import ONDB_AP_NONE
from utilities import ONDB_AP_SIMPLE_MAJORITY
from utilities import ONDB_CONSISTENCY
from utilities import ONDB_CURRENT_ROW_VERSION
from utilities import ONDB_DAYS
from utilities import ONDB_DELETE
from utilities import ONDB_DELETE_IF_VERSION
from utilities import ONDB_DIRECTION
from utilities import ONDB_DURABILITY
from utilities import ONDB_END_INCLUSIVE
from utilities import ONDB_END_VALUE
from utilities import ONDB_ERROR_MESSAGE
from utilities import ONDB_EXECUTION_ID
from utilities import ONDB_FIELD
from utilities import ONDB_FIELD_RANGE
from utilities import ONDB_FORWARD
from utilities import ONDB_HOURS
from utilities import ONDB_IF_ABSENT
from utilities import ONDB_IF_PRESENT
from utilities import ONDB_IF_VERSION
from utilities import ONDB_INCLUDED_TABLES
from utilities import ONDB_INFO
from utilities import ONDB_INFO_AS_JSON
from utilities import ONDB_IS_CANCELLED
from utilities import ONDB_IS_DONE
from utilities import ONDB_IS_SUCCESSFUL
from utilities import ONDB_MASTER_SYNC
from utilities import ONDB_MAX_RESULTS
from utilities import ONDB_NONE_REQUIRED
from utilities import ONDB_NONE_REQUIRED_NO_MASTER
from utilities import ONDB_OPERATION
from utilities import ONDB_OPERATION_TYPE
from utilities import ONDB_PERMISSIBLE_LAG
from utilities import ONDB_PREVIOUS_ROW
from utilities import ONDB_PREVIOUS_ROW_VERSION
from utilities import ONDB_PUT
from utilities import ONDB_PUT_IF_ABSENT
from utilities import ONDB_PUT_IF_PRESENT
from utilities import ONDB_PUT_IF_VERSION
from utilities import ONDB_RC_ALL
from utilities import ONDB_RC_NONE
from utilities import ONDB_RC_VALUE
from utilities import ONDB_RC_VERSION
from utilities import ONDB_READ_OPTIONS
from utilities import ONDB_REPLICA_ACK
from utilities import ONDB_REPLICA_SYNC
from utilities import ONDB_RESULT
from utilities import ONDB_RETURN_CHOICE
from utilities import ONDB_REVERSE
from utilities import ONDB_ROW
from utilities import ONDB_SIMPLE_CONSISTENCY
from utilities import ONDB_SP_NO_SYNC
from utilities import ONDB_SP_SYNC
from utilities import ONDB_SP_WRITE_NO_SYNC
from utilities import ONDB_START_INCLUSIVE
from utilities import ONDB_START_VALUE
from utilities import ONDB_STATEMENT
from utilities import ONDB_TABLE_NAME
from utilities import ONDB_TIMEOUT
from utilities import ONDB_TIMEUNIT
from utilities import ONDB_TIME_CONSISTENCY
from utilities import ONDB_TTL_TIMEUNIT
from utilities import ONDB_TTL_VALUE
from utilities import ONDB_TYPE_STRING
from utilities import ONDB_UNORDERED
from utilities import ONDB_UPDATE_TTL
from utilities import ONDB_VERSION
from utilities import ONDB_VERSION_CONSISTENCY
from utilities import ONDB_WAS_SUCCESSFUL

logger = logging.getLogger('nosqldb')
# create default empty handler for logs
logger.addHandler(logging.NullHandler())
