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
# jsonutils.py
#     This file contains two classes:
#     RestrictedDict: This class is a base class for all the special dicts that
#         are used as parameter for the different API calls.
#     DataManager: This class is made to convert from the different dict, JSON
#     only and RestrictedDict based, to the native Thrift parameters to be sent
#     to the Proxy.

import base64
import json
import logging

from utilities import ONDB_ABORT_IF_UNSUCCESSFUL
from utilities import ONDB_ABSOLUTE
from utilities import ONDB_AP_ALL
from utilities import ONDB_AP_NONE
from utilities import ONDB_AP_SIMPLE_MAJORITY
from utilities import ONDB_CONSISTENCY
from utilities import ONDB_DAYS
from utilities import ONDB_DELETE
from utilities import ONDB_DELETE_IF_VERSION
from utilities import ONDB_DIRECTION
from utilities import ONDB_DURABILITY
from utilities import ONDB_END_INCLUSIVE
from utilities import ONDB_END_VALUE
from utilities import ONDB_FIELD
from utilities import ONDB_FIELD_RANGE
from utilities import ONDB_FORWARD
from utilities import ONDB_HOURS
from utilities import ONDB_INCLUDED_TABLES
from utilities import ONDB_MASTER_SYNC
from utilities import ONDB_MAX_RESULTS
from utilities import ONDB_NONE_REQUIRED
from utilities import ONDB_NONE_REQUIRED_NO_MASTER
from utilities import ONDB_OPERATION
from utilities import ONDB_OPERATION_TYPE
from utilities import ONDB_PERMISSIBLE_LAG
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
from utilities import ONDB_RETURN_CHOICE
from utilities import ONDB_REVERSE
from utilities import ONDB_ROW
from utilities import ONDB_SIMPLE_CONSISTENCY
from utilities import ONDB_SP_NO_SYNC
from utilities import ONDB_SP_SYNC
from utilities import ONDB_SP_WRITE_NO_SYNC
from utilities import ONDB_START_INCLUSIVE
from utilities import ONDB_START_VALUE
from utilities import ONDB_TABLE_NAME
from utilities import ONDB_TIMEOUT
from utilities import ONDB_TIMEUNIT
from utilities import ONDB_TIME_CONSISTENCY
from utilities import ONDB_TTL_TIMEUNIT
from utilities import ONDB_TTL_VALUE
from utilities import ONDB_UNORDERED
from utilities import ONDB_UPDATE_TTL
from utilities import ONDB_VERSION
from utilities import ONDB_VERSION_CONSISTENCY

from oracle.kv.proxy.gen.ttypes import TConsistency
from oracle.kv.proxy.gen.ttypes import TDirection
from oracle.kv.proxy.gen.ttypes import TDurability
from oracle.kv.proxy.gen.ttypes import TFieldRange
from oracle.kv.proxy.gen.ttypes import TOperation
from oracle.kv.proxy.gen.ttypes import TOperationType
from oracle.kv.proxy.gen.ttypes import TReadOptions
from oracle.kv.proxy.gen.ttypes import TReplicaAckPolicy
from oracle.kv.proxy.gen.ttypes import TReturnChoice
from oracle.kv.proxy.gen.ttypes import TRow
from oracle.kv.proxy.gen.ttypes import TSimpleConsistency
from oracle.kv.proxy.gen.ttypes import TSyncPolicy
from oracle.kv.proxy.gen.ttypes import TTimeConsistency
from oracle.kv.proxy.gen.ttypes import TTimeToLive
from oracle.kv.proxy.gen.ttypes import TTimeUnit
from oracle.kv.proxy.gen.ttypes import TVersionConsistency
from oracle.kv.proxy.gen.ttypes import TWriteOptions

logger = logging.getLogger('nosqldb')


class RestrictedDict(dict):
    """
    dict wrapper that restrict you to add only a predefined set
    of keys. This is for custom parameters that expect certain
    keys.
    """
    def __init__(self, allowed_keys, seq=(), **kwargs):
        """
        This is exactly the same as a regular dict init
        but it has a allowed_keys parameter that let you
        restrict the valid keys in this dict.
        """
        super(RestrictedDict, self).__init__()
        self._allowed_keys = tuple(allowed_keys)
        # normalize arguments to a (key, value) iterable
        if hasattr(seq, 'keys'):
            get = seq.__getitem__
            seq = ((k, get(k)) for k in seq.keys())
        if kwargs:
            from itertools import chain
            seq = chain(seq, kwargs.iteritems())
        # scan the items keeping track of the keys' order
        for k, v in seq:
            self.__setitem__(k, v)

    def __setitem__(self, key, value):
        """Checks if key is a valid one before setting it"""
        if key in self._allowed_keys:
            super(RestrictedDict, self).__setitem__(key, value)
        else:
            raise KeyError("%s is not allowed as key" % key)

    def update(self, e=None, **kwargs):
        """
        Same as regular dict update but using RestrictedDict.__setitem__()
        instead of regular dict.__setitem__()
        """
        try:
            for k in e:
                self.__setitem__(k, e[k])
        except AttributeError:
            for (k, v) in e:
                self.__setitem__(k, v)
        for k in kwargs:
            self.__setitem__(k, kwargs[k])

    def __eq__(self, other):
        """
        Add the comparison for allowed_keys in addition to the regular
        __eq__()
        """
        if other is None:
            return False
        try:
            allowedcmp = (self._allowed_keys == other._allowed_keys)
            if allowedcmp:
                return super(RestrictedDict, self).__eq__(other)
            else:
                return False
        except AttributeError:
            # other is not a RestrictedDict
            return False 

    def __ne__(self, other):
        """Equivalent to not __eq__() """
        return not self.__eq__(other)

    def print_allowed_keys(self):
        """Print the set of allowed keys"""
        return 'Allowed Keys(%s)' % (self._allowed_keys.__repr__())

    def validate_return_opts(self, ret_opts):
        if (ret_opts is not None):
            if((type(ret_opts) is str or type(ret_opts) is unicode) and
                (ret_opts == ONDB_RC_ALL or
                ret_opts == ONDB_RC_NONE or
                ret_opts == ONDB_RC_VALUE or
                ret_opts == ONDB_RC_VERSION)):
                    return True
            else:
                return False
        else:
            return True

    def validate(self):
        """This method should be filled in children"""
        pass


class DataManager:
    # This class is in charge of making the appropiate changes from JSON to
    # the native Thrift classes needed for the protocol.
    @staticmethod
    def dict_to_trow(in_dict):
        """
          Convert from a native python dict to a TRow.
          @param in_dict: the dictoinary to be converted to TRow.
          @return: a TRow with the desired information.
        """
        t_ttl = None
        if (in_dict is not None):
            res = json.dumps(in_dict)
            try:
                ttl = in_dict.get_timetolive()
                if (ttl is not None):
                    val = ttl.get(ONDB_TTL_VALUE)
                    timeunit = ttl.get(ONDB_TTL_TIMEUNIT)
                    t_timeunit = DataManager.from_json_to_ttimeunit(timeunit)
                    t_ttl = TTimeToLive(value=val, timeUnit=t_timeunit)
            except AttributeError:
                pass
        else:
            res = {}
        return TRow(jsonRow=res, ttl=t_ttl)

    @staticmethod
    def encode_binary(data):
        # Encode to Base64 the given data
        if (data is not None):
            return base64.b64encode(data)
        else:
            return None

    @staticmethod
    def decode_binary(data):
        # Decode from Base64 the given data
        if (data is not None):
            return base64.b64decode(data)
        else:
            return None

    @staticmethod
    def trow_to_dict(trow):
        # Convert back from a TRow to a dict with native python values.
        # @param twriteresult: the resulting data from the put operation.
        # @return: a dictionary with the data returned.
        if (trow is not None):
            res = json.loads(trow.jsonRow)
        else:
            res = None
        return res

    @staticmethod
    def from_json_to_tsync_policy(policy):
        # This method converts from synch policy to the Thrift equivalent
        if (policy is not None):
            if (policy == ONDB_SP_NO_SYNC):
                return TSyncPolicy.NO_SYNC
            elif (policy == ONDB_SP_SYNC):
                return TSyncPolicy.SYNC
            elif (policy == ONDB_SP_WRITE_NO_SYNC):
                return TSyncPolicy.WRITE_NO_SYNC
        return None

    @staticmethod
    def from_json_to_treplica_ack_policy(policy):
        # This method converts from replica ack policy to Thrift equivalent
        if (policy is not None):
            if (policy == ONDB_AP_ALL):
                return TReplicaAckPolicy.ALL
            elif (policy == ONDB_AP_NONE):
                return TReplicaAckPolicy.NONE
            elif (policy == ONDB_AP_SIMPLE_MAJORITY):
                return TReplicaAckPolicy.SIMPLE_MAJORITY
        return None

    @staticmethod
    def from_json_to_tdurability(durability):
        # This method converts from Durability to the Thrift equivalent
        master_sync_val = replica_sync_val = replica_ack_val = None
        if (durability is not None and isinstance(durability, dict)):
            master_sync_val = DataManager.from_json_to_tsync_policy(
                durability.get(ONDB_MASTER_SYNC, None))
            replica_sync_val = DataManager.from_json_to_tsync_policy(
                durability.get(ONDB_REPLICA_SYNC, None))
            replica_ack_val = DataManager.from_json_to_treplica_ack_policy(
                durability.get(ONDB_REPLICA_ACK, None))
        logger.debug("master_sync={0} replica_sync={1} replica_ack={2}".
            format(master_sync_val, replica_sync_val, replica_ack_val))
        # return something as long as there is relevant information to encode
        if (master_sync_val is None and
           replica_sync_val is None and
           replica_ack_val is None):
            return None
        else:
            return TDurability(masterSync=master_sync_val,
                               replicaSync=replica_sync_val,
                               replicaAck=replica_ack_val)

    @staticmethod
    def from_json_to_treturn_choice(return_opt):
        # This method converts from Return Choice to the Thrift equivalent
        if (return_opt is not None):
            if (return_opt == ONDB_RC_NONE):
                return TReturnChoice.NONE
            elif (return_opt == ONDB_RC_ALL):
                return TReturnChoice.ALL
            elif (return_opt == ONDB_RC_VALUE):
                return TReturnChoice.ONLY_VALUE
            elif (return_opt == ONDB_RC_VERSION):
                return TReturnChoice.ONLY_VERSION
        return None

    @staticmethod
    def from_json_to_twrite_options(write_opts):
        # This method converts from Write Options to the Thrift equivalent
        durability_val = timeout_val = return_opt_val = None
        if (write_opts is not None):
            # encode durability
            dur = write_opts.get(ONDB_DURABILITY, None)
            if (dur is not None):
                durability_val = DataManager.from_json_to_tdurability(dur)
            else:
                durability_val = None
            # encode timeout
            timeout_val = int(write_opts.get(ONDB_TIMEOUT, 0))
            # encode the Return Choice
            return_opt_val = DataManager.from_json_to_treturn_choice(
                write_opts.get(ONDB_RETURN_CHOICE, None))
            update_val = write_opts.get(ONDB_UPDATE_TTL)
        # return something as long as there is something relevant to encode
        if (durability_val is None and timeout_val is None and
            return_opt_val is None and update_val is None):
            return None
        else:
            return TWriteOptions(durability=durability_val,
                                 timeoutMs=timeout_val,
                                 returnChoice=return_opt_val,
                                 updateTTL=update_val)

    @staticmethod
    def from_json_to_tsimple_consistency(consistency):
        # This method converts from simple consistency to the Thrift equivalent
        if (consistency is not None):
            if (consistency == ONDB_NONE_REQUIRED):
                return TConsistency(simple=TSimpleConsistency.NONE_REQUIRED)
            elif (consistency == ONDB_ABSOLUTE):
                return TConsistency(simple=TSimpleConsistency.ABSOLUTE)
            elif (consistency == ONDB_NONE_REQUIRED_NO_MASTER):
                return TConsistency(
                    simple=TSimpleConsistency.NONE_REQUIRED_NO_MASTER)
        return None

    @staticmethod
    def from_json_to_ttime_consistency(t_consistency):
        # This method converts from TimeConsistency to Thrift equivalent
        if (t_consistency is not None):
            lag = t_consistency.get(ONDB_PERMISSIBLE_LAG, None)
            to = t_consistency.get(ONDB_TIMEOUT, None)
            return TConsistency(
                time=TTimeConsistency(permissibleLag=lag, timeoutMs=to))
        return None

    @staticmethod
    def from_json_to_tversion_consistency(v_consistency):
        # This method converts from VersionConsistency to Thrift equivalent
        if (v_consistency is not None):
            ver = v_consistency.get(ONDB_VERSION, None)
            to = v_consistency.get(ONDB_TIMEOUT, None)
            return TConsistency(
                version=TVersionConsistency(version=ver, timeoutMs=to))
        return None

    @staticmethod
    def from_json_to_tread_options(read_opts):
        # This method converts from Read Options to the Thrift equivalent
        consistency_val = timeout_val = None
        if (read_opts is not None and type(read_opts) is dict):
            # encode Consistency
            con = read_opts.get(ONDB_CONSISTENCY, None)
            if (con is not None):
                simple_con = con.get(ONDB_SIMPLE_CONSISTENCY, None)
                if (simple_con is not None):
                    consistency_val = \
                        DataManager.from_json_to_tsimple_consistency(simple_con)
                else:
                    time_con = read_opts.get(ONDB_TIME_CONSISTENCY, None)
                    if (time_con is not None):
                        consistency_val = \
                            DataManager.from_json_to_ttime_consistency(time_con)
                    else:
                        version_con = read_opts.get(
                            ONDB_VERSION_CONSISTENCY, None)
                        if (version_con is not None):
                            consistency_val = \
                                DataManager.from_json_to_tversion_consistency(
                                    version_con)
            # encode timeout
            timeout_val = int(read_opts.get(ONDB_TIMEOUT, 0))
        # return something as long as there is something to encode
        if (consistency_val is None and timeout_val is None):
            return None
        else:
            return TReadOptions(consistency=consistency_val,
                                timeoutMs=timeout_val)

    @staticmethod
    def from_json_to_tfield_range(field_range):
        # This method converts from Field Range to the Thrift equivalent
        if (field_range is not None):
            # get all the relevant information
            field = field_range.get(ONDB_FIELD, None)
            start_value = field_range.get(ONDB_START_VALUE, None)
            end_value = field_range.get(ONDB_END_VALUE, None)
            start_inclusive = field_range.get(ONDB_START_INCLUSIVE, None)
            end_inclusive = field_range.get(ONDB_END_INCLUSIVE, None)
            # encode it as a TFieldRange
            return TFieldRange(fieldName=field,
                               startValue=start_value,
                               startIsInclusive=start_inclusive,
                               endValue=end_value,
                               endIsInclusive=end_inclusive)
        return None

    @staticmethod
    def from_json_to_tmultirow_options(multirow_opts):
        # This method converts from Multirow Options to the Thrift equivalent
        t_field_range = None
        t_tables = None
        if (multirow_opts is not None):
            # get field range
            t_field_range = DataManager.from_json_to_tfield_range(
                multirow_opts.get(ONDB_FIELD_RANGE, None))
            # get the list of parent and children tables and consolidate
            t_tables = multirow_opts.get(ONDB_INCLUDED_TABLES, None)
        # return the data
        return t_field_range, t_tables

    @staticmethod
    def from_json_to_tdirection(direction):
        # This method converts from Direction to the Thrift equivalent
        res = None
        if (isinstance(direction, dict)):
            d = direction.get(ONDB_DIRECTION, None)
            if (d == ONDB_FORWARD):
                res = TDirection.FORWARD
            elif (d == ONDB_REVERSE):
                res = TDirection.REVERSE
            elif (d == ONDB_UNORDERED):
                res = TDirection.UNORDERED
        return res

    @staticmethod
    def from_json_to_ttable_iterator_options(table_iterator_opts):
        # This method converts from Table Iterator Options to the Thrift
        # equivalent
        t_dir = None
        max_results = 0
        tr_opts = None
        if (table_iterator_opts is not None):
            # get direction, max results and read options
            direction = table_iterator_opts.get(ONDB_DIRECTION, None)
            max_results_by_batch = table_iterator_opts.get(
                ONDB_MAX_RESULTS,
                0)
            read_opts = table_iterator_opts.get(ONDB_READ_OPTIONS, None)
            t_dir = DataManager.from_json_to_tdirection(direction)
            if (max_results_by_batch == 0 or max_results_by_batch is None):
                max_results = 0
            else:
                max_results = int(max_results_by_batch)
            tr_opts = DataManager.from_json_to_tread_options(read_opts)
        # return the Thrift data
        return t_dir, max_results, tr_opts

    @staticmethod
    def from_json_to_toperation_type(operation_type):
        # This methid converts from Operation Type
        op_type = operation_type.get(ONDB_OPERATION_TYPE, None)
        if (op_type == ONDB_DELETE):
            return TOperationType.DELETE
        elif (op_type == ONDB_DELETE_IF_VERSION):
            return TOperationType.DELETE_IF_VERSION
        elif (op_type == ONDB_PUT):
            return TOperationType.PUT
        elif (op_type == ONDB_PUT_IF_ABSENT):
            return TOperationType.PUT_IF_ABSENT
        elif (op_type == ONDB_PUT_IF_PRESENT):
            return TOperationType.PUT_IF_PRESENT
        elif (op_type == ONDB_PUT_IF_VERSION):
            return TOperationType.PUT_IF_VERSION
        return None

    @staticmethod
    def from_json_to_toperations(table_operations):
        # This method converts from Operations list to the Thrift equivalent
        operations = []
        if (isinstance(table_operations, list)):
            for oper in table_operations:
                # get table name, operation type, abort option, row,
                # return choice and version
                tn = oper.get(ONDB_TABLE_NAME, None)
                op_str = oper.get(ONDB_OPERATION, None)
                op = DataManager.from_json_to_toperation_type(
                    op_str)
                abort = oper.get(ONDB_ABORT_IF_UNSUCCESSFUL, False)
                rd = oper.get(ONDB_ROW, None)
                row = DataManager.dict_to_trow(rd)
                r_choice = DataManager.from_json_to_treturn_choice(
                    rd.get(ONDB_RETURN_CHOICE, None))
                ver = oper.get(ONDB_VERSION, None)
                # convert it as TOperation and append it to the list
                t_oper = TOperation(tn, op, row, r_choice, abort, ver)
                operations.append(t_oper)
        # return the list
        return operations

    @staticmethod
    def from_json_to_ttimeunit(timeunit):
        # This method converts from TimeUnit to the Thrift equivalent
        tu = timeunit.get(ONDB_TIMEUNIT, None)
        if (tu == ONDB_HOURS):
            return TTimeUnit.HOURS
        elif (tu == ONDB_DAYS):
            return TTimeUnit.DAYS
        return None
