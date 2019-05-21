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

# nosqldb.py: This file contains most of the public classes available for
# the Oracle NoSQL DB API for Python:

#    Factory: This class has only static methods that serve in the creation
#    of the several kind of connections. In addition to that, this class
#    includes static methods to convert from iterators from Proxy to the
#    native python iterators.

#    Store: This is the main class, you must not create an instance of this
#    class, instead you must call Factory.open() or Factory.connect(), those
#    methods will return a Store object connected to the Proxy that will
#    provide you with the mechanisms to access and modify the DB.

#    Row: This class represents a row in the DB. That is the default return
#    value for those functions that return rows. This is underneath a simple
#    dict with two extra contents, the table name and the version of the row.

#    ResultIterator: This class represents an iterator from the DB, and it
#    works as a regular iterator in python so you can iterate over its
#    contents through the use of a for or simply by executing next(). Note
#    that if you want to abort the iteration you must manually call close() on
#    that object.

#    Consistency, Durability, ReadOptions, WriteOptions, FieldRange,
#    MultiRowOptions, Direction, TableIteratorOptions, OperationType,
#    Operation: These other classes model the possible options to pass
#    to the several methods in Store, see individual methods declaration
#    in order to check what classes are needed for each of them.

#    ConnectionException, IllegalArgumentException, DurabilityException,
#    RequestTimeoutException, ConsistencyException, FaultException,
#    UnsupportedOperationException, ExecutionException,
#    OperationExecutionException, ProxyException, CancellationException,
#    InterruptionException, TimeoutException: These other classes are exception
#    instances for each individual exception conditions that can araise in
#    each of the API methods. See the documentation of each individual API
#    methods for details on them.

"""
The nosqldb.py module provides Python client access to Oracle NoSQL
Database stores. For overview information on how to use this module, see
the *Oracle NoSQL Database Python Driver Quick Start.*

This module requires the use of a proxy server which translates network
activity between your Python client and the store. The proxy server is
written in Java. For best results, run the proxy server on the same local
machine as your Python client, and use just one proxy for each Python
client in use.
"""
import logging
import logging.handlers
import os
import socket
import subprocess
import sys
import time

#from utilities import EnvironmentVariables
from config import ProxyConfig
from config import StoreConfig
from jsonutils import DataManager
from jsonutils import RestrictedDict
from utilities import IllegalArgumentException
from utilities import IllegalStateException
from utilities import MAX_LONG_VALUE
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
from utilities import ONDB_PRIMARY_FIELD
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
from utilities import ONDB_SECONDARY_FIELD
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

from oracle.kv.proxy.gen.ONDB import Client
from oracle.kv.proxy.gen.constants import PROTOCOL_VERSION
from oracle.kv.proxy.gen.ttypes import TConsistencyException
from oracle.kv.proxy.gen.ttypes import TDurabilityException
from oracle.kv.proxy.gen.ttypes import TExecutionException
from oracle.kv.proxy.gen.ttypes import TFaultException
from oracle.kv.proxy.gen.ttypes import TIllegalArgumentException
from oracle.kv.proxy.gen.ttypes import TInterruptedException
from oracle.kv.proxy.gen.ttypes import TModuleInfo
from oracle.kv.proxy.gen.ttypes import TProxyException
from oracle.kv.proxy.gen.ttypes import TRequestTimeoutException
from oracle.kv.proxy.gen.ttypes import TTableOpExecutionException
from oracle.kv.proxy.gen.ttypes import TTimeoutException
from oracle.kv.proxy.gen.ttypes import TUnverifiedConnectionException

from thrift import Thrift
from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.transport.TTransport import TTransportException

from multiprocessing import Process

logger = logging.getLogger('nosqldb')


class Factory:
    """
    Creates and opens a store connection using the :py:meth:`open` method,
    which returns a :py:class:`Store` instance. This instance is used for
    all store read and write operations.
    """
    _sleep_before_starting_server = 10

    # This method will be called to start the proxy
    @staticmethod
    def _start_server(port, store_config, proxy_config):
        # use the envars and/or proxy_config data
        # to build the classpath
        path_to_store_jar = None
        path_to_proxy_jar = None
        if (proxy_config is not None):
            path_to_store_jar = proxy_config.get_kv_store_path_to_jar()
            path_to_proxy_jar = proxy_config.get_kv_proxy_path_to_jar()

        if (path_to_proxy_jar is None):
            path_to_proxy_jar = os.environ.get('KVPROXY_JAR')
        if (path_to_store_jar is None):
            path_to_store_jar = os.environ.get('KVSTORE_JAR')

        if (path_to_proxy_jar is None):
            path_to_proxy_jar = os.path.join(os.path.abspath(
                os.path.dirname(__file__)), 'kvproxy/lib/kvproxy.jar')
        if (path_to_store_jar is None):
            path_to_store_jar = os.path.join(os.path.abspath(
                os.path.dirname(__file__)), 'kvproxy/lib/kvclient.jar')

        if (path_to_proxy_jar is None or path_to_store_jar is None):
            raise IllegalArgumentException(
                'Attempt to start the proxy server without setting the ' +
                'proxy and store paths in ProxyConfig and without using ' +
                'KVPROXY_JAR and KVSTORE_JAR environment variables')

        sep = ':'
        if sys.platform == 'win32':
            sep = ';'

        classpath = '{a}{sep}{b}'.format(a=path_to_proxy_jar,
                                         sep=sep, b=path_to_store_jar)

        logger.debug("classpath to start the proxy is: {0}".format(classpath))
        # credentials are not used yet
        user = None
        props_file = None
        # helper hosts, port and store name are also needed to start
        # the proxy
        helper_host = None
        store_name = None
        # get the data needed from store config
        if (store_config.get_user() is not None):
            user = store_config.get_user()
        helper_host = store_config.get_helper_hosts()
        store_name = store_config.get_store_name()
        read_zones = store_config.get_read_zones()
        # get the data from the proxy config
        if (proxy_config.get_security_props_file() is not None):
            props_file = proxy_config.get_security_props_file()
        verbose = proxy_config.get_verbose()
        max_active_requests = proxy_config.get_max_active_requests()
        node_limit_percent = proxy_config.get_node_limit_percent()
        request_threshold_percent = \
            proxy_config.get_request_threshold_percent()
        request_timeout = proxy_config.get_request_timeout()
        socket_open_timeout = proxy_config.get_socket_open_timeout()
        socket_read_timeout = proxy_config.get_socket_read_timeout()
        max_iterator_result = proxy_config.get_max_iterator_results()
        iterator_expiration = proxy_config.get_iterator_expiration_ms()
        max_open_iterators = proxy_config.get_max_open_iterators()
        num_pool_threads = proxy_config.get_num_pool_threads()
        max_current_requests = proxy_config.get_max_concurrent_requests()
        max_results_batches = proxy_config.get_max_results_batches()
        # prepare the execution array with the command to be invoked
        executing_array = ["java", "-cp", classpath,
                           "oracle.kv.proxy.KVProxy"]
        # port, helper_hosts and store_name can't be null since
        # they are mandatory parameters for store_config
        executing_array += ["-port", str(port)]
        executing_array += ["-helper-hosts"]
        for e in helper_host:
            executing_array.append(e)
        executing_array += ["-store", store_name]
        if (read_zones is not None and len(read_zones) > 0):
            executing_array += ["-read-zones"]
            for r in read_zones:
                executing_array.append(r)
        # optional parameters
        if (verbose):
            executing_array += ["-verbose"]
        if (user is not None):
            executing_array += ["-username", user]
        if (props_file is not None):
            executing_array += ["-security", props_file]
        if (max_active_requests is not None):
            executing_array += ["-max-active-requests",
                str(max_active_requests)]
        if (node_limit_percent is not None):
            executing_array += ["-node-limit-percent", str(node_limit_percent)]
        if (request_threshold_percent is not None):
            executing_array += ["-request-threshold-percent",
                str(request_threshold_percent)]
        if (request_timeout is not None):
            executing_array += ["-request-timeout", str(request_timeout)]
        if (socket_open_timeout is not None):
            executing_array += ["-socket-open-timeout",
                str(socket_open_timeout)]
        if (socket_read_timeout is not None):
            executing_array += ["-socket-read-timeout",
                str(socket_read_timeout)]
        if (max_iterator_result is not None):
            executing_array += ["-max-iterator-result",
                str(max_iterator_result)]
        if (iterator_expiration is not None):
            executing_array += ["-iterator-expiration",
                str(iterator_expiration)]
        if (max_open_iterators is not None):
            executing_array += ["-max-open-iterators", str(max_open_iterators)]
        if (num_pool_threads is not None):
            executing_array += ["-num-pool-threads", str(num_pool_threads)]
        if (max_current_requests is not None):
            executing_array += ["-max-concurrent-requests",
                str(max_current_requests)]
        if (max_results_batches is not None):
            executing_array += ["-max-results-batches",
                str(max_results_batches)]
        logger.info('Params for starting proxy: {0}'.format(executing_array))
        # execute the command
        subprocess.Popen(executing_array)

    # This method is going to try to connect to the proxy.
    # In case that the connection is unsuccessful and the
    # method is supposed to start the proxy, call _start_server()
    @staticmethod
    def _check_proxy_and_connect(host, port, transport, should_start,
                                 store_config, proxy_config):
        # try to connect
        try:
            # check if the transport can connect
            transport.open()
            logger.debug('Server already running.')
        except Thrift.TException, tx:
            # check if the host is local (local proxy)
            # if so then you can start the proxy
            hn = socket.gethostname()
            addr = ''
            try:
                addr = socket.gethostbyname(hn)
            except socket.gaierror:
                pass
            is_local = False
            host_only = host
            if ((host_only == addr) or (host_only == '127.0.0.1') or
               (host_only == "localhost")):
                is_local = True
            if (should_start and is_local):
                # if exception then there is nobody listening at that
                # host and port
                logger.debug('Server is not running. Starting server.')
                pss = Process(
                    target=Factory._start_server,
                    args=(port, store_config, proxy_config))
                pss.start()
                # join works because the new process only serves to spawn a
                # subprocess then it exits.  Maybe there's a better way to
                # do this that provides for easier debugging of failures to
                # start the proxy.
                pss.join()

                # wait for the server to load.  Use a loop with a one-second
                # delay to keep trying to connect.  If the server doesn't start
                # within a certain period, fail.  All times are in seconds.
                start_time = time.time()
                current_time = start_time + 1
                server_started = False
                tx = None
                while (current_time <
                        (start_time + Factory._sleep_before_starting_server)):
                    try:
                        # check if the transport can connect
                        transport.open()
                        logger.debug('Server started correctly.')
                        server_started = True
                        break
                    except Thrift.TException, tx:
                        time.sleep(1)
                        current_time = time.time()
                        continue
                if (not server_started):
                    # nothing to do but raise an exception
                    msg = \
                    'Cannot connect to proxy at {host}:{port}, {ex}'.format(
                        host=host, port=str(port), ex=tx.message)
                    logger.error(msg)
                    raise ConnectionException(msg)
            else:
                # don't try to start the proxy just fail
                logger.error('Proxy not running. {0}'.format(tx.message))
                raise ConnectionException('Proxy not running. ' + tx.message)

    # This method will verify that the parameters passed are valid.
    # In case that they are, this method will merge them with the
    # config objects.
    @staticmethod
    def _check_params(server_addr, store_config, proxy_config, start_proxy):
        # check if server_addr is valid
        parts = []
        Factory._check_str(server_addr, 'server_addr')
        parts = server_addr.split(":")
        if (len(parts) < 2):
            logger.error('server_addr must be a string with' +
                ' the form host:port.')
            logger.error('    passed: {0}'.format(server_addr))
            raise IllegalArgumentException('server_addr must be a string ' +
                'of the form host:port.')
        # check if store config is valid
        if (isinstance(store_config, StoreConfig) is not True):
            logger.error('store_config must be a StoreConfig object.')
            logger.error('    passed: {0}'.format(store_config))
            raise IllegalArgumentException(
                'store_config must be a StoreConfig object.')
        logger.debug('Server: {0} port: {1}'.format(parts[0], parts[1]))
        # if we are going to need to start the proxy
        # check proxy config too
        if (start_proxy is True):
            if (proxy_config is None or
               isinstance(proxy_config, ProxyConfig) is not True):
                logger.error('proxy_config must be a ProxyConfig object.')
                logger.error('    passed: {0}'.format(proxy_config))
                raise IllegalArgumentException(
                    'proxy_config must be a ProxyConfig object.')
        return parts[0], int(parts[1])

    @staticmethod
    def open(server_addr, store_config, proxy_config=None):
        """
        Opens a connection to the proxy server and, from there, to the
        Oracle NoSQL Database store. If a properly configured
        :py:class:`ProxyConfig` instance is provided to this method, then
        this method is capable of automatically starting the proxy server
        if it is not already running.

        When you are done with the :py:class:`Store` object returned by
        this method, close it using the :py:meth:`Store.close` method.

        For example, to open a connection to a store where the proxy server
        is already running::

            from nosqldb import ConnectionException
            from nosqldb import Factory
            from nosqldb import StoreConfig

            import logging
            import sys

            store_host_port = "n1.example.org:5000"
            proxy_host_port = "localhost:7010"

            # configure and open the store
            def open_store():
                try:
                    kvstoreconfig =
                        StoreConfig('MyNoSQLStore', [store_host_port])
                    return Factory.open(proxy_host_port, kvstoreconfig)
                except ConnectionException, ce:
                    logging.error("Store connection failed.")
                    logging.error(ce.message)
                    sys.exit(-1)

        :param server_addr: A string in the format of ``hostname:port``
            which represents the location where the proxy server can
            be found.  This parameter is required.
        :param store_config: A :py:class:`StoreConfig` object. This
            parameter is required.
        :param proxy_config: A :py:class:`ProxyConfig` class instance.
        :return: A :py:class:`Store` object if successful, and an
            exception otherwise.
        :raises ConnectionException: If the connection was unsuccessful.
        """
        # check what version of open() to execute
        if (proxy_config is None):
            should_start = False
        else:
            should_start = True
        host, port = Factory._check_params(
            server_addr,
            store_config,
            proxy_config,
            should_start)
        logger.debug('open params: {0}, {1}, {2}'.format(server_addr,
            store_config, proxy_config))
        return Factory._get_store(host, port, store_config, proxy_config,
                                  should_start)

    # This method is going to establish a connection with the proxy and
    # return a Store object out of the parameters passed by open()
    @staticmethod
    def _get_store(host, port, store_config, proxy_config, should_start):
        logger.debug(
            'getStore() parameters: {0}, {1}, {2}, {3}, {4}'.format(
                host, port, store_config, proxy_config, should_start))
        # try to connect
        transport = TSocket.TSocket(host, port)
        if (transport is None):
            logger.error('Impossible to build transport with host: {0}'
                          ' and port: {1}'.format(host, port))
            raise ConnectionException(
                'Impossible to build transport ' +
                'with host: ' + host +
                ' and port: ' + port)
        transport = TTransport.TFramedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = Client(protocol)
        # verify parameters
        properties = store_config.get_verify_values()
        # check if the proxy is up
        Factory._check_proxy_and_connect(host, port, transport, should_start,
                                         store_config, proxy_config)
        try:
            # ping it
            client.ping()
        except TTransportException as tex:
            msg = ': The amount of permitted files opened must be increased.'
            too_many_files = False
            for a in tex.args:
                if 'Too many open files' in a:
                    too_many_files = True
                    break
            if too_many_files:
                tex.args = tex.args + (msg,)
            raise
        except Exception:
            raise
        # verify results
        try:
            result = client.verify(properties)
        except TUnverifiedConnectionException as uve:
            raise ConnectionException(str(uve))
        logger.debug('Verify proxy result: {0}'.format(result))
        # check if we are fine. If so return the Store
        if (result.proxyProtocolVersion < PROTOCOL_VERSION):
            logger.error('Error in proxy version: {0} < {1}'.format(
                result.proxyProtocolVersion, PROTOCOL_VERSION))
            raise IllegalArgumentException('Error. Proxy version: {0}' +
                ' Minimum version needed: {1}.'.format(
                result.proxyProtocolVersion, PROTOCOL_VERSION))
        if (not result.isConnected):
            logger.error('{0}'.format(result))
            raise ConnectionException(str(result))
        else:
            store = Store(client, transport, store_config)
            return store

    # This method is going to convert from the Iterator result to
    # a Returned Row
    # TBD: does the Row cast below result in a data copy? If so, maybe there's
    # a way to load JSON directly into a Row.
    @staticmethod
    def _get_returned_row(row, id_to_table_names):
        ret_row = Row(DataManager.trow_to_dict(row))
        ret_row.set_version(row.rowVersion)
        ret_row.set_table_name(id_to_table_names[row.tableId])
        ret_row.set_expiration(row.expiration)
        return ret_row

    @staticmethod
    def _check_str(data, name):
        if (type(data) is not str and type(data) is not unicode):
            raise IllegalArgumentException(name + ' must be a string type.')

    @staticmethod
    def _check_int(data, name):
        if (type(data) is not int):
            raise IllegalArgumentException(
                name + ' must be an integer. Got:' + str(data))

    @staticmethod
    def _check_bool(data, name):
        if (type(data) is not bool):
            logger.error('invalid ' + name)
            raise IllegalArgumentException(
                name + ' must be a boolean parameter.')

    @staticmethod
    def _from_tstatement_result_to_execution_future(statement_result, client):
        # This method converts from a TStatementResult to a ExecutionFuture
        stmt_res = Factory._from_tstatement_result_to_statement_result(
            statement_result)
        return ExecutionFuture(stmt_res, statement_result.executionId,
            client)

    @staticmethod
    def _from_tstatement_result_to_statement_result(statement_result):
        # This method converts from TStatementResult to StatementResult
        result = {}
        # copy each field
        result[ONDB_INFO] = statement_result.info
        result[ONDB_INFO_AS_JSON] = statement_result.infoAsJson
        result[ONDB_ERROR_MESSAGE] = statement_result.errorMessage
        result[ONDB_IS_CANCELLED] = statement_result.isCancelled
        result[ONDB_IS_SUCCESSFUL] = statement_result.isSuccessful
        result[ONDB_IS_DONE] = statement_result.isDone
        result[ONDB_STATEMENT] = statement_result.statement
        result[ONDB_EXECUTION_ID] = statement_result.executionId
        if (statement_result.result is not None):
            result[ONDB_RESULT] = Result(statement_result.result.stringResult)
        # return the statement result
        return StatementResult(result)

    @staticmethod
    def from_twrite_result_to_write_result_array(write_results):
        # This method converts from TWriteResult to JSON
        result = []
        # for each element in the list
        for w in write_results:
            # copy the actual result to a json
            w_json = {}
            w_json[ONDB_CURRENT_ROW_VERSION] = w.currentRowVersion
            w_json[ONDB_PREVIOUS_ROW] = DataManager.trow_to_dict(w.previousRow)
            w_json[ONDB_PREVIOUS_ROW_VERSION] = w.previousRowVersion
            w_json[ONDB_WAS_SUCCESSFUL] = w.wasDeleted
            # append it to another list
            result.append(OperationResult(w_json))
        # return result
        return result


class Row(dict):
    """
    Represents a table row read from the Oracle NoSQL Database store, or to
    be written to the store. Row data is represented as a dictionary, with
    keys representing row field names, and values as the respective row
    value.

    This class also provides several methods related to table name and
    version management.
    """
    def __init__(self, *args, **kwargs):
        super(Row, self).__init__(*args, **kwargs)
        self._version = None
        self._table_name = None
        self._ttl = None
        self._expiration = None
        self.pos = 0

    def get_table_name(self):
        """
        Returns the name of the table to which the row belongs.
        """
        return self._table_name

    def set_table_name(self, table_name):
        """
        Sets the table name to which the row belongs.
        """
        self._table_name = table_name

    def get_version(self):
        """
        Returns a bytearray representing the row version information. Every
        time a table row is modified in the store, a new version is
        automatically assigned to it.
        """
        return self._version

    def set_version(self, version):
        """
        Sets the table row version.
        """
        self._version = version

    def get_timetolive(self):
        """
        Returns the time to live (TTL) value for this row or null if it
        has not been set by a call to set_timetolive().
        """
        return self._ttl

    def set_timetolive(self, ttl):
        """
        Sets a time to live (TTL) value for the row to be used when the row
        is inserted into a store.
        """
        if ttl is not None and not isinstance(ttl, TimeToLive):
            raise IllegalArgumentException('set_timetolive() take a' +
                ' instance of class TimeToLive as input parameter.')
        self._ttl = ttl

    def get_expiration(self):
        """
        Returns the expiration time of the row. A zero value indicates that the
        row does not expire. If the row does not have a valid expiration time,
        an exception is thrown.
        """
        if (self._expiration >= 0):
            return self._expiration
        raise IllegalStateException('Row expiration time is not defined for ' +
            'this instance.')

    def set_expiration(self, expiration):
        """
        Sets expiration time on output. This is used internally by methods that
        create Row instances retrieved from the server. This method is not used
        for input.
        """
        if (expiration < 0 or expiration > MAX_LONG_VALUE):
            raise IllegalArgumentException('Expiration time should be a ' +
                'non-negative long!')
        self._expiration = expiration

    def split_key_pair(self):
        """
        Returns a tuple that contains the primary key and the secondary
        index key. This method will only work for results from a key-only
        index iterator. If called from a different type of result it
        raises :py:class:`IllegalArgumentException`.
        """
        if ONDB_PRIMARY_FIELD not in self or ONDB_SECONDARY_FIELD not in self:
            raise IllegalArgumentException(
                "Cannot split key pair, 'primary' and 'secondary' keys are " +
                "required: " + str(self))
        return (self[ONDB_PRIMARY_FIELD], self[ONDB_SECONDARY_FIELD])

    def __repr__(self):
        """
        Display a printable representation for this object.
        """
        s = ''
        if (self._table_name is not None):
            s += self._table_name + ' '
        return s + super(Row, self).__repr__()


class ResultIterator:
    """
    Provides iteration over a list of results returned by
    :py:class:`Store.table_iterator` and :py:class:`Store.index_iterator`.
    This iterator consumes blocks of data from the store on demand (by
    default, 100 results for each internal retrieval from the store). In
    this way, this iterator can potentially save memory over a blind bulk
    retrieval of an entire result set.

    Iterate over the result set through the use of a simple ``for`` loop,
    or by calling the :py:meth:`next()` method.

    If you want to abort the iteration, you must manually call the
    :py:meth:`close` method.
    """
    def __init__(self, itId, batch, hasmore, client):
        self._iterator_id = itId
        self._multirow_result = batch
        self._has_more = hasmore
        self._client = client
        self._pos = 0

    def __iter__(self):
        return self

    def next(self):
        """
        Returns the next :py:class:`Row` in the result set, or ``None``.

        :raises StopIteration: when no further results are available
            in the result set.
        """
        # check if we are in the edge of the block
        if (self._pos == len(self._multirow_result.rowsWithMetadata)):
            if (self._has_more is False):
                # no more data so stop iterator
                raise StopIteration
            else:
                # ask for the next bunch
                titer_res = self._client.iteratorNext(self._iterator_id)
                self._multirow_result = titer_res.result
                self._has_more = titer_res.hasMore
                self._pos = 0
        # create a returned_row type with the data
        rr = Factory._get_returned_row(
            self._multirow_result.rowsWithMetadata[self._pos],
            self._multirow_result.idToTableNames)
        # inc pointer
        self._pos += 1
        return rr

    def close(self):
        """
        Close the iterator regardless of whether the iterator was
        exhausted.
        """
        # close the iterator in the proxy
        if (self._has_more is True):
            self._client.iteratorClose(self._iterator_id)
            self._has_more = False
        self._pos = len(self._multirow_result.rowsWithMetadata)


class ExecutionFuture:
    """
    Provides a handle to a DDL statement that has been issued
    asynchronously and is currently being processed by the kvstore. This
    class provides a way to check on the interim status of the DDL
    operation, wait for the operation completion, or cancel the operation.

    This class is instantiated by :py:meth:`Store.execute` and
    :py:meth:`Store.get_execution_future`.

    If you want to cancel the DDL statement before it has completed, use
    :py:meth:`Store.execution_future_cancel`.
    """
    def __init__(self, statement_res, execution_id, client):
        self._statement_result = statement_res
        self._execution_id = execution_id
        self._client = client

    def __repr__(self):
        str_res = str(self._execution_id)
        if (self._statement_result):
            str_res += ' ' + str(self._statement_result)
        return str_res

    # this method is defined specifically to update the internal values
    # after each operation that potentially chnaged them.
    def _set(self, execution_future):
        self._set_execution_id(execution_future.get_execution_id())
        self._set_statement_result(execution_future.get_statement_result())

    def get_statement_result(self):
        """
        Returns the :py:class:`StatementResult` object representing this
        execution future.
        """
        return self._statement_result

    def _set_statement_result(self, statement_result):
        if (isinstance(statement_result, StatementResult)):
            self._statement_result = statement_result
        elif (type(statement_result) == dict):
            self._statement_result = StatementResult(statement_result)
        else:
            raise IllegalArgumentException(
                'statement_result must be a StatementResult object.')

    def get_execution_id(self):
        """
        Returns the unique id associated with this execution future.
        """
        return self._execution_id

    def _set_execution_id(self, execution_id):
        if (isinstance(execution_id, (bytearray, str))):
            self._execution_id = execution_id
        else:
            raise IllegalArgumentException(
                'execution_id must be a bytearray or string.')

    def get_statement(self):
        """
        Returns the DDL statement encapsulated by this class.
        """
        if (self._statement_result is not None):
            return self._statement_result[ONDB_STATEMENT]
        else:
            return None

    def cancel(self, may_interrupt_if_running=None):
        """
        Attempts to cancel the asynchronous execution of a DDL statement
        that was started using :py:meth:`execute`.

        Cancel a DDL operation executed with :py:meth:`Store.execute` then
        return a boolean result indicating if the operation was successful.

        :param may_interrupt_if_running: Boolean indicating whether the
            operation can be interrupted if is currently in progress.
        :return: ``True`` if the operation was cancelled and passed to FAILED
            state. Otherwise, ``False`` if the operation could not be
            cancelled, most probably because the operation completed before
            the DDL statement was executed.
        :raise FaultException: If the operation cannot be completed for any
                reason.
        :raise IllegalArgumentException: If an incorrect argument is used,
                or a required argument is missing.
        :raise ProxyException: If communication with the proxy server somehow
                failed.
        """
        try:
            # verify the parameters and execute the cancel
            logger.debug('parameters = interrupt: {0}'.
                format(may_interrupt_if_running))
            if (may_interrupt_if_running is not None and
                type(may_interrupt_if_running) is not bool):
                    logger.error('invalid may_interrupt_if_running format.')
                    raise IllegalArgumentException(
                        'mayInterruptIfRunning must be a boolean parameter')
            elif (self._statement_result is not None and
                self._statement_result.get(ONDB_IS_DONE, None) is True):
                    return False
            else:
                may_intr = False
                if (may_interrupt_if_running):
                    may_intr = True
                return self._client.executionFutureCancelV2(
                    self._execution_id,
                    may_intr)
        except TFaultException, e:
            raise FaultException(str(e))
        except TIllegalArgumentException, e:
            raise IllegalArgumentException(str(e))
        except TProxyException, e:
            raise ProxyException(str(e))

    def get(self, timeout=None):
        """
        This method does not return (blocks) until the DDL statement
        encapsulated by this class finishes.

        :param timeout: The amount of time in milliseconds you are willing
            to wait for the statement to complete. If this timeout value is
            reached, this method throw :py:class:`TimeoutException`.
        :raise FaultException: If the operation cannot be completed for any
                reason.
        :raise IllegalArgumentException: If an incorrect argument is used,
                or a required argument is missing.
        :raise TimeoutException: If the timeout value specified to this
                method is reached.
        :raise ExecutionException: If the execution of this instruction failed
            for any reason in the server.
        :raise InterruptedException: If the operation was interrupted for
            any reason.
        :raise ProxyException: If communication with the proxy server somehow
                failed.
        """
        try:
            # verify the parameters and execute the operation
            logger.debug('parameters = timeout: {0}'.format(timeout))
            # timeout = None is the default
            if (timeout is not None and
                (type(timeout) is not int or
                timeout < 0)):
                    logger.error('timeout is not integer or is less than 0.')
                    raise IllegalArgumentException(
                        'timeout must be an integer greater or equal to 0')
            if (self._statement_result is not None and
                self._statement_result.get(ONDB_IS_DONE, None) is True):
                    return
            if (timeout is not None):
                res = self._client.executionFutureGetTimeoutV2(
                    self._execution_id, timeout)
            else:
                res = self._client.executionFutureGetV2(
                    self._execution_id)
            self._set(
                Factory._from_tstatement_result_to_execution_future(
                    res, self._client))
        except TFaultException, e:
            raise FaultException(str(e))
        except TIllegalArgumentException, e:
            raise IllegalArgumentException(str(e))
        except TTimeoutException, te:
            raise TimeoutException(str(te))
        except TExecutionException, ee:
            raise ExecutionException(str(ee))
        except TInterruptedException, ie:
            raise InterruptionException(str(ie))
        except TProxyException, pe:
            raise ProxyException(str(pe))

    def update_status(self):
        """
        Updates the current status of the DDL operation encapsulated by
        this class.  You can then retrieve the current status using
        :py:meth:`get_statement_result`.

        :raise FaultException: If the operation cannot be completed for any
                reason.
        :raise IllegalArgumentException: If an incorrect argument is used,
                or a required argument is missing.
        :raise ProxyException: If communication with the proxy server somehow
                failed.
        """
        try:
            # if the operation is done just return
            if (self._statement_result is not None and
                self._statement_result.get(ONDB_IS_DONE, None) is True):
                    return
            res = self._client.executionFutureUpdateStatusV2(self._execution_id)
            self._set(
                Factory._from_tstatement_result_to_execution_future(
                    res, self._client))
        except TFaultException, e:
            raise FaultException(str(e))
        except TIllegalArgumentException, e:
            raise IllegalArgumentException(str(e))
        except TProxyException, pe:
            raise ProxyException(str(pe))


class StatementResult(RestrictedDict):
    """
     Provides information about the execution and outcome of a table
     statement. If this object is obtained asynchronously, it can represent
     information about either a completed or in progress operation. If
     obtained synchronously, it represents the final status of a finished
     operation.

     This class is obtained using the
     :py:meth:`ExecutionFuture.get_statement_result` method.

     This class functions as a dictionary with the following keys:

     * ONDB_INFO
            Contains a textual representation of the statement result.
     * ONDB_INFO_AS_JSON
            Contains a JSON representation of the result's info.
     * ONDB_ERROR_MESSAGE
            Contains the error message, if any, returned by the operation.
     * ONDB_IS_CANCELLED
            ``True`` if the operation was cancelled.
     * ONDB_IS_DONE
            ``True`` if the operation was terminated.
     * ONDB_IS_SUCCESSFUL
            ``True`` if the operation completed successfully.
     * ONDB_STATEMENT
            Contains the table statement that was executed.
     * ONDB_RESULT
            A :py:class:`Result` class instance.
     * ONDB_EXECUTION_ID
            The unique ID assigned to this statement.
    """
    _statement_result_allowed_keys = (ONDB_INFO, ONDB_INFO_AS_JSON,
        ONDB_ERROR_MESSAGE, ONDB_IS_CANCELLED, ONDB_IS_DONE,
        ONDB_IS_SUCCESSFUL, ONDB_STATEMENT, ONDB_RESULT, ONDB_EXECUTION_ID)

    def __init__(self, seq=(), **kwargs):
        super(StatementResult, self).__init__(
            self._statement_result_allowed_keys, seq, **kwargs)


class OperationResult(RestrictedDict):
    """
    Contains a single result from an sequence of results returned by
    :py:meth:`Store.execute_updates`. This class functions as a dictionary with
    the following keys:

    * ONDB_CURRENT_ROW_VERSION
        Byte array representing the row's current version.
    * ONDB_PREVIOUS_ROW
        :py:class:`Row` instance representing the row as it existed prior
        to the execution of the operation.
    * ONDB_PREVIOUS_ROW_VERSION
        Byte array representing the row's version before the operation was
        executed.
    * ONDB_WAS_SUCCESSFUL
        Boolean indicating where the operation was successful. `True` if it
        was successful.
    """
    _write_result_allowed_keys = (ONDB_CURRENT_ROW_VERSION, ONDB_PREVIOUS_ROW,
        ONDB_PREVIOUS_ROW_VERSION, ONDB_WAS_SUCCESSFUL)

    def __init__(self, seq=(), **kwargs):
        super(OperationResult, self).__init__(
            self._write_result_allowed_keys, seq, **kwargs)


class Result:
    """
    Contains the result status of a
    :py:class:`StatementResult`. It encapsulates a result type and possibly
    type-dependent data.  At this time the only possible result type is
    ONDB_TYPE_STRING.
    """
    def __init__(self, string_result=None):
        self._set_string_result(string_result)

    def get_type(self):
        """
        Returns the type of the result.
        """
        return ONDB_TYPE_STRING

    def get_string_result(self):
        """
        Returns the string result.
        """
        return self._string_result

    def _set_string_result(self, result):
        Factory._check_str(result, "result")
        self._string_result = result


class Durability(RestrictedDict):
    """
    Represents a durability guarantee. This class functions as a dictionary
    with the following keys:

    * ONDB_MASTER_SYNC
            The synchronization policy on the shard's master
            server.
    * ONDB_REPLICA_SYNC
            The synchronization policy on the shard's replicas.
    * ONDB_REPLICA_ACK
            The acknowledgement policy.

    For synchronization policies, the allowable values are:

    * ONDB_SP_NO_SYNC
            Write but do not synchronously flush the log on transaction
            commit.
    * ONDB_SP_SYNC
            Write and synchronously flush the log on transaction commit.
    * ONDB_SP_WRITE_NO_SYNC
            Do not write or synchronously flush the log on transaction
            commit.

    For the acknowledgment policy, the allowable values are:

    * ONDB_AP_ALL
        All replicas must acknowledge that they have committed the
        transaction.
    * ONDB_AP_NONE
        No transaction commit acknowledgments are required and the master
        will never wait for replica acknowledgments.
    * ONDB_AP_SIMPLE_MAJORITY
        A simple majority of replicas must acknowledge that they have
        committed the transaction.

    The following convenience durability policies are also available. These
    represent the most commonly used durability policies::

        COMMIT_SYNC = Durability({
            ONDB_MASTER_SYNC: ONDB_SP_SYNC,
            ONDB_REPLICA_SYNC: ONDB_SP_NO_SYNC,
            ONDB_REPLICA_ACK: ONDB_AP_SIMPLE_MAJORITY})
        COMMIT_NO_SYNC = Durability({
            ONDB_MASTER_SYNC: ONDB_SP_NO_SYNC,
            ONDB_REPLICA_SYNC: ONDB_SP_NO_SYNC,
            ONDB_REPLICA_ACK: ONDB_AP_SIMPLE_MAJORITY})
        COMMIT_WRITE_NO_SYNC = Durability({
            ONDB_MASTER_SYNC: ONDB_SP_WRITE_NO_SYNC,
            ONDB_REPLICA_SYNC: ONDB_SP_WRITE_NO_SYNC,
            ONDB_REPLICA_ACK: ONDB_AP_SIMPLE_MAJORITY})
    """
    _durability_opts_allowed_keys = (
        ONDB_MASTER_SYNC, ONDB_REPLICA_SYNC, ONDB_REPLICA_ACK)

    def __init__(self, seq=(), **kwargs):
        super(Durability, self).__init__(
            self._durability_opts_allowed_keys, seq, **kwargs)

    def validate(self):
        """
        Validates the values provided to this class. If they are not valid,
        this method raises :py:class:`IllegalArgumentException`.
        """
        # For Durability, all three parameters are needed
        if (ONDB_MASTER_SYNC not in self):
            raise IllegalArgumentException(
                '"' + ONDB_MASTER_SYNC + '" value must be included!')
        if (ONDB_REPLICA_SYNC not in self):
            raise IllegalArgumentException(
                '"' + ONDB_REPLICA_SYNC + '" value must be included!')
        if (ONDB_REPLICA_ACK not in self):
            raise IllegalArgumentException(
                '"' + ONDB_REPLICA_ACK + '" value must be included!')
        val1 = self.get(ONDB_MASTER_SYNC)
        val2 = self.get(ONDB_REPLICA_SYNC)
        # The following lines are in replacement of a function,
        # what this does is to check two different parameters
        # that have the same expected values with a cycle.
        # This is because a cycle is faster in general terms than
        # a function and also because it just happen a couple of times.
        vals = [(val1, ONDB_MASTER_SYNC), (val2, ONDB_REPLICA_SYNC)]
        for v in vals:
            if (v[0] != ONDB_SP_SYNC and
                v[0] != ONDB_SP_NO_SYNC and
                v[0] != ONDB_SP_WRITE_NO_SYNC):
                    raise IllegalArgumentException(
                        '"' + v[1] + '" value is not valid! Got: ' + v[0])
        val = self.get(ONDB_REPLICA_ACK)
        if (val != ONDB_AP_ALL and
            val != ONDB_AP_NONE and
            val != ONDB_AP_SIMPLE_MAJORITY):
            raise IllegalArgumentException(
                '"' + ONDB_REPLICA_ACK + '" value is not valid! Got: ' + val)


COMMIT_SYNC = Durability({
    ONDB_MASTER_SYNC: ONDB_SP_SYNC,
    ONDB_REPLICA_SYNC: ONDB_SP_NO_SYNC,
    ONDB_REPLICA_ACK: ONDB_AP_SIMPLE_MAJORITY})
COMMIT_NO_SYNC = Durability({
    ONDB_MASTER_SYNC: ONDB_SP_NO_SYNC,
    ONDB_REPLICA_SYNC: ONDB_SP_NO_SYNC,
    ONDB_REPLICA_ACK: ONDB_AP_SIMPLE_MAJORITY})
COMMIT_WRITE_NO_SYNC = Durability({
    ONDB_MASTER_SYNC: ONDB_SP_WRITE_NO_SYNC,
    ONDB_REPLICA_SYNC: ONDB_SP_WRITE_NO_SYNC,
    ONDB_REPLICA_ACK: ONDB_AP_SIMPLE_MAJORITY})


class WriteOptions(RestrictedDict):
    """
    Provides options used by store write methods, such as
    :py:meth:`Store.put`. This class functions as a dictionary with the
    following keys:

    * ONDB_DURABILITY
        A :py:class:`Durability` class instance.
    * ONDB_TIMEOUT
            The time, specified in milliseconds, allowed for processing the
            operation. If zero, the default request timeout is used.
    * ONDB_RETURN_CHOICE
            Identifies what should be returned as the result of the write
            operation. Possible values are:

            * 'NONE'
                    Do not return a value or a version.
            * 'ALL'
                    Return both the value and the version.
            * 'VALUE'
                    Return the new value only.
            * 'VERSION'
                    Return the version only.
    * ONDB_IF_ABSENT
        Boolean value. If ``True`` causes the store write operation to
        proceed only if the row to be written does not currently exist in
        the store.
    * ONDB_IF_PRESENT
        Boolean value. If ``True`` causes the store write operation to
        proceed only if the row to be written currently exists in the
        store.
    * ONDB_IF_VERSION
        Provides a byte array representing a version that must be
        matched in order for the write operation to proceed. Version
        byte arrays can be obtained from :py:meth:`Row.get_version`.
    * ONDB_UPDATE_TTL
        Boolean value, sets whether absolute expiration time will be modified
        during update. If false and the operation updates a record, the
        record' expiration time will not change.
    """
    _write_opts_allowed_keys = (
        ONDB_DURABILITY, ONDB_TIMEOUT, ONDB_RETURN_CHOICE,
        ONDB_IF_ABSENT, ONDB_IF_PRESENT, ONDB_IF_VERSION,
        ONDB_UPDATE_TTL)

    def __init__(self, seq=(), **kwargs):
        super(WriteOptions, self).__init__(
            self._write_opts_allowed_keys, seq, **kwargs)

    def validate(self):
        """
        Validates the values provided to this class. If they are not valid,
        this method raises :py:class:`IllegalArgumentException`.
        """
        # For WriteOptions, none of the parameters are mandatory
        if (ONDB_DURABILITY in self):
            val = self.get(ONDB_DURABILITY)
            if (val is not None):
                if (isinstance(val, Durability)):
                    val.validate()
                elif (type(val) is dict):
                    dur = Durability(val)
                    dur.validate()
                    self.__setitem__(ONDB_DURABILITY, dur)
                else:
                    raise IllegalArgumentException(
                        ONDB_DURABILITY + ' value must be a Durability class' +
                        ' or a dictionary. Got: ' + str(val))
            else:
                raise IllegalArgumentException(
                    ONDB_DURABILITY + ' must not be null.')
        # timeouts are measure in milliseconds
        if (ONDB_TIMEOUT in self):
            val = self.get(ONDB_TIMEOUT)
            if (val is not None and type(val) is not int):
                raise IllegalArgumentException(
                    ONDB_TIMEOUT + ' must be an integer greater or equal to' +
                    '0. Got: ' + str(val))
        if (ONDB_RETURN_CHOICE in self):
            val = self.get(ONDB_RETURN_CHOICE)
            if (not self.validate_return_opts(val)):
                raise IllegalArgumentException(
                    ONDB_RETURN_CHOICE + ' value is not valid. Got: ' +
                    str(val))
        if (ONDB_UPDATE_TTL in self):
            val = self.get(ONDB_UPDATE_TTL)
            if (type(val) is not bool):
                raise IllegalArgumentException(
                    ONDB_UPDATE_TTL + ' must be a boolean.')

class TimeUnit(RestrictedDict):
    """
    TimeUnit represents time durations at a given unit of granularity.

    This class functions as a dictionary with just one key, which is
    mandatory: ``ONDB_TIMEUNIT``. You may specify one of the following
    values for this key:

    * ONDB_HOURS
        TimeUnit in HOURS.
    * ONDB_DAYS
        TimeUnit in DAYS.
    """
    _time_unit_allowed_keys = (ONDB_TIMEUNIT,)

    def __init__(self, seq=(), **kwargs):
        super(TimeUnit, self).__init__(
            self._time_unit_allowed_keys, seq, **kwargs)

    def __str__(self):
        return 'HOURS' if self.get(ONDB_TIMEUNIT) is ONDB_HOURS else 'DAYS'

    def validate(self):
        # ONDB_TIMEUNIT is mandatory
        """
        Validates the values provided to this class. If they are not valid,
        this method raises :py:class:`IllegalArgumentException`.
        """
        val = self.get(ONDB_TIMEUNIT, None)
        if (val != ONDB_HOURS and val != ONDB_DAYS):
            raise IllegalArgumentException(ONDB_TIMEUNIT + ' value must ' +
                'be set to ' + ONDB_HOURS + ' or ' + ONDB_DAYS)

class TimeToLive(RestrictedDict):
    """
    TimeToLive is a utility class that represents a period of time, similar to
    Java 8's java.time.Duration, but specialized to the needs of Oracle NoSQL
    Database.

    This class is restricted to durations of days and hours. It is only used
    as input related to time to live (TTL) for :py:class'Row' instances.
    Construction allows only day and hour durations for efficiency reasons.

    This class functions as a dictionary with the following keys:

    * ONDB_TTL_VALUE 
        The time period. Long value, zero indicates that the row should not
        expire.
    * ONDB_TTL_TIMEUNIT
        The value specified for this key must be a :py:class:`TimeUnit`
        class instance.
    """
    _time_to_live_allowed_keys = (ONDB_TTL_VALUE, ONDB_TTL_TIMEUNIT)

    def __init__(self, seq=(), **kwargs):
        super(TimeToLive, self).__init__(
            self._time_to_live_allowed_keys, seq, **kwargs)

    def __str__(self):
        """
        Convert the TimeToLive instance to string.
        """
        return (str(self.get(ONDB_TTL_VALUE)) + " " +
                str(self.get(ONDB_TTL_TIMEUNIT).get(ONDB_TIMEUNIT)))

    def validate(self):
        """
        Validates the values provided to this class. If they are not valid,
        this method raises :py:class:`IllegalArgumentException`.
        """
        if (ONDB_TTL_VALUE not in self):
            raise IllegalArgumentException(
                '"' + ONDB_TTL_VALUE + '" value must be included!')
        if (ONDB_TTL_TIMEUNIT not in self):
            raise IllegalArgumentException(
                '"' + ONDB_TTL_TIMEUNIT + '" value must be included!')
        val = self.get(ONDB_TTL_VALUE)
        if (val < 0 or val > MAX_LONG_VALUE):
            raise IllegalArgumentException('"' + ONDB_TTL_VALUE +
                '" value should be a non-negative long!')
        val = self.get(ONDB_TTL_TIMEUNIT)
        if (isinstance(val, TimeUnit)):
            val.validate()
        elif (type(val) is dict):
            timeunit = TimeUnit(val)
            timeunit.validate()
            self.__setitem__(ONDB_TTL_TIMEUNIT, timeunit)
        else:
            raise IllegalArgumentException(ONDB_TTL_TIMEUNIT +
                ' value must be a TimeUnit object or a dict.')

    def calculate_expiration(self, reference_milliseconds):
        """
        Returns an absolute time in milliseconds representing the duration plus
        the parameter reference_milliseconds. reference_milliseconds should be
        an absolute time in milliseconds since January 1, 1970.
        """
        if (self.get(ONDB_TTL_VALUE) is 0):
            return 0
        hours = (24 if self.get(ONDB_TTL_TIMEUNIT).get(ONDB_TIMEUNIT)
                 is ONDB_DAYS else 1)
        return (reference_milliseconds +
                hours * self.get(ONDB_TTL_VALUE) * 60 * 60 * 1000)


ONDB_TTL_DO_NOT_EXPIRE = TimeToLive(
    {ONDB_TTL_VALUE: 0, ONDB_TTL_TIMEUNIT: {ONDB_TIMEUNIT: ONDB_DAYS}})


class TimeConsistency(RestrictedDict):
    """
    A consistency policy which describes the amount of time the replica is
    allowed to lag the master. The application can use this policy to
    ensure that the replica node sees all transactions that were committed
    on the master before the lag interval.

    Effective use of this policy requires that the clocks on the master and
    replica are synchronized by using a protocol like NTP.

    This class functions as a dictionary with the following keys:

    * ONDB_PERMISSIBLE_LAG
            A long value representing the total number of milliseconds the
            replica is allowed to lag the master when initiating a
            transaction.
    * ONDB_TIMEOUT
            A long value representing how long a replica may wait for the
            desired consistency to be achieved before giving up.

            All KVStore read operations support a consistency
            specification, as well as a separate operation timeout. The
            KVStore client driver implements a read operation by choosing a
            node (usually a replica) from the proper replication group, and
            sending it a request. If the replica cannot guarantee the
            desired consistency within the consistency timeout, it replies
            to the request with a failure indication. If there is still
            time remaining within the operation timeout, the client driver
            picks another node and tries the request again (transparent to
            the application).

            It makes sense to think of the operation timeout as the maximum
            amount of time the application is willing to wait for the
            operation to complete. The consistency timeout is like a
            performance hint to the implementation, suggesting that it can
            generally expect that a healthy replica usually should be able
            to become consistent within the given amount of time, and that
            if it does not, then it is probably more likely worth the
            overhead of abandoning the request attempt and retrying with a
            different replica. Note that for the consistency timeout to be
            meaningful it must be smaller than the operation timeout.

            Choosing a value for the operation timeout depends on the needs
            of the application. Finding a good consistency timeout value is
            more likely to depend on observations made of real system
            performance.
    """
    _time_consistency_allowed_keys = (ONDB_PERMISSIBLE_LAG, ONDB_TIMEOUT)

    def __init__(self, seq=(), **kwargs):
        super(TimeConsistency, self).__init__(
            self._time_consistency_allowed_keys, seq, **kwargs)

    def validate(self):
        """
        Validates the values provided to this class. If they are not valid,
        this method raises :py:class:`IllegalArgumentException`.
        """
        # For TimeConsistency, all parameters are needed
        # check that all data is right
        val = self.get(ONDB_PERMISSIBLE_LAG, None)
        # ONDB_PERMISSIBLE_LAG must be integer greater or equal to 0
        # This measure is in milliseconds
        if (val is None):
            raise IllegalArgumentException(
                ONDB_PERMISSIBLE_LAG + ' key must exist.')
        else:
            Factory._check_int(val, ONDB_PERMISSIBLE_LAG)
            if (int(val) < 0):
                raise IllegalArgumentException(
                    ONDB_PERMISSIBLE_LAG + ' must be greater or equal to 0 ' +
                    '- Got: ' + str(val))
        val = self.get(ONDB_TIMEOUT, None)
        # check that ONDB_TIMEOUT is integer greater or equal to 0
        # This measure is in milliseconds
        if (val is None):
            raise IllegalArgumentException(
                ONDB_TIMEOUT + ' key must exist.')
        else:
            Factory._check_int(val, ONDB_TIMEOUT)
            if (int(val) < 0):
                raise IllegalArgumentException(
                    ONDB_TIMEOUT + ' must be greater or equal to 0 - Got: ' +
                    str(val))


class VersionConsistency(RestrictedDict):
    """
    A consistency policy which ensures that the environment on a replica
    node is at least as current as denoted by the specified version byte
    array.

    The specified version represents a point in the serialized
    transaction schedule created by the master. In other words, the version
    is like a bookmark, representing a particular transaction commit in the
    replication stream. The replica ensures that the commit identified by
    the version has been executed before allowing the transaction on the
    replica to proceed.

    For example, suppose the application is a web application. Each request
    to the web server consists of an update operation followed by read
    operations (say from the same client). The read operations naturally
    expect to see the data from the updates executed by the same request.
    However, the read operations might have been routed to a replica node
    that did not execute the update.

    In such a case, the update request would generate a version byte array,
    which would be resubmitted by the browser, and then passed with the
    subsequent read requests to the store. The read request may be directed
    by the store's load balancer to any one of the available replicas. If
    the replica servicing the request is already current (with respect to
    the version token), it will immediately execute the transaction and
    satisfy the request. If not, the transaction will stall until the
    replica replay has caught up and the change is available at that node.

    This class functions as a dictionary with the following keys:

    * ONDB_VERSION
            A byte array representing the version that must be matched in
            order for the consistency guarantee to be met.
    * ONDB_TIMEOUT
            A long value representing how long a replica may wait for the
            desired consistency to be achieved before giving up.

            All KVStore read operations support a consistency
            specification, as well as a separate operation timeout. The
            KVStore client driver implements a read operation by choosing a
            node (usually a replica) from the proper replication group, and
            sending it a request. If the replica cannot guarantee the
            desired consistency within the consistency timeout, it replies
            to the request with a failure indication. If there is still
            time remaining within the operation timeout, the client driver
            picks another node and tries the request again (transparent to
            the application).

            It makes sense to think of the operation timeout as the maximum
            amount of time the application is willing to wait for the
            operation to complete. The consistency timeout is like a
            performance hint to the implementation, suggesting that it can
            generally expect that a healthy replica usually should be able
            to become consistent within the given amount of time, and that
            if it does not, then it is probably more likely worth the
            overhead of abandoning the request attempt and retrying with a
            different replica. Note that for the consistency timeout to be
            meaningful it must be smaller than the operation timeout.

            Choosing a value for the operation timeout depends on the needs
            of the application. Finding a good consistency timeout value is
            more likely to depend on observations made of real system
            performance.
    """
    _version_consistency_allowed_keys = (ONDB_VERSION, ONDB_TIMEOUT)

    def __init__(self, seq=(), **kwargs):
        super(VersionConsistency, self).__init__(
            self._version_consistency_allowed_keys, seq, **kwargs)

    def validate(self):
        """
        Validates the values provided to this class. If they are not valid,
        this method raises :py:class:`IllegalArgumentException`.
        """
        # both parameters are mandatory
        # check that all data is right
        val = self.get(ONDB_VERSION, None)
        # ONDB_VERSION must be integer greater or equal to 0
        if (val is None):
            raise IllegalArgumentException(
                ONDB_VERSION + ' key must exists.')
        else:
            if (type(val) is not bytearray and
                type(val) is not str):
                raise IllegalArgumentException(
                    ONDB_VERSION + ' value must be a bytearray.')
        val = self.get(ONDB_TIMEOUT, None)
        # check that ONDB_TIMEOUT is integer greater or equal to 0
        if (val is None):
            raise IllegalArgumentException(
                ONDB_TIMEOUT + ' must be greater or equal to 0 - ' +
                'Got: ' + str(val))
        else:
            Factory._check_int(val, ONDB_TIMEOUT)
            if (int(val) < 0):
                raise IllegalArgumentException(
                    ONDB_TIMEOUT + ' must be greater or equal to 0 - Got: ' +
                    str(val))


class SimpleConsistency(RestrictedDict):
    """
    Used to specify a simple consistency guarantee. This class functions as
    a dictionary with just one key: ``ONDB_SIMPLE_CONSISTENCY``. This key
    may have one of the following values:

    * ONDB_ABSOLUTE
        Requires that a transaction be serviced on the master so that
        consistency is absolute.
    * ONDB_NONE_REQUIRED
        Lets a transaction on a replica using this policy proceed
        regardless of the state of the replica relative to the master.
    * ONDB_NONE_REQUIRED_NO_MASTER
         Requires that a read operation be serviced on a replica; never the
         master.
    """
    _simple_consistency_allowed_keys = (ONDB_SIMPLE_CONSISTENCY,)

    def __init__(self, seq=(), **kwargs):
        super(SimpleConsistency, self).__init__(
            self._simple_consistency_allowed_keys, seq, **kwargs)

    def validate(self):
        """
        Validates the values provided to this class. If they are not valid,
        this method raises :py:class:`IllegalArgumentException`.
        """
        # this parameter is mandatory
        # ONDB_SIMPLE_CONSISTENCY must exist
        val = self.get(ONDB_SIMPLE_CONSISTENCY)
        if (val is None or
            (val != ONDB_ABSOLUTE and
            val != ONDB_NONE_REQUIRED and
            val != ONDB_NONE_REQUIRED_NO_MASTER)):
                raise IllegalArgumentException(
                    ONDB_CONSISTENCY + ' value must be ' + ONDB_ABSOLUTE +
                    ', ' + ONDB_NONE_REQUIRED + ' or ' +
                    ONDB_NONE_REQUIRED_NO_MASTER)


class Consistency(RestrictedDict):
    """
    Used to provide consistency guarantees for read operations.

    In general, read operations may be serviced either at a master or
    replica node. When serviced at the master node, consistency is always
    absolute. If absolute consistency is required, ABSOLUTE may be
    specified to force the operation to be serviced at the master. For
    other types of consistency, when the operation is serviced at a replica
    node, the transaction will not begin until the consistency policy is
    satisfied.

    The consistency is specified as an argument to all read operations, for
    example, :py:meth:`Store.get`.

    This class functions as a dictionary with the following keys. NOTE:
    Only one of the following keys may be used at any given time for this
    class:

    * ONDB_SIMPLE_CONSISTENCY
            Provides a :py:class:`SimpleConsistency` instance.
    * ONDB_TIME_CONSISTENCY
            Provides a :py:class:`TimeConsistency` instance.
    * ONDB_VERSION_CONSISTENCY
            Provides a :py:class:`VersionConsistency` instance.

    The following convenience constants are also available::

        NONE_REQUIRED =
            Consistency({ONDB_SIMPLE_CONSISTENCY: ONDB_NONE_REQUIRED})
        ABSOLUTE = Consistency({ONDB_SIMPLE_CONSISTENCY: ONDB_ABSOLUTE})
        NONE_REQUIRED_NO_MASTER = Consistency({
            ONDB_SIMPLE_CONSISTENCY: ONDB_NONE_REQUIRED_NO_MASTER})

    """
    _consistency_allowed_keys = (ONDB_SIMPLE_CONSISTENCY, ONDB_TIME_CONSISTENCY,
        ONDB_VERSION_CONSISTENCY)

    def __init__(self, seq=(), **kwargs):
        super(Consistency, self).__init__(
            self._consistency_allowed_keys, seq, **kwargs)

    def validate(self):
        """
        Validates the values provided to this class. If they are not valid,
        this method raises :py:class:`IllegalArgumentException`.
        """
        # one of these 3 kind of consistencies must exist
        if (len(self) > 1):
            raise IllegalArgumentException(
                'Consistency dict must have only one kind of Consistency.')
        if (ONDB_SIMPLE_CONSISTENCY in self):
            con = SimpleConsistency(self)
            con.validate()
        if (ONDB_TIME_CONSISTENCY in self):
            val = self.get(ONDB_TIME_CONSISTENCY)
            if (isinstance(val, TimeConsistency)):
                val.validate()
            elif (type(val) is dict):
                con = TimeConsistency(val)
                con.validate()
            else:
                raise IllegalArgumentException(
                    ONDB_TIME_CONSISTENCY + ' must be a dict or a ' +
                    'Time Consistency type. Got: ' + str(val))
        if (ONDB_VERSION_CONSISTENCY in self):
            val = self.get(ONDB_VERSION_CONSISTENCY)
            if (isinstance(val, VersionConsistency)):
                val.validate()
            elif (type(val) is dict):
                con = VersionConsistency(val)
                con.validate()
            else:
                raise IllegalArgumentException(
                    ONDB_TIME_CONSISTENCY + ' must be a dict or a ' +
                    'Version Consistency type. Got: ' + str(val))


NONE_REQUIRED = Consistency({ONDB_SIMPLE_CONSISTENCY: ONDB_NONE_REQUIRED})
ABSOLUTE = Consistency({ONDB_SIMPLE_CONSISTENCY: ONDB_ABSOLUTE})
NONE_REQUIRED_NO_MASTER = Consistency({
    ONDB_SIMPLE_CONSISTENCY: ONDB_NONE_REQUIRED_NO_MASTER})


class ReadOptions(RestrictedDict):
    """
    Used to provide various options for read operations. This class
    functions as a dictionary with the following keys:

    * ONDB_CONSISTENCY
        The :py:class:`Consistency` guarantee that you want to use for this
        read operation.
    * ONDB_TIMEOUT
        An integer value representing the amount of time in milliseconds
        that is permitted to elapse before the read operation times out.
    """
    _read_opts_allowed_keys = (ONDB_CONSISTENCY, ONDB_TIMEOUT)

    def __init__(self, seq=(), **kwargs):
        super(ReadOptions, self).__init__(
            self._read_opts_allowed_keys, seq, **kwargs)

    def validate(self):
        """
        Validates the values provided to this class. If they are not valid,
        this method raises :py:class:`IllegalArgumentException`.
        """
        # None of these contents are mandatory
        # check for Consistency
        if (ONDB_CONSISTENCY in self):
            val = self.get(ONDB_CONSISTENCY)
            # check for their types
            if (isinstance(val, Consistency)):
                val.validate()
            elif (type(val) is dict):
                con = Consistency(val)
                con.validate()
            else:
                raise IllegalArgumentException(
                    ONDB_CONSISTENCY + ' value must be a dict with simple' +
                    ' time or version Consistencies. Got: ' + str(val))
        # check for timeout
        # this measure is in milliseconds
        if (ONDB_TIMEOUT in self):
            val = self.get(ONDB_TIMEOUT, None)
            if (val is not None):
                Factory._check_int(val, ONDB_TIMEOUT)
                if (val < 0):
                    raise IllegalArgumentException(
                        ONDB_TIMEOUT + ' must be greater or equal to 0. Got: ' +
                        str(val))
            else:
                raise IllegalArgumentException(
                        ONDB_TIMEOUT + ' must not be None.')


class FieldRange(RestrictedDict):
    """
    Defines a range of values to be used in a table or index iteration, or
    multi_get operation. A field range is used as the least significant
    component in a partially specified primary or index key in order to
    create a value range for an operation that returns multiple rows or
    keys. The data types supported by a field range are limited to those which
    are valid for primary keys and/or index keys.

    Field ranges are provided to a read operation using a
    :py:class:`MultiRowOptions` class instance.

    This class functions as a dictionary with the following keys:

    * ONDB_FIELD
        The name of the field limited by this range.
    * ONDB_START_VALUE
        The lower bound of the range, as a string regardless of the field's
        actual type. If no lower bound is to be enforced, do not specify
        this key. But in that case, ``ONDB_END_VALUE`` must be specified.
    * ONDB_START_INCLUSIVE
        Boolean value indicating whether the start value is included in the
        range. ``True`` if it is included.
    * ONDB_END_VALUE
        The upper bound of the range, as a string regardless of the field's
        actual type. If no upper bound is to be enforced, do not specify
        this key. But in that case, ``ONDB_START_VALUE`` must be specified.
    * ONDB_END_INCLUSIVE
        Boolean value indicating whether the end value is included in the
        range. ``True`` if it is included.

    For example, to specify a range on field ``id``, which is an integer,
    so that the range is ``1`` to ``3``, inclusive::

        fr = FieldRange({ ONDB_FIELD : 'id',
                          ONDB_START_VALUE : '1',
                          ONDB_END_VALUE : '3'})

    For complex data types, such as a record, use dot notation to specify
    the field inside the data type. For example, if you have an embedded
    record, ``address``,  and you indexed its field, ``city``, then to
    specify a field range that includes ``Chicago`` to ``New York``,
    inclusive, use::

        fr = FieldRange({ ONDB_FIELD : 'address.city',
                          ONDB_START_VALUE : 'Chicago',
                          ONDB_END_VALUE : 'New York'})


    """
    _field_range_allowed_keys = (ONDB_FIELD, ONDB_START_VALUE, ONDB_END_VALUE,
        ONDB_START_INCLUSIVE, ONDB_END_INCLUSIVE)

    def __init__(self, seq=(), **kwargs):
        super(FieldRange, self).__init__(
            self._field_range_allowed_keys, seq, **kwargs)

    def validate(self):
        """
        Validates the values provided to this class. If they are not valid,
        this method raises :py:class:`IllegalArgumentException`.
        """
        if (ONDB_FIELD not in self):
            raise IllegalArgumentException(
                ONDB_FIELD + ' key must exist.')
        val = self.get(ONDB_FIELD)
        Factory._check_str(val, ONDB_FIELD)
        if (ONDB_START_VALUE not in self and
            ONDB_END_VALUE not in self):
                raise IllegalArgumentException(
                    ONDB_START_VALUE + ' key or ' + ONDB_END_VALUE +
                    ' or both key must exist.')
        val1 = None
        val2 = None
        if (ONDB_START_VALUE in self):
            val1 = self.get(ONDB_START_VALUE)
            if (val1 is None or not isinstance(val1, (str, unicode))):
                raise IllegalArgumentException(
                    ONDB_START_VALUE + ' key must be a string encoded JSON.')
        if (ONDB_END_VALUE in self):
            val2 = self.get(ONDB_END_VALUE)
            if (val2 is None or not isinstance(val2, (str, unicode))):
                raise IllegalArgumentException(
                    ONDB_END_VALUE + ' key must be a string encoded JSON.')
        val1 = self.get(ONDB_START_INCLUSIVE, None)
        val2 = self.get(ONDB_END_INCLUSIVE, None)
        # The following lines are in replacement of a function,
        # what this does is to check two different parameters
        # that have the same expected values with a cycle.
        # This is because a cycle is faster in general terms than
        # a function and also because it just happen a couple of times.
        vals = [(val1, ONDB_START_INCLUSIVE), (val2, ONDB_END_INCLUSIVE)]
        for v in vals:
            if (v[0] is not None and type(v[0]) is not bool):
                raise IllegalArgumentException(
                    '"' + v[1] + '" key must be associate to a boolean value.')


class MultiRowOptions(RestrictedDict):
    """
    Used to provide various options for read operations in which multiple
    table rows are returned. This class functions as a dictionary with the
    following keys:

    * ONDB_FIELD_RANGE
        The :py:class:`FieldRange` that limits the rows returned by this
        operation.
    * ONDB_INCLUDED_TABLES
        List of ancestor or child tables to be included in an operation
        that returns multiple rows or keys. Note that when specifying child
        table names, they must be fully-qualified 'parent.child' names.
    """
    _multirow_opts_allowed_keys =(ONDB_FIELD_RANGE, ONDB_INCLUDED_TABLES)

    def __init__(self, seq=(), **kwargs):
        super(MultiRowOptions, self).__init__(
            self._multirow_opts_allowed_keys, seq, **kwargs)

    def validate(self):
        """
        Validates the values provided to this class. If they are not valid,
        this method raises :py:class:`IllegalArgumentException`.
        """
        if (ONDB_FIELD_RANGE in self):
            val = self.get(ONDB_FIELD_RANGE)
            if (isinstance(val, FieldRange)):
                val.validate()
            elif(type(val) is dict):
                fr = FieldRange(val)
                fr.validate()
                self.__setitem__(ONDB_FIELD_RANGE, fr)
            else:
                raise IllegalArgumentException(
                    ONDB_FIELD_RANGE +
                    ' value must bene, a FieldRange object ' +
                    'or a dict. Got: ' + str(val))
        if (ONDB_INCLUDED_TABLES in self):
            val1 = self.get(ONDB_INCLUDED_TABLES)
            if (type(val1) is not list):
                raise IllegalArgumentException(
                    '"' + ONDB_INCLUDED_TABLES +
                    '" value must be a list of table names.')
            for t in val1:
                Factory._check_str(t, ONDB_INCLUDED_TABLES + ' elements')


class Direction(RestrictedDict):
    """
    Used with iterator operations to specify the order that keys are
    returned.

    This class functions as a dictionary with just one key, which is
    mandatory: ``ONDB_DIRECTION``. You may specify one of the following
    values for this key:

    * ONDB_FORWARD
        Iterate in ascending key order.
    * ONDB_REVERSE
        Iterate in descending key order.
    * ONDB_UNORDERED
        Iterate in no particular key order.
    """
    _direction_allowed_keys = (ONDB_DIRECTION,)

    def __init__(self, seq=(), **kwargs):
        super(Direction, self).__init__(
            self._direction_allowed_keys, seq, **kwargs)

    def validate(self):
        # ONDB_DIRECTION is mandatory
        """
        Validates the values provided to this class. If they are not valid,
        this method raises :py:class:`IllegalArgumentException`.
        """
        val = self.get(ONDB_DIRECTION, None)
        if (val is not None):
            if (val != ONDB_FORWARD and
                val != ONDB_REVERSE and
                val != ONDB_UNORDERED):
                    raise IllegalArgumentException(
                        ONDB_DIRECTION + ' value must be one of ' +
                        ONDB_FORWARD + ', ' + ONDB_REVERSE + ' or ' +
                        ONDB_UNORDERED)
        else:
            raise IllegalArgumentException(
                ONDB_DIRECTION + ' value must not be None.')


class TableIteratorOptions(RestrictedDict):
    """
    Used with store read operations that return iterators. It overrides any
    default values that might have been specified when the store handle was
    configured.

    This class functions as dictionary with the following keys:

    * ONDB_DIRECTION
        The value specified for this key must be a :py:class:`Direction`
        class instance.
    * ONDB_MAX_RESULTS
        Specifies an integer value representing the maximum number of
        results batches that can be held in the NoSQL Database client
        process.
    * ONDB_READ_OPTIONS
        The value specified for this key must be a :py:class:`ReadOptions`
        class instance.
    """
    _table_iterator_opts_allowed_keys = (ONDB_DIRECTION, ONDB_MAX_RESULTS,
        ONDB_READ_OPTIONS)

    def __init__(self, seq=(), **kwargs):
        super(TableIteratorOptions, self).__init__(
            self._table_iterator_opts_allowed_keys, seq, **kwargs)

    def validate(self):
        # None of these options are mandatory
        """
        Validates the values provided to this class. If they are not valid,
        this method raises :py:class:`IllegalArgumentException`.
        """
        if (ONDB_DIRECTION in self):
            val = self.get(ONDB_DIRECTION, None)
            if (isinstance(val, Direction)):
                val.validate()
            elif (type(val) is dict):
                dr = Direction(val)
                dr.validate()
                self.__setitem__(ONDB_DIRECTION, dr)
            else:
                raise IllegalArgumentException(
                    ONDB_DIRECTION + ' value must be a Direction object or ' +
                    'a dict.')
        if (ONDB_MAX_RESULTS in self):
            val = self.get(ONDB_MAX_RESULTS)
            Factory._check_int(val, ONDB_MAX_RESULTS)
        if (ONDB_READ_OPTIONS in self):
            val = self.get(ONDB_READ_OPTIONS, None)
            if (isinstance(val, ReadOptions)):
                val.validate()
            elif (type(val) is dict):
                ro = ReadOptions(val)
                ro.validate()
                self.__setitem__(ONDB_READ_OPTIONS, ro)
            else:
                raise IllegalArgumentException(
                    ONDB_READ_OPTIONS + ' value must be a ReadOptions object' +
                    ' or a dict.')


class OperationType(RestrictedDict):
    """
    Specifies the type of operation to be performed. This class functions
    as a dictionary with just one key, which is mandatory:
    ``ONDB_OPERATION_TYPE``. You may specify one of the following values
    for this key:

    * DELETE
        A delete operation.
    * DELETE_IF_VERSION
        A delete operation, but only if the version information is
        satisfied.
    * PUT
        A put operation.
    * PUT_IF_ABSENT
        A put operation, but only if the table row does not currently
        exist.
    * PUT_IF_PRESENT
        A put operation, but only if the table row currently exists.
    * PUT_IF_VERSION
        A put operation, but only if the version information is satisfied.
    """
    _operation_type_allowed_keys = (ONDB_OPERATION_TYPE,)

    def __init__(self, seq=(), **kwargs):
        super(OperationType, self).__init__(
            self._operation_type_allowed_keys, seq, **kwargs)

    def validate(self):
        # Operation type is mandatory
        """
        Validates the values provided to this class. If they are not valid,
        this method raises :py:class:`IllegalArgumentException`.
        """
        val = self.get(ONDB_OPERATION_TYPE, None)
        if (val is not None):
            if (val != ONDB_DELETE and
                val != ONDB_DELETE_IF_VERSION and
                val != ONDB_PUT and
                val != ONDB_PUT_IF_ABSENT and
                val != ONDB_PUT_IF_PRESENT and
                val != ONDB_PUT_IF_VERSION):
                    raise IllegalArgumentException(
                        ONDB_OPERATION_TYPE + ' value must be one of the ' +
                        'following: ' + ONDB_DELETE + ', ' +
                        ONDB_DELETE_IF_VERSION + ', ' + ONDB_PUT + ',' +
                        ONDB_PUT_IF_ABSENT + ', ' + ONDB_PUT_IF_PRESENT +
                        ' or' + ONDB_PUT_IF_VERSION)
        else:
            raise IllegalArgumentException(
                ONDB_OPERATION_TYPE + ' value must not be None.')


class Operation(RestrictedDict):
    """
    Defines a single operation in a sequence of operations. Instances of
    this class are added to a list, which is then used with
    :py:meth:`Store.execute_updates`.

    This class functions as a dictionary with the following keys:

    * ONDB_TABLE_NAME
        The name of the table that the operation is acting upon. This key
        is required.
    * ONDB_OPERATION
        An :py:class:`OperationType` class instance. This key is required.
    * ONDB_ABORT_IF_UNSUCCESSFUL
        Boolean indicating whether the entire operation should be aborted
        if this operation is unsuccessful. If ``True`` the entire operation
        is aborted upon a non-success result. Default is ``False``.
    * ONDB_ROW
        If the operation type is a put, this must be a dictionary or a
        :py:class:`Row` instance representing the data to be written to the
        store. If the operation is a delete, this must be a dictionary
        :py:class:`Row` instance representing the primary key of the row to
        be deleted.
    * ONDB_RETURN_CHOICE
        Specifies whether to return the row value, version, both or
        neither.

        For best performance, it is important to choose only the properties
        that are required. The store is optimized to avoid I/O when the
        requested properties are in cache.

        The value for this key must be one of:

        * ONDB_RC_ALL
            Return both the value and the version.
        * ONDB_RC_NONE
            Do not return the value or the version.
        * ONDB_RC_VALUE
            Return the value only.
        * ONDB_RC_VERSION
            Return the version only.

    * ONDB_VERSION
        A byte array representing the version that must be matched if any
        of the ``xxx_IF_VERSION`` operation types are in use.
    """
    _operation_allowed_keys = (ONDB_TABLE_NAME, ONDB_OPERATION,
        ONDB_ABORT_IF_UNSUCCESSFUL, ONDB_ROW, ONDB_RETURN_CHOICE, ONDB_VERSION)

    def __init__(self, seq=(), **kwargs):
        super(Operation, self).__init__(
            self._operation_allowed_keys, seq, **kwargs)

    def validate(self):
        # ONDB_TABLE_NAME, ONDB_OPERATION and ONDB_ROW are mandatory
        """
        Validates the values provided to this class. If they are not valid,
        this method raises :py:class:`IllegalArgumentException`.
        """
        val = self.get(ONDB_TABLE_NAME, None)
        if (val is not None):
            Factory._check_str(val, ONDB_TABLE_NAME)
        else:
            raise IllegalArgumentException(
                ONDB_TABLE_NAME + ' must be included in an Operation.')
        op = self.get(ONDB_OPERATION, None)
        if (op is not None):
            if (isinstance(op, OperationType)):
                op.validate()
            elif (type(op) is dict):
                ot = OperationType(op)
                ot.validate()
                self.__setitem__(ONDB_OPERATION, ot)
            else:
                raise IllegalArgumentException(
                    ONDB_OPERATION + ' value must be a OperationType object' +
                    ' or a dict.')
        else:
            raise IllegalArgumentException(
                ONDB_OPERATION + ' must be included in an Operation.')
        val = self.get(ONDB_ABORT_IF_UNSUCCESSFUL, None)
        if (val is not None and
            type(val) is not bool):
                raise IllegalArgumentException(
                    ONDB_ABORT_IF_UNSUCCESSFUL + ' value must be a boolean.')
        if (ONDB_ROW in self):
            val = self.get(ONDB_ROW, None)
            if (val is None or not isinstance(val, (Row, dict))):
                raise IllegalArgumentException(
                    ONDB_ROW + ' value must be a Row object or ' +
                    'a dict.')
        else:
            raise IllegalArgumentException(
                ONDB_ROW + ' must be included in an Operation.')
        if (ONDB_RETURN_CHOICE in self):
            val = self.get(ONDB_RETURN_CHOICE)
            if (not self.validate_return_opts(val)):
                raise IllegalArgumentException(
                    ONDB_RETURN_CHOICE + ' value is not valid - ' +
                    val)
        # ONDB_VERSION is mandatory in case of PUT_IF_VERSION or
        # DELETE_IF_VERSION
        val = self.get(ONDB_VERSION, None)
        op_str = op.get(ONDB_OPERATION_TYPE)
        if (val is not None):
            if (type(val) is not bytearray and
                type(val) is not str):
                raise IllegalArgumentException(
                    ONDB_VERSION + ' value must be a bytearray.')
        elif (op_str == ONDB_DELETE_IF_VERSION or
            op_str == ONDB_PUT_IF_VERSION):
                raise IllegalArgumentException(
                    ONDB_VERSION + ' value must exist when ' +
                    ONDB_OPERATION_TYPE + ' value is ' + ONDB_DELETE_IF_VERSION
                    + ' or ' + ONDB_PUT_IF_VERSION)


# Constants for the module ids for version and status
ONDB_JAVA_CLIENT_ID = TModuleInfo.JAVA_CLIENT
ONDB_PROXY_SERVER_ID = TModuleInfo.PROXY_SERVER
_array_of_module_ids = [ONDB_JAVA_CLIENT_ID, ONDB_PROXY_SERVER_ID]


class Store:
    """
    A handle to a store that is running remotely. Use this handle to
    perform all store read and write operations, as well as to execute
    grouped sequences of operations. To create an instance of
    this class, use :py:meth:`Factory.open`.
    """

    def __init__(self, client, transport, store_config):
        self._client = client
        self._transport = transport
        self._store_config = store_config

    def _set_default_durability(self, wo):
        if (wo.get(ONDB_DURABILITY, None) is None):
            def_val = COMMIT_NO_SYNC
            if (self._store_config.get_durability() is not None):
                    def_val = self._store_config.get_durability()
            wo[ONDB_DURABILITY] = def_val
        return wo

    def _set_default_consistency(self, ro):
        if (ro.get(ONDB_CONSISTENCY, None) is None and
            ro.get(ONDB_TIME_CONSISTENCY, None) is None and
            ro.get(ONDB_VERSION_CONSISTENCY, None) is None):
                def_val = NONE_REQUIRED
                if (self._store_config.get_consistency() is not None):
                        def_val = self._store_config.get_consistency()
                ro[ONDB_CONSISTENCY] = def_val
        return ro

    def _set_default_max_results(self, tio):
        max_res = tio.get(ONDB_MAX_RESULTS, None)
        if (max_res is None or int(max_res) <= 0):
            max_conf = self._store_config.get_max_results()
            if (max_conf > 0):
                tio[ONDB_MAX_RESULTS] = max_conf
        return tio

    def _set_default_timeout(self, opt):
        to = opt.get(ONDB_TIMEOUT, None)
        if (to is None or int(to) <= 0):
            def_to = self._store_config.get_request_timeout()
            if (def_to > 0):
                opt[ONDB_TIMEOUT] = def_to
        return opt

    # This method is called for each API operation performed in order to
    # validate the parameters before sending them to a server.  If the piece
    # of data is already an instance of an expected class it is assumed to be
    # validated already.  If it is a raw dictionary, an instance of the
    # expected class is created to perform the validation.  Validation failures
    # raise IllegalArgumentException.
    def _validate_data(self, data, option):
        if (type(data) is dict):
            if (option == 'WO' and not isinstance(data, WriteOptions)):
                data = WriteOptions(data)
            elif (option == 'RO' and not isinstance(data, ReadOptions)):
                data = ReadOptions(data)
            elif (option == 'MRO' and not isinstance(data, MultiRowOptions)):
                data = MultiRowOptions(data)
            elif (option == 'TIO' and
                not isinstance(data, TableIteratorOptions)):
                    data = TableIteratorOptions(data)
            elif (option == 'OP' and not isinstance(data, Operation)):
                data = Operation(data)
            elif (option == 'ROW' and not isinstance(data, Row)):
                data = Row(data)
            else:
                raise IllegalArgumentException('{0} is not a valid option'.
                    format(option))
                logger.error(option + ' is not a valid option')
        elif (option == 'ROW' and not isinstance(data, (Row, dict))):
            raise IllegalArgumentException(
                ONDB_ROW + ' value must be a dict or a Row. Got: ' + data)
            logger.error('{0} value must be a dict or a Row. Got: {1}'.
                format(ONDB_ROW, data))
        if (data is not None and option != 'ROW'):
            data.validate()
        # set values based on StoreConfig and system defaults
        if (option == 'WO'):
            if (data is None):
                data = {}
            data = self._set_default_durability(data)
            data = self._set_default_timeout(data)
        if (option == 'RO'):
            if (data is None):
                data = {}
            data = self._set_default_consistency(data)
            data = self._set_default_timeout(data)
        if (option == 'TIO'):
            if (data is None):
                data = {}
            data = self._set_default_max_results(data)
            ro = data.get(ONDB_READ_OPTIONS, None)
            if (ro is None):
                ro = {}
            ro = self._set_default_consistency(ro)
            ro = self._set_default_timeout(ro)
            data[ONDB_READ_OPTIONS] = ro
        return data

    def get_store_config(self):
        """
        Returns the :py:class:`StoreConfig` object used by this handle to
        the store.
        """
        return self._store_config

    def put(self, table, row, write_opts=None):
        """
        Write a row of data to the table. If the specified row already
        exists, it is modified. If it does not exist, a new row is added to
        the table. Both of these default behaviors can be modified through
        the use of a :py:class:`WriteOptions` class instance.

        :param table: The name of the table to be written. This parameter
                    is required.
        :param row: The row to be written as a :py:class:`Row` class
                    instance. This parameter is required.
        :param write_opts: A :py:class:`WriteOptions` class instance.
        :return: A tuple, the contents of which differs. If the operation
            was unsucessful, the tuple contains ``(False, None)``. If
            sucessful then:

            * ``(True, NONE)`` if ``ONDB_RETURN_CHOICE`` on the
              :py:class:`WriteOptions` provided to this call is
              ``NONE``.
            * ``(True, <Row>)`` where ``<Row>`` contains the old row's data
              and version if ``ONDB_RETURN_CHOICE`` on the
              :py:class:`WriteOptions` provided to this call is ``ALL``.
            * ``(True, <Row>)`` where ``<Row>`` contains just the old row's
              data if ``ONDB_RETURN_CHOICE`` on the
              :py:class:`WriteOptions` provided to this call is ``VALUE``.
            * ``(True, <Row>)`` where ``<Row>`` contains just the old row's
              version if ``ONDB_RETURN_CHOICE`` on the
              :py:class:`WriteOptions` provided to this call is ``VERSION``.

        :raise DurabilityException: If the durability guarantee set for
                this write operation cannot be satisfied.
        :raise RequestTimeoutException: If the requested timeout interval
                in use for this call is exceeded.
        :raise FaultException: If the operation cannot be completed for any
                reason.
        :raise IllegalArgumentException: If an incorrect argument is used,
                or a required argument is missing.
        :raise ProxyException: If communication with the proxy server somehow
                failed.
        """
        try:
            # get the data and the options translated
            logger.debug('row: {0}'.format(row))
            row = self._validate_data(row, 'ROW')
            t_data = DataManager.dict_to_trow(row)
            # be sure that we are dealing with a WriteOptions object
            write_opts = self._validate_data(write_opts, 'WO')
            w_opts = DataManager.from_json_to_twrite_options(write_opts)
            logger.debug('write_opts to use: {0}'.format(w_opts))
            # check what specific flavor of put is going to be used
            # and invoke it
            done = False
            if (write_opts is not None):
                if (write_opts.get(ONDB_IF_ABSENT, False)):
                    result = self._client.putIfAbsent(table, t_data, w_opts)
                    done = True
                elif (write_opts.get(ONDB_IF_PRESENT, False)):
                    result = self._client.putIfPresent(table, t_data, w_opts)
                    done = True
                elif (write_opts.get(ONDB_IF_VERSION, None) is not None):
                    result = self._client.putIfVersion(
                        table,
                        t_data,
                        write_opts[ONDB_IF_VERSION],
                        w_opts)
                    done = True
            if (not done):
                result = self._client.put(table, t_data, w_opts)
            # translate the result as appropiate and return
            data = None
            o_version = None
            if (write_opts is not None):
                # are we going to need the previous version?
                if (write_opts.get(ONDB_RETURN_CHOICE, None) == ONDB_RC_ALL or
                   write_opts.get(ONDB_RETURN_CHOICE, None) == ONDB_RC_VERSION):
                    o_version = result.previousRowVersion
                # are we going to need the previous row?
                if (write_opts.get(ONDB_RETURN_CHOICE, None) == ONDB_RC_ALL or
                   write_opts.get(ONDB_RETURN_CHOICE, None) == ONDB_RC_VALUE):
                    data = DataManager.trow_to_dict(result.previousRow)
            row.set_expiration(result.expiration)
            if (data is not None):
                returned_row = Row(data)
            else:
                returned_row = Row()
            returned_row.set_version(o_version)
            returned_row.set_table_name(table)
            return result.currentRowVersion, returned_row
        except TDurabilityException, de:
            raise DurabilityException(str(de))
        except TRequestTimeoutException, re:
            raise RequestTimeoutException(str(re))
        except TFaultException, fe:
            raise FaultException(str(fe))
        except TIllegalArgumentException, ae:
            raise IllegalArgumentException(str(ae))
        except TProxyException, pe:
            raise ProxyException(str(pe))

    def put_if_absent(self, table, row, write_opts=None):
        """
        Write a row of data to the table if and only if the row is not
        already present in the table.

        :param table: The name of the table to be written. This parameter
                    is required.
        :param row: The row to be written as a :py:class:`Row` class
                    instance. This parameter is required.
        :param write_opts: A :py:class:`WriteOptions` class instance.
        :return: A tuple, the contents of which differs. If the operation
            was unsucessful, the tuple contains ``(False, None)``. If
            sucessful then:

            * ``(True, NONE)`` if ``ONDB_RETURN_CHOICE`` on the
              :py:class:`WriteOptions` provided to this call is
              ``NONE``.
            * ``(True, <Row>)`` where ``<Row>`` contains the old row's data
              and version if ``ONDB_RETURN_CHOICE`` on the
              :py:class:`WriteOptions` provided to this call is ``ALL``.
            * ``(True, <Row>)`` where ``<Row>`` contains just the old row's
              data if ``ONDB_RETURN_CHOICE`` on the
              :py:class:`WriteOptions` provided to this call is ``VALUE``.
            * ``(True, <Row>)`` where ``<Row>`` contains just the old row's
              version if ``ONDB_RETURN_CHOICE`` on the
              :py:class:`WriteOptions` provided to this call is ``VERSION``.

        :raise DurabilityException: If the durability guarantee set for
                this write operation cannot be satisfied.
        :raise RequestTimeoutException: If the requested timeout interval
                in use for this call is exceeded.
        :raise FaultException: If the operation cannot be completed for any
                reason.
        :raise IllegalArgumentException: If an incorrect argument is used,
                or a required argument is missing.
        :raise ProxyException: If communication with the proxy server somehow
                failed.
        """
        # set ONDB_IF_ABSENT to True in write options
        if (write_opts is not None):
            write_opts[ONDB_IF_ABSENT] = True
            write_opts[ONDB_IF_PRESENT] = False
            write_opts[ONDB_IF_VERSION] = None
        else:
            write_opts = WriteOptions({ONDB_IF_ABSENT: True})
        return self.put(table, row, write_opts)

    def put_if_present(self, table, row, write_opts=None):
        """
        Write a row of data to the table if and only if the row is
        already present in the table.

        :param table: The name of the table to be written. This parameter
                    is required.
        :param row: The row to be written as a :py:class:`Row` class
                    instance. This parameter is required.
        :param write_opts: A :py:class:`WriteOptions` class instance.
        :return: A tuple, the contents of which differs. If the operation
            was unsucessful, the tuple contains ``(False, None)``. If
            sucessful then:

            * ``(True, NONE)`` if ``ONDB_RETURN_CHOICE`` on the
              :py:class:`WriteOptions` provided to this call is
              ``NONE``.
            * ``(True, <Row>)`` where ``<Row>`` contains the old row's data
              and version if ``ONDB_RETURN_CHOICE`` on the
              :py:class:`WriteOptions` provided to this call is ``ALL``.
            * ``(True, <Row>)`` where ``<Row>`` contains just the old row's
              data if ``ONDB_RETURN_CHOICE`` on the
              :py:class:`WriteOptions` provided to this call is ``VALUE``.
            * ``(True, <Row>)`` where ``<Row>`` contains just the old row's
              version if ``ONDB_RETURN_CHOICE`` on the
              :py:class:`WriteOptions` provided to this call is ``VERSION``.

        :raise DurabilityException: If the durability guarantee set for
                this write operation cannot be satisfied.
        :raise RequestTimeoutException: If the requested timeout interval
                in use for this call is exceeded.
        :raise FaultException: If the operation cannot be completed for any
                reason.
        :raise IllegalArgumentException: If an incorrect argument is used,
                or a required argument is missing.
        :raise ProxyException: If communication with the proxy server somehow
                failed.
        """
        # set ONDB_IF_PRESENT to True in write options
        if (write_opts is not None):
            write_opts[ONDB_IF_ABSENT] = False
            write_opts[ONDB_IF_PRESENT] = True
            write_opts[ONDB_IF_VERSION] = None
        else:
            write_opts = WriteOptions({ONDB_IF_PRESENT: True})
        return self.put(table, row, write_opts)

    def put_if_version(self, table, row, version, write_opts=None):
        """
        Write a row of data to the table if and only if the row currently
        existing in the store has a version that is equal to the version
        provided here.

        :param table: The name of the table to be written. This parameter
                    is required.
        :param row: The row to be written as a :py:class:`Row` class
                    instance. This parameter is required.
        :param version: The version that must match the version currently in
                the store in order for this write operation to proceed.
                This parameter is required.
        :param write_opts: A :py:class:`WriteOptions` class instance.
        :return: A tuple, the contents of which differs. If the operation
            was unsucessful, the tuple contains ``(False, None)``. If
            sucessful then:

            * ``(True, NONE)`` if ``ONDB_RETURN_CHOICE`` on the
              :py:class:`WriteOptions` provided to this call is
              ``NONE``.
            * ``(True, <Row>)`` where ``<Row>`` contains the old row's data
              and version if ``ONDB_RETURN_CHOICE`` on the
              :py:class:`WriteOptions` provided to this call is ``ALL``.
            * ``(True, <Row>)`` where ``<Row>`` contains just the old row's
              data if ``ONDB_RETURN_CHOICE`` on the
              :py:class:`WriteOptions` provided to this call is ``VALUE``.
            * ``(True, <Row>)`` where ``<Row>`` contains just the old row's
              version if ``ONDB_RETURN_CHOICE`` on the
              :py:class:`WriteOptions` provided to this call is ``VERSION``.

        :raise DurabilityException: If the durability guarantee set for
                write operation cannot be satisfied.
        :raise RequestTimeoutException: If the requested timeout interval
                in use for this call is exceeded.
        :raise FaultException: If the operation cannot be completed for any
                reason.
        :raise IllegalArgumentException: If an incorrect argument is used,
                or a required argument is missing.
        :raise ProxyException: If communication with the proxy server somehow
                failed.
        """
        # set ONDB_IF_VERSION to True in write options
        if (write_opts is not None):
            write_opts[ONDB_IF_ABSENT] = False
            write_opts[ONDB_IF_PRESENT] = False
            write_opts[ONDB_IF_VERSION] = version
        else:
            write_opts = WriteOptions({ONDB_IF_VERSION: version})
        return self.put(table, row, write_opts)

    def get(self, table, primary_key, read_opts=None):
        """
        Get the row associated with the primary key.

        :param table: The name of the table from which you want to retrieve
                the row. This parameter is required.
        :param primary_key: A :py:class:`Row` object populated with the
                primary key fields for the row that you want returned.
                Alternatively, this can be a simple dictionary populated
                with the primary key fields. This parameter is required.
        :param read_opts: A :py:class:`ReadOptions` class instance.
        :return: A :py:class:`Row` object, or ``None`` if the primary key
                does not exist on the named table.
        :raise ConsistencyException: If the consistency policy set for
                read operation cannot be satisfied.
        :raise RequestTimeoutException: If the requested timeout interval
                in use for this call is exceeded.
        :raise FaultException: If the operation cannot be completed for any
                reason.
        :raise IllegalArgumentException: If an incorrect argument is used,
                or a required argument is missing.
        :raise ProxyException: If communication with the proxy server somehow
                failed.
        """
        try:
            logger.debug('row: {0}'.format(primary_key))
            # check read options and primary key here
            read_opts = self._validate_data(read_opts, 'RO')
            primary_key = self._validate_data(primary_key, 'ROW')
            t_pkey = DataManager.dict_to_trow(primary_key)
            r_opts = DataManager.from_json_to_tread_options(read_opts)
            # now get the data
            t_vdata = self._client.get(table, t_pkey, r_opts)
            # prepare the data as expected and return
            if (t_vdata.currentRow is not None):
                data = DataManager.trow_to_dict(t_vdata.currentRow)
                returned_row = Row(data)
                returned_row.set_table_name(table)
                returned_row.set_version(t_vdata.currentRowVersion)
                returned_row.set_expiration(t_vdata.expiration)
            else:
                returned_row = None
            return returned_row
        except TConsistencyException, ce:
            raise ConsistencyException(str(ce))
        except TRequestTimeoutException, re:
            raise RequestTimeoutException(str(re))
        except TFaultException, fe:
            raise FaultException(str(fe))
        except TIllegalArgumentException, ae:
            raise IllegalArgumentException(str(ae))
        except TProxyException, pe:
            raise ProxyException(str(pe))

    def multi_get(self, table, partial_key, key_only, multirow_opts=None,
                  read_opts=None):
        """
        Return a list of rows/keys associated with a partial primary key in
        an atomic manner. Rows or keys are returned in primary key order.
        The key provided to this method may be a partial key, but it
        must contain all of the fields defined for the table's shard key.

        :param table: The name of the table from which you want to retrieve
            the rows or keys. This parameter is required.
        :param partial_key: A :py:class:`Row` object containing a primary
            key, which can be a partial key but it must contain the entire
            shard key. This parameter is required.
        :param key_only: A boolean indicating whether you want just keys
            returned, or the entire row. ``True`` if you want just keys.
            This parameter is required.
        :param multirow_opts: A :py:class:`MultiRowOptions` instance.
        :param read_opts: A :py:class:`ReadOptions` instance.
        :return: A list of rows or keys that matches the provided shard key.
        :raise ConsistencyException: If the consistency policy set for
                read operation cannot be satisfied.
        :raise RequestTimeoutException: If the requested timeout interval
                in use for this call is exceeded.
        :raise FaultException: If the operation cannot be completed for any
                reason.
        :raise IllegalArgumentException: If an incorrect argument is used,
                or a required argument is missing.
        :raise ProxyException: If communication with the proxy server somehow
                failed.
        """
        try:
            # verify the partial key, key_only and the multirow options
            logger.debug('partial_key: {0}'.format(partial_key))
            partial_key = self._validate_data(partial_key, 'ROW')
            t_pkey = DataManager.dict_to_trow(partial_key)
            logger.debug('key_only: {0}'.format(key_only))
            Factory._check_bool(key_only, 'key_only')
            logger.debug('multirow_opts: {0}'.format(multirow_opts))
            multirow_opts = self._validate_data(multirow_opts, 'MRO')
            # get them translated
            f_range, inc_tables = DataManager.from_json_to_tmultirow_options(
                multirow_opts)
            # verify the read options and get them translated
            logger.debug('read_opts: {0}'.format(read_opts))
            read_opts = self._validate_data(read_opts, 'RO')
            r_opts = DataManager.from_json_to_tread_options(
                read_opts)
            # get the result from proxy
            if (key_only):
                ret = self._client.multiGetKeys(
                    table, t_pkey, f_range, inc_tables, r_opts)
            else:
                ret = self._client.multiGet(
                    table, t_pkey, f_range, inc_tables, r_opts)
            # get all as a list of Rows and return them
            r_rows = []
            for row in ret.rowsWithMetadata:
                ret_row = Factory._get_returned_row(row, ret.idToTableNames)
                r_rows.append(ret_row)
            logger.debug('returning {0} rows'.format(len(r_rows)))
            return r_rows
        except TConsistencyException, ce:
            raise ConsistencyException(str(ce))
        except TRequestTimeoutException, re:
            raise RequestTimeoutException(str(re))
        except TFaultException, fe:
            raise FaultException(str(fe))
        except TIllegalArgumentException, ie:
            raise IllegalArgumentException(str(ie))
        except TProxyException, pe:
            raise ProxyException(str(pe))

    def delete(self, table, primary_key, write_opts=None):
        """
        Deletes a row of data from the named table in the store.

        :param table: The name of the table from which the row is to be
            deleted. This parameter is required.
        :param primary_key: A :py:class:`Row` object populated with the
                primary key fields for the row that you want returned.
                Alternatively, this can be a simple dictionary populated
                with the primary key fields. This parameter is required.
        :param write_opts: A :py:class:`WriteOptions` class instance.
        :return: A tuple, the contents of which differs. If the operation
            was unsucessful, the tuple contains ``(False, None)``. If
            sucessful then:

            * ``(True, NONE)`` if ``ONDB_RETURN_CHOICE`` on the
              :py:class:`WriteOptions` provided to this call is
              ``NONE``.
            * ``(True, <Row>)`` where ``<Row>`` contains the old row's data
              and version if ``ONDB_RETURN_CHOICE`` on the
              :py:class:`WriteOptions` provided to this call is ``ALL``.
            * ``(True, <Row>)`` where ``<Row>`` contains just the old row's
              data if ``ONDB_RETURN_CHOICE`` on the
              :py:class:`WriteOptions` provided to this call is ``VALUE``.
            * ``(True, <Row>)`` where ``<Row>`` contains just the old row's
              version if ``ONDB_RETURN_CHOICE`` on the
              :py:class:`WriteOptions` provided to this call is ``VERSION``.

        :raise DurabilityException: If the durability guarantee set for
                this write operation cannot be satisfied.
        :raise RequestTimeoutException: If the requested timeout interval
                in use for this call is exceeded.
        :raise FaultException: If the operation cannot be completed for any
                reason.
        :raise IllegalArgumentException: If an incorrect argument is used,
                or a required argument is missing.
        :raise ProxyException: If communication with the proxy server somehow
                failed.
        """
        try:
            # verify the write options and the primary key
            logger.debug('write_opts: {0}'.format(write_opts))
            write_opts = self._validate_data(write_opts, 'WO')
            w_opts = DataManager.from_json_to_twrite_options(write_opts)
            logger.debug('primary_key: {0}'.format(primary_key))
            primary_key = self._validate_data(primary_key, 'ROW')
            t_row = DataManager.dict_to_trow(primary_key)
            if (write_opts is not None and
               write_opts.get(ONDB_IF_VERSION, None) is not None):
                # deleteIfVersion
                t_write_result = self._client.deleteRowIfVersion(
                    table, t_row, write_opts[ONDB_IF_VERSION], w_opts)
            else:
                # normal delete
                t_write_result = self._client.deleteRow(
                    table, t_row, w_opts)
            # check if the delete was performed correctly,
            # if not then return False and None
            if (t_write_result.wasDeleted):
                # check if we are going to return the old value or
                # the old version
                logger.debug('row deleted')
                if (write_opts is not None):
                    ret_val_ver = write_opts.get(ONDB_RETURN_CHOICE, None)
                else:
                    ret_val_ver = None
                logger.debug('RETURN_CHOICE: {0}'.format(ret_val_ver))
                if (not (ret_val_ver is None or ret_val_ver == ONDB_RC_NONE)):
                    if (ret_val_ver == ONDB_RC_VALUE or
                        ret_val_ver == ONDB_RC_ALL):
                            data = DataManager.trow_to_dict(
                                t_write_result.previousRow)
                            r_row = Row(data)
                    else:
                        r_row = Row({})
                    if (ret_val_ver == ONDB_RC_VERSION or
                        ret_val_ver == ONDB_RC_ALL):
                            r_row.set_version(t_write_result.previousRowVersion)
                else:
                    r_row = None
                # check what is going to be returned
                if (r_row is not None):
                    r_row.set_table_name(table)
                return True, r_row
            else:
                return False, None
        except TDurabilityException, de:
            raise DurabilityException(str(de))
        except TRequestTimeoutException, re:
            raise RequestTimeoutException(str(re))
        except TFaultException, fe:
            raise FaultException(str(fe))
        except TIllegalArgumentException, ie:
            raise IllegalArgumentException(str(ie))
        except TProxyException, pe:
            raise ProxyException(str(pe))

    def delete_if_version(self, table, primary_key, version, write_opts=None):
        """
        Deletes a row of data from the named table in the store if and only
        if the row currently existing in the store has a version that is
        equal to the version provided here.

        :param table: The name of the table from which the row is to be
            deleted. This parameter is required.
        :param primary_key: A :py:class:`Row` object populated with the
                primary key fields for the row that you want returned.
                Alternatively, this can be a simple dictionary populated
                with the primary key fields. This parameter is required.
        :param version: The version that must match the version currently in
                the store in order for this delete operation to proceed.
                This parameter is required.
        :param write_opts: A :py:class:`WriteOptions` class instance.
        :return: A tuple, the contents of which differs. If the operation
            was unsucessful, the tuple contains ``(False, None)``. If
            sucessful then:

            * ``(True, NONE)`` if ``ONDB_RETURN_CHOICE`` on the
              :py:class:`WriteOptions` provided to this call is
              ``NONE``.
            * ``(True, <Row>)`` where ``<Row>`` contains the old row's data
              and version if ``ONDB_RETURN_CHOICE`` on the
              :py:class:`WriteOptions` provided to this call is ``ALL``.
            * ``(True, <Row>)`` where ``<Row>`` contains just the old row's
              data if ``ONDB_RETURN_CHOICE`` on the
              :py:class:`WriteOptions` provided to this call is ``VALUE``.
            * ``(True, <Row>)`` where ``<Row>`` contains just the old row's
              version if ``ONDB_RETURN_CHOICE`` on the
              :py:class:`WriteOptions` provided to this call is ``VERSION``.

        :raise DurabilityException: If the durability guarantee set for
                this write operation cannot be satisfied.
        :raise RequestTimeoutException: If the requested timeout interval
                in use for this call is exceeded.
        :raise FaultException: If the operation cannot be completed for any
                reason.
        :raise IllegalArgumentException: If an incorrect argument is used,
                or a required argument is missing.
        :raise ProxyException: If communication with the proxy server somehow
                failed.
        """
        # prepare the write_opts to make delete() work as delete_if_version()
        if (write_opts is not None):
            write_opts[ONDB_IF_VERSION] = version
        else:
            write_opts = WriteOptions({ONDB_IF_VERSION: version})
        return self.delete(table, primary_key, write_opts)

    def multi_delete(self, table, partial_key, multirow_opts=None,
                     write_opts=None):
        """
        Delete all rows matching a primary key in an atomic
        manner. The provided primary key may be a partial key, but it must
        contain the row's full shard key.

        :param table: The name of the table from which you want to delete
            the rows. This parameter is required.
        :param partial_key: A :py:class:`Row` object containing the shard
            key (which can also be a full or partial primary key). This
            parameter is required.
        :param multirow_opts: A :py:class:`MultiRowOptions` instance.
        :param write_opts: A :py:class:`WriteOptions` instance.
        :return: The number of rows deleted.
        :raise DurabilityException: If the durability guarantee set for
                this write operation cannot be satisfied.
        :raise RequestTimeoutException: If the requested timeout interval
                in use for this call is exceeded.
        :raise FaultException: If the operation cannot be completed for any
                reason.
        :raise IllegalArgumentException: If an incorrect argument is used,
                or a required argument is missing.
        :raise ProxyException: If communication with the proxy server somehow
                failed.
        """
        try:
            # verify the writeopts, the multirow options and the partial key
            logger.debug('write_opts: {0}'.format(write_opts))
            write_opts = self._validate_data(write_opts, 'WO')
            w_opts = DataManager.from_json_to_twrite_options(write_opts)
            logger.debug('partial_key: {0}'.format(partial_key))
            partial_key = self._validate_data(partial_key, 'ROW')
            t_row = DataManager.dict_to_trow(partial_key)
            logger.debug('multirow_opts: {0}'.format(multirow_opts))
            multirow_opts = self._validate_data(multirow_opts, 'MRO')
            # get them translated
            t_field_range, t_tables = \
                DataManager.from_json_to_tmultirow_options(multirow_opts)
            # and get the result from the proxy and return the amount
            # of deletes
            amount_deletes = self._client.multiDelete(
                table, t_row, t_field_range, t_tables, w_opts)
            logger.debug('%d rows were deleted', amount_deletes)
            return amount_deletes
        except TDurabilityException, de:
            raise DurabilityException(str(de))
        except TRequestTimeoutException, re:
            raise RequestTimeoutException(str(re))
        except TFaultException, fe:
            raise FaultException(str(fe))
        except TIllegalArgumentException, ie:
            raise IllegalArgumentException(str(ie))
        except TProxyException, pe:
            raise ProxyException(str(pe))

    def execute_updates(self, ex_list, write_opts=None):
        """
        This method provides an efficient and transactional mechanism for
        executing a sequence of operations associated with tables that
        share the same shard key portion of their primary keys. The
        efficiency results from the use of a single network interaction to
        accomplish the entire sequence of operations.

        The operations passed to this method are a list of
        :py:class:`Operation` objects.

        All the operations specified are executed within the scope of a
        single transaction that effectively provides serializable
        isolation. The transaction is started and either committed or
        aborted by this method. If the method returns without throwing an
        exception, then all operations were executed atomically, the
        transaction was committed, and the returned list contains the
        result of each operation.

        If the transaction is aborted for any reason, an exception is
        thrown. An abort may occur for two reasons:

        #. An operation or transaction results in an exception that is
           considered a fault, such as a durability or consistency error, a
           failure due to message delivery or networking error, and so forth.
        #. An individual operation returns normally but is unsuccessful as
           defined by the particular operation (that is, a delete operation
           for a non-existent key) and ``True`` was specified for the
           operation's ``ONDB_ABORT_IF_UNSUCCESSFUL`` key.

        Operations are not executed in the sequence they appear in the
        operations list, but are instead executed in an internally defined
        sequence that prevents deadlocks. Additionally, if there are two
        operations for the same key, their relative order of execution is
        arbitrary; this should be avoided.

        :param ex_list: A list of :py:class:`Operation` objects. This
            parameter is required.
        :param write_opts: A :py:class:`WriteOptions` class instance.
        :return: An iterable object, each element of which is a
            :py:class:`OperationResult` class object.
        :raise OperationExecutionException: If any operation failed when
            the operation's ``ONDB_ABORT_IF_UNSUCCESSFUL`` key is set to
            ``True``.
        :raise DurabilityException: If the durability guarantee set for
                this write operation cannot be satisfied.
        :raise RequestTimeoutException: If the requested timeout interval
                in use for this call is exceeded.
        :raise FaultException: If the operation cannot be completed for any
                reason.
        :raise IllegalArgumentException: If an incorrect argument is used,
                or a required argument is missing.
        :raise ProxyException: If communication with the proxy server somehow
                failed.
        """
        try:
            # verify the list of operations and the write options
            logger.debug('amount of opertions to execute: {0}'.
                format(len(ex_list)))
            if (type(ex_list) is not list):
                raise IllegalArgumentException('ex_list parameter must be' +
                ' a list of Operations.')
            for i in range(len(ex_list)):
                op = ex_list[i]
                op = self._validate_data(op, 'OP')
                ex_list[i] = op
            opers = DataManager.from_json_to_toperations(ex_list)
            logger.debug('write_opts: {0}'.format(write_opts))
            write_opts = self._validate_data(write_opts, 'WO')
            # check that there is no ONDB_RETURN_CHOICE set in WriteOptions
            if (write_opts is not None):
                if (write_opts.get(ONDB_RETURN_CHOICE, None) is not None):
                    raise IllegalArgumentException(
                        'WriteOptions must not contain ' +
                        ONDB_RETURN_CHOICE + ' when used in' +
                        'execute_updates().')
            # get the write options translated, execute the operations
            # and return the results
            w_opts = DataManager.from_json_to_twrite_options(write_opts)
            res = self._client.executeUpdates(opers, w_opts)
            return Factory.from_twrite_result_to_write_result_array(res)
        except TDurabilityException, de:
            raise DurabilityException(str(de))
        except TTableOpExecutionException, ee:
            raise OperationExecutionException(str(ee))
        except TFaultException, e:
            raise FaultException(str(e))
        except TIllegalArgumentException, e:
            raise IllegalArgumentException(str(e))
        except TProxyException, e:
            raise ProxyException(str(e))

    def execute_sync(self, command):
        """
        Synchronously execute a DDL statement. The method will only
        return when the statement has finished.

        :param command: The DDL statement to be executed. This parameter is
            required.
        :return: A :py:class:`StatementResult` object.
        :raise ExecutionException: If any operation failed when
            the operation's ``ONDB_ABORT_IF_UNSUCCESSFUL`` key is set to
            ``True``.
        :raise FaultException: If the operation cannot be completed for any
                reason.
        :raise IllegalArgumentException: If an incorrect argument is used,
                or a required argument is missing.
        :raise ProxyException: If communication with the proxy server somehow
                failed.
        """
        try:
            # verify that the command is a valid string and execute it
            logger.debug('command to execute: {0}'.format(command))
            if (type(command) is str or type(command) is unicode):
                res = self._client.executeSyncV2(command)
                if (res.isSuccessful is not True):
                    raise ExecutionException(res.errorMessage)
                return Factory._from_tstatement_result_to_statement_result(
                    res)
            else:
                logger.error('invalid format for command')
                raise IllegalArgumentException('Command passed must be a' +
                                               ' string')
        except TFaultException, e:
            raise FaultException(str(e))
        except TIllegalArgumentException, e:
            raise IllegalArgumentException(str(e))
        except TProxyException, e:
            raise ProxyException(str(e))

    def execute(self, command):
        """
        Asynchronously execute a DDL statement. The method returns
        immediately without waiting for the statement to run in the store.

        :param command: The DDL statement to be executed. This parameter is
            required.
        :return: An :py:class:`ExecutionFuture` object.
        :raise FaultException: If the operation cannot be completed for any
                reason.
        :raise IllegalArgumentException: If an incorrect argument is used,
                or a required argument is missing.
        :raise ProxyException: If communication with the proxy server somehow
                failed.
        """
        try:
            # verify that the command is a valid string and execute it
            logger.debug('command to execute: {0}'.format(command))
            if (type(command) is str or type(command) is unicode):
                res = self._client.executeV2(command)
                return Factory._from_tstatement_result_to_execution_future(
                    res, self._client)
            else:
                logger.error('invalid command format')
                raise IllegalArgumentException('Command passed must be a' +
                                               ' string')
        except TFaultException, e:
            raise FaultException(str(e))
        except TIllegalArgumentException, e:
            raise IllegalArgumentException(str(e))
        except TProxyException, e:
            raise ProxyException(str(e))

    def get_execution_future(self, execution_id):
        """
        Returns the :py:class:`ExecutionFuture` object encapsulating the
        operation represented by the provided execution future ID. You can
        obtain the execution future ID using
        :py:meth:`ExecutionFuture.get_execution_id`.

        :raise IllegalArgumentException: If an incorrect argument is used,
                or a required argument is missing.
        """
        if (not isinstance(execution_id, (bytearray, str))):
            logger.exception('execution_id is not bytearray nor str.')
            raise IllegalArgumentException(
                'execution_id must be a bytearray or str.')
        return ExecutionFuture(None, execution_id, self._client)

    def refresh_tables(self):
        """
        Refresh table changes so that they are available to your client
        code. Nothing is returned by this method. Use this method if you
        suspect that table data has been altered. That is, if you
        suspect there are new tables added to the store, old tables altered
        in the store, indexes added or altered, and so forth.

        :raise FaultException: If the operation cannot be completed for any
                reason.
        """
        try:
            self._client.refreshTables()
        except TFaultException, fe:
            raise FaultException(str(fe))

    def _prepare_iterator_params(self, table, partial_key, index_name,
                                 key_only, multirow_opts,
                                 table_iterator_opts):
        # This method verifies all formats and convert to the appropiate
        # version needed for calling the proxy
        # verify the multirow options
        logger.debug('multirow_opts: {0}'.format(multirow_opts))
        multirow_opts = self._validate_data(multirow_opts, 'MRO')
        # get the options translated
        t_field_range, t_tables = \
            DataManager.from_json_to_tmultirow_options(multirow_opts)
        logger.debug('table_iterator_opts: {0}'.format(table_iterator_opts))
        table_iterator_opts = self._validate_data(
            table_iterator_opts, 'TIO')
        td, mr, tro = DataManager.from_json_to_ttable_iterator_options(
            table_iterator_opts)
        # check the partial key format and convert it to TRow
        if(partial_key is not None):
            logger.debug('key: {0}'.format(partial_key))
            partial_key = self._validate_data(partial_key, 'ROW')
            t_row = DataManager.dict_to_trow(partial_key)
        else:
            t_row = None
        Factory._check_bool(key_only, 'key_only')
        if (index_name is not None):
            # check the index name type
            logger.debug('index_name: {0}'.format(index_name))
            Factory._check_str(index_name, 'index_name')
        if (table is None):
            logger.error('invalid table')
            raise IllegalArgumentException(
                'table name must not be None.')
        Factory._check_str(table, 'table')
        return t_row, t_field_range, t_tables, td, mr, tro

    def _prepare_multi_keys(self, keys):
        # This method turns the keys int a list of TRow
        if(keys is None):
            raise IllegalArgumentException(
                'List of keys must not be empty.')
        list_of_keys = []
        for key in keys:
            if (key is None):
                raise IllegalArgumentException('keys may not have value None.')
            list_of_keys.append(DataManager.dict_to_trow(key))
        return list_of_keys


    def table_iterator(self, table, key, key_only,
                       multirow_opts=None, table_iterator_opts=None):
        """
        Returns an iterable list of :py:class:`Row` objects or keys which
        match the provided partial key.

        :param table: The name of the table from which you want to retrieve
            the keys. This parameter is required.
        :param key: A :py:class:`Row` object containing the partial primary
            key. This parameter is required.
        :param key_only: ``True`` if you want to obtain only keys from the
            iterator.  ``False`` if you want to retrieve complete rows
            instead. This parameter is required.
        :param multirow_opts: A :py:class:`MultiRowOptions` instance.
        :param table_iterator_opts: A :py:class:`TableIteratorOptions`
            instance.
        :return: An iterable list of objects matching the partial key.
            This list is returned lazily, which is to say that the entire
            list is not returned at once. Rather, list items are retrieved
            as the list iterator requires them.
        :raise ConsistencyException: If the consistency policy set for
                read operation cannot be satisfied.
        :raise RequestTimeoutException: If the requested timeout interval
                in use for this call is exceeded.
        :raise FaultException: If the operation cannot be completed for any
                reason.
        :raise IllegalArgumentException: If an incorrect argument is used,
                or a required argument is missing.
        :raise UnsupportedOperationException: If the multirow options
            provided to this method specify the return of child tables.
        :raise ProxyException: If communication with the proxy server somehow
                failed.
        """
        try:
            # verify all parameters and get them converted.
            t_row, t_field_range, t_tables, td, mr, tro = \
                self._prepare_iterator_params(table, key, None,
                    key_only, multirow_opts, table_iterator_opts)
            # prepare the list of keys
            if (key_only):
                t_iter_res = self._client.tableKeyIterator(
                    table, t_row, t_field_range, t_tables, tro, td, mr)
            else:
                t_iter_res = self._client.tableIterator(
                    table, t_row, t_field_range, t_tables, tro, td, mr)
            return ResultIterator(t_iter_res.iteratorId, t_iter_res.result,
                                  t_iter_res.hasMore, self._client)
        except TDurabilityException, de:
            raise DurabilityException(str(de))
        except TRequestTimeoutException, re:
            raise RequestTimeoutException(str(re))
        except TFaultException, fe:
            raise FaultException(str(fe))
        except TIllegalArgumentException, ie:
            raise IllegalArgumentException(str(ie))
        except TProxyException, pe:
            raise ProxyException(str(pe))

    def multi_key_table_iterator(self, table, keys, key_only,
                        multirow_opts=None, table_iterator_opts=None):
        """
        Returns an iterable list of :py:class:`Row` objects or keys which
        match the provided partial key.

        :param table: The name of the table from which you want to retrieve
            the keys. This parameter is required.
        :param keys: A list of :py:class:`Row` objects containing keys to
            use. These keys may be partial but must contain all of the
            fields in the table's shard key. This parameter is required.
        :param key_only: ``True`` if you want to obtain only keys from the
            iterator.  ``False`` if you want to retrieve complete rows
            instead. This parameter is required.
        :param multirow_opts: A :py:class:`MultiRowOptions` instance.
        :param table_iterator_opts: A :py:class:`TableIteratorOptions`
            instance.
        :return: An iterable list of objects matching the keys in the list.
            This list is returned lazily, which is to say that the entire
            list is not returned at once. Rather, list items are retrieved
            as the list iterator requires them.
        :raise ConsistencyException: If the consistency policy set for
                read operation cannot be satisfied.
        :raise RequestTimeoutException: If the requested timeout interval
                in use for this call is exceeded.
        :raise FaultException: If the operation cannot be completed for any
                reason.
        :raise IllegalArgumentException: If an incorrect argument is used,
                or a required argument is missing.
        :raise UnsupportedOperationException: If the multirow options
            provided to this method specify the return of child tables.
        :raise ProxyException: If communication with the proxy server somehow
                failed.
        """
        try:
            # verify all parameters and get them converted
            t_row, t_field_range, t_tables, td, mr, tro = \
                self._prepare_iterator_params(table, None, None,
                    key_only, multirow_opts, table_iterator_opts)
            t_keys = self._prepare_multi_keys(keys)
            if (key_only):
                t_iter_res = self._client.tableKeyIteratorMulti(
                    table, t_keys, t_field_range, t_tables, tro, td, mr, 0)
            else:
                t_iter_res = self._client.tableIteratorMulti(
                    table, t_keys, t_field_range, t_tables, tro, td, mr, 0)
            return ResultIterator(t_iter_res.iteratorId, t_iter_res.result,
                                  t_iter_res.hasMore, self._client)
        except TDurabilityException, de:
            raise DurabilityException(str(de))
        except TRequestTimeoutException, re:
            raise RequestTimeoutException(str(re))
        except TFaultException, fe:
            raise FaultException(str(fe))
        except TIllegalArgumentException, ie:
            raise IllegalArgumentException(str(ie))
        except TProxyException, pe:
            raise ProxyException(str(pe))

    def index_iterator(self, table, index_name, key, key_only,
                            multirow_opts=None, table_iterator_opts=None):
        """
        Returns an iterable list of :py:class:`Row` objects or keys which
        match the identified index. For key only iterators the result will
        contain two columns "primary" - containing the fields of primary key
        and "secondary" - containing the fields of the secondary index key.

        :param table: The name of the table from which you want to retrieve
            the keys. This parameter is required.
        :param index_name: The index you want to use. This parameter is
            required.
        :param key: A dictionary containing the indexed field name and
            value to be retrieved. All rows matching this search criteria
            will be returned. This parameter is required.
        :param key_only: ``True`` if you want to obtain only keys from the
            iterator.  ``False`` if you want to complete rows instead.
            This parameter is required.
        :param multirow_opts: A :py:class:`MultiRowOptions` instance.
        :param table_iterator_opts: A :py:class:`TableIteratorOptions`
            instance.
        :return: An iterable list of objects matching the partial key.
            This list is returned lazily, which is to say that the entire
            list is not returned at once. Rather, list items are retrieved
            as the list iterator requires them.
        :raise ConsistencyException: If the consistency policy set for
                read operation cannot be satisfied.
        :raise RequestTimeoutException: If the requested timeout interval
                in use for this call is exceeded.
        :raise FaultException: If the operation cannot be completed for any
                reason.
        :raise IllegalArgumentException: If an incorrect argument is used,
                or a required argument is missing.
        :raise UnsupportedOperationException: If the multirow options
            provided to this method specify the return of child tables.
        :raise ProxyException: If communication with the proxy server somehow
                failed.
        """
        try:
            # verify all parameters and get them converted
            t_row, t_field_range, t_tables, td, mr, tro = \
                self._prepare_iterator_params(table, key, index_name,
                    key_only, multirow_opts, table_iterator_opts)
            if (key_only):
                t_iter_res = self._client.indexKeyIterator(
                    table, index_name, t_row, t_field_range, t_tables,
                    tro, td, mr)
            else:
                t_iter_res = self._client.indexIterator(
                    table, index_name, t_row, t_field_range, t_tables,
                    tro, td, mr)
            return ResultIterator(t_iter_res.iteratorId, t_iter_res.result,
                                  t_iter_res.hasMore, self._client)
        except TDurabilityException, de:
            raise DurabilityException(str(de))
        except TRequestTimeoutException, re:
            raise RequestTimeoutException(str(re))
        except TFaultException, fe:
            raise FaultException(str(fe))
        except TIllegalArgumentException, ie:
            raise IllegalArgumentException(str(ie))
        except TProxyException, pe:
            raise ProxyException(str(pe))

    def get_version(self, which_module):
        """
        Returns the version associated with a specific module ID. Possible
        module IDs are:

        * ``ONDB_JAVA_CLIENT_ID``
            Returns the version of the Java client code currently in use.
        * ``ONDB_PROXY_SERVER_ID``
            Returns the version of the proxy server currently in use.
        """
        logger.debug('which_module: {0}'.format(which_module))
        if (which_module in _array_of_module_ids):
            return self._client.version(which_module)
        else:
            logger.error('invalid which_module')
            raise IllegalArgumentException(
                'Module ID invalid. Got: ' + str(which_module))

    def close(self):
        """
        Close the connecion with the proxy.
        """
        logger.debug('Closing transport.')
        self._transport.close()

    def shutdown(self):
        """
        Shutdown the proxy if possible.
        """
        logger.debug('Sending shutdown to proxy.')
        self._client.shutdown()

    @staticmethod
    def encode_base_64(data):
        """
        Base 64 encode the specified data.
        """
        return DataManager.encode_binary(data)

    @staticmethod
    def decode_base_64(data):
        """
        Decode the specified Base 64 encoded data.
        """
        return DataManager.decode_binary(data)


# Exceptions declaration
class ConnectionException(Exception):
    """
    The connection to the proxy is not working. The proxy may not be
    running, the proxy configuration information might not be correct, or
    (if the proxy is running on a remote host) the network connection
    between your client and the proxy might be down.
    """
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)


class DurabilityException(Exception):
    """
    The :py:class:`Durability` guarantee could not be met for a store write
    operation.
    """
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)


class RequestTimeoutException(Exception):
    """
    A store operation could not be completed within the time allowed.
    """
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)


class FaultException(Exception):
    """
    A fault exception was raised in the proxy server.
    """
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)


class ConsistencyException(Exception):
    """
    The :py:class:`Consistency` policy for a store read operation could not
    be met.
    """
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)


class UnsupportedOperationException(Exception):
    """
    An operation was attempted that is not supported by the proxy.
    """
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)


class ProxyException(Exception):
    """
    The proxy server failed to process a request.
    """
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)


class CancellationException(Exception):
    """
    An error occurred in a DDL statement. One possible reason is that
    :py:meth:`Store.execution_future_get` was cancelled before the DDL
    statement completed.
    """
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)


class OperationExecutionException(Exception):
    """
    An error occurred when synchronously executing a group of operations,
    and the operation's ``ONDB_ABORT_IF_UNSUCCESSFUL`` key was set to
    ``True``.
    """
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)


class ExecutionException(Exception):
    """
    An error occurred when asynchronously executing a group of operations,
    and the operation's ``ONDB_ABORT_IF_UNSUCCESSFUL`` key was set to
    ``True``.
    """
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)


class InterruptionException(Exception):
    """
    A group of operations were being executed asynchronously, and their
    operations were interrupted.
    """
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)


class TimeoutException(Exception):
    """
    A group of operations were being executed asynchronously, and they
    did not complete in the time allowed.
    """
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)
