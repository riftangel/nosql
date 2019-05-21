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
This file contains the StoreConfig and ProxyConfig classes.

StoreConfig contains configuration information required to locate and
connect to the store.

ProxyConfig contains configuration information required to connect to the
proxy server.
"""
import logging

from utilities import IllegalArgumentException
from utilities import LOG_LOCATION
from utilities import MAX_AMOUNT_OF_LOG_FILES
from utilities import MAX_SIZE_BEFORE_ROTATION

from oracle.kv.proxy.gen.ttypes import TVerifyProperties


logger = logging.getLogger('nosqldb')


def _check_if_str(data):
    if (data is not None and
        isinstance(data, (str, unicode))):
            return True
    return False


def _check_if_int_ge_zero(data):
    if (data is not None and
        isinstance(data, int) and
        data >= 0):
            return True
    return False


def _check_if_dict(data):
    if (data is not None and
        isinstance(data, dict)):
            return True
    return False


class StoreConfig:
    """
    Contains information needed to connect to the store. An instance of
    this class is required by :py:meth:`Factory.open`.

    Initializing this class requires the name of the store to which your
    code is connecting.  The store name was defined when your store was
    first started.  If you are using  kvlite instance, the store name is
    ``kvlite.`` For any other store, see the personnel who installed and
    configured it.

    This class also requires a list or tuple of strings with the format
    ``host:port,`` where ``host`` is the name of a host where one of your
    store's Storage Nodes can be found, and ``port`` is the Storage Node's
    listening port.  At least one host:port pair must be given, and that
    pair must represent a currently operational Storage Node. If you are
    using kvlite, then provide only one host:port pair; typically:
    ``localhost:5000.`` For any other store, see the personnel who
    installed and configured it.

    :param store_name: The name of the store to which you are connecting.
        This parameter is required.

    :param helper_hosts: A list or tuple of hostname:port pairs.
        This parameter is required.

    """
    def __init__(self, store_name, helper_hosts):
        """
        Inits a StoreConfig object.
        """
        self._store_name = None
        self.set_store_name(store_name)
        self._helper_hosts = list()
        self.set_helper_hosts(helper_hosts)
        self._user = None
        self._read_zones = list()
        self._max_results = 0
        self._request_timeout = None
        self._consistency_opts = None
        self._durability_opts = None

    def __str__(self):
        """
        Display a printable representation of this object.
        """
        retstr = ''
        if (self._store_name):
            retstr += self._store_name
        if (self._helper_hosts):
            retstr += ' ' + str(self._helper_hosts)
        if (self._user):
            retstr += ' ' + str(self._user)
        if (self._consistency_opts):
            retstr += ' ' + str(self._consistency_opts)
        if (self._durability_opts):
            retstr += ' ' + str(self._durability_opts)
        if (self._read_zones):
            retstr += ' ' + str(self._read_zones)
        if (self._max_results):
            retstr += ' ' + str(self._max_results)
        if (self._request_timeout):
            retstr += ' ' + str(self._request_timeout)
        return retstr

    def get_consistency(self):
        """
        Returns the :py:class:`Consistency` object in use for this store
        configuration.
        """
        return self._consistency_opts

    def set_consistency(self, consistency):
        """
        Sets the :py:class:`Consistency` object you want to use as the
        default consistency policy for this store configuration. This
        default value can be modified on a per-operation basis. See
        :py:class:`ReadOptions` for details.
        """
        if (isinstance(consistency, dict)):
            self._consistency_opts = consistency
        else:
            raise IllegalArgumentException(
                'consistency must be a dict.')

    def get_durability(self):
        """
        Returns the :py:class:`Durability` object in use for this store
        configuration.
        """
        return self._durability_opts

    def set_durability(self, durability):
        """
        Sets the :py:class:`Durability` object you want to use as the
        default durability guarantee for this store configuration. This
        default value can be modified on a per-operation basis. See
        :py:class:`WriteOptions` for details.
        """
        if (isinstance(durability, dict)):
            self._durability_opts = durability
        else:
            raise IllegalArgumentException(
                'durability must be a dict.')

    def get_max_results(self):
        """
        Returns the default maximum number of results retrieved by
        :py:meth:`Store.table_iterator` or :py:meth:`Store.index_iterator`
        in a single call to the store.
        """
        return self._max_results

    def set_max_results(self, results):
        """
        Sets a positive integer representing the default maximum number of
        results retrieved by :py:meth:`Store.table_iterator` or
        :py:meth:`Store.index_iterator` in a single call to the store. This
        can modified on a per-operation basis using
        :py:class:`TableIteratorOptions`.
        """
        if (_check_if_int_ge_zero(results)):
            self._max_results = results
        else:
            raise IllegalArgumentException(
                'results must be an int greater or equal to 0.')

    def get_helper_hosts(self):
        """
        Returns a tuple of strings in the format ``host:port`` which
        represent where your store's Storage Nodes can be located.
        """
        return tuple(self._helper_hosts)

    def set_helper_hosts(self, host_list):
        """
        Sets a tuple or list of strings in the format ``host:port`` which
        represent where your store's Storage Nodes can be located. To
        connect to the store, at least one active Store Node must be
        identified in the host list.
        """
        if (isinstance(host_list, (list, tuple))):
            del self._helper_hosts[:]
            for e in host_list:
                self._helper_hosts.append(e)
        else:
            raise IllegalArgumentException(
                'host_list must be a list of tuple.')

    def get_read_zones(self):
        """
        Returns the zones in which nodes must be located to be used for
        read operations, or ``None`` if read operations can be performed on
        nodes in any zone.
        """
        return self._read_zones

    def set_read_zones(self, zone_list):
        """
        Sets a tuple or list of zone names in which nodes must be located
        to be used for read operations.
        """
        if (zone_list is not None and
            isinstance(zone_list, (tuple, list))):
                self._read_zones = zone_list
        else:
            raise IllegalArgumentException(
                'zone_list must be a list or tuple.')

    def get_request_timeout(self):
        """
        Returns the default request timeout.
        """
        return self._request_timeout

    def set_request_timeout(self, timeout):
        """
        Sets a positive integer value representing
        the request timeout value in milliseconds.
        """
        if (_check_if_int_ge_zero(timeout)):
            self._request_timeout = timeout
        else:
            raise IllegalArgumentException(
                'timeout must be an int greater or equal to 0.')

    def get_store_name(self):
        """
        Returns the name of the store in use for this configuration.
        """
        return self._store_name

    def set_store_name(self, new_name):
        """
        Sets the name of the store you want to use for this configuration.
        """
        if (_check_if_str(new_name)):
            self._store_name = new_name
        else:
            raise IllegalArgumentException(
                'new_name must be a string.')

    def get_user(self):
        """
        Returns the name of the user authenticating to the store.
        """
        return self._user

    def set_user(self, user):
        """
        Sets the name of the user authenticating to the store.
        """
        if (_check_if_str(user)):
            self._user = user
        else:
            raise IllegalArgumentException(
                'user must be a string.')

    def get_verify_values(self):
        """
        Returns the relevant parameters for verifying the proxy connection
        in Thrift format.
        """
        verify_vals = TVerifyProperties(
                kvStoreName=self._store_name,
                kvStoreHelperHosts=self._helper_hosts,
                username=self._user,
                readZones=self._read_zones)
        return verify_vals

    @staticmethod
    def change_log(level=None, f_name=None):
        """
        Turn on logging with the logging level and the file name to log to.
        level must be one of:

        * ``NOTSET``
        * ``DEBUG``
        * ``INFO``
        * ``WARNING``
        * ``ERROR``
        * ``CRITICAL``
        """
        # get rid of the current handlers
        for h in logger.handlers:
            logger.removeHandler(h)
        # check what handler to implement based on parameters
        r_level = logging.DEBUG
        if (level is not None):
            r_level = getattr(logging, level, None)
        logger.setLevel(r_level)
        filename = LOG_LOCATION
        if (f_name is not None):
            filename = f_name
        fileh = logging.handlers.RotatingFileHandler(
            filename, 'a', MAX_SIZE_BEFORE_ROTATION,
            MAX_AMOUNT_OF_LOG_FILES)
        formatter = logging.Formatter('%(asctime)s.%(msecs)d ' +
            '%(levelname)s - %(funcName)s: %(message)s', )
        fileh.setFormatter(formatter)
        fileh.setLevel(r_level)
        logger.addHandler(fileh)

    @staticmethod
    def turn_off_log():
        # stop the logger
        for h in logger.handlers:
            logger.removeHandler(h)
        logger.addHandler(logging.NullHandler())


class ProxyConfig:
    """
    Contains information required to start a proxy in the event that a
    proxy is not currently running. To initialize this class, you can optionally
    provide paths to ``kvstore.jar`` and ``kvproxy.jar`` files located on your
    local drive.  By default the system attempts to use the versions of these
    files bundled with the installation.  Specify these if your installation
    does not have them or you want to use non-default versions.

    The location of the jar files provided to this class overrides whatever
    is specified in your ``KVSTORE_JAR`` and ``KVPROXY_JAR`` environment
    variables.

    :param kvstore_jar: The location of the kvclient jar.
        This parameter is required.
    :param kvproxy_jar: The location of the proxy jar.

    """
    def __init__(self, kvstore_jar=None, kvproxy_jar=None):
        """
        Inits a Proxy Config object with rwo optional values that are the
        location of the store jar and the location of the proxy jar.

        :param kvstore_jar: The location of the kvclient jar.
        :param kvproxy_jar: The location of the proxy jar.
        """
        self._kv_proxy_path_to_jar = kvproxy_jar
        self._kv_store_path_to_jar = kvstore_jar
        self._request_limit = None
        self._max_iterator_results = None
        self._iterator_expiration_ms = None
        self._max_open_iterators = None
        self._num_pool_threads = None
        self._request_timeout = None
        self._socket_open_timeout = None
        self._socket_read_timeout = None
        self._max_active_requests = None
        self._request_threshold_percent = None
        self._node_limit_percent = None
        self._security_props_file = None
        self._max_concurrent_requests = None
        self._max_results_batches = None
        self._verbose = False

    def __str__(self):
        """
        Display a printable representation of this object.
        """
        retstr = ''
        if (self._kv_proxy_path_to_jar is not None):
            retstr += self._kv_proxy_path_to_jar + ' '
        if (self._kv_store_path_to_jar is not None):
            retstr += self._kv_store_path_to_jar + ' '
        if (self._request_limit is not None):
            retstr += str(self._request_limit) + ' '
        if (self._max_iterator_results is not None):
            retstr += str(self._max_iterator_results) + ' '
        if (self._iterator_expiration_ms is not None):
            retstr += str(self._iter_iterator_expiration_ms) + ' '
        if (self._max_open_iterators is not None):
            retstr += str(self._max_open_iterators) + ' '
        if (self._num_pool_threads is not None):
            retstr += str(self._num_pool_threads) + ' '
        if (self._request_timeout is not None):
            retstr += str(self._request_timeout) + ' '
        if (self._socket_open_timeout is not None):
            retstr += str(self._socket_open_timeout) + ' '
        if (self._socket_read_timeout is not None):
            retstr += str(self._socket_read_timeout) + ' '
        if (self._max_active_requests is not None):
            retstr += str(self._max_active_requests) + ' '
        if (self._request_threshold_percent is not None):
            retstr += str(self._request_threshold_percent) + ' '
        if (self._node_limit_percent is not None):
            retstr += str(self._node_limit_percent) + ' '
        if (self._security_props_file is not None):
            retstr += str(self._security_props_file) + ' '
        if (self._verbose is not None):
            retstr += str(self._verbose)
        return retstr

    def set_kv_proxy_path_to_jar(self, path_to_jar):
        """
        Sets the path to the ``kvproxy.jar`` file. Overrides whatever path
        was provided when this class was initialized, if any. Also
        overrides any information provided using the ``KVPROXY_JAR``
        environment variable.
        """
        if (_check_if_str(path_to_jar)):
            self._kv_proxy_path_to_jar = path_to_jar
        else:
            raise IllegalArgumentException(
                'path_to_jar must be a string.')

    def get_kv_proxy_path_to_jar(self):
        """
        Returns the path to the ``kvproxy.jar`` file.
        """
        return self._kv_proxy_path_to_jar

    def set_kv_store_path_to_jar(self, path_to_jar):
        """
        Sets the path to the ``kvstore.jar`` file. Overrides the path
        that was provided when this class was initialized. Also
        overrides any information provided using the ``KVSTORE_JAR``
        environment variable.
        """
        if (_check_if_str(path_to_jar)):
            self._kv_store_path_to_jar = path_to_jar
        else:
            raise IllegalArgumentException(
                'path_to_jar must be a string.')

    def get_kv_store_path_to_jar(self):
        """
        Returns the path to the ``kvstore.jar`` file.
        """
        return self._kv_store_path_to_jar

    def set_request_limit(self, limit):
        """
        Sets a positive integer representing the maximum number of requests
        that this client can have active for a node in the store.
        """
        if (_check_if_int_ge_zero(limit)):
            self._request_limit = limit
        else:
            raise IllegalArgumentException(
                'limit must be an int greater or equal to 0.')

    def get_request_limit(self):
        """
        Returns the maximum number of requests that this client can have
        active for a node in the store.
        """
        return self._request_limit

    def set_max_iterator_results(self, results):
        """
        Sets a positive integer representing the number of results
        retrieved by :py:meth:`Store.table_iterator` or
        :py:meth:`Store.index_iterator` in a single call to the server. The
        default value is 100.
        """
        if (_check_if_int_ge_zero(results)):
            self._max_iterator_results = results
        else:
            raise IllegalArgumentException(
                'results must be an int greater or equal to 0.')

    def get_max_iterator_results(self):
        """
        Returns the number of results retrieved by
        :py:meth:`Store.table_iterator` or :py:meth:`Store.index_iterator`
        in a single call to the server.
        """
        return self._max_iterator_results

    def set_iterator_expiration_ms(self, ms):
        """
        Sets a positive integer representing the expiration timeout for an
        iterator in milliseconds. The proxy maintains iterator state when
        an iteration begins. This timeout is used to expire that state if
        the client does not complete the iteration in a timely manner. The
        default value is 60000 (60 seconds).
        """
        if (_check_if_int_ge_zero(ms)):
            self._iterator_expiration_ms = ms
        else:
            raise IllegalArgumentException(
                'ms must be an int greater or equal to 0.')

    def get_iterator_expiration_ms(self):
        """
        Returns the expiration timeout for an iterator in milliseconds.
        """
        return self._iterator_expiration_ms

    def set_max_open_iterators(self, iterators):
        """
        Sets a positive integer representing the maximum number of open
        iterators in a connection. Because the proxy maintains state for
        iterators, an excessive number of active (not completed or timed
        out) iterators can result in memory problems. This method allows
        specification of a limit. The default value is 10000.
        """
        if (_check_if_int_ge_zero(iterators)):
            self._max_open_iterators = iterators
        else:
            raise IllegalArgumentException(
                'iterators must be an int greater or equal to 0.')

    def get_max_open_iterators(self):
        """
        Returns the number for maximum number of active iterators on a
        connection.
        """
        return self._max_open_iterators

    def set_num_pool_threads(self, threads):
        """
        Sets a positive integer representing the number of threads to use
        for request handling in the proxy server. The default value is 20.
        """
        if (_check_if_int_ge_zero(threads)):
            self._num_pool_threads = threads
        else:
            raise IllegalArgumentException(
                'threads must be an int greater or equal to 0.')

    def get_num_pool_threads(self):
        """
        Returns the number of request handler threads configured for the
        proxy.
        """
        return self._num_pool_threads

    def set_socket_read_timeout(self, timeout):
        """
        Sets a positive integer representing the read timeout (in
        milliseconds) associated with sockets used to make client requests.
        """
        if (_check_if_int_ge_zero(timeout)):
            self._socket_read_timeout = timeout
        else:
            raise IllegalArgumentException(
                'timeout must be an int greater or equal to 0.')

    def get_socket_read_timeout(self):
        """
        Returns the read timeout associated with sockets used to make
        client requests.
        """
        return self._socket_read_timeout

    def set_request_timeout(self, timeout):
        """
        Sets a positive integer representing the default request timeout
        value (in milliseconds) for operations performed by this client
        against the store.
        """
        if (_check_if_int_ge_zero(timeout)):
            self._request_timeout = timeout
        else:
            raise IllegalArgumentException(
                'timeout must be an int greater or equal to 0.')

    def get_request_timeout(self):
        """
        Returns the default request timeout.
        """
        return self._request_timeout

    def set_socket_open_timeout(self, timeout):
        """
        Sets a positive integer representing the amount of time (in
        millseconds) this client will wait when opening a socket to the
        store.
        """
        if(_check_if_int_ge_zero(timeout)):
            self._socket_open_timeout = timeout
        else:
            raise IllegalArgumentException(
                'timeout must be an int greater or equal to 0.')

    def get_socket_open_timeout(self):
        """
        Returns the socket open timeout value.
        """
        return self._socket_open_timeout

    def set_max_active_requests(self, requests):
        """
        Sets a positive integer representing the maximum number of requests
        that this client can have active for a node in the KVStore.
        """
        if(_check_if_int_ge_zero(requests)):
            self._max_active_requests = requests
        else:
            raise IllegalArgumentException(
                'requests must be an int greater or equal to 0.')

    def get_max_active_requests(self):
        """
        Returns the maximum number of requests that this client can have
        active for a node in the store.
        """
        return self._max_active_requests

    def set_request_threshold_percent(self, percent):
        """
        Sets a positive integer representing the threshold computed as a
        percentage of max_active_requests at which requests are limited.
        """
        if(_check_if_int_ge_zero(percent)):
            self._request_threshold_percent = percent
        else:
            raise IllegalArgumentException(
                'percent must be an int between 0 and 100.')

    def get_request_threshold_percent(self):
        """
        Returns the threshold computed as a percentage of
        max_active_requests at which requests are limited.
        """
        return self._request_threshold_percent

    def set_node_limit_percent(self, percent):
        """
        Sets a positive integer representing the maximum number of active
        requests that can be associated with a node when the request
        limiting mechanism is active.
        """
        if(_check_if_int_ge_zero(percent)):
            self._node_limit_percent = percent
        else:
            raise IllegalArgumentException(
                'percent must be an int between 0 and 100.')

    def get_node_limit_percent(self):
        """
        Returns the maximum number of active requests that can be
        associated with a node when the request limiting mechanism is
        active.
        """
        return self._node_limit_percent

    def set_verbose(self, v):
        """
        Sets a boolean value indicating whether the client and proxy will
        issue verbose messaging. Use ``True`` if you want verbose
        messaging. Turning this on can be helpful when troubleshooting
        problems with connecting to the store.
        """
        if (type(v) is bool):
            self._verbose = v
        else:
            raise IllegalArgumentException(
                'v must be a bool.')

    def get_verbose(self):
        """
        Returns a boolean value indicating whether verbose messaging is in
        use. ``True`` means that it is in use.
        """
        return self._verbose

    def get_security_props_file(self):
        """
        Returns the location of the security property configuration file.
        """
        return self._security_props_file

    def set_security_props_file(self, f):
        """
        Sets the location of the security property configuration file. Note
        that the security property configuration file is located on the
        same machine as is running the proxy server.
        """
        if (_check_if_str(f)):
            self._security_props_file = f
        else:
            raise IllegalArgumentException(
                'f must be a string.')

    def get_max_concurrent_requests(self):
        """
        Returns the maximum number of active requests allowed between the
        proxy server and the store.
        """
        return self._max_concurrent_requests

    def set_max_concurrent_requests(self, max_reqs):
        """
        Sets the maximum number of active requests allowed between the
        proxy server and the store.
        """
        if (_check_if_int_ge_zero(max_reqs)):
            self._max_concurrent_requests = max_reqs
        else:
            raise IllegalArgumentException(
                'max_reqs must be an int greater or equal to 0.')

    def get_max_results_batches(self):
        """
        Returns an integer value representing the maximum number of result
        batches that can be held in the proxy server process.
        """
        return self._max_results_batches

    def set_max_results_batches(self, max_res):
        """
        Specifies an integer value representing the maximum number of
        result batches that can be held in the proxy server process.
        """
        if (_check_if_int_ge_zero(max_res)):
            self._max_results_batches = max_res
        else:
            raise IllegalArgumentException(
                'max_res must be an int greater or equal to 0.')
