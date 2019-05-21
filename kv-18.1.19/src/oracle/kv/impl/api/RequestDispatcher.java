/*-
 * Copyright (C) 2011, 2018 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */

package oracle.kv.impl.api;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Map;
import java.util.Set;

import oracle.kv.FaultException;
import oracle.kv.ResultHandler;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.rgstate.RepGroupStateTable;
import oracle.kv.impl.api.rgstate.RepNodeState;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.table.Table;

import com.sleepycat.utilint.Latency;

/**
 * The RemoteRequestDispatcher is responsible for dispatching requests over the
 * network to a suitable RN, where they are handled by the
 * {@link RequestHandler}. The KV client contains an instance of the
 * RequestDispatcher, as does each RN.
 */
public interface RequestDispatcher {

    /**
     * Dispatches a request to a suitable RN, based upon the contents of the
     * request, where it is either executed, or forwarded on to some other RN if
     * the targeted RN is unsuitable.
     * <p>
     * Note that all attempts to recover, e.g. work around some RMI or
     * networking issue, retry the request at different RN, etc. are performed
     * by the execute method. If all such attempts to recover fail, an
     * appropriate {@link FaultException} is thrown and typically propagated
     * all the way back to the KV client request invoker.
     *
     * @param request the request to process remotely at some RN
     * @param excludeRNs the RNs to exclude from consideration for the
     * dispatch, or null for no exclusion
     * @param loginMgr the login manager for the request or null
     * @return the Response associated with the Request
     * @throws FaultException if the requested execution failed
     */
    public Response execute(Request request,
                            Set<RepNodeId> excludeRNs,
                            LoginManager loginMgr)
        throws FaultException;

    /**
     * Dispatches a request when no RNs are being excluded.
     *
     * @see #execute(Request, Set, LoginManager)
     */
    public Response execute(Request request, LoginManager loginMgr)
        throws FaultException;

    /**
     * Dispatches a request asynchronously to a suitable RN, based upon the
     * contents of the request, where it is either executed, or forwarded on to
     * some other RN if the targeted RN is unsuitable.
     *
     * <p> Note that all attempts to recover, e.g. work around a networking
     * issue, retry the request at different RN, etc. are performed by the
     * execute method. If all such attempts to recover fail, an appropriate
     * {@link FaultException} will be passed to the result handler and is
     * typically propagated all the way back to the KV client.
     *
     * <p> This method throws {@link UnsupportedOperationException} for
     * instances that only support synchronous operations.
     *
     * @param request the request to process remotely at some RN
     * @param excludeRNs the RNs to exclude from consideration for the
     * dispatch, or null for no exclusion
     * @param loginMgr the login manager for the request or null
     * @param handler the result handler
     * @throws FaultException
     * @throws UnsupportedOperationException if asynchronous operations are not
     * supported
     */
    public void execute(Request request,
                        Set<RepNodeId> excludeRNs,
                        LoginManager loginMgr,
                        ResultHandler<Response> handler);

    /**
     * Executes an internal  NOP request at the target node
     *
     * @param targetRN the node when the NOP is to be executed
     * @param timeoutMs the timeout associated with the request.
     * @param loginMgr the null-allowable login manager which which to
     *        authenticate the call
     *
     * @return the response to the NOP request or null if no RMI handle to the
     * RN was currently available.
     *
     * @throws Exception if the request could not be satisfied
     */
    public Response executeNOP(RepNodeState targetRN,
                               int timeoutMs,
                               LoginManager loginMgr)
        throws Exception;

    /**
     * Returns the resource id associated with the remote request dispatcher.
     */
    public ResourceId getDispatcherId();

    /**
     * Returns the Topology used for request dispatching.
     *
     * @return the topology
     */
    public Topology getTopology();

    /**
     * Returns the topology manager used as the basis for dispatching requests
     * and maintaining the Topology
     */
    public TopologyManager getTopologyManager();

    /**
     * Returns the {@link RepGroupStateTable associated} with the
     * RemoteRequestDispatcher.
     */
    public RepGroupStateTable getRepGroupStateTable();

    /**
     * Returns the Id of the partition associated with a given key.
     * Convenience method.
     */
    public PartitionId getPartitionId(byte[] keyBytes);

    /**
     * Shuts down the request dispatcher. The argument is non-null if it's
     * an abnormal shutdown.
     */
    public void shutdown(Throwable exception);

    /**
     * Returns the registry utils currently associated with this dispatcher.
     * Note that the instance could change with changes in Topology
     */
    public RegistryUtils getRegUtils();

    /**
     * Returns a snapshot of the operation latency stats associated with the
     * request dispatcher.
     *
     * @param clear if true clears the stats
     */
    public Map<OpCode, Latency> getLatencyStats(boolean clear);

    /**
     * The total number of requests that were retried by the request dispatcher.
     */
    public long getTotalRetryCount(boolean clear);

    /**
     * Returns the dispatchers exception handler. An exception caught by this
     * handler results in the process being restarted by the SNA.
     *
     * @see "RepNodeService.ThreadExceptionHandler"
     *
     * @return the dispatchers exception handler
     */
    public UncaughtExceptionHandler getExceptionHandler();

    /**
     * Updates the current login manager, which is used for topology access,
     * NOP execution, and RequestHandler resolution.  It is not used for
     * general Request execution. A null value may be specified, which is fine
     * for a non-secure KVStore, but will prevent further Topology access in
     * a secure install until a non-null LoginManager is supplied.
     */
    public void setRegUtilsLoginManager(LoginManager loginMgr);

    /**
     * Returns the IDs of the zones that can be used for read operations, or
     * {@code null} if not restricted.
     *
     * @return the zone IDs or {@code null}
     */
    public int[] getReadZoneIds();

    public Table getTable(KVStoreImpl store, String namespace, String tableName);

    public TableMetadata getTableMetadata();

    /**
     * Retrieve table instance by the specific table id.
     * @param store the handle of store instance
     * @param tableId the table id mapping specific table
     * @return handle of table instance
     */
    public Table getTableById(KVStoreImpl store, long tableId);
}
