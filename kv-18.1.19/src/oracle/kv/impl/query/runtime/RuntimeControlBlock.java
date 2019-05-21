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

package oracle.kv.impl.query.runtime;

import java.math.MathContext;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import oracle.kv.Consistency;
import oracle.kv.Durability;
import oracle.kv.ParallelScanIterator;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.ops.TableQuery;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.TableMetadataHelper;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.util.UserDataControl;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.table.FieldValue;

/**
 *
 */
public class RuntimeControlBlock {

    /*
     * KVStoreImpl is set on the client side and is used by the dispatch
     * code that sends queries to the server side. Not applicable to the
     * server RCBs.
     */
    private final KVStoreImpl theStore;

    private final Logger theLogger;

    /*
     * TableMetadataHelper is required by operations to resolve table and
     * index names.
     */
    private final TableMetadataHelper theMetadataHelper;

    /*
     * To achieve improved client side parallelization when satisfying a
     * query, some clients (for example, the Hive/BigDataSQL mechanism)
     * will distribute requests among separate processes and then scan
     * either a set of partitions or shards; depending on the distribution
     * kind (ALL_PARTITIONS, SINGLE_PARTITION, ALL_SHARDS) computed for
     * the given query. Not applicable to the server RCBs.
     */
    private final Set<Integer> thePartitions;

    private final Set<RepGroupId> theShards;

    /*
     * ExecuteOptions are options set by the application and used to control
     * some aspects of database access, such as Consistency, timeouts, batch
     * sizes, etc.
     */
    private final ExecuteOptions theExecuteOptions;

    private final byte theTraceLevel;

    /* The TableQuery operation. Not applicable to the client RCB. */
    private TableQuery theQueryOp;

    private final PlanIter theRootIter;

    /*
     * See javadoc for ServerIterFactory. Not applicable to the client RCB.
     */
    private final ServerIterFactory theServerIterFactory;

    /*
     * The state array contains as many elements as there are PlanIter instances
     * in the query plan to be executed. Not applicable to the client RCB.
     */
    private final PlanIterState[] theIteratorStates;

    /*
     * The register array contains as many elements as required by the
     * instances in the query plan to be executed. Not applicable to the
     * client RCB.
     */
    private final FieldValueImpl[] theRegisters;

    /*
     * An array storing the values of the extenrnal variables set for the
     * operation. These come from the map in the BoundStatement.
     */
    private final FieldValue[] theExternalVars;

    /* Not applicable to the client RCB. */
    private final int theCurrentMaxReadKB;

    /*  Not applicable to the server RCBs. */
    private byte[] theContinuationKey;

    /* The total readKB/writeKB during execution of a query */
    private final AtomicInteger theReadKB;

    private final AtomicInteger theWriteKB;

    /* The total number of records returned */
    private int theResultSize;

    /* The flag indicates if reaches the size-based or number-based limit. */
    private boolean theReachedLimit;

    /*
     * The RCB holds the TableIterator for the current remote call from the
     * ReceiveIter if there is one. This is here so that the query results
     * objects can return partition and shard metrics for the distributed
     * query operation. Not applicable to the server RCBs.
     */
    private ParallelScanIterator<FieldValueImpl> theTableIterator;

    /**
     * Constructor used at the client only.
     */
    public RuntimeControlBlock(
        KVStoreImpl store,
        Logger logger,
        TableMetadataHelper mdHelper,
        Set<Integer> partitions,
        Set<RepGroupId> shards,
        ExecuteOptions executeOptions,
        PlanIter rootIter,
        int numIters,
        int numRegs,
        FieldValue[] externalVars) {

        this(store,
             logger,
             mdHelper,
             partitions,
             shards,
             executeOptions,
             null, /*TableQuery*/
             null, /*serverIterFactory*/
             rootIter,
             numIters,
             numRegs,
             externalVars);
    }

    public RuntimeControlBlock(
        KVStoreImpl store,
        Logger logger,
        TableMetadataHelper mdHelper,
        Set<Integer> partitions,
        Set<RepGroupId> shards,
        ExecuteOptions executeOptions,
        TableQuery queryOp,
        ServerIterFactory serverIterFactory,
        PlanIter rootIter,
        int numIters,
        int numRegs,
        FieldValue[] externalVars) {

        theStore = store;
        theLogger = logger;
        theMetadataHelper = mdHelper;

        thePartitions = partitions;
        theShards = shards;

        theExecuteOptions = executeOptions;
        theTraceLevel = (executeOptions != null ?
                         executeOptions.getTraceLevel() :
                         0);

        theQueryOp = queryOp;

        theRootIter = rootIter;

        theServerIterFactory = serverIterFactory;
        theIteratorStates = new PlanIterState[numIters];
        theRegisters = new FieldValueImpl[numRegs];
        theExternalVars = externalVars;

        theCurrentMaxReadKB =  (isServerRCB() ?
                                theQueryOp.getCurrentMaxReadKB() :
                                0);

        theContinuationKey = (theExecuteOptions != null ?
                              theExecuteOptions.getContinuationKey() :
                              null);

        theReadKB = new AtomicInteger();
        theWriteKB = new AtomicInteger();
    }

    boolean isServerRCB() {
        return theStore == null;
    }

    public KVStoreImpl getStore() {
        return theStore;
    }

    public Logger getLogger() {
        return theLogger;
    }

    public TableMetadataHelper getMetadataHelper() {
        return theMetadataHelper;
    }

    public Set<Integer> getPartitionSet() {
        return thePartitions;
    }

    public Set<RepGroupId> getShardSet() {
        return theShards;
    }

    public ExecuteOptions getExecuteOptions() {
        return theExecuteOptions;
    }

    public byte getTraceLevel() {
        return theTraceLevel;
    }

    public void trace(String msg) {
        if (!UserDataControl.hideUserData()) {
            theLogger.info("QUERY:" + msg);
        }
    }

    Consistency getConsistency() {
        return theExecuteOptions.getConsistency();
    }

    Durability getDurability() {
        return theExecuteOptions.getDurability();
    }

    long getTimeout() {
        return theExecuteOptions.getTimeout();
    }

    TimeUnit getTimeUnit() {
        return theExecuteOptions.getTimeoutUnit();
    }

    public MathContext getMathContext() {
        return theExecuteOptions.getMathContext();
    }

    int getBatchSize() {
        return theExecuteOptions.getResultsBatchSize();
    }

    public boolean getUseBatchSizeAsLimit() {
        return theExecuteOptions.getUseBatchSizeAsLimit();
    }

    public int getMaxReadKB() {
        return theExecuteOptions.getMaxReadKB();
    }

    public TableQuery getQueryOp() {
        return theQueryOp;
    }

    public ResumeInfo getResumeInfo() {
        if (theQueryOp == null) {
            return null;
        }
        return theQueryOp.getResumeInfo();
    }

    public ServerIterFactory getServerIterFactory() {
        return theServerIterFactory;
    }

    PlanIter getRootIter() {
        return theRootIter;
    }

    public void setState(int pos, PlanIterState state) {
        theIteratorStates[pos] = state;
    }

    public PlanIterState getState(int pos) {
        return theIteratorStates[pos];
    }

    public FieldValueImpl[] getRegisters() {
        return theRegisters;
    }

    public FieldValueImpl getRegVal(int regId) {
        return theRegisters[regId];
    }

    public void setRegVal(int regId, FieldValueImpl value) {
        theRegisters[regId] = value;
    }

    FieldValue[] getExternalVars() {
        return theExternalVars;
    }

    FieldValueImpl getExternalVar(int id) {

        if (theExternalVars == null) {
            return null;
        }
        return (FieldValueImpl)theExternalVars[id];
    }

    public int getCurrentMaxReadKB() {
        return theCurrentMaxReadKB;
    }

    public byte[] getContinuationKey() {
        return theContinuationKey;
    }

    void setContinuationKey(byte[] key) {
        theContinuationKey = key;
    }

    public void tallyReadKB(int nkb) {
        theReadKB.addAndGet(nkb);
    }

    public void tallyWriteKB(int nkb) {
        theWriteKB.addAndGet(nkb);
    }

    public int getReadKB() {
        return theReadKB.get();
    }

    public int getWriteKB() {
        return theWriteKB.get();
    }

    public void tallyResultSize(int size) {
        theResultSize += size;
    }

    public int getResultSize() {
        return theResultSize;
    }

    public void setReachedLimit(boolean value) {
        theReachedLimit = value;
    }

    public boolean getReachedLimit() {
        return theReachedLimit;
    }

    void setTableIterator(ParallelScanIterator<FieldValueImpl> iter) {
        theTableIterator = iter;
    }

    public ParallelScanIterator<FieldValueImpl> getTableIterator() {
        return theTableIterator;
    }
}
