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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static oracle.kv.impl.util.SerialVersion.UNKNOWN;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullByteArray;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.Durability;
import oracle.kv.ResultHandler;
import oracle.kv.StoreIteratorException;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.Request;
import oracle.kv.impl.api.StoreIteratorParams;
import oracle.kv.impl.api.ops.Result;
import oracle.kv.impl.api.ops.Result.QueryResult;
import oracle.kv.impl.api.ops.TableQuery;
import oracle.kv.impl.api.parallelscan.PartitionScanIterator;
import oracle.kv.impl.api.parallelscan.ShardScanIterator;
import oracle.kv.impl.api.query.PreparedStatementImpl.DistributionKind;
import oracle.kv.impl.api.table.BinaryValueImpl;
import oracle.kv.impl.api.table.BooleanValueImpl;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.api.table.NumberValueImpl;
import oracle.kv.impl.api.table.PrimaryKeyImpl;
import oracle.kv.impl.api.table.RecordDefImpl;
import oracle.kv.impl.api.table.RecordValueImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TimestampValueImpl;
import oracle.kv.impl.api.table.TupleValue;
import oracle.kv.impl.async.AsyncTableIterator;
import oracle.kv.impl.async.IterationHandleNotifier;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.compiler.QueryFormatter;
import oracle.kv.impl.query.compiler.SortSpec;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.stats.DetailedMetrics;

/**
 * ReceiveIter are placed at the boundaries between parts of the query that
 * execute on different "machines". Currently, there can be only one ReceiveIter
 * in the whole query plan. It executes at a "client machine" and its child
 * subplan executes at a "server machine". The child subplan may actually be
 * replicated on several server machines (RNs), in which case the ReceiveIter
 * acts as a UNION ALL expr, collecting and propagating the results it receives
 * from its children. Furthermore, the ReceiveIter may perform a merge-sort over
 * its inputs (if the inputs return sorted results).
 *
 * If the ReceiveIter is the root iter, it just propagates to its output the
 * FieldValues (most likely RecordValues) it receives from the RNs. Otherwise,
 * if its input iter produces tuples, the ReceiveIter will recreate these tuples
 * at its output by unnesting into tuples the RecordValues arriving from the RNs.
 */
public class ReceiveIter extends PlanIter {

    private static class ReceiveIterState extends PlanIterState {

        final PartitionId thePartitionId;

        AsyncTableIterator<FieldValueImpl> theRemoteResultsIter;

        Throwable theRemoteResultsIterCloseException;

        HashSet<BinaryValueImpl> thePrimKeysSet;

        ReceiveIterState(PartitionId pid, boolean eliminateIndexDups) {

            thePartitionId = pid;

            if (eliminateIndexDups) {
                thePrimKeysSet = new HashSet<BinaryValueImpl>(1000);
            }
        }

        @Override
        public void done() {
            super.done();
            clear();
        }

        @Override
        public void reset(PlanIter iter) {
            super.reset(iter);
            clear();
        }

        @Override
        public void close() {
            super.close();
            if (theRemoteResultsIter != null) {
                theRemoteResultsIterCloseException =
                    theRemoteResultsIter.getCloseException();
            }
            clear();
        }

        void clear() {
            if (theRemoteResultsIter != null) {
                theRemoteResultsIter.close();
                theRemoteResultsIter = null;
            }
            if (thePrimKeysSet != null) {
                thePrimKeysSet.clear();
            }
        }
    }

    private static class CachedBinaryPlan {

        private byte[] thePlan = null;
        private short theSerialVersion = UNKNOWN;

        private CachedBinaryPlan(
            byte[] plan,
            short serialVersion) {
            thePlan = plan;
            theSerialVersion = serialVersion;
        }

        public static CachedBinaryPlan create(
            byte[] plan,
            short serialVersion) {
            return new CachedBinaryPlan(plan, serialVersion);
        }

        byte[] getPlan() {
            return thePlan;
        }

        short getSerialVersion() {
            return theSerialVersion;
        }
    }

    private final PlanIter theInputIter;

    private transient volatile CachedBinaryPlan theSerializedInputIter = null;

    private final FieldDefImpl theInputType;

    /* added in QUERY_VERSION_2 */
    private final boolean theMayReturnNULL;

    private final int[] theSortFieldPositions;

    private final SortSpec[] theSortSpecs;

    private final int[] thePrimKeyPositions;

    private final int[] theTupleRegs;

    private final DistributionKind theDistributionKind;

    private final RecordValueImpl thePrimaryKey;

    private PartitionId thePartitionId;

    private final long theTableId;

    private final String theTableName;

    private final String theNamespace;

    private final PlanIter[] thePushedExternals;

    private final int theNumRegs;

    private final int theNumIters;

    /* added in QUERY_VERSION_5 */
    private final boolean theIsUpdate;

    private transient volatile IterationHandleNotifier
        theAsyncIterHandleNotifier;

    public ReceiveIter(
        Expr e,
        int resultReg,
        PlanIter input,
        FieldDefImpl inputType,
        boolean mayReturnNULL,
        int[] sortFieldPositions,
        SortSpec[] sortSpecs,
        int[] primKeyPositions,
        DistributionKind distrKind,
        PrimaryKeyImpl primKey,
        PlanIter[] pushedExternals,
        int numRegs,
        int numIters,
        boolean isUpdate) {

        super(e, resultReg);

        theInputIter = input;
        theInputType = inputType;
        theMayReturnNULL = mayReturnNULL;
        theSortFieldPositions = sortFieldPositions;
        theSortSpecs = sortSpecs;
        thePrimKeyPositions = primKeyPositions;
        theDistributionKind = distrKind;
        thePushedExternals = pushedExternals;

        /*
         * If the ReceiveIter is the root iter, it just propagates to its
         * output the FieldValues (most likely RecordValues) it receives from
         * the RNs. Otherwise, if its input iter produces tuples, the
         * ReceiveIter will recreate these tuples at its output by unnesting
         * into tuples the RecordValues arriving from the RNs.
         */
        if (input.producesTuples() && e.getQCB().getRootExpr() != e) {
            theTupleRegs = input.getTupleRegs();
        } else {
            theTupleRegs = null;
        }

        theTableId = e.getQCB().getTargetTableId();

        if (primKey != null) {
            thePrimaryKey = primKey;
            theTableName = primKey.getTable().getFullName();
            theNamespace = primKey.getTable().getNamespace();

            /*
             * If it's a SINGLE_PARTITION query with no external vars, compute
             * the partition id now. Otherwise, it is computed in the open().
             */
            if (theDistributionKind == DistributionKind.SINGLE_PARTITION &&
                (thePushedExternals == null ||
                 thePushedExternals.length == 0)) {
                thePartitionId =
                    primKey.getPartitionId(e.getQCB().getStore());
            }
        } else {
            thePrimaryKey = null;
            theTableName = null;
            theNamespace = null;
        }

        theNumRegs = numRegs;
        theNumIters = numIters;
        theIsUpdate = isUpdate;

        assert(!theIsUpdate ||
               theDistributionKind == DistributionKind.SINGLE_PARTITION);
    }

    /**
     * FastExternalizable constructor.
     */
    public ReceiveIter(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);

        theNumRegs = readPositiveInt(in);
        theNumIters = readPositiveInt(in);
        theInputType = (FieldDefImpl) deserializeFieldDef(in, serialVersion);

        theMayReturnNULL = in.readBoolean();

        theSortFieldPositions = deserializeIntArray(in, serialVersion);
        theSortSpecs = deserializeSortSpecs(in, serialVersion);
        thePrimKeyPositions = deserializeIntArray(in, serialVersion);
        theTupleRegs = deserializeIntArray(in, serialVersion);

        short ordinal = in.readShort();
        theDistributionKind = DistributionKind.values()[ordinal];

        theTableId = in.readLong();

        theTableName = SerializationUtil.readString(in, serialVersion);
        if (theTableName != null) {
            theNamespace = SerializationUtil.readString(in, serialVersion);
            thePrimaryKey = deserializeKey(in, serialVersion);
        } else {
            thePrimaryKey = null;
            theNamespace = null;
        }

        thePushedExternals = deserializeIters(in, serialVersion);

        theIsUpdate = in.readBoolean();

        if (theDistributionKind == DistributionKind.SINGLE_PARTITION &&
            (thePushedExternals == null ||
             thePushedExternals.length == 0)) {
            thePartitionId = new PartitionId(in.readInt());
        }

        byte[] bytes = SerializationUtil.readNonNullByteArray(in);
        setSerializedIter(bytes, serialVersion);

        /* keeps compiler happy regarding final members */
        theInputIter = null;
    }

    /**
     * FastExternalizable writer.  Must call superclass method first to
     * write common elements.
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

        super.writeFastExternal(out, serialVersion);

        out.writeInt(theNumRegs);
        out.writeInt(theNumIters);

        /*
         * theInputIter is not serialized. It is the server side of the query
         * plan and does not need to be used in this path.
         */
        serializeFieldDef(theInputType, out, serialVersion);

        out.writeBoolean(theMayReturnNULL);

        serializeIntArray(theSortFieldPositions, out, serialVersion);
        serializeSortSpecs(theSortSpecs, out, serialVersion);
        serializeIntArray(thePrimKeyPositions, out, serialVersion);
        serializeIntArray(theTupleRegs, out, serialVersion);

        out.writeShort(theDistributionKind.ordinal());

        out.writeLong(theTableId);
        SerializationUtil.writeString(out, serialVersion, theTableName);
        if (theTableName != null) {
            SerializationUtil.writeString(out, serialVersion, theNamespace);
            serializeKey(thePrimaryKey, out, serialVersion);
        }
        serializeIters(thePushedExternals, out, serialVersion);

        out.writeBoolean(theIsUpdate);

        if (theDistributionKind == DistributionKind.SINGLE_PARTITION &&
            (thePushedExternals == null ||
             thePushedExternals.length == 0)) {
            out.writeInt(thePartitionId.getPartitionId());
        }

        byte[] bytes = ensureSerializedIter(serialVersion);
        SerializationUtil.writeNonNullByteArray(out, bytes);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.RECV;
    }

    /*
     * These are public so that PreparedStatementImpl can reconstruct itself from
     * a serialized format.
     */
    public int getNumRegisters() {
        return theNumRegs;
    }

    public int getNumIterators() {
        return theNumIters;
    }

    /*
     * This should be compile-time only so it should be safe to *not* include
     * theInputIter in serialization/deserialization.
     */
    @Override
    public int[] getTupleRegs() {
        return theInputIter.getTupleRegs();
    }

    private boolean doesSort() {
        return (theSortFieldPositions != null);
    }

    @Override
    public void setIterationHandleNotifier(
        IterationHandleNotifier iterHandleNotifier) {

        theAsyncIterHandleNotifier = iterHandleNotifier;
    }

    /**
     * Sets or updates the cached serialized version of the query plan under
     * this ReceiveIter.
     *
     * The method is called from TableQuery.writeFastExternal(). This implies
     * that it will be called by each parallel-scan stream that is created
     * by "this", which further implies that it can be called concurrently by
     * multiple threads.
     *
     * The method cannot be called earlier, because each stream must generate
     * its own serialized plan. This is because the plan generated depends on
     * the version of the RN that the stream connects with (and the RN may
     * change every time the stream requests a new batch of results). Of course,
     * unless a system upgrade is going on, all RNs will have the same version,
     * and all streams will generate the same binary plan. So, to avoid the
     * same plan to be generated again and again, by each stream and each batch
     * of results, whenever one stream generates a binary plan, that plan, as
     * well as the version used for its generation, are cached in the
     * ReceiveIter. If another stream finds a cached plan whose version is the
     * same as the current version used by the stream, the stream can use the
     * cached plan, instead of generating it again.
     */
    public byte[] ensureSerializedIter(short serialVersion) {

        CachedBinaryPlan cachedPlan = theSerializedInputIter;

        if (cachedPlan != null &&
            cachedPlan.getPlan() != null &&
            cachedPlan.getSerialVersion() == serialVersion) {
            return cachedPlan.thePlan;
        }

        synchronized (this) {
            try {
                final ByteArrayOutputStream baos =
                    new ByteArrayOutputStream();
                final DataOutput dataOut = new DataOutputStream(baos);

                PlanIter.serializeIter(theInputIter, dataOut, serialVersion);
                byte[] ba = baos.toByteArray();
                cachedPlan = CachedBinaryPlan.create(ba, serialVersion);
                theSerializedInputIter = cachedPlan;

                return ba;
            }
            catch (IOException ioe) {
                throw new QueryException(ioe);
            }
        }
    }

    public synchronized void setSerializedIter(byte[] bytes,
                                               short serialVersion) {
        assert theSerializedInputIter == null;
        theSerializedInputIter = CachedBinaryPlan.create(bytes, serialVersion);
    }

    /**
     * This method executes a query on the server side and stores in the
     * iterator state a ParalleScanIterator over the results.
     *
     * At some point a refactor of how parallel scan and index scan work may
     * be necessary to take into consideration these facts:
     *  o a query may be an update or read-only (this can probably be known
     *  ahead of time once the query is prepared). In any case the type of
     *  query and Durability specified will affect routing of the query.
     *  o some iterator params are not relevant (direction, keys, ranges, Depth)
     */
    private void ensureIterator(
        RuntimeControlBlock rcb,
        ReceiveIterState state) {

        if (state.theRemoteResultsIter != null) {
            return;
        }

        switch (theDistributionKind) {
        case SINGLE_PARTITION:
            state.theRemoteResultsIter = runOnOnePartition(rcb);
            break;
        case ALL_PARTITIONS:
            state.theRemoteResultsIter = runOnAllPartitions(rcb);
            break;
        case ALL_SHARDS:
            state.theRemoteResultsIter = runOnAllShards(rcb);
            break;
        default:
            throw new QueryStateException(
                "Unknown distribution kind: " + theDistributionKind);
        }

        rcb.setTableIterator(state.theRemoteResultsIter);
    }

    /**
     * Execute the child plan of this ReceiveIter on all partitions
     */
    private AsyncTableIterator<FieldValueImpl> runOnAllPartitions(
        final RuntimeControlBlock rcb) {

        if (rcb.getMaxReadKB() > 0 || rcb.getUseBatchSizeAsLimit()) {
            return new SequentialPartitionsIterator(rcb, null/*partitions*/);
        }

        ExecuteOptions options = rcb.getExecuteOptions();

        /*
         * Compute the direction to be stored in the BaseParallelScanIterator.
         * Because the actual comparisons among the query results are done by
         * the streams, the BaseParallelScanIterator just needs to know whether
         * sorting is needed or not in order to invoke the comparison method or
         * not. So, we just need to pass UNORDERED or FORWARD.
         */
        Direction dir = (theSortFieldPositions != null ?
                         Direction.FORWARD :
                         Direction.UNORDERED);

        StoreIteratorParams params =
            new StoreIteratorParams(
                dir,
                rcb.getBatchSize(),
                null, // key bytes
                null, // key range
                Depth.PARENT_AND_DESCENDANTS,
                rcb.getConsistency(),
                rcb.getTimeout(),
                rcb.getTimeUnit(),
                rcb.getPartitionSet());

        return new PartitionScanIterator<FieldValueImpl>(
            rcb.getStore(), options, params, theAsyncIterHandleNotifier) {

            @Override
            protected QueryPartitionStream createStream(
                RepGroupId groupId,
                int partitionId) {
                return new QueryPartitionStream(groupId, partitionId);
            }

            @Override
            protected TableQuery generateGetterOp(byte[] resumeKey) {
                throw new QueryStateException("Unexpected call");
            }

            @Override
            protected void convertResult(
                Result result,
                List<FieldValueImpl> elementList) {

                List<FieldValueImpl> queryResults = result.getQueryResults();

                // TODO: try to avoid this useless loop
                for (FieldValueImpl res : queryResults) {
                    elementList.add(res);
                }
            }

            @Override
            protected int compare(FieldValueImpl one, FieldValueImpl two) {
                throw new QueryStateException("Unexpected call");
            }

            class QueryPartitionStream extends PartitionStream {

                private ResumeInfo theResumeInfo = new ResumeInfo(rcb);

                QueryPartitionStream(RepGroupId groupId, int partitionId) {
                    super(groupId, partitionId, null);
                }

                @Override
                protected Request makeReadRequest() {

                    TableQuery op = new TableQuery(
                        DistributionKind.ALL_PARTITIONS,
                        theInputType,
                        theMayReturnNULL,
                        ReceiveIter.this,
                        rcb.getExternalVars(),
                        theNumIters,
                        theNumRegs,
                        theTableId,
                        rcb.getMathContext(),
                        rcb.getTraceLevel(),
                        rcb.getBatchSize(),
                        0, /* maxCurrentReadKB */
                        0, /* maxReadKB*/
                        theResumeInfo,
                        1 /* emptyReadFactor */);

                    return storeImpl.makeReadRequest(
                        op,
                        new PartitionId(partitionId),
                        storeIteratorParams.getConsistency(),
                        storeIteratorParams.getTimeout(),
                        storeIteratorParams.getTimeoutUnit(),
                        null);
                }

                @Override
                protected void setResumeKey(Result result) {

                    QueryResult res = (QueryResult)result;
                    theResumeInfo.refresh(res.getResumeInfo());

                    if (rcb.getTraceLevel() >= 1) {
                        rcb.trace("Received " + res.getNumRecords() +
                                  " results from group : " + groupId +
                                  " partition " + partitionId);
                    }

                    if (rcb.getTraceLevel() >= 4) {
                        rcb.trace(theResumeInfo.toString());
                    }
                }

                @Override
                protected int compareInternal(Stream o) {

                    QueryPartitionStream other = (QueryPartitionStream)o;
                    int cmp;

                    FieldValueImpl v1 =
                        currentResultSet.getQueryResults().
                        get(currentResultPos);

                    FieldValueImpl v2 =
                        other.currentResultSet.getQueryResults().
                        get(other.currentResultPos);

                    if (theInputType.isRecord()) {
                        RecordValueImpl rec1 = (RecordValueImpl)v1;
                        RecordValueImpl rec2 = (RecordValueImpl)v2;
                        cmp = compareRecords(rec1, rec2);
                    } else {
                        cmp = compareAtomics(v1, v2, 0);
                    }

                    if (cmp == 0) {
                        return (partitionId < other.partitionId ? -1 : 1);
                    }

                    return cmp;
                }
            }
        };
    }

    /**
     * Execute the child plan of this ReceiveIter on a single partition
     */
    private AsyncTableIterator<FieldValueImpl> runOnOnePartition(
        final RuntimeControlBlock rcb) {

        ReceiveIterState state = (ReceiveIterState)rcb.getState(theStatePos);
        final PartitionId pid = state.thePartitionId;

        PartitionId[] partitions = new PartitionId[1];
        partitions[0] = pid;

        return new SequentialPartitionsIterator(rcb, partitions);
    }

    /**
     * Execute the child plan of this ReceiveIter on all shards
     * TODO: remove duplicates in result
     */
    private AsyncTableIterator<FieldValueImpl> runOnAllShards(
        final RuntimeControlBlock rcb) {

        if (rcb.getMaxReadKB() > 0 || rcb.getUseBatchSizeAsLimit()) {
            /* If size limit is specified, scan shards sequentially. */
            return new SequentialShardsIterator(rcb);
        }

        ExecuteOptions options = rcb.getExecuteOptions();

        /*
         * Compute the direction to be stored in the BaseParallelScanIterator.
         * Because the actual comparisons among the query results are done by
         * the streams, the BaseParallelScanIterator just needs to know whether
         * sorting is needed or not in order to invoke the comparison method or
         * not. So, we just need to pass UNORDERED or FORWARD.
         */
        Direction dir = (theSortFieldPositions != null ?
                         Direction.FORWARD :
                         Direction.UNORDERED);

        return new ShardScanIterator<FieldValueImpl>(
             rcb.getStore(), options, dir, rcb.getShardSet(),
             theAsyncIterHandleNotifier) {

            @Override
            protected QueryShardStream createStream(RepGroupId groupId) {
                return new QueryShardStream(groupId);
            }

            @Override
            protected TableQuery createOp(
                byte[] resumeSecondaryKey,
                byte[] resumePrimaryKey) {
                throw new QueryStateException("Unexpected call");
            }

            @Override
            protected void convertResult(
                Result result,
                List<FieldValueImpl> elementList) {

                List<FieldValueImpl> queryResults = result.getQueryResults();
                for (FieldValueImpl res : queryResults) {
                    elementList.add(res);
                }
            }

            @Override
            protected int compare(FieldValueImpl one, FieldValueImpl two) {
                throw new QueryStateException("Unexpected call");
            }

            class QueryShardStream extends ShardStream {

                private ResumeInfo theResumeInfo = new ResumeInfo(rcb);

                QueryShardStream(RepGroupId groupId) {
                    super(groupId, null, null);
                }

                @Override
                protected Request makeReadRequest() {

                    TableQuery op = new TableQuery(
                        DistributionKind.ALL_SHARDS,
                        theInputType,
                        theMayReturnNULL,
                        ReceiveIter.this,
                        rcb.getExternalVars(),
                        theNumIters,
                        theNumRegs,
                        theTableId,
                        rcb.getMathContext(),
                        rcb.getTraceLevel(),
                        rcb.getBatchSize(),
                        0, /* maxCurrentReadKB */
                        0, /* maxReadKB */
                        theResumeInfo,
                        1 /* emptyReadFactor */);

                    return storeImpl.makeReadRequest(
                        op,
                        groupId,
                        consistency,
                        requestTimeoutMs,
                        MILLISECONDS,
                        null);
                }

                @Override
                protected void setResumeKey(Result result) {

                    QueryResult res = (QueryResult)result;
                    theResumeInfo.refresh(res.getResumeInfo());

                    if (rcb.getTraceLevel() >= 1) {
                        rcb.trace("Received " + res.getNumRecords() +
                                  " results from group : " + groupId +
                                  " shard " + groupId);
                    }

                    if (rcb.getTraceLevel() >= 4) {
                        rcb.trace(theResumeInfo.toString());
                    }
                }

                @Override
                protected int compareInternal(Stream o) {

                    QueryShardStream other = (QueryShardStream)o;
                    int cmp;

                    FieldValueImpl v1 =
                        currentResultSet.getQueryResults().
                        get(currentResultPos);

                    FieldValueImpl v2 =
                        other.currentResultSet.getQueryResults().
                        get(other.currentResultPos);

                    if (theInputType.isRecord()) {
                        RecordValueImpl rec1 = (RecordValueImpl)v1;
                        RecordValueImpl rec2 = (RecordValueImpl)v2;
                        cmp = compareRecords(rec1, rec2);
                    } else {
                        cmp = compareAtomics(v1, v2, 0);
                    }

                    if (cmp == 0) {
                        return getGroupId().compareTo(other.getGroupId());
                    }

                    return cmp;
                }
            }
        };
    }

    @Override
    public void open(RuntimeControlBlock rcb) {

        boolean alwaysFalse = false;

        PartitionId pid = PartitionId.NULL_ID;

        if (theDistributionKind == DistributionKind.SINGLE_PARTITION) {

            if (thePushedExternals != null &&
                thePushedExternals.length > 0) {

                /*
                 * Make a copy of thePrimaryKey in order to replace its
                 * "dummy", placeholder values with the corresponding values
                 * of the external-variable expressions
                 *
                 * Optimize the local case where thePrimaryKey is an actual
                 * PrimaryKeyImpl, avoiding a potentially costly getTable()
                 * call.
                 */
                PrimaryKeyImpl primaryKey;
                TableImpl table;
                if (thePrimaryKey instanceof PrimaryKeyImpl) {
                    primaryKey = (PrimaryKeyImpl) thePrimaryKey;
                    table = (TableImpl) primaryKey.getTable();
                } else {
                    table = rcb.getMetadataHelper().
                        getTable(theNamespace, theTableName);
                    primaryKey = table.createPrimaryKey(thePrimaryKey);
                }

                int size = thePushedExternals.length;

                for (int i = 0; i < size; ++i) {

                    PlanIter iter = thePushedExternals[i];

                    if (iter == null) {
                        continue;
                    }

                    iter.open(rcb);
                    iter.next(rcb);
                    FieldValueImpl val = rcb.getRegVal(iter.getResultReg());
                    iter.close(rcb);

                    FieldValueImpl newVal = BaseTableIter.castValueToIndexKey(
                        table, null, i, val, FuncCode.OP_EQ);

                    if (newVal != val) {
                        if (newVal == BooleanValueImpl.falseValue) {
                            alwaysFalse = true;
                            break;
                        }

                        val = newVal;
                    }

                    String colName = table.getPrimaryKeyColumnName(i);
                    primaryKey.put(colName, val);
                }

                pid = primaryKey.getPartitionId(rcb.getStore());

            } else {
                pid = thePartitionId;
            }
        }

        ReceiveIterState state =
            new ReceiveIterState(pid, (thePrimKeyPositions != null));

        rcb.setState(theStatePos, state);

        if (theTupleRegs != null) {
            TupleValue tuple = new TupleValue((RecordDefImpl)theInputType,
                                              rcb.getRegisters(),
                                              theTupleRegs);
            rcb.setRegVal(theResultReg, tuple);
        }

        if (alwaysFalse) {
            state.done();
        }
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {
        return nextInternal(rcb, false /* localOnly */);
    }

    @Override
    public boolean nextLocal(RuntimeControlBlock rcb) {
        return nextInternal(rcb, true /* localOnly */);
    }

    private boolean nextInternal(RuntimeControlBlock rcb, boolean localOnly) {

        /*
         * Catch StoreIteratorException and if the cause is a QueryException,
         * throw that instead to provide more information to the caller.
         */
        try {
            ReceiveIterState state =
                (ReceiveIterState)rcb.getState(theStatePos);

            if (state.isDone()) {
                return false;
            }

            ensureIterator(rcb, state);

            FieldValueImpl res;

            do {
                if (localOnly) {
                    res = state.theRemoteResultsIter.nextLocal();

                    if (res == null) {
                        if (state.theRemoteResultsIter.isClosed() &&
                            !state.isClosed()) {
                            state.done();
                        }
                        return false;
                    }
                } else {
                    boolean more = state.theRemoteResultsIter.hasNext();

                    if (!more) {
                        state.done();
                        return false;
                    }

                    res = state.theRemoteResultsIter.next();
                }

                /* Eliminate index duplicates */
                if (thePrimKeyPositions != null) {
                    BinaryValueImpl binPrimKey = createBinaryPrimKey(res);
                    boolean added = state.thePrimKeysSet.add(binPrimKey);
                    if (!added) {
                        continue;
                    }
                }

                break;

            } while (true);

            if (theTupleRegs != null) {
                TupleValue tuple = (TupleValue)rcb.getRegVal(theResultReg);
                tuple.toTuple((RecordValueImpl)res, doesSort());
            } else if (doesSort() && res.isRecord()) {
                ((RecordValueImpl)res).convertEmptyToNull();
                rcb.setRegVal(theResultReg, res);
            } else {
                rcb.setRegVal(theResultReg,
                              res.isEMPTY() ? NullValueImpl.getInstance() : res);
            }

            return true;

        } catch (StoreIteratorException sie) {
            final Throwable cause = sie.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new IllegalStateException("Unexpected exception: " + cause,
                                            cause);
        }
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {
        ReceiveIterState state = (ReceiveIterState)rcb.getState(theStatePos);
        state.reset(this);
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        ReceiveIterState state = (ReceiveIterState)rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        state.close();
    }

    @Override
    public Throwable getCloseException(RuntimeControlBlock rcb) {

        final ReceiveIterState state =
            (ReceiveIterState) rcb.getState(theStatePos);

        if (state == null) {
            return null;
        }

        if (state.theRemoteResultsIter != null) {
            return state.theRemoteResultsIter.getCloseException();
        }

        return state.theRemoteResultsIterCloseException;
    }

    private BinaryValueImpl createBinaryPrimKey(FieldValueImpl result) {

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutput out = new DataOutputStream(baos);

        try {
            if (!result.isRecord()) {
                assert(thePrimKeyPositions.length == 1);
                writeValue(out, result, 0);

            } else {
                for (int i = 0; i < thePrimKeyPositions.length; ++i) {

                    FieldValueImpl fval =
                        ((RecordValueImpl)result).get(thePrimKeyPositions[i]);

                    writeValue(out, fval, i);
                }
            }
        } catch (IOException e) {
            throw new QueryStateException(
                "Failed to create binary prim key due to IOException:\n" +
                e.getMessage());
        }

        byte[] bytes = baos.toByteArray();
        return FieldDefImpl.binaryDef.createBinary(bytes);
    }

    private void writeValue(DataOutput out, FieldValueImpl val, int i)
        throws IOException {

        switch (val.getType()) {
        case INTEGER:
            SerializationUtil.writePackedInt(out, val.getInt());
            break;
        case LONG:
            SerializationUtil.writePackedLong(out, val.getLong());
            break;
        case DOUBLE:
            out.writeDouble(val.getDouble());
            break;
        case FLOAT:
            out.writeFloat(val.getFloat());
            break;
        case STRING:
            /* Use the current format */
            SerializationUtil.writeString(
                out, SerialVersion.CURRENT, val.getString());
            break;
        case ENUM:
            out.writeShort(val.asEnum().getIndex());
            break;
        case TIMESTAMP:
            TimestampValueImpl ts = (TimestampValueImpl)val;
            writeNonNullByteArray(out, ts.getBytes());
            break;
        case NUMBER:
            NumberValueImpl num = (NumberValueImpl)val;
            writeNonNullByteArray(out, num.getBytes());
            break;
        default:
            throw new QueryStateException(
                "Unexpected type for primary key column : " +
                val.getType() + ", at result column " + i);
        }
    }

    @Override
    protected void displayContent(StringBuilder sb, QueryFormatter formatter) {

        if (theSortFieldPositions != null) {
            formatter.indent(sb);
            sb.append("Sort Field Positions : ");
            for (int i = 0; i < theSortFieldPositions.length; ++i) {
                sb.append(theSortFieldPositions[i]);
                if (i < theSortFieldPositions.length - 1) {
                    sb.append(", ");
                }
            }
            sb.append(",\n");
        }

        if (thePrimKeyPositions != null) {
            formatter.indent(sb);
            sb.append("Primary Key Positions : ");
            for (int i = 0; i < thePrimKeyPositions.length; ++i) {
                sb.append(thePrimKeyPositions[i]);
                if (i < thePrimKeyPositions.length - 1) {
                    sb.append(", ");
                }
            }
            sb.append(",\n");
        }

        formatter.indent(sb);
        sb.append("DistributionKind : ").append(theDistributionKind);
        sb.append(",\n");

        if (thePushedExternals != null) {
            sb.append("\n");
            formatter.indent(sb);
            sb.append("EXTERNAL KEY EXPRS: ").append(thePushedExternals.length);

            for (PlanIter iter : thePushedExternals) {

                sb.append("\n");
                if (iter != null) {
                    iter.display(sb, formatter);
                } else {
                    formatter.indent(sb);
                    sb.append("null");
                }
            }
            sb.append(",\n\n");
        }

        formatter.indent(sb);
        sb.append("Number of Registers :").append(theNumRegs);
        sb.append(",\n");
        formatter.indent(sb);
        sb.append("Number of Iterators :").append(theNumIters);
        sb.append(",\n");
        theInputIter.display(sb, formatter);
    }

    int compareRecords(RecordValueImpl rec1, RecordValueImpl rec2) {

        for (int i = 0; i < theSortFieldPositions.length; ++i) {
            int pos = theSortFieldPositions[i];
            FieldValueImpl v1 = rec1.get(pos);
            FieldValueImpl v2 = rec2.get(pos);

            int comp = compareAtomics(v1, v2, i);

            if (comp != 0) {
                return comp;
            }
        }

        /* they must be equal */
        return 0;
    }

    int compareAtomics(FieldValueImpl v1, FieldValueImpl v2, int sortPos) {

        int comp;

        if (v1.isNull()) {
            if (v2.isNull()) {
                comp = 0;
            } else {
                comp = (theSortSpecs[sortPos].theNullsFirst ? -1 : 1);
            }

        } else if (v2.isNull()) {
            comp = (theSortSpecs[sortPos].theNullsFirst ? 1 : -1);

        } else if (v1.isEMPTY()) {
            if (v2.isEMPTY()) {
                comp = 0;
            } else if (v2.isJsonNull()) {
                comp = (theSortSpecs[sortPos].theNullsFirst ? 1 : -1);
            } else {
                comp = (theSortSpecs[sortPos].theNullsFirst ? -1 : 1);
            }

        } else if (v2.isEMPTY()) {
            if (v1.isJsonNull()) {
                comp = (theSortSpecs[sortPos].theNullsFirst ? -1 : 1);
            } else {
                comp = (theSortSpecs[sortPos].theNullsFirst ? 1 : -1);
            }

        } else if (v1.isJsonNull()) {
            if (v1.isJsonNull()) {
                comp = 0;
            } else {
                comp = (theSortSpecs[sortPos].theNullsFirst ? -1 : 1);
            }

        } else if (v2.isJsonNull()) {
            comp = (theSortSpecs[sortPos].theNullsFirst ? 1 : -1);

        } else {
            comp = v1.compareTo(v2);
        }

        return (theSortSpecs[sortPos].theIsDesc ? -comp : comp);
    }

    /**
     * The partitions iterator that scans all partitions from 1 to N
     * sequentially.
     */
    private class SequentialPartitionsIterator
        implements AsyncTableIterator<FieldValueImpl> {

        private final RuntimeControlBlock theRCB;

        private final PartitionId[] thePartitions;

        /*
         * A single-partition iterator used to scan the current partition.
         */
        private AbstractScanIterator thePartitionIter;

        /*
         * The position within thePartitions of the partition being scanned
         * currently.
         */
        private int thePidIdx;

        /*
         * theInitResumeInfo stores the deserialized values from
         * the continuation key given back to us by the app.
         */
        private ResumeInfo theInitResumeInfo;

        SequentialPartitionsIterator(
            RuntimeControlBlock rcb,
            PartitionId[] partitions) {

            theRCB = rcb;
            byte[] contKey = rcb.getContinuationKey();

            if (partitions != null) {
                thePartitions = partitions;
            } else {
                Set<PartitionId> pids = rcb.getStore().getTopology().
                                        getPartitionMap().getAllIds();
                thePartitions = pids.toArray(new PartitionId[pids.size()]);
            }

            if (contKey != null) {
                parseContinuationKey(contKey);
            }

            /* Set emptyReadFactor to 1 for single partition scan. */
            int emptyReadFactor = (thePartitions.length == 1) ? 1 : 0;
            thePartitionIter =
                new AbstractScanIterator(theRCB,
                                         thePartitions[thePidIdx],
                                         null,/* group id */
                                         theInitResumeInfo,
                                         theRCB.getBatchSize(),
                                         theRCB.getMaxReadKB(),
                                         emptyReadFactor);
        }

        /**
         * Initializes the partitions iterator according to the specified
         * continuation key, if the continuation key is null, then create
         * iterator for the 1st partition.
         */
        private void parseContinuationKey(byte[] contKey) {

            final ByteArrayInputStream bais =
                new ByteArrayInputStream(contKey);
            final DataInput in = new DataInputStream(bais);

            short v = SerialVersion.CURRENT;

            try {
                thePidIdx = in.readInt();

                if (thePidIdx < 0 || thePidIdx >= thePartitions.length) {
                    throw new IllegalArgumentException(
                        "Invalid partition id in continuation key: " +
                        thePidIdx);
                }

                theInitResumeInfo = new ResumeInfo(in, v);

            } catch (IOException e) {
                throw new QueryStateException(
                    "Failed to parse continuation key");
            }
        }

        private void createContinuationKey() {

            final ByteArrayOutputStream baos =
                new ByteArrayOutputStream();
            final DataOutput out = new DataOutputStream(baos);

            short v = SerialVersion.CURRENT;

            try {
                out.writeInt(thePidIdx);
                thePartitionIter.theResumeInfo.writeFastExternal(out, v);
            } catch (IOException e) {
                throw new QueryStateException(
                    "Failed to create continuation key. Reason:\n" +
                    e.getMessage());
            }

            byte[] contKey = baos.toByteArray();
            theRCB.setContinuationKey(contKey);
        }

        @Override
        public boolean hasNext() {

            if (thePartitionIter == null) {
                return false;
            }

            while (!thePartitionIter.hasNext()) {

                 if (theRCB.getReachedLimit()) {
                    createContinuationKey();
                    return false;
                }

                if ((++thePidIdx) == thePartitions.length) {
                    /* Return false if no more partition to scan */
                    thePartitionIter.close();
                    thePartitionIter = null;
                    theRCB.setContinuationKey(null);
                    return false;
                }

                /*
                 * Calculates the read size limit for scanning on the next
                 * partition, return false if reached limit.
                 */
                int maxReadKB = 0;
                if (theRCB.getMaxReadKB() > 0) {
                    maxReadKB = theRCB.getMaxReadKB() - theRCB.getReadKB();
                    if (maxReadKB <= 0) {
                        createContinuationKey();
                        return false;
                    }
                }

                /*
                 * Calculates the batch size limit for scanning on the next
                 * partition, return false if reached limit.
                 */
                int maxReadNum;
                if (theRCB.getUseBatchSizeAsLimit()) {
                    maxReadNum = theRCB.getBatchSize() - theRCB.getResultSize();
                    if (maxReadNum <= 0) {
                        createContinuationKey();
                        return false;
                    }
                } else {
                    maxReadNum = theRCB.getBatchSize();
                }

                /* Open iterator on next partition */
                /*
                 * Set emptyReadFactor to 1 if no entry read until scan on the
                 * last partition.
                 */
                int emptyReadFactor =
                    (theRCB.getReadKB() == 0 &&
                     thePidIdx == thePartitions.length - 1) ? 1 : 0;
                thePartitionIter.initForNextPartition(thePartitions[thePidIdx],
                                                      maxReadNum,
                                                      maxReadKB,
                                                      emptyReadFactor);
            }

            return true;
        }

        @Override
        public FieldValueImpl nextLocal() {

            if (thePartitionIter == null) {
                return null;
            }

            /*
             * Note that the various size limit checks performed by hasNext,
             * which is called by next, are not needed here because this local
             * version does not advance to another iterator.
             */
            return thePartitionIter.nextLocal();
        }

        @Override
        public FieldValueImpl next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return thePartitionIter.next();
        }

        @Override
        public void close() {
            if (thePartitionIter != null) {
                thePartitionIter.close();
                thePartitionIter = null;
                thePidIdx = 0;
            }
        }

        @Override
        public boolean isClosed() {
            return thePartitionIter == null || thePartitionIter.isClosed();
        }

        @Override
        public Throwable getCloseException() {
            return (thePartitionIter != null ?
                    thePartitionIter.getCloseException() :
                    null);
        }

        @Override
        public List<DetailedMetrics> getPartitionMetrics() {
            return Collections.emptyList();
        }

        @Override
        public List<DetailedMetrics> getShardMetrics() {
            return Collections.emptyList();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * The all shards iterator that scans all shards sequentially.
     */
    private class SequentialShardsIterator
        implements AsyncTableIterator<FieldValueImpl> {

        private final RuntimeControlBlock theRCB;

        private final RepGroupId[] theShards;

        /* The index of shard being scanned, zero-based index */
        private int theShardIdx;

        /* The current shard scan iterator */
        private AbstractScanIterator theShardIter;

        /*
         * theInitResumeInfo stores the deserialized values from
         * the continuation key given back to us by the app.
         */
        private ResumeInfo theInitResumeInfo;

        SequentialShardsIterator(RuntimeControlBlock rcb) {

            theRCB = rcb;

            Set<RepGroupId> gpIds =
                theRCB.getStore().getTopology().getRepGroupIds();
            theShards = gpIds.toArray(new RepGroupId[gpIds.size()]);

            byte[] contKey = theRCB.getContinuationKey();

            if (contKey != null) {
                parseContinuationKey(contKey);
            }

            /* Set emptyReadFactor to 1 for single shard scan. */
            int emptyReadFactor = (theShards.length == 1) ? 1 : 0;
            theShardIter = new AbstractScanIterator(theRCB,
                                                    null, /*partition id*/
                                                    theShards[theShardIdx],
                                                    theInitResumeInfo,
                                                    theRCB.getBatchSize(),
                                                    theRCB.getMaxReadKB(),
                                                    emptyReadFactor);
        }

        private void parseContinuationKey(byte[] contKey) {

            final ByteArrayInputStream bais =
                new ByteArrayInputStream(contKey);
            final DataInput in = new DataInputStream(bais);

            short v = SerialVersion.CURRENT;

            try {
                theShardIdx = in.readInt();

                if (theShardIdx < 0 || theShardIdx >= theShards.length) {
                    throw new IllegalArgumentException(
                        "Invalid shard id in continuation key: " +
                        theShardIdx);
                }

                theInitResumeInfo = new ResumeInfo(in, v);

            } catch (IOException e) {
                throw new QueryStateException(
                    "Failed to parse continuation key");
            }
        }

        private void createContinuationKey() {

            final ByteArrayOutputStream baos =
                new ByteArrayOutputStream();
            final DataOutput out = new DataOutputStream(baos);

            short v = SerialVersion.CURRENT;

            try {
                out.writeInt(theShardIdx);
                theShardIter.theResumeInfo.writeFastExternal(out, v);
            } catch (IOException e) {
                throw new QueryStateException(
                    "Failed to create continuation key. Reason:\n" +
                    e.getMessage());
            }

            byte[] contKey = baos.toByteArray();
            theRCB.setContinuationKey(contKey);
        }

        @Override
        public boolean hasNext() {

            if (theShardIter == null) {
                return false;
            }

            while (!theShardIter.hasNext()) {

                if (theRCB.getReachedLimit()) {
                    createContinuationKey();
                    return false;
                }

                if ((++theShardIdx) == theShards.length) {
                    /* Return false if no more shard to scan */
                    theShardIter.close();
                    theShardIter = null;
                    theRCB.setContinuationKey(null);
                    return false;
                }

                /*
                 * Calculates the read size limit for scanning on the next
                 * shard, return false if reached limit.
                 */
                int maxReadKB = 0;
                if (theRCB.getMaxReadKB() > 0) {
                    maxReadKB = theRCB.getMaxReadKB() - theRCB.getReadKB();
                    if (maxReadKB <= 0) {
                        createContinuationKey();
                        return false;
                    }
                }

                /*
                 * Calculates the batch size limit for scanning on the next
                 * shard, return false if reached limit.
                 */
                int maxReadNum;
                if (theRCB.getUseBatchSizeAsLimit()) {
                    maxReadNum = theRCB.getBatchSize() - theRCB.getResultSize();
                    if (maxReadNum <= 0) {
                        createContinuationKey();
                        return false;
                    }
                } else {
                    maxReadNum = theRCB.getBatchSize();
                }

                /* Open iterator on next shard */

                /*
                 * Set emptyReadFactor to 1 if no entry read until scan on the
                 * last shard.
                 */
                int emptyReadFactor =
                    (theRCB.getReadKB() == 0 &&
                     theShardIdx == theShards.length - 1) ? 1 : 0;
                theShardIter.initForNextShard(theShards[theShardIdx],
                                              maxReadNum,
                                              maxReadKB,
                                              emptyReadFactor);
            }

            return true;
        }

        @Override
        public FieldValueImpl next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return theShardIter.next();
        }

        @Override
        public FieldValueImpl nextLocal() {
            if (theShardIter == null) {
                return null;
            }
            return theShardIter.nextLocal();
        }

        @Override
        public void close() {
            if (theShardIter != null) {
                theShardIter = null;
                theShardIdx = 0;
            }
        }

        @Override
        public boolean isClosed() {
            return theShardIter == null || theShardIter.isClosed();
        }

        @Override
        public Throwable getCloseException() {
            return (theShardIter != null ?
                    theShardIter.getCloseException() :
                    null);
        }

        @Override
        public List<DetailedMetrics> getPartitionMetrics() {
            return Collections.emptyList();
        }

        @Override
        public List<DetailedMetrics> getShardMetrics() {
            return Collections.emptyList();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Implements iterative table scan in a single partition/shard. Used by
     * the SequentialPartitionsIterator and SequentialShardsIterator.
     *
     * Note: No synchronization is needed for async mode, because there can
     * only a single pending remote request in the cases where an
     * AbstractScanIterator is used.
     */
    private class AbstractScanIterator implements
        AsyncTableIterator<FieldValueImpl> {

        private final RuntimeControlBlock theRCB;

        private PartitionId thePid;

        private RepGroupId theGroupId;

        private final ResumeInfo theResumeInfo;

        private int theMaxReadNum;

        private int theMaxReadKB;

        private int theEmptyReadFactor;

        private Iterator<FieldValueImpl> theResultsIter;

        private boolean theMoreRemoteResults;

        private Throwable theAsyncCloseException;

        private boolean theIsClosed;

        private boolean theAsyncRequestExecuting;

        public AbstractScanIterator(
            RuntimeControlBlock rcb,
            PartitionId pid,
            RepGroupId gid,
            ResumeInfo resumeInfo,
            int maxReadNum,
            int maxReadKB,
            int emptyReadFactor) {

            theRCB = rcb;

            thePid = pid;
            theGroupId = gid;

            if (resumeInfo == null) {
                theResumeInfo = new ResumeInfo(rcb);
            } else {
                theResumeInfo = resumeInfo;
                theResumeInfo.setRCB(rcb);
            }

            theMaxReadNum = maxReadNum;
            theMaxReadKB = maxReadKB;

            theMoreRemoteResults = true;
            theResultsIter = null;

            theEmptyReadFactor = emptyReadFactor;
        }

        void initForNextPartition(
            PartitionId pid,
            int maxReadNum,
            int maxReadKB,
            int emptyReadFactor) {
            initForNextScan(pid, null, maxReadNum, maxReadKB, emptyReadFactor);
        }

        void initForNextShard(
            RepGroupId gid,
            int maxReadNum,
            int maxReadKB,
            int emptyReadFactor) {
            initForNextScan(null, gid, maxReadNum, maxReadKB, emptyReadFactor);
        }

        private void initForNextScan(
            PartitionId pid,
            RepGroupId gid,
            int maxReadNum,
            int maxReadKB,
            int emptyReadFactor) {

            thePid = pid;
            theGroupId = gid;

            theResumeInfo.reset();

            theMaxReadNum = maxReadNum;
            theMaxReadKB = maxReadKB;

            theMoreRemoteResults = true;
            theResultsIter = null;

            theEmptyReadFactor = emptyReadFactor;
        }

        /* Create request for TableQuery operation */
        Request createRequest() {

            TableQuery op = new TableQuery(
                        theDistributionKind,
                        theInputType,
                        theMayReturnNULL,
                        ReceiveIter.this,
                        theRCB.getExternalVars(),
                        theNumIters,
                        theNumRegs,
                        theTableId,
                        theRCB.getMathContext(),
                        theRCB.getTraceLevel(),
                        theMaxReadNum,
                        theMaxReadKB,
                        theRCB.getMaxReadKB(),
                        theResumeInfo,
                        theEmptyReadFactor);

            final Consistency consistency = theRCB.getConsistency();
            final Durability durability = theRCB.getDurability();
            final long timeout = theRCB.getTimeout();
            final TimeUnit timeUnit = theRCB.getTimeUnit();
            final KVStoreImpl store = theRCB.getStore();

            if (thePid != null) {

                if (theIsUpdate) {
                    return store.makeWriteRequest(op, thePid, durability,
                                                  timeout, timeUnit, null);
                }

                return store.makeReadRequest(op, thePid, consistency,
                                             timeout, timeUnit, null);
            }

            return store.makeReadRequest(op, theGroupId, consistency,
                                         timeout, timeUnit, null);
        }

        @Override
        public boolean hasNext() {

            if (theResultsIter != null && theResultsIter.hasNext()) {
                return true;
            }

            theResultsIter = null;

            /*
             * Stop to fetch next batch if no more elements to fetch or has
             * reached the size or number limit.
             */
            if (!theMoreRemoteResults || theRCB.getReachedLimit()) {
                return false;
            }

            Request req = createRequest();

            KVStoreImpl store = theRCB.getStore();
            QueryResult result = (QueryResult)store.executeRequest(req);

            return processResults(result);
        }

        private boolean processResults(QueryResult result) {

            final List<FieldValueImpl> results = result.getQueryResults();

            theMoreRemoteResults = result.hasMoreElements();

            theResumeInfo.refresh(result.getResumeInfo());

            theRCB.tallyReadKB(result.getReadKB());
            theRCB.tallyWriteKB(result.getWriteKB());
            if (theRCB.getUseBatchSizeAsLimit()) {
                theRCB.tallyResultSize(results.size());
            }

            if (results.isEmpty()) {
                assert(result.getExceededSizeLimit() || !theMoreRemoteResults);
                if (result.getExceededSizeLimit()) {
                    theRCB.setReachedLimit(true);
                }
                return false;
            }

            theResultsIter = results.iterator();

            if (theMoreRemoteResults) {
                /*
                 * So far, there still has more elements to fetch.
                 *
                 * 1. If there is size limit specified and reached the
                 *    the size limit, store the resume key and mark the
                 *    flag in RCB.
                 * 2. If no size limit or not reaches size limit, there is
                 *    a number limit specified, then store the resume key
                 *    and mark the flag in RCB, because "theMoreRemoteResults
                 *    is true" implies the "batchSize" number of records
                 *    are returned, and still has more.
                 * 3. If no for 1 and 2, then calculates the left size limit
                 *    for next batch's fetching.
                 */
                boolean reachSizeLimit =
                    (result.getExceededSizeLimit() ||
                     (theMaxReadKB > 0 && theMaxReadKB == result.getReadKB()));

                if (reachSizeLimit || theRCB.getUseBatchSizeAsLimit()) {
                    theRCB.setReachedLimit(true);
                } else {
                    /* Calculates the left maxReadKB for next fetch */
                    if (theMaxReadKB > 0) {
                        theMaxReadKB -= result.getReadKB();
                    }
                }
            }

            return true;
        }

        @Override
        public FieldValueImpl nextLocal() {

            /*
             * This method must be called without the lock held to avoid lock
             * problems that could arise if notifyNext were called in the
             * current thread and then called back into this class.
             */
            if (Thread.holdsLock(this)) {
                throw new IllegalStateException(
                    "nextLocal called with lock held");
            }

            /* Return next */
            if (theResultsIter != null && theResultsIter.hasNext()) {
                return theResultsIter.next();
            }

            /* Throw any close exception */
            if (theAsyncCloseException instanceof RuntimeException) {
                throw (RuntimeException) theAsyncCloseException;
            }

            if (theAsyncCloseException instanceof Error) {
                throw (Error) theAsyncCloseException;
            }

            if (theAsyncCloseException != null) {
                throw new IllegalStateException(
                    "Unexpected exception from async iteration: " +
                    theAsyncCloseException,
                    theAsyncCloseException);
            }

            if (isClosed()) {
                return null;
            }

            /* Initiate a request if one isn't already underway */
            if (theAsyncRequestExecuting) {
                return null;
            }

            Request request = createRequest();

            theAsyncRequestExecuting = true;
            theRCB.getStore().executeRequest(
                request,
                new ResultHandler<Result>() {
                    @Override
                    public void onResult(Result r, Throwable e) {
                        theAsyncRequestExecuting = false;
                        handleExecuteResult(r, e);
                    }
                });

            return null;
        }

        private void handleExecuteResult(Result r, Throwable e) {

            assert !Thread.holdsLock(this);

            if (r != null) {
                processResults((QueryResult) r);
            } else {
                theAsyncCloseException = e;
                close();
            }

            theAsyncIterHandleNotifier.notifyNext();
        }

        @Override
        public FieldValueImpl next() {
            return theResultsIter.next();
        }

        @Override
        public void close() {
            theResultsIter = null;
            theIsClosed = true;
        }

        @Override
        public boolean isClosed() {

            if (theIsClosed) {
                return true;
            }

            if (theResultsIter != null && theResultsIter.hasNext()) {
                return false;
            }

            if (!theMoreRemoteResults) {
                close();
                return true;
            }

            return false;
        }

        @Override
        public synchronized Throwable getCloseException() {
            return theAsyncCloseException;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<DetailedMetrics> getPartitionMetrics() {
            return Collections.emptyList();
        }

        @Override
        public List<DetailedMetrics> getShardMetrics() {
            return Collections.emptyList();
        }
    }
}
