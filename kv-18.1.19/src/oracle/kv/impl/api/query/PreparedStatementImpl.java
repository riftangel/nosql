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

package oracle.kv.impl.api.query;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldDefSerialization;
import oracle.kv.impl.api.table.FieldDefFactory;
import oracle.kv.impl.api.table.FieldMap;
import oracle.kv.impl.api.table.PrimaryKeyImpl;
import oracle.kv.impl.api.table.RecordDefImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.compiler.QueryControlBlock;
import oracle.kv.impl.query.compiler.StaticContext.VarInfo;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.SerialVersion;

import oracle.kv.AsyncExecutionHandle;
import oracle.kv.StatementResult;
import oracle.kv.query.BoundStatement;
import oracle.kv.query.PreparedStatement;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.RecordDef;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;

/**
 * Implementation of a PreparedStatement. This class contains the query plan,
 * along with enough information to construct a runtime context in which the
 * query can be executed (RuntimeControlBlock).
 *
 * An instance of PreparedStatementImpl is created by CompilerAPI.prepare(),
 * after the query has been compiled.
 */
public class PreparedStatementImpl
    implements PreparedStatement,
               InternalStatement {

    /**
     * The type of distribution for the query.
     */
    public enum DistributionKind {
        SINGLE_PARTITION, /** the query goes to a single partition */
        ALL_PARTITIONS,   /** the query goes to all partitions */
        ALL_SHARDS,       /** the query goes to all shards (it uses an index) */
        UNKNOWN           /** the distribution is UNKNOWN */
    }

    /*
     * The query plan
     */
    private final PlanIter queryPlan;

    /*
     * The type of the result
     */
    private final RecordDefImpl resultDef;

    /*
     * Whether the query result should be wrapped in a one-field record
     */
    private boolean wrapResultInRecord;

    /*
     * The number of registers required to run the plan
     */
    private final int numRegisters;

    /*
     * The number of iterators in the plan
     */
    private final int numIterators;

    /*
     * externalVars maps the name of each external var declared in the query
     * to the numeric id and data type for that var.
     */
    private final Map<String, VarInfo> externalVars;

    /*
     * The DistributionKind
     */
    private final DistributionKind distributionKind;

    /*
     * The PartitionId for SINGLE_PARTITION distribution, null or
     * PartitionID.NULL_ID otherwise.
     */
    private final PartitionId partitionId;

    /*
     * The PrimaryKey for SINGLE_PARTITION distribution. It is only valid for
     * SINGLE_PARTITION distribution. It is null or empty otherwise.
     */
    private final PrimaryKeyImpl shardKey;

    private final long tableId;

    private final String tableName;

    /*
     * Needed for unit testing only
     */
    private QueryControlBlock qcb;

    public PreparedStatementImpl(
        PlanIter queryPlan,
        FieldDefImpl resultDef,
        int numRegisters,
        int numIterators,
        Map<String, VarInfo> externalVars,
        QueryControlBlock qcb) {

        this.queryPlan = queryPlan;
        this.numRegisters = numRegisters;
        this.numIterators = numIterators;
        this.externalVars = externalVars;
        this.qcb = qcb;
        this.distributionKind = qcb.getPushedDistributionKind();
        this.tableId = qcb.getTargetTableId();
        this.tableName = qcb.getTargetTableName();
        PrimaryKeyImpl pkey = qcb.getPushedPrimaryKey();

        if (pkey != null && pkey.isEmpty()) {
            this.shardKey = null;
        } else {
            this.shardKey = pkey;
        }

        if (shardKey != null &&
            distributionKind == DistributionKind.SINGLE_PARTITION) {
            partitionId = shardKey.getPartitionId(qcb.getStore());
        } else {
            partitionId = PartitionId.NULL_ID;
        }

        if (qcb.wrapResultInRecord()) {

            wrapResultInRecord = true;

            String fname = qcb.getResultColumnName();
            if (fname == null) {
                fname = "Column_1";
            }

            FieldMap fieldMap = new FieldMap();
            fieldMap.put(fname,
                         resultDef,
                         true/*nullable*/,
                         null/*defaultValue*/);

            this.resultDef = FieldDefFactory.createRecordDef(fieldMap, null);

        } else {
            wrapResultInRecord = false;
            this.resultDef = (RecordDefImpl)resultDef;
        }
    }

    @Override
    public RecordDef getResultDef() {
        return resultDef;
    }

    boolean wrapResultInRecord() {
        return wrapResultInRecord;
    }

    @Override
    public Map<String, FieldDef> getVariableTypes() {
        return getExternalVarsTypes();
    }

    @Override
    public FieldDef getVariableType(String variableName) {
        return getExternalVarType(variableName);
    }

    @Override
    public BoundStatement createBoundStatement() {
        return new BoundStatementImpl(this);
    }

    public boolean hasSort() {
        return qcb.hasSort();
    }

    /*
     * Needed for unit testing only
     */
    public QueryControlBlock getQCB() {
        return qcb;
    }

    public PlanIter getQueryPlan() {
    	return queryPlan;
    }

    public int getNumRegisters() {
        return numRegisters;
    }

    public int getNumIterators() {
        return numIterators;
    }

    public Map<String, FieldDef> getExternalVarsTypes() {

        Map<String, FieldDef> varsMap = new HashMap<String, FieldDef>();

        if (externalVars == null) {
            return varsMap;
        }

        for (Map.Entry<String, VarInfo> entry : externalVars.entrySet()) {
            String varName = entry.getKey();
            VarInfo vi = entry.getValue();
            varsMap.put(varName, vi.getType().getDef());
        }

        return varsMap;
    }

    boolean hasExternalVars() {
        return (externalVars != null && !externalVars.isEmpty());
    }

    public FieldDef getExternalVarType(String name) {

        if (externalVars == null) {
            return null;
        }

        VarInfo vi = externalVars.get(name);
        if (vi != null) {
            return vi.getType().getDef();
        }

        return null;
    }

    long getTableId() {
        return tableId;
    }

    public String getTableName() {
        return tableName;
    }

    /**
     * Convert the map of external vars (maping names to values) to an array
     * with the values only. The array is indexed by an internalid assigned
     * to each external variable. This method also checks that all the external
     * vars declared in the query have been bound.
     */
    public FieldValue[] getExternalVarsArray(Map<String, FieldValue> vars) {

        if (externalVars == null) {
            assert(vars.isEmpty());
            return null;
        }

        int count = 0;
        for (Map.Entry<String, VarInfo> entry : externalVars.entrySet()) {
            String name = entry.getKey();
            ++count;

            if (vars.get(name) == null) {
                throw new IllegalArgumentException(
                    "Variable " + name + " has not been bound");
            }
        }

        FieldValue[] array = new FieldValue[count];

        count = 0;
        for (Map.Entry<String, FieldValue> entry : vars.entrySet()) {
            String name = entry.getKey();
            FieldValue value = entry.getValue();

            VarInfo vi = externalVars.get(name);
            if (vi == null) {
                throw new IllegalStateException(
                    "Variable " + name + " does not appear in query");
            }

            array[vi.getId()] = value;
            ++count;
        }

        assert(count == array.length);
        return array;
    }

    @Override
    public String toString() {
        return queryPlan.display();
    }

    @Override
    public StatementResult executeSync(
        KVStoreImpl store,
        ExecuteOptions options) {

        if (options == null) {
            options = new ExecuteOptions();
        }

        return new QueryStatementResultImpl(
            store.getTableAPIImpl(), options, this, false /* async */);
    }

    @Override
    public AsyncExecutionHandle executeAsync(KVStoreImpl store,
                                             ExecuteOptions options) {

        if (options == null) {
            options = new ExecuteOptions();
        }

        final QueryStatementResultImpl result =
            new QueryStatementResultImpl(store.getTableAPIImpl(), options,
                                         this, true /* async */);
        return result.getExecutionHandle();
    }

    /*
     * These methods may eventually be part of the public PreparedStatement
     * interface. If done, DistributionKind would move there as well.
     */

    /**
     * Returns the DistributionKind for the prepared query
     */
    public DistributionKind getDistributionKind() {
        return distributionKind;
    }

    /**
     * Returns the shard key for the prepared query. It is only valid if
     * DistributionKind is SINGLE_PARTITION. Otherwise it returns null.
     */
    public PrimaryKey getShardKey() {
        return shardKey;
    }

    /**
     * Returns the PartitionId for queries with DistributionKind of
     * SINGLE_PARTITION. NOTE: for queries with variables that are part of the
     * shard key this method will return an incorrect PartitionId. This is
     * because the query compiler puts a placeholder value in the PrimaryKey
     * that is a valid value for the type. TBD: detect this and throw an
     * exception if this method is called on such a query.
     */
    public PartitionId getPartitionId() {
        return partitionId;
    }

    /**
     * Not part of the public PreparedStatement interface available to
     * external clients. This method is employed when the Oracle NoSQL DB
     * Hive/BigDataSQL integration mechanism is used to process a query,
     * and disjoint partition sets are specified for each split.
     */
    public StatementResult executeSyncPartitions(
        KVStoreImpl store,
        ExecuteOptions options,
        final Set<Integer> partitions) {

        return new QueryStatementResultImpl(
            store.getTableAPIImpl(), options, this, false /* async */,
            partitions, null);
    }

    /**
     * Not part of the public PreparedStatement interface available to
     * external clients. This method is employed when the Oracle NoSQL DB
     * Hive/BigDataSQL integration mechanism is used to process a query,
     * and disjoint shard sets are specified for each split.
     */
    public StatementResult executeSyncShards(
        final KVStoreImpl store,
        final ExecuteOptions options,
        final Set<RepGroupId> shards) {

        return new QueryStatementResultImpl(
            store.getTableAPIImpl(), options, this, false /* async */, null,
            shards);
    }

    /**
     * Serialize a prepared statement into a byte array for use by remote
     * callers such as the cloud driver.
     * The information is:
     *   serialVersion
     *   distributionKind
     *   table name
     *   table id
     *   externalVars
     *   client-side query plan (to ReceiveIter)
     *   numIterators and numRegisters
     *   resultDef
     *   boolean queryReturnsRecord
     */
    public byte[] toByteArray()
        throws IOException {

        assert qcb != null;

        /*
         * The current version of the client serves as the serial version
         * for the entire prepared query
         */
        final short version = SerialVersion.CURRENT;
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutput out = new DataOutputStream(baos);

        out.writeShort(version);
        out.writeByte((distributionKind != null) ?
            distributionKind.ordinal() : -1);
        SerializationUtil.writeNonNullString(out, version, tableName);
        SerializationUtil.writePackedLong(out, tableId);

        /* external variables, may be null */
        writeExternalVars(out, version);

        /* Query Plan */
        PlanIter.serializeIter(queryPlan, out, version);

        /* numIterators and numRegisters */
        out.writeInt(numIterators);
        out.writeInt(numRegisters);

        FieldDefSerialization.writeFieldDef(resultDef, out, version);
        out.writeBoolean(wrapResultInRecord);

        return baos.toByteArray(); /* is there a more efficient way? */
    }

    public PreparedStatementImpl(DataInput in)
        throws IOException {

        try {
            final short version = in.readShort();
            if (version < SerialVersion.V16 || version > SerialVersion.CURRENT) {
                raiseDeserializeError("unexpected version value: " + version);
            }

            byte ordinal = in.readByte();
            if (ordinal != (byte)(-1)) {
                if (ordinal < 0 || ordinal > DistributionKind.values().length) {
                    raiseDeserializeError("unexpected value for DistributionKind");
                }
                distributionKind = DistributionKind.values()[ordinal];
            } else {
                distributionKind = null;
            }

            tableName = SerializationUtil.readString(in, version);
            if (tableName == null) {
                raiseDeserializeError("tableName should not be null");
            }
            tableId = SerializationUtil.readPackedLong(in);
            if (tableId < 0) {
                raiseDeserializeError("tableId should not be negative value");
            }

            /* external variables, if any */
            externalVars = readExternalVars(in, version);

            /* Query plan */
            queryPlan = PlanIter.deserializeIter(in, version);
            if (queryPlan == null) {
                raiseDeserializeError("query plan is null");
            }

            /* numIterators and numRegisters */
            numIterators = in.readInt();
            if (numIterators < 1) {
                raiseDeserializeError
                    ("numIterators should not be 0 or negative value");
            }
            numRegisters = in.readInt();
            if (numRegisters < 0) {
                raiseDeserializeError
                    ("numRegisters should not be 0 or negative value");
            }

            resultDef = (RecordDefImpl)
                FieldDefSerialization.readFieldDef(in, version);

            wrapResultInRecord = in.readBoolean();

            /* to make the compiler happy about final members that aren't used */
            partitionId = null;
            shardKey = null;
        } catch (QueryException qe) {
            throw qe.getIllegalArgument();
        } catch (RuntimeException re) {
            throw new IllegalArgumentException
            ("Read PreparedStatement failed: " + re);
        }
    }


    private void raiseDeserializeError(String msg) {
        throw new QueryException
            ("Deserializing PreparedStatement failed: " + msg);
    }

    private void writeExternalVars(DataOutput out, short version)
        throws IOException {

        if (externalVars == null) {
            out.writeInt(0);
        }
        out.writeInt(externalVars.size());
        for (Map.Entry<String, VarInfo> entry : externalVars.entrySet()) {
            SerializationUtil.writeNonNullString(out, version, entry.getKey());
            VarInfo info = entry.getValue();
            out.writeInt(info.getId());
            PlanIter.serializeExprType(info.getType(), out, version);
        }
    }

    private Map<String, VarInfo> readExternalVars(DataInput in, short version)
        throws IOException {
        int numVars = in.readInt();
        if (numVars == 0) {
            return null;
        }

        if (numVars < 0) {
            raiseDeserializeError("Unexpected negtive value: " + numVars);
        }
        try {
            Map<String, VarInfo> vars = new HashMap<String, VarInfo>(numVars);
            for (int i = 0; i < numVars; i++) {
                String name = SerializationUtil.readString(in, version);
                VarInfo info = VarInfo.createVarInfo(
                    in.readInt(), // id
                    PlanIter.deserializeExprType(in, version));
                vars.put(name, info);
            }
            return vars;
        } catch (RuntimeException re) {
            raiseDeserializeError(re.getMessage());
        }
        return null;
    }
}
