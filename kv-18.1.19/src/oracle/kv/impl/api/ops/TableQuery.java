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

package oracle.kv.impl.api.ops;

import static oracle.kv.impl.api.ops.InternalOperationHandler.MIN_READ;
import static oracle.kv.impl.util.SerialVersion.EMPTY_READ_FACTOR_VERSION;
import static oracle.kv.impl.util.SerialVersion.MAXKB_ITERATE_VERSION;
import static oracle.kv.impl.util.SerialVersion.QUERY_VERSION;
import static oracle.kv.impl.util.SerialVersion.QUERY_VERSION_2;
import static oracle.kv.impl.util.SerialVersion.QUERY_VERSION_3;
import static oracle.kv.impl.util.SerialVersion.QUERY_VERSION_4;
import static oracle.kv.impl.util.SerialVersion.QUERY_VERSION_5;
import static oracle.kv.impl.util.SerialVersion.QUERY_VERSION_6;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.MathContext;
import java.math.RoundingMode;

import oracle.kv.impl.api.query.PreparedStatementImpl.DistributionKind;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldDefSerialization;
import oracle.kv.impl.api.table.FieldValueSerialization;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.runtime.ReceiveIter;
import oracle.kv.impl.query.runtime.ResumeInfo;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.table.FieldValue;



/**
 * TableQuery represents and drives the execution of a query subplan at a
 * server site, over one partition or one shard.
 *
 * Instances of TableQuery are created via the parallel-scan infrastructure,
 * when invoked from the open() method of the ReceiveIter in the query plan.
 *
 * This class contains the result schema (resultDef) so it can be passed
 * to the result class (Result.QueryResult) and serialized only once for each
 * batch of results. If this were not done, each RecordValue result would have
 * its associated RecordDef serialized (redundantly) along with it.
 */
public class TableQuery extends InternalOperation {

    private final FieldDefImpl resultDef;

    /*
     * added in QUERY_VERSION_2
     */
    private final boolean mayReturnNULL;

    private final PlanIter queryPlan;

    /*
     * Optional Bind Variables. If none exist or are not set this is null.
     * If it would be easier for callers this could be made an empty Map.
     */
    private final FieldValue[] externalVars;

    private final int numIterators;

    private final int numRegisters;

    /*
     * If known this is used by the server side for resource tracking.
     * Otherwise, it's zero.
     */
    private final long tableId;

    /*
     * Added in QUERY_VERSION_4
     */
    private final MathContext mathContext;

    private final byte traceLevel;

    private final int batchSize;

    /*
     * The maximum number of KB that the query is allowed to read during
     * the execution of this TableQuery operation. This will be <= to the
     * maxReadKB field below, because the query may have already consumed
     * some bytes in a previous incarnation.
     */
    private final int currentMaxReadKB;

    /*
     * The maximum number of KB that the query is allowed to read during
     * the execution of any TableQuery op created by the query.
     */
    private final int maxReadKB;

    private final ResumeInfo resumeInfo;

    private final int emptyReadFactor;

    public TableQuery(
        DistributionKind distKind,
        FieldDefImpl resultDef,
        boolean mayReturnNULL,
        PlanIter queryPlan,
        FieldValue[] externalVars,
        int numIterators,
        int numRegisters,
        long tableId,
        MathContext mathContext,
        byte traceLevel,
        int batchSize,
        int currentMaxReadKB,
        int maxReadKB,
        ResumeInfo resumeInfo,
        int emptyReadFactor) {

        /*
         * The distinct OpCodes are primarily for a finer granularity of
         * statistics, allowing the different types of queries to be tallied
         * independently.
         */
        super(distKind == DistributionKind.ALL_PARTITIONS ?
              OpCode.QUERY_MULTI_PARTITION :
              (distKind == DistributionKind.ALL_SHARDS ?
               OpCode.QUERY_MULTI_SHARD :
               OpCode.QUERY_SINGLE_PARTITION));
        this.resultDef = resultDef;
        this.mayReturnNULL = mayReturnNULL;
        this.queryPlan = queryPlan;
        this.externalVars = externalVars;
        this.numIterators = numIterators;
        this.numRegisters = numRegisters;
        this.tableId = tableId;
        this.mathContext = mathContext;
        this.traceLevel = traceLevel;
        this.batchSize = batchSize;
        this.currentMaxReadKB = currentMaxReadKB;
        this.maxReadKB = maxReadKB;
        this.resumeInfo = resumeInfo;
        /* emptyReadFactor is serialized as a byte */
        assert emptyReadFactor <= Byte.MAX_VALUE;
        this.emptyReadFactor = emptyReadFactor;
    }

    FieldDefImpl getResultDef() {
        return resultDef;
    }

    boolean mayReturnNULL() {
        return mayReturnNULL;
    }

    PlanIter getQueryPlan() {
        return queryPlan;
    }

    FieldValue[] getExternalVars() {
        return externalVars;
    }

    int getNumIterators() {
        return numIterators;
    }

    int getNumRegisters() {
        return numRegisters;
    }

    @Override
    public long getTableId() {
        return tableId;
    }

    public MathContext getMathContext() {
        return mathContext;
    }

    byte getTraceLevel() {
        return traceLevel;
    }

    int getBatchSize() {
        return batchSize;
    }

    public int getCurrentMaxReadKB() {
        return currentMaxReadKB;
    }

    public int getMaxReadKB() {
        return maxReadKB;
    }

    public ResumeInfo getResumeInfo() {
        return resumeInfo;
    }

    @Override
    void addEmptyReadCharge() {
        /* Override to factor in the emptyReadFactor */
        addReadBytes(MIN_READ * emptyReadFactor);
    }

    public int getEmptyReadFactor() {
        return emptyReadFactor;
    }

    /**
     * FastExternalizable writer.  Must call superclass method first to write
     * common elements.
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);

        if (serialVersion < QUERY_VERSION_2) {
            /* write query-plan version */
            out.writeShort(1);
        }

        /*
         * A TableQuery instance is always created at the client initially,
         * by the receive iterator, which passes itself as the queryPlan arg
         * of the TableQuery constructor. So, initially, this.queryPlan is a
         * ReceiveIter. However, when the TableQuery is serialized and sent
         * to the server for execution, it is not the ReceiveIter that is
         * serialized, but it's child (which is an SFWIter normally). So, when
         * the TableQuery is deserialized and instantiated at the server,
         * this.queryPlan is an SFWIter.
         *
         * If something goes "wrong" at the server, for example the partition
         * has migrated, the server may try to forward the TableQuery to another
         * server and calls TableQuery.writeFastExternal() again. In this case,
         * this.queryPlan is not a ReceiveIter and the plan must be serialized
         * "from scratch" again.
         */
        if (queryPlan instanceof ReceiveIter) {
            byte[] serializedQueryPlan =
                ((ReceiveIter)queryPlan).ensureSerializedIter(serialVersion);
            out.write(serializedQueryPlan);
        } else {
            final ByteArrayOutputStream baos =
                new ByteArrayOutputStream();
            final DataOutput dataOut = new DataOutputStream(baos);

            PlanIter.serializeIter(queryPlan, dataOut, serialVersion);
            out.write(baos.toByteArray());
        }

        FieldDefSerialization.writeFieldDef(resultDef, out, serialVersion);

        if (serialVersion >= QUERY_VERSION_2) {
            out.writeBoolean(mayReturnNULL);
        }

        writeExternalVars(externalVars, out, serialVersion);

        out.writeInt(numIterators);
        out.writeInt(numRegisters);
        out.writeInt(batchSize);

        if (serialVersion >= QUERY_VERSION_3) {
            out.writeByte(traceLevel);
        }

        if (serialVersion >= QUERY_VERSION_6) {
            resumeInfo.writeFastExternal(out, serialVersion);
        } else {
            if (serialVersion >= QUERY_VERSION_5) {
                out.writeInt(resumeInfo.getCurrentIndexRange());
            }

            byte[] primKey = resumeInfo.getPrimResumeKey();
            byte[] secKey = resumeInfo.getSecResumeKey();

            if (primKey == null) {
                out.writeShort(-1);
            } else {
                out.writeShort(primKey.length);
            out.write(primKey);
            }
            if (secKey == null) {
                out.writeShort(-1);
            } else {
                out.writeShort(secKey.length);
                out.write(secKey);
            }

            out.writeLong(resumeInfo.getNumResultsComputed());
        }

        if (serialVersion >= QUERY_VERSION_4) {
            writeMathContext(mathContext, out);
        }

        if (serialVersion >= QUERY_VERSION_6) {
            out.writeLong(tableId);
        }

        if (serialVersion >= MAXKB_ITERATE_VERSION) {
            out.writeInt(currentMaxReadKB);
            out.writeInt(maxReadKB);
        }

        if (serialVersion >= EMPTY_READ_FACTOR_VERSION) {
            out.writeByte(emptyReadFactor);
        }
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    protected TableQuery(OpCode opCode, DataInput in, short serialVersion)
        throws IOException {

        super(opCode, in, serialVersion);

        if (serialVersion < QUERY_VERSION) {
            String required =
                SerialVersion.getKVVersion(QUERY_VERSION).
                getNumericVersionString();
            String found =
                SerialVersion.getKVVersion(serialVersion).
                getNumericVersionString();
            throw new UnsupportedOperationException(
                "Query operations require a client version equal to or greater"
                + " than " + required + ". The client version detected is " +
                found);
        }

        try {

            if (serialVersion < QUERY_VERSION_2) {
                /* read and skip the query-plan version */
                in.readShort();
            }

            queryPlan = PlanIter.deserializeIter(in, serialVersion);
            resultDef = FieldDefSerialization.readFieldDef(in, serialVersion);

            if (serialVersion < QUERY_VERSION_2) {
                mayReturnNULL = false;
            } else {
                mayReturnNULL = in.readBoolean();
            }
            externalVars = readExternalVars(in, serialVersion);

            numIterators = in.readInt();
            numRegisters = in.readInt();
            batchSize = in.readInt();

            if (serialVersion < QUERY_VERSION_3) {
                traceLevel = 0;
            } else {
                traceLevel = in.readByte();
            }

            if (serialVersion >= QUERY_VERSION_6) {
                resumeInfo = new ResumeInfo(in, serialVersion);
            } else {
                resumeInfo = new ResumeInfo(null);

                if (serialVersion < QUERY_VERSION_5) {
                    resumeInfo.setCurrentIndexRange(0);
                } else {
                    resumeInfo.setCurrentIndexRange(in.readInt());
                }

                int keyLen = in.readShort();
                if (keyLen < 0) {
                    resumeInfo.setPrimResumeKey(null);
                } else {
                    byte[] key = new byte[keyLen];
                    in.readFully(key);
                    resumeInfo.setPrimResumeKey(key);
                }

                keyLen = in.readShort();
                if (keyLen < 0) {
                    resumeInfo.setSecResumeKey(null);
                } else {
                    byte[] key = new byte[keyLen];
                    in.readFully(key);
                    resumeInfo.setSecResumeKey(key);
                }

                resumeInfo.setNumResultsComputed((int)in.readLong());
            }

            if (serialVersion < QUERY_VERSION_4) {
                mathContext = MathContext.DECIMAL32;
            } else {
                mathContext = readMathContext(in);
            }

            if (serialVersion < QUERY_VERSION_6) {
                tableId = 0;
            } else {
                tableId = in.readLong();
            }

            if (serialVersion < MAXKB_ITERATE_VERSION) {
                currentMaxReadKB = 0;
                maxReadKB = 0;
            } else {
                currentMaxReadKB = in.readInt();
                maxReadKB = in.readInt();
            }

            if (serialVersion < EMPTY_READ_FACTOR_VERSION) {
                emptyReadFactor = 0;
            } else {
                emptyReadFactor = in.readByte();
            }

        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        } catch (RuntimeException re) {
            re.printStackTrace();
            throw new QueryStateException("Read TableQuery failed: " + re);
        }
    }

    static void writeExternalVars(
        FieldValue[] vars,
        DataOutput out,
        short serialVersion)
        throws IOException {

        if (vars != null && vars.length > 0) {
            int numVars = vars.length;
            out.writeInt(numVars);

            for (int i = 0; i < numVars; ++i) {
                FieldValueSerialization.writeFieldValue(vars[i],
                                                        true, // writeValDef
                                                        out, serialVersion);
            }
        } else {
            out.writeInt(0);
        }
    }

    static FieldValue[] readExternalVars(DataInput in, short serialVersion)
        throws IOException {

        int numVars = in.readInt();
        if (numVars == 0) {
            return null;
        }

        FieldValue[] vars = new FieldValue[numVars];

        for (int i = 0; i < numVars; i++) {
            FieldValue val =
                FieldValueSerialization.readFieldValue(null, // def
                                                       in, serialVersion);

            vars[i] = val;
        }
        return vars;
    }

    static void writeMathContext(
        MathContext mathContext,
        DataOutput out)
        throws IOException {

        if (mathContext == null) {
            out.writeByte(0);
        } else if (MathContext.DECIMAL32.equals(mathContext)) {
            out.writeByte(1);
        } else if (MathContext.DECIMAL64.equals(mathContext)) {
            out.writeByte(2);
        } else if (MathContext.DECIMAL128.equals(mathContext)) {
            out.writeByte(3);
        } else if (MathContext.UNLIMITED.equals(mathContext)) {
            out.writeByte(4);
        } else {
            out.writeByte(5);
            out.writeInt(mathContext.getPrecision());
            out.writeInt(mathContext.getRoundingMode().ordinal());
        }
    }

    static MathContext readMathContext(DataInput in)
        throws IOException {

        int code = in.readByte();

        switch (code) {
        case 0:
            return null;
        case 1:
            return MathContext.DECIMAL32;
        case 2:
            return MathContext.DECIMAL64;
        case 3:
            return MathContext.DECIMAL128;
        case 4:
            return MathContext.UNLIMITED;
        case 5:
            int precision = in.readInt();
            int roundingMode = in.readInt();
            return
                new MathContext(precision, RoundingMode.valueOf(roundingMode));
        default:
            throw new QueryStateException("Unknown MathContext code.");
        }
    }
}
