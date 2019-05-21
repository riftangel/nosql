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

import java.util.ArrayList;
import java.util.List;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.api.table.TableMetadataHelper;
import oracle.kv.impl.api.table.TupleValue;
import oracle.kv.impl.fault.WrappedClientException;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryRuntimeException;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.runtime.ResumeInfo;
import oracle.kv.impl.query.runtime.RuntimeControlBlock;
import oracle.kv.impl.query.runtime.server.ServerIterFactoryImpl;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.query.ExecuteOptions;
import com.sleepycat.je.Transaction;

/**
 * Server handler for {@link TableQuery}.
 */
class TableQueryHandler extends InternalOperationHandler<TableQuery> {

    TableQueryHandler(OperationHandler handler, OpCode opCode) {
        super(handler, opCode, TableQuery.class);
    }

    @Override
    List<? extends KVStorePrivilege> getRequiredPrivileges(TableQuery op) {
        /*
         * Checks the basic privilege for authentication here, and leave the
         * keyspace checking and the table access checking in
         * {@code verifyTableAccess()}.
         */
        return SystemPrivilege.usrviewPrivList;
    }

    @Override
    Result execute(TableQuery op,
                   Transaction txn,
                   PartitionId partitionId) {

        final int batchSize = op.getBatchSize();

        final List<FieldValueImpl> results =
            new ArrayList<FieldValueImpl>(batchSize);

        TableMetadataHelper mdHelper = getMetadataHelper();

        ExecuteOptions options = new ExecuteOptions();
        options.setResultsBatchSize(batchSize);
        options.setTraceLevel(op.getTraceLevel());
        options.setMathContext(op.getMathContext());
        options.setMaxReadKB(op.getMaxReadKB());

        RuntimeControlBlock rcb = new RuntimeControlBlock(
            null, /* KVStoreImpl not needed and not available */
            getLogger(),
            mdHelper,
            null, /* partitions */
            null, /* shards */
            options,
            op,
            new ServerIterFactoryImpl(txn, partitionId, operationHandler),
            op.getQueryPlan(),
            op.getNumIterators(),
            op.getNumRegisters(),
            op.getExternalVars());

        ResumeInfo ri = op.getResumeInfo();
        ri.setRCB(rcb);

        if (rcb.getTraceLevel() >= 1) {
            rcb.trace("Executing query on partition " + partitionId);
        }

        if (rcb.getTraceLevel() >= 2) {
            rcb.trace("Resume Info:\n" + ri);
        }

        executeQueryPlan(op, rcb, results, partitionId);

        /*
         * Resume key is both input and output parameter for RCB. If set on
         * output there are more keys to be found in this iteration.
         */
        ri.setNumResultsComputed(results.size());

        byte[] newPrimaryResumeKey = ri.getPrimResumeKey();
        byte[] newSecondaryResumeKey = ri.getSecResumeKey();
        boolean more = (rcb.getReachedLimit() ||
                        (results.size() == batchSize &&
                         (newPrimaryResumeKey != null ||
                          newSecondaryResumeKey != null)));

        if (rcb.getTraceLevel() >= 1) {
            rcb.trace("Produced a batch of " + results.size() +
                      " results on partition " + partitionId +
                      " number of KB read = " + rcb.getReadKB() +
                      " more results = " + more);

            if (rcb.getTraceLevel() >= 2) {
                rcb.trace(ri.toString());
            }
        }

        return new Result.QueryResult(getOpCode(),
                                      rcb.getReadKB(),
                                      rcb.getWriteKB(),
                                      results,
                                      op.getResultDef(),
                                      op.mayReturnNULL(),
                                      more,
                                      ri,
                                      rcb.getReachedLimit());
    }

    /**
     * Returns a TableMetadataHelper instance available on this node.
     */
    private TableMetadataHelper getMetadataHelper() {
        final TableMetadata md =
            (TableMetadata) getRepNode().getMetadata(MetadataType.TABLE);
        if (md == null) {
            final String msg = "Query execution unable to get metadata";
            getLogger().warning(msg);

            /*
             * Wrap this exception into one that can be thrown to the client.
             */
            new QueryStateException(msg).throwClientException();
        }
        return md;
    }

    private void executeQueryPlan(
        TableQuery op,
        RuntimeControlBlock rcb,
        List<FieldValueImpl> results,
        PartitionId pid) {

        final int batchSize = op.getBatchSize();
        final PlanIter queryPlan = op.getQueryPlan();
        FieldValueImpl res = null;
        boolean noException = false;

        try {
            queryPlan.open(rcb);

            while ((batchSize == 0 || results.size() < batchSize) &&
                   queryPlan.next(rcb)) {

                res = rcb.getRegVal(queryPlan.getResultReg());
                if (res.isTuple()) {
                    res = ((TupleValue)res).toRecord();
                }

                if (rcb.getTraceLevel() >= 2) {
                    rcb.trace("Produced result on " + pid + " :\n" +
                              res);
                }

                results.add(res);
            }

            noException = true;
        } catch (QueryException qe) {

            /*
             * For debugging and tracing this can temporarily use level INFO
             */
            getLogger().fine("Query execution failed: " + qe);

            /*
             * Turn this into a wrapped IllegalArgumentException so that it can
             * be passed to the client.
             */
            throw qe.getWrappedIllegalArgument();
        } catch (QueryStateException qse) {

            /*
             * This exception indicates a bug or problem in the engine. Log
             * it. It will be thrown through to the client side.
             */
            getLogger().warning(qse.toString());

            /*
             * Wrap this exception into one that can be thrown to the client.
             */
            qse.throwClientException();

        } catch (IllegalArgumentException e) {
            throw new WrappedClientException(e);
        } catch (RuntimeException re) {
            throw new QueryRuntimeException(re);
        } finally {
            try {
                queryPlan.close(rcb);
            } catch (RuntimeException re) {
                if (noException) {
                    throw new QueryRuntimeException(re);
                }
            }
        }
    }
}
