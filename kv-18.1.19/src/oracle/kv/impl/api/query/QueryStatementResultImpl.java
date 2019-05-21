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

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.logging.Logger;

import oracle.kv.AsyncExecutionHandle;
import oracle.kv.FastExternalizableException;
import oracle.kv.StatementResult;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TupleValue;
import oracle.kv.impl.async.AsyncIterationHandleImpl;
import oracle.kv.impl.async.AsyncTableIterator;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.runtime.RuntimeControlBlock;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.query.Statement;
import oracle.kv.stats.DetailedMetrics;
import oracle.kv.table.FieldValue;
import oracle.kv.table.RecordDef;
import oracle.kv.table.RecordValue;
import oracle.kv.table.TableIterator;


/**
 * Implementation of StatementResult when statement is a query.
 */
public class QueryStatementResultImpl implements StatementResult {

    private final PreparedStatementImpl statement;

    private final AsyncExecutionHandleImpl executionHandle;

    private final QueryResultIterator iterator;

    private boolean closed;

    public QueryStatementResultImpl(TableAPIImpl tableAPI,
                                    ExecuteOptions options,
                                    PreparedStatementImpl stmt,
                                    boolean async) {
        this(tableAPI, options, stmt, null, async, null, null);
    }

    public QueryStatementResultImpl(TableAPIImpl tableAPI,
                                    ExecuteOptions options,
                                    BoundStatementImpl stmt,
                                    boolean async) {
        this(tableAPI, options, stmt.getPreparedStmt(),
             stmt.getPreparedStmt().getExternalVarsArray(stmt.getVariables()),
             async, null, null);
    }

    public QueryStatementResultImpl(TableAPIImpl tableAPI,
                                    ExecuteOptions options,
                                    PreparedStatementImpl stmt,
                                    boolean async,
                                    Set<Integer> partitions,
                                    Set<RepGroupId> shards) {
        this(tableAPI, options, stmt, null, async, partitions, shards);
    }

    private QueryStatementResultImpl(TableAPIImpl tableAPI,
                                     ExecuteOptions options,
                                     PreparedStatementImpl ps,
                                     FieldValue[] externalVars,
                                     boolean async,
                                     final Set<Integer> partitions,
                                     final Set<RepGroupId> shards) {

        if (ps.hasExternalVars() && (externalVars == null)) {
            throw new QueryException(
                "The query contains external variables, none of which " +
                "has been bound. Create a BoundStatement to bind the " +
                "variables");
        }

        statement = ps;
        PlanIter iter = ps.getQueryPlan();
        RecordDef resultDef = ps.getResultDef();

        Logger logger = tableAPI.getStore().getLogger();

        executionHandle = (async ? new AsyncExecutionHandleImpl(logger) : null);

        RuntimeControlBlock rcb = new RuntimeControlBlock(
            tableAPI.getStore(),
            tableAPI.getStore().getLogger(),
            tableAPI.getTableMetadataHelper(),
            partitions,
            shards,
            options, /* ExecuteOptions */
            iter,
            ps.getNumIterators(),
            ps.getNumRegisters(),
            externalVars);

        this.iterator = new QueryResultIterator(rcb, iter, resultDef);
        closed = false;
    }

    @Override
    public void close() {
        iterator.close();
        closed = true;
    }

    @Override
    public RecordDef getResultDef() {
        if (closed) {
            throw new IllegalStateException("Statement result already closed.");
        }

        return iterator.getResultDef();
    }


    @Override
    public TableIterator<RecordValue> iterator() {

        if (executionHandle != null) {
            throw new IllegalStateException(
                "Application-driven iteration is not allowed for queries " +
                "executed in asynchronous mode");
        }

        if (closed) {
            throw new IllegalStateException("Statement result already closed.");
        }

        return iterator;
    }

    public AsyncExecutionHandle getExecutionHandle() {
        return executionHandle;
    }

    @Override
    public int getPlanId() {
        return 0;
    }

    @Override
    public String getInfo() {
        return null;
    }

    @Override
    public String getInfoAsJson() {
        return null;
    }

    @Override
    public String getErrorMessage() {
        return null;
    }

    @Override
    public boolean isSuccessful() {
        return true;
    }

    @Override
    public boolean isDone() {
        return !iterator.hasNext();
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public String getResult() {
        return null;
    }

    @Override
    public Kind getKind() {
        return Kind.QUERY;
    }

    /**
     * Returns the KB read during the execution of operation.
     */
    public int getReadKB() {
        return iterator.getReadKB();
    }

    /**
     * Returns the KB written during the execution of operation.
     */
    public int getWriteKB() {
        return iterator.getWriteKB();
    }

    /**
     * Returns the continuation key for the next execution.
     */
    public byte[] getContinuationKey() {
        return iterator.getContinuationKey();
    }

    private class QueryResultIterator
            implements AsyncTableIterator<RecordValue> {

        private final RuntimeControlBlock rcb;
        private final PlanIter rootIter;
        private final RecordDef resultDef;

        private boolean hasNext;
        private boolean hasNextLocal;

        QueryResultIterator(
            RuntimeControlBlock rcb,
            PlanIter iter,
            RecordDef resultDef) {

            this.rcb = rcb;
            rootIter = iter;
            this.resultDef = resultDef;

            if (executionHandle != null) {

                /*
                 * Store the notifier in the iterator, which will supply it to
                 * its children, if any.  That way, children can notify the
                 * execution handle directly.  Requests the handle makes to
                 * obtain more iteration results will still need to filter down
                 * to the children.
                 */
                iter.setIterationHandleNotifier(executionHandle);
                executionHandle.setIterator(this);
            }

            try {
                rootIter.open(rcb);
                updateHasNext(executionHandle != null);
            } catch (QueryStateException qse) {
                /*
                 * Log the exception if a logger is available.
                 */
                Logger logger = rcb.getStore().getLogger();
                if (logger != null) {
                    logger.warning(qse.toString());
                }
                throw new IllegalStateException(qse.toString());
            } catch (QueryException qe) {
                /* A QueryException thrown at the client; rethrow as IAE */
                throw qe.getIllegalArgument();
            } catch (IllegalArgumentException iae) {
                throw iae;
            } catch (FastExternalizableException fee) {
                throw fee;
            } catch (RuntimeException re) {
                /* why log this as WARNING? */
                String msg = "Query execution failed: " + re;
                Logger logger = rcb.getStore().getLogger();
                if (logger != null) {
                    logger.warning(msg);
                }
                throw re;
            }
        }

        private void updateHasNext(boolean localOnly) {
            if (localOnly) {
                hasNextLocal = rootIter.nextLocal(rcb);
                hasNext = hasNextLocal || !rootIter.isDone(rcb);
            } else {
                hasNext = rootIter.next(rcb);
            }
        }

        RecordDef getResultDef() {
            return resultDef;
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public RecordValue next() {

            if (!hasNext) {
                throw new NoSuchElementException();
            }

            return nextInternal(false /* localOnly */);
        }

        @Override
        public RecordValue nextLocal() {

            if (!hasNextLocal) {
                updateHasNext(true);
            }
            if (!hasNextLocal) {
                return null;
            }

            return nextInternal(true /* localOnly */);
        }

        private RecordValue nextInternal(boolean localOnly) {

            final RecordValue record;

            try {
                FieldValueImpl resVal = rcb.getRegVal(rootIter.getResultReg());

                if (statement.wrapResultInRecord()) {

                    if (resVal.isTuple()) {
                        assert(resultDef.equals(resVal.getDefinition()));
                        resVal = ((TupleValue)resVal).toRecord();
                    }

                    record = resultDef.createRecord();
                    record.put(0, resVal);

                } else if (resVal.isTuple()) {
                    assert(resultDef.equals(resVal.getDefinition()));
                    record = ((TupleValue)resVal).toRecord();

                } else {
                    assert(resVal.isRecord());
                    record = (RecordValue)resVal;
                }

                updateHasNext(localOnly);

            } catch (QueryStateException qse) {
                /*
                 * Log the exception if a logger is available.
                 */
                Logger logger = rcb.getStore().getLogger();
                if (logger != null) {
                    logger.warning(qse.toString());
                }
                throw new IllegalStateException(qse.toString());
            } catch (QueryException qe) {
                /* A QueryException thrown at the client; rethrow as IAE */
                throw qe.getIllegalArgument();
            }

            return record;
        }

        @Override
        public Throwable getCloseException() {
            return rootIter.getCloseException(rcb);
        }

        @Override
        public void close() {
            if (!isClosed()) {
                rootIter.close(rcb);
                hasNext = false;
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<DetailedMetrics> getPartitionMetrics() {
            if (rcb.getTableIterator() != null) {
                return rcb.getTableIterator().getShardMetrics();
            }
            return Collections.emptyList();
        }

        @Override
        public List<DetailedMetrics> getShardMetrics() {
            if (rcb.getTableIterator() != null) {
                return rcb.getTableIterator().getShardMetrics();
            }
            return Collections.emptyList();
        }

        @Override
        public boolean isClosed() {
            return rootIter.isDone(rcb);
        }

        /**
         * Returns the KB read during the execution of operation.
         */
        public int getReadKB() {
            return rcb.getReadKB();
        }

        /**
         * Returns the KB written during the execution of operation.
         */
        public int getWriteKB() {
            return rcb.getWriteKB();
        }

        /**
         * Returns the continuation key for the next execution.
         */
        public byte[] getContinuationKey() {
            return rcb.getContinuationKey();
        }
    }

    private class AsyncExecutionHandleImpl
            extends AsyncIterationHandleImpl<RecordValue>
            implements AsyncExecutionHandle {

        AsyncExecutionHandleImpl(Logger logger) {
            super(logger);
        }

        @Override
        public Kind getKind() {
            return QueryStatementResultImpl.this.getKind();
        }

        @Override
        public Statement getStatement() {
            return QueryStatementResultImpl.this.statement;
        }

        @Override
        public int getPlanId() {
            return QueryStatementResultImpl.this.getPlanId();
        }

        @Override
        public String getInfo() {
            return QueryStatementResultImpl.this.getInfo();
        }

        @Override
        public String getInfoAsJson() {
            return QueryStatementResultImpl.this.getInfoAsJson();
        }
    }
}
