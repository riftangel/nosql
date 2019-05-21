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

package oracle.kv.impl.api.table;

import static java.util.Collections.singletonList;
import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.AsyncIterationHandle;
import oracle.kv.BulkWriteOptions;
import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.Durability;
import oracle.kv.DurabilityException;
import oracle.kv.EntryStream;
import oracle.kv.FaultException;
import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.KeyValueVersion;
import oracle.kv.Operation;
import oracle.kv.OperationExecutionException;
import oracle.kv.OperationResult;
import oracle.kv.ResultHandler;
import oracle.kv.ReturnValueVersion;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.Version;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.Request;
import oracle.kv.impl.api.bulk.BulkPut;
import oracle.kv.impl.api.ops.Execute.OperationFactoryImpl;
import oracle.kv.impl.api.ops.Execute.OperationImpl;
import oracle.kv.impl.api.ops.InternalOperation;
import oracle.kv.impl.api.ops.MultiDeleteTable;
import oracle.kv.impl.api.ops.MultiGetTable;
import oracle.kv.impl.api.ops.MultiGetTableKeys;
import oracle.kv.impl.api.ops.Put;
import oracle.kv.impl.api.ops.Result;
import oracle.kv.impl.api.ops.ResultKey;
import oracle.kv.impl.api.ops.ResultKeyValueVersion;
import oracle.kv.impl.api.table.ValueSerializer.RowSerializer;
import oracle.kv.impl.async.AsyncIterationHandleImpl;
import oracle.kv.impl.async.AsyncTableIterator;
import oracle.kv.impl.client.admin.DdlFuture;
import oracle.kv.impl.query.runtime.QueryKeyRange;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.util.contextlogger.LogContext;
import oracle.kv.table.FieldRange;
import oracle.kv.table.IndexKey;
import oracle.kv.table.KeyPair;
import oracle.kv.table.MultiGetResult;
import oracle.kv.table.MultiRowOptions;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.ReadOptions;
import oracle.kv.table.ReturnRow;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableIteratorOptions;
import oracle.kv.table.TableOpExecutionException;
import oracle.kv.table.TableOperation;
import oracle.kv.table.TableOperationFactory;
import oracle.kv.table.TableOperationResult;
import oracle.kv.table.TimeToLive;
import oracle.kv.table.WriteOptions;

/**
 * Implementation of the TableAPI interface.  It also manages materialization
 * of tables from metadata and caches retrieved tables.
 *
 * TableAPIImpl maintains a cache of TableImpl tables that have been explicitly
 * fetched by TableImpl because of schema evolution.  If TableImpl encounters
 * a table version higher than its own then it will fetch that version so it
 * can deserialize records written from a later version.  It is assumed that
 * this cache will be small and is not used for user calls to getTable().
 */
public class TableAPIImpl implements TableAPI {
    final static String TABLE_PREFIX = "Tables";
    final static String SCHEMA_PREFIX = "Schemas";
    final static String SEPARATOR = ".";
    private final KVStoreImpl store;
    private final OpFactory opFactory;

    /*
     * Cache of TableImpl that have been fetched because the user's
     * table version is older than the latest table version.
     */
    final private ConcurrentHashMap<String, TableImpl> fetchedTables;

    /*
     * The cached TableMetadata seqNum.
     */
    private int metadataSeqNum;

    /*
     * The callback handler that will be invoked if it is detected that table
     * metadata has been changed.
     */
    private TableMetadataCallback metadataCallback;

    /*
     * An optional TableMetadataHelper instance that allows a user to
     * implement a Table cache. This is used by the cloud proxy, which
     * has a Table cache, to avoid going to servers to fetch a Table
     * in order to prepare queries. It is not used in the normal
     * getTable() path, which is unchanged.
     */
    private TableMetadataHelper metadataHelper;

    /*
     * This must be public for KVStoreImpl to use it.
     */
    public TableAPIImpl(KVStoreImpl store) {
        this.store = store;
        opFactory = new OpFactory(store.getOperationFactory());
        fetchedTables = new ConcurrentHashMap<String, TableImpl>();
        metadataSeqNum = 0;
    }

    /*
     * Table metadata methods
     */
    @Override
    public Table getTable(String tableName)
        throws FaultException {
        return getTable(null, tableName);
    }

    @Override
    public Table getTable(String namespace, String tableName)
        throws FaultException {
        return store.getDispatcher().getTable(store, namespace, tableName);
    }

    @Override
    public Table getTableById(long tableId)
        throws FaultException {
        return store.getDispatcher().getTableById(store, tableId);
    }

    /**
     * Sets the TableMetadataCallback handler.
     *
     * @param handler the handler
     */
    public void setTableMetadataCallback(TableMetadataCallback handler) {
        metadataCallback = handler;
    }

    /**
     * Returns the MetadataCallback handler or null if not registered.
     */
    public TableMetadataCallback getTableMetadataCallback() {
        return metadataCallback;
    }

    /**
     * Sets the TableMetadataHelper helper to one provided by an
     * application which, for example, may cache Table handles.
     *
     * @param helper the helper
     */
    public void setCachedMetadataHelper(TableMetadataHelper helper) {
        metadataHelper = helper;
    }

    /**
     * Returns the cached TableMetadataHelper helper or null if
     * not set.
     *
     * @return the helper, or null if it has not been set by the
     * application
     */
    public TableMetadataHelper getCachedMetadataHelper() {
        return metadataHelper;
    }

    /**
     * Notifies the TableMetadataCallback handler if table metadata has been
     * changed. If no TableMetadataCallback handler is registered, this call
     * do nothing.
     *
     * Compares the specified {@code seqNum} with the local metadata seqNum,
     * if the specified {@code seqNum} is higher than local seqNum, then invoke
     * {@link TableMetadataCallback#metadataChanged}.
     */
    public void metadataNotification(int remoteSeqNum) {
        if (metadataCallback != null && remoteSeqNum > metadataSeqNum) {
            synchronized(this) {
                if (remoteSeqNum > metadataSeqNum) {
                    if (metadataCallback != null) {
                        metadataCallback.metadataChanged(metadataSeqNum,
                                                         remoteSeqNum);
                    }
                    metadataSeqNum = remoteSeqNum;
                }
            }
        }
    }

    /*
     * Note: the 2 getTables() interfaces are generally discouraged as they
     * pull the entire TableMetadata object from a server into a client.
     */
    @Override
    public Map<String, Table> getTables()
        throws FaultException {

        return getTables(null);
    }

    @Override
    public Map<String, Table> getTables(String namespace)
        throws FaultException {

        TableMetadata md = getTableMetadata();
        return md == null ? Collections.<String, Table>emptyMap() :
            md.getTables(namespace);
    }

    /**
     * Gets the TableMetadata object from a RepNode.
     * It is also used by the public getTables() interface.
     *
     * This method should never be used by clients directly. Fetching a
     * entire TableMetadata instance from a server node is highly
     * discouraged. Most normal applications will only ever need to get a
     * table at a time.
     *
     * This should not be public but unfortunately it is public so that the
     * Thrift-based proxy can use it to cache table information. That should
     * change but old proxies still need to work, so leave it for now.
     */
    public TableMetadata getTableMetadata()
        throws FaultException {
        return store.getDispatcher().getTableMetadata();
    }

    public List<TableImpl> getSystemTables() {
        List<TableImpl> sysTables = new ArrayList<TableImpl>();
        for (Table table : getTables(null).values()) {
            if (((TableImpl)table).isSystemTable()) {
                sysTables.add((TableImpl)table);
            }
        }
        return sysTables;
    }

    /*
     * Runtime interfaces
     */

    @Override
    public Row get(PrimaryKey rowKeyArg,
                   ReadOptions readOptions)
        throws FaultException {

        PrimaryKeyImpl rowKey = (PrimaryKeyImpl)rowKeyArg;
        Result result = getInternal(rowKey, readOptions, null);
        return processGetResult(result, rowKey);
    }

    Row processGetResult(Result result, PrimaryKeyImpl rowKey) {
        ValueReader<RowImpl> reader =
            rowKey.getTableImpl().initRowReader(null);
        createRowFromGetResult(result, rowKey, reader);
        return reader.getValue();
    }

    /* public for use by cloud driver */
    public void createRowFromGetResult(Result result,
                                       RowSerializer rowKey,
                                       ValueReader<?> reader) {

        final ValueVersion vv = KVStoreImpl.processGetResult(result);
        if (vv == null) {
            reader.reset();
            return;
        }
        ((TableImpl)rowKey.getTable()).readKeyFields(reader, rowKey);
        getRowFromValueVersion(vv, rowKey, result.getPreviousExpirationTime(),
            false, reader);
    }

    /**
     * Internal use only
     *
     * Public for use by cloud proxy
     */
    public Result getInternal(RowSerializer rowKey,
                              ReadOptions readOptions,
                              LogContext lc)
        throws FaultException {

        return store.executeRequest(makeGetRequest(rowKey, readOptions, lc));
    }

    private Request makeGetRequest(RowSerializer rowKey,
                                   ReadOptions readOptions,
                                   LogContext lc) {
        TableImpl table = (TableImpl) rowKey.getTable();
        Key key = table.createKeyInternal(rowKey, false);
        return store.makeGetRequest(key,
                                    table.getId(),
                                    getConsistency(readOptions),
                                    getTimeout(readOptions),
                                    getTimeoutUnit(readOptions),
                                    lc);
    }

    @Override
    public void getAsync(PrimaryKey key,
                         ReadOptions readOptions,
                         ResultHandler<Row> handler) {
        checkNull("key", key);
        final PrimaryKeyImpl rowKey = (PrimaryKeyImpl) key;
        store.executeRequest(
            makeGetRequest(rowKey, readOptions, null),
            new OperationResultHandler<Row>(handler) {
                @Override
                Row getResultValue(Result result) {
                    return processGetResult(result, rowKey);
                }
            });
    }

    /**
     * A result handler that converts Result objects to the value to be
     * returned to the user, and that logs any failures that occur when
     * delivering results to the user-supplied result handler.
     */
    private abstract class OperationResultHandler<V>
            implements ResultHandler<Result> {
        private final ResultHandler<V> handler;
        OperationResultHandler(ResultHandler<V> handler) {
            this.handler = checkNull("handler", handler);
        }
        /**
         * Returns the user value associated with the result.  If this method
         * throws an exception, it will be delivered to the user's result
         * handler.
         */
        abstract V getResultValue(Result result) throws Exception;
        @Override
        public void onResult(Result result, Throwable exception) {
            if (exception != null) {
                try {
                    handler.onResult(null, exception);
                } catch (Throwable t) {
                    final Logger logger = store.getLogger();
                    if (logger.isLoggable(Level.FINEST)) {
                        logger.finest(
                            "Problem delivering exception to" +
                            " result handler: " + handler +
                            " exception being delivered: " + exception +
                            " exception from handler: " + t);
                    }
                }
                return;
            }
            final V value;
            try {
                value = getResultValue(result);
            } catch (Throwable t) {
                try {
                    handler.onResult(null, t);
                } catch (Throwable t2) {
                    final Logger logger = store.getLogger();
                    if (logger.isLoggable(Level.FINEST)) {
                        logger.finest(
                            "Problem delivering exception to" +
                            " result handler: " + handler +
                            " exception being delivered: " + t +
                            " exception from handler: " + t2);
                    }
                }
                return;
            }
            try {
                handler.onResult(value, null);
            } catch (Throwable t) {
                final Logger logger = store.getLogger();
                if (logger.isLoggable(Level.FINEST)) {
                    logger.finest(
                        "Problem delivering result to result handler: " +
                        handler +
                        " result: " + value +
                        " exception from handler: " + t);
                }
            }
        }
    }

    @Override
    public Version put(Row rowArg,
                       ReturnRow prevRowArg,
                       WriteOptions writeOptions)
        throws FaultException {

        RowImpl row = (RowImpl) rowArg;
        Result result = putInternal(row, prevRowArg, writeOptions, null);
        return processPutResult(result, row, prevRowArg);
    }

    Version processPutResult(Result result,
                             RowImpl row,
                             ReturnRow prevRowArg) {
        if (result.getSuccess()) {
            row.setExpirationTime(result.getNewExpirationTime());
        }

        initReturnRow(prevRowArg, row, result, null);
        return KVStoreImpl.getPutResult(result);
    }

    /**
     * Internal use only
     *
     * Public for use by cloud proxy
     */
    public void initReturnRowFromResult(ReturnRow rr,
                                        RowSerializer row,
                                        Result result,
                                        ValueReader<?> reader) {
        initReturnRow(rr, row, result, reader);
    }

    /**
     * Internal use only
     *
     * Public for use by cloud proxy
     */
    public Result putInternal(RowSerializer row,
                              ReturnRow prevRowArg,
                              WriteOptions writeOptions,
                              LogContext lc)
        throws FaultException {

        ReturnValueVersion rvv = makeRVV(prevRowArg);
        return store.executeRequestWithPrev(
            makePutRequest(row, rvv, writeOptions, lc), rvv);
    }

    private Request makePutRequest(RowSerializer row,
                                   ReturnValueVersion rvv,
                                   WriteOptions writeOptions,
                                   LogContext lc) {
        TableImpl table = (TableImpl)row.getTable();
        Key key = table.createKeyInternal(row, false);
        Value value = table.createValueInternal(row);
        return store.makePutRequest(key, value, rvv,
                                    table.getId(),
                                    getDurability(writeOptions),
                                    getTimeout(writeOptions),
                                    getTimeoutUnit(writeOptions),
                                    getTTL(row.getTTL(), table),
                                    getUpdateTTL(writeOptions),
                                    lc);
    }

    @Override
    public void putAsync(Row row,
                         final ReturnRow prevRowArg,
                         WriteOptions writeOptions,
                         final ResultHandler<Version> handler) {
        checkNull("row", row);
        final RowImpl rowImpl = (RowImpl) row;
        final ReturnValueVersion rvv = makeRVV(prevRowArg);
        store.executeRequest(
            makePutRequest(rowImpl, rvv, writeOptions, null),
            new OperationResultHandler<Version>(handler) {
                @Override
                Version getResultValue(Result result) {
                    return processPutResultWithPrev(result, rowImpl,
                                                    prevRowArg, rvv);
                }
            });
    }

    Version processPutResultWithPrev(Result result,
                                     RowImpl row,
                                     ReturnRow prevRowArg,
                                     ReturnValueVersion prevValue) {
        KVStoreImpl.resultSetPreviousValue(result, prevValue);
        return processPutResult(result, row, prevRowArg);
    }

    @Override
    public Version putIfAbsent(Row rowArg,
                               ReturnRow prevRowArg,
                               WriteOptions writeOptions)
        throws FaultException {

        RowImpl row = (RowImpl) rowArg;
        Result result = putIfAbsentInternal(row,
                                            prevRowArg,
                                            writeOptions,
                                            null);
        return processPutResult(result, row, prevRowArg);
    }

    /**
     * Internal use only
     *
     * Public for use by cloud proxy
     */
    public Result putIfAbsentInternal(RowSerializer row,
                                      ReturnRow prevRowArg,
                                      WriteOptions writeOptions,
                                      LogContext lc)
        throws FaultException {

        ReturnValueVersion rvv = makeRVV(prevRowArg);
        return store.executeRequestWithPrev(
            makePutIfAbsentRequest(row, rvv, writeOptions, lc), rvv);
    }

    private Request makePutIfAbsentRequest(RowSerializer row,
                                           ReturnValueVersion rvv,
                                           WriteOptions writeOptions,
                                           LogContext lc) {
        TableImpl table = (TableImpl)row.getTable();
        final Key key = table.createKeyInternal(row, false);
        final Value value = table.createValueInternal(row);

        return store.makePutIfAbsentRequest(key, value, rvv,
                                            table.getId(),
                                            getDurability(writeOptions),
                                            getTimeout(writeOptions),
                                            getTimeoutUnit(writeOptions),
                                            getTTL(row.getTTL(), table),
                                            getUpdateTTL(writeOptions),
                                            lc);
    }

    @Override
    public void putIfAbsentAsync(Row row,
                                 final ReturnRow prevRowArg,
                                 WriteOptions writeOptions,
                                 ResultHandler<Version> handler) {
        checkNull("row", row);
        final RowImpl rowImpl = (RowImpl) row;
        final ReturnValueVersion rvv = makeRVV(prevRowArg);
        store.executeRequest(
            makePutIfAbsentRequest(rowImpl, rvv, writeOptions, null),
            new OperationResultHandler<Version>(handler) {
                @Override
                Version getResultValue(Result result) {
                    return processPutResultWithPrev(result, rowImpl,
                                                    prevRowArg, rvv);
                }
            });
    }

    @Override
    public Version putIfPresent(Row rowArg,
                                ReturnRow prevRowArg,
                                WriteOptions writeOptions)
        throws FaultException {

        RowImpl row = (RowImpl) rowArg;
        Result result =
            putIfPresentInternal(row, prevRowArg, writeOptions, null);
        return processPutResult(result, row, prevRowArg);
    }

    /**
     * Internal use only
     *
     * Public for use by cloud proxy
     */
    public Result putIfPresentInternal(RowSerializer row,
                                       ReturnRow prevRowArg,
                                       WriteOptions writeOptions,
                                       LogContext lc)
        throws FaultException {

        ReturnValueVersion rvv = makeRVV(prevRowArg);
        return store.executeRequestWithPrev(
            makePutIfPresentRequest(row, rvv, writeOptions, lc), rvv);
    }

    private Request makePutIfPresentRequest(RowSerializer row,
                                            ReturnValueVersion rvv,
                                            WriteOptions writeOptions,
                                            LogContext lc) {
        TableImpl table = (TableImpl)row.getTable();
        final Key key = table.createKeyInternal(row, false);
        final Value value = table.createValueInternal(row);

        return store.makePutIfPresentRequest(key, value, rvv,
                                             table.getId(),
                                             getDurability(writeOptions),
                                             getTimeout(writeOptions),
                                             getTimeoutUnit(writeOptions),
                                             getTTL(row.getTTL(), table),
                                             getUpdateTTL(writeOptions),
                                             lc);
    }

    @Override
    public void putIfPresentAsync(Row row,
                                  final ReturnRow prevRowArg,
                                  WriteOptions writeOptions,
                                  ResultHandler<Version> handler) {
        checkNull("row", row);
        final RowImpl rowImpl = (RowImpl) row;
        final ReturnValueVersion rvv = makeRVV(prevRowArg);
        store.executeRequest(
            makePutIfPresentRequest(rowImpl, rvv, writeOptions, null),
            new OperationResultHandler<Version>(handler) {
                @Override
                Version getResultValue(Result result) {
                    return processPutResultWithPrev(result, rowImpl,
                                                    prevRowArg, rvv);
                }
            });
    }

    @Override
    public Version putIfVersion(Row rowArg,
                                Version matchVersion,
                                ReturnRow prevRowArg,
                                WriteOptions writeOptions)
        throws FaultException {

        RowImpl row = (RowImpl) rowArg;
        Result result = putIfVersionInternal(row,
                                             matchVersion,
                                             prevRowArg,
                                             writeOptions,
                                             null);
        return processPutResult(result, row, prevRowArg);
    }

    /**
     * Internal use only
     *
     * Public for use by cloud proxy
     */
    public Result putIfVersionInternal(RowSerializer row,
                                       Version matchVersion,
                                       ReturnRow prevRowArg,
                                       WriteOptions writeOptions,
                                       LogContext lc)
        throws FaultException {

        ReturnValueVersion rvv = makeRVV(prevRowArg);
        return store.executeRequestWithPrev(
            makePutIfVersionRequest(row, matchVersion, rvv, writeOptions, lc),
            rvv);
    }

    private Request makePutIfVersionRequest(RowSerializer row,
                                            Version matchVersion,
                                            ReturnValueVersion rvv,
                                            WriteOptions writeOptions,
                                            LogContext lc) {

        TableImpl table = (TableImpl) row.getTable();
        final Key key = table.createKeyInternal(row, false);
        final Value value = table.createValueInternal(row);
        return store.makePutIfVersionRequest(key, value, matchVersion, rvv,
                                             table.getId(),
                                             getDurability(writeOptions),
                                             getTimeout(writeOptions),
                                             getTimeoutUnit(writeOptions),
                                             getTTL(row.getTTL(), table),
                                             getUpdateTTL(writeOptions),
                                             lc);
    }

    @Override
    public void putIfVersionAsync(Row row,
                                  Version matchVersion,
                                  final ReturnRow prevRowArg,
                                  WriteOptions writeOptions,
                                  ResultHandler<Version> handler) {
        checkNull("row", row);
        final RowImpl rowImpl = (RowImpl) row;
        final ReturnValueVersion rvv = makeRVV(prevRowArg);
        store.executeRequest(
            makePutIfVersionRequest(rowImpl, matchVersion, rvv,
                                    writeOptions, null),
            new OperationResultHandler<Version>(handler) {
                @Override
                Version getResultValue(Result result) {
                    return processPutResultWithPrev(result, rowImpl,
                                                    prevRowArg, rvv);
                }
            });
    }

    @Override
    public void put(List<EntryStream<Row>> rowStreams,
                    BulkWriteOptions writeOptions) {

        if (rowStreams == null || rowStreams.isEmpty()) {
            throw new IllegalArgumentException("The stream list cannot be " +
                "null or empty.");
        }

        if (rowStreams.contains(null)) {
            throw new IllegalArgumentException("Elements of stream list " +
                "must not be null.");
        }

        /*
         * Track all tables in the operation for use later. A map is used
         * to keep the comparison simple, based on full table name.
         * TableImpl.equals() does a lot of work (perhaps overkill).
         */
        final Map<String, TableImpl> tablesUsed =
            new HashMap<String, TableImpl>();

        final BulkWriteOptions options =
            (writeOptions != null) ?
             writeOptions : new BulkWriteOptions(getDurability(writeOptions),
                                                 getTimeout(writeOptions),
                                                 getTimeoutUnit(writeOptions));

        final BulkPut<Row> bulkPut =
            new BulkPut<Row>(store, options, rowStreams, store.getLogger()) {

                @Override
                public BulkPut<Row>.StreamReader<Row>
                    createReader(int streamId, EntryStream<Row> stream) {
                    return new StreamReader<Row>(streamId, stream) {

                        @Override
                        protected Key getKey(Row row) {
                            return ((RowImpl)row).getPrimaryKey(false);
                        }

                        @Override
                        protected Value getValue(Row row) {
                            return ((RowImpl)row).createValue();
                        }

                        @Override
                        protected long getTableId(Row row) {
                            /*
                             * Return the table id but also put the table
                             * into the map of tables used in the operation
                             */
                            TableImpl table = (TableImpl)row.getTable();
                            tablesUsed.put(table.getFullName(), table);
                            return table.getId();
                        }

                        @Override
                        protected TimeToLive getTTL(Row row) {
                            return TableAPIImpl.getTTL((RowImpl)row,
                                                       row.getTable());
                        }
                    };
                }

                @Override
                protected Row convertToEntry(Key key, Value value) {
                    final byte[] keyBytes =
                        store.getKeySerializer().toByteArray(key);
                    final TableImpl table = (TableImpl)findTableByKey(keyBytes);
                    if (table == null) {
                        return null;
                    }
                    final RowImpl row =
                        table.createRowFromKeyBytes(keyBytes);
                    assert(row != null);
                    final ValueVersion vv = new ValueVersion(value, null);
                    return row.rowFromValueVersion(vv, false) ? row : null;
                }

                private Table findTableByKey(final byte[] keyBytes) {
                    for (TableImpl table : tablesUsed.values()) {
                        final TableImpl target = table.findTargetTable(keyBytes);
                        if (target != null) {
                            return target;
                        }
                    }
                    return null;
                }
        };

        try {
            bulkPut.execute();
        } catch (InterruptedException e) {
            throw new IllegalStateException("Unexpected interrupt during " +
                                            "putBulk()", e);
        }
    }

    /**
     * Deprecated in favor of KVStore.execute. Delegate over to that newer
     * method.
     */
    @Deprecated
    @Override
    public oracle.kv.table.ExecutionFuture execute(String statement)
            throws IllegalArgumentException, FaultException {
        return new DeprecatedResults.ExecutionFutureWrapper(store.execute(statement));
    }

    @Deprecated
    @Override
    public oracle.kv.table.StatementResult executeSync(String statement)
        throws FaultException {
        return new DeprecatedResults.StatementResultWrapper
                (store.executeSync(statement));
    }

    @Deprecated
    @Override
    public oracle.kv.table.ExecutionFuture getFuture(int planId) {
        if (planId < 1) {
            throw new IllegalArgumentException("PlanId " + planId +
                                               " isn't valid, must be > 1");
        }
        byte[] futureBytes = DdlFuture.toByteArray(planId);
        return new DeprecatedResults.ExecutionFutureWrapper
                (store.getFuture(futureBytes));
    }

    /*
     * Multi/iterator ops
     */
    @Override
    public List<Row> multiGet(PrimaryKey rowKeyArg,
                              MultiRowOptions getOptions,
                              ReadOptions readOptions)
        throws FaultException {

        return processMultiResults(
            rowKeyArg, getOptions,
            store.executeRequest(
                makeMultiGetTableRequest(rowKeyArg, getOptions, readOptions)));
    }

    private Request makeMultiGetTableRequest(PrimaryKey rowKey,
                                             MultiRowOptions getOptions,
                                             ReadOptions readOptions) {
        Table table = rowKey.getTable();
        TableKey key = TableKey.createKey(table, rowKey, true);
        if (!key.getMajorKeyComplete()) {
            throw new IllegalArgumentException
                ("Cannot perform multiGet on a primary key without a " +
                 "complete major path");
        }

        if (getOptions != null) {
            validateMultiRowOptions(getOptions, table, false);
        }
        final byte[] parentKeyBytes =
            store.getKeySerializer().toByteArray(key.getKey());
        final PartitionId partitionId =
            store.getDispatcher().getPartitionId(parentKeyBytes);
        final MultiGetTable get =
            new MultiGetTable(parentKeyBytes,
                              makeTargetTables(table, getOptions),
                              makeKeyRange(key, getOptions));
        return store.makeReadRequest(get, partitionId,
                                     getConsistency(readOptions),
                                     getTimeout(readOptions),
                                     getTimeoutUnit(readOptions),
                                     null);
    }

    @Override
    public void multiGetAsync(final PrimaryKey key,
                              final MultiRowOptions getOptions,
                              ReadOptions readOptions,
                              ResultHandler<List<Row>> handler) {
        checkNull("key", key);
        store.executeRequest(
            makeMultiGetTableRequest(key, getOptions, readOptions),
            new OperationResultHandler<List<Row>>(handler) {
                @Override
                List<Row> getResultValue(Result result) {
                    return processMultiResults(key, getOptions, result);
                }
            });
    }

    @Override
    public List<PrimaryKey> multiGetKeys(PrimaryKey rowKeyArg,
                                         MultiRowOptions getOptions,
                                         ReadOptions readOptions)
        throws FaultException {

        final Result result = store.executeRequest(
            makeMultiGetTableKeysRequest(rowKeyArg, getOptions, readOptions));
        return processMultiResults(rowKeyArg, getOptions, result.getKeyList());
    }

    private Request makeMultiGetTableKeysRequest(PrimaryKey rowKey,
                                                 MultiRowOptions getOptions,
                                                 ReadOptions readOptions) {
        Table table = rowKey.getTable();
        TableKey key = TableKey.createKey(table, rowKey, true);
        if (!key.getMajorKeyComplete()) {
            throw new IllegalArgumentException
                ("Cannot perform multiGet on a primary key without a " +
                 "complete major path");
        }

        if (getOptions != null) {
            validateMultiRowOptions(getOptions, table, false);
        }
        final byte[] parentKeyBytes =
            store.getKeySerializer().toByteArray(key.getKey());
        final PartitionId partitionId =
            store.getDispatcher().getPartitionId(parentKeyBytes);
        final MultiGetTableKeys get =
            new MultiGetTableKeys(parentKeyBytes,
                                  makeTargetTables(table, getOptions),
                                  makeKeyRange(key, getOptions),
                                  1 /* emptyReadFactor */);
        return store.makeReadRequest(get, partitionId,
                                     getConsistency(readOptions),
                                     getTimeout(readOptions),
                                     getTimeoutUnit(readOptions),
                                     null);
    }

    @Override
    public void multiGetKeysAsync(final PrimaryKey key,
                                  final MultiRowOptions getOptions,
                                  ReadOptions readOptions,
                                  ResultHandler<List<PrimaryKey>> handler) {
        checkNull("key", key);
        store.executeRequest(
            makeMultiGetTableKeysRequest(key, getOptions, readOptions),
            new OperationResultHandler<List<PrimaryKey>>(handler) {
                @Override
                List<PrimaryKey> getResultValue(Result result) {
                    return processMultiResults(key, getOptions,
                                               result.getKeyList());
                }
            });
    }

    @Override
    public TableIterator<Row> tableIterator(PrimaryKey rowKeyArg,
                                            MultiRowOptions getOptions,
                                            TableIteratorOptions iterateOptions)
        throws FaultException {
        return tableIterator(rowKeyArg, getOptions, iterateOptions, null);
    }

    /**
     * @hidden
     */
    public TableIterator<Row> tableIterator(PrimaryKey rowKeyArg,
                                            MultiRowOptions getOptions,
                                            TableIteratorOptions iterateOptions,
                                            Set<Integer> partitions)
        throws FaultException {

        return tableIterator(rowKeyArg, getOptions, iterateOptions, partitions,
                             null);
    }

    private AsyncTableIterator<Row> tableIterator(
        PrimaryKey rowKey,
        MultiRowOptions getOptions,
        TableIteratorOptions iterateOptions,
        Set<Integer> partitions,
        AsyncIterationHandleImpl<Row> iterationHandle) throws FaultException {

        final Table table = rowKey.getTable();
        final TableKey key = TableKey.createKey(table, rowKey, true);

        if (getOptions != null) {

            validateMultiRowOptions(getOptions, table, false);
        }
        return TableScan.createTableIterator(this, key, getOptions,
                                             iterateOptions, partitions,
                                             iterationHandle);
    }

    @Override
    public AsyncIterationHandle<Row> tableIteratorAsync(
        PrimaryKey key,
        MultiRowOptions getOptions,
        TableIteratorOptions iterateOptions) throws FaultException {

        checkNull("key", key);

        final AsyncIterationHandleImpl<Row> iterationHandle =
            new AsyncIterationHandleImpl<Row>(store.getLogger());
        iterationHandle.setIterator(
            tableIterator(key, getOptions, iterateOptions, null,
                          iterationHandle));
        return iterationHandle;
    }

    /**
     * For HTTP Proxy use only.
     * @hidden
     *
     * Return the rows associated with a partial primary key in pagination
     * manner.
     *
     * The number of rows returned per batch is controlled by batchResultSize
     * and maxReadKB of {@code TableIteratorOptions}.
     *
     * The continuationKey references start position the scan from, it is
     * returned in the result of last execution of this operation.
     *
     * @since 18.1
     */
    public MultiGetResult<Row> multiGet(PrimaryKey rowKey,
                                        byte[] continuationKey,
                                        MultiRowOptions getOptions,
                                        TableIteratorOptions iterateOptions,
                                        LogContext lc)
        throws FaultException {

        checkNull("rowKey", rowKey);
        final TableKey key =
            getMultiGetKey(rowKey, getOptions, iterateOptions);
        return TableScan.multiGet(this, key, continuationKey, getOptions,
                                  iterateOptions, lc);
    }

    /**
     * For HTTP Proxy use only.
     *
     * @hidden
     * @since 18.1
     */
    public void multiGetAsync(PrimaryKey rowKey,
                              byte[] continuationKey,
                              MultiRowOptions getOptions,
                              TableIteratorOptions iterateOptions,
                              ResultHandler<MultiGetResult<Row>> handler) {
        checkNull("rowKey", rowKey);
        final TableKey key =
            getMultiGetKey(rowKey, getOptions, iterateOptions);
        TableScan.multiGetAsync(this, key, continuationKey, getOptions,
                                iterateOptions, handler, null);
    }

    /**
     * For HTTP Proxy use only.
     * @hidden
     *
     * Return the primary keys associated with a partial primary key in
     * pagination manner.
     *
     * The number of keys returned per batch is controlled by batchResultSize
     * and maxReadKB of {@code TableIteratorOptions}.
     *
     * The continuationKey references start position the scan from, it is
     * returned in the result of last execution of this operation.
     *
     * @since 18.1
     */
    public MultiGetResult<PrimaryKey>
        multiGetKeys(PrimaryKey rowKey,
                     byte[] continuationKey,
                     MultiRowOptions getOptions,
                     TableIteratorOptions iterateOptions,
                     LogContext lc)
        throws FaultException {

        final TableKey key =
            getMultiGetKey(rowKey, getOptions, iterateOptions);
        return TableScan.multiGetKeys(this, key, continuationKey,
                                      getOptions, iterateOptions, lc);
    }

    /**
     * For HTTP Proxy use only.
     *
     * @hidden
     */
    public void multiGetKeysAsync(
        PrimaryKey rowKey,
        byte[] continuationKey,
        MultiRowOptions getOptions,
        TableIteratorOptions iterateOptions,
        ResultHandler<MultiGetResult<PrimaryKey>> handler) {

        checkNull("rowKey", rowKey);
        final TableKey key =
            getMultiGetKey(rowKey, getOptions, iterateOptions);
        TableScan.multiGetKeysAsync(this, key, continuationKey, getOptions,
                                    iterateOptions, handler, null);
    }

    private TableKey getMultiGetKey(PrimaryKey rowKey,
                                    MultiRowOptions getOptions,
                                    TableIteratorOptions iterateOptions) {
        final Table table = rowKey.getTable();
        final TableKey key = TableKey.createKey(table, rowKey, true);

        boolean hasAncestorOrChild = false;
        if (getOptions != null) {
            validateMultiRowOptions(getOptions, table, false);
            hasAncestorOrChild =
                    (getOptions.getIncludedParentTables() != null ||
                     getOptions.getIncludedChildTables() != null);
        }

        if (iterateOptions != null) {
            if (iterateOptions.getDirection() != Direction.UNORDERED) {
                throw new IllegalArgumentException("Direction must be " +
                        "Direction.UNORDERED for this operation");
            }
            if (hasAncestorOrChild && iterateOptions.getMaxReadKB() != 0) {
                throw new IllegalArgumentException("Ancestor or child table " +
                        "returns are not supported if the size limitation " +
                        "'maxReadKB' of TableIteratorOptions is specified.");
            }
        }
        return key;
    }

    /**
     * For HTTP Proxy use only.
     * @hidden
     *
     * Return the rows associated with a partial index key in pagination manner.
     *
     * The number of rows returned per batch is controlled by batchResultSize
     * and maxReadKB of {@code TableIteratorOptions}.
     *
     * The continuationKey references start position the scan from, it is
     * returned in the result of last execution of this operation.
     *
     * @since 18.1
     */
    public MultiGetResult<Row> multiGet(IndexKey indexKeyArg,
                                        byte[] continuationKey,
                                        MultiRowOptions getOptions,
                                        TableIteratorOptions iterateOptions,
                                        LogContext lc)
        throws FaultException {

        final IndexKeyImpl indexKey = (IndexKeyImpl)indexKeyArg;
        checkIndexMultiGetKeyOptions(indexKey.getTable(), getOptions,
                                     iterateOptions);
        return IndexScan.multiGet(this, indexKey, continuationKey,
                                  getOptions, iterateOptions, lc);

    }

    /**
     * For HTTP Proxy use only.
     *
     * @hidden
     * @since 18.1
     */
    public void multiGetAsync(IndexKey indexKeyArg,
                              byte[] continuationKey,
                              MultiRowOptions getOptions,
                              TableIteratorOptions iterateOptions,
                              ResultHandler<MultiGetResult<Row>> handler)
        throws FaultException {

        final IndexKeyImpl indexKey =
            (IndexKeyImpl) checkNull("indexKeyArg", indexKeyArg);
        checkIndexMultiGetKeyOptions(indexKey.getTable(), getOptions,
                                     iterateOptions);
        IndexScan.multiGetAsync(this, indexKey, continuationKey,
                                getOptions, iterateOptions, handler);
    }

    /**
     * For HTTP Proxy use only.
     * @hidden
     *
     * Return the primary and index keys associated with a partial index key in
     * pagination manner.
     *
     * The number of primary and index keys returned per batch is controlled by
     * batchResultSize and maxReadKB of {@code TableIteratorOptions}.
     *
     * The continuationKey references start position the scan from, it is
     * returned in the result of last execution of this operation.
     *
     * @since 18.1
     */
    public MultiGetResult<KeyPair>
        multiGetKeys(IndexKey indexKeyArg,
                     byte[] continuationKey,
                     MultiRowOptions getOptions,
                     TableIteratorOptions iterateOptions,
                     LogContext lc)
        throws FaultException {

        final IndexKeyImpl indexKey = (IndexKeyImpl)indexKeyArg;
        checkIndexMultiGetKeyOptions(indexKey.getTable(), getOptions,
                                     iterateOptions);
        return IndexScan.multiGetKeys(this, indexKey, continuationKey,
                                      getOptions, iterateOptions, lc);
    }

    /**
     * For HTTP Proxy use only.
     *
     * @hidden
     * @since 18.1
     */
    public void multiGetKeysAsync(IndexKey indexKeyArg,
                                  byte[] continuationKey,
                                  MultiRowOptions getOptions,
                                  TableIteratorOptions iterateOptions,
                                  ResultHandler<MultiGetResult<KeyPair>> hand)
        throws FaultException {

        final IndexKeyImpl indexKey =
            (IndexKeyImpl) checkNull("indexKeyArg", indexKeyArg);
        checkIndexMultiGetKeyOptions(indexKey.getTable(), getOptions,
                                     iterateOptions);
        IndexScan.multiGetKeysAsync(this, indexKey, continuationKey,
                                    getOptions, iterateOptions, hand);
    }

    private void checkIndexMultiGetKeyOptions(
        Table table,
        MultiRowOptions getOptions,
        TableIteratorOptions iterateOptions) {

        boolean hasAncestor = false;
        if (getOptions != null) {
            validateMultiRowOptions(getOptions, table, true);
            hasAncestor = (getOptions.getIncludedParentTables() != null);
        }

        if (iterateOptions != null) {
            if (iterateOptions.getDirection() != Direction.UNORDERED) {
                throw new IllegalArgumentException("Direction must be " +
                        "Direction.UNORDERED for this operation");
            }
            if (hasAncestor && iterateOptions.getMaxReadKB() != 0) {
                throw new IllegalArgumentException("Ancestor returns are not " +
                        "supported if the size limitation 'maxReadKB' of " +
                        "TableIteratorOptions is specified");
            }
        }
    }

    @Override
    public TableIterator<PrimaryKey> tableKeysIterator(
        PrimaryKey rowKey,
        MultiRowOptions getOptions,
        TableIteratorOptions iterateOptions) throws FaultException {

        return tableKeysIterator(rowKey, getOptions, iterateOptions, null);
    }

    private AsyncTableIterator<PrimaryKey> tableKeysIterator(
        PrimaryKey rowKey,
        MultiRowOptions getOptions,
        TableIteratorOptions iterateOptions,
        AsyncIterationHandleImpl<PrimaryKey> iterationHandle)
        throws FaultException {

        final Table table = rowKey.getTable();
        final TableKey key = TableKey.createKey(table, rowKey, true);

        if (getOptions != null) {
            validateMultiRowOptions(getOptions, table, false);
        }
        return TableScan.createTableKeysIterator(
            this, key, getOptions, iterateOptions, iterationHandle);
    }

    @Override
    public AsyncIterationHandle<PrimaryKey>
        tableKeysIteratorAsync(PrimaryKey key,
                               MultiRowOptions getOptions,
                               TableIteratorOptions iterateOptions)
        throws FaultException {

        checkNull("key", key);
        final AsyncIterationHandleImpl<PrimaryKey> iterationHandle =
            new AsyncIterationHandleImpl<PrimaryKey>(store.getLogger());
        iterationHandle.setIterator(
            tableKeysIterator(key, getOptions, iterateOptions,
                              iterationHandle));
        return iterationHandle;
    }

    @Override
    public boolean delete(PrimaryKey rowKeyArg,
                          ReturnRow prevRowArg,
                          WriteOptions writeOptions)
        throws FaultException {

        RowSerializer rowKey = (PrimaryKeyImpl)rowKeyArg;
        Result result = deleteInternal(rowKey,
                                       prevRowArg,
                                       writeOptions, null);
        initReturnRow(prevRowArg, rowKey, result, null);
        return result.getSuccess();
    }

    /**
     * Internal use only
     *
     * Public for use by cloud proxy
     */
    public Result deleteInternal(RowSerializer rowKey,
                                 ReturnRow prevRowArg,
                                 WriteOptions writeOptions,
                                 LogContext lc)
        throws FaultException {

        checkNull("rowKey", rowKey);
        ReturnValueVersion rvv = makeRVV(prevRowArg);
        return store.executeRequestWithPrev(
            makeDeleteRequest(rowKey, rvv, writeOptions, lc), rvv);
    }

    private Request makeDeleteRequest(RowSerializer rowKey,
                                      ReturnValueVersion rvv,
                                      WriteOptions writeOptions,
                                      LogContext lc) {
        TableImpl table = (TableImpl)rowKey.getTable();
        Key key = table.createKeyInternal(rowKey, false);
        return store.makeDeleteRequest(key,
                                       rvv,
                                       getDurability(writeOptions),
                                       getTimeout(writeOptions),
                                       getTimeoutUnit(writeOptions),
                                       table.getId(), lc);
    }

    @Override
    public void deleteAsync(PrimaryKey key,
                            final ReturnRow prevRowArg,
                            WriteOptions writeOptions,
                            ResultHandler<Boolean> handler) {
        checkNull("key", key);
        final RowSerializer rowKey = (PrimaryKeyImpl) key;
        final ReturnValueVersion rvv = makeRVV(prevRowArg);
        store.executeRequest(
            makeDeleteRequest(rowKey, rvv, writeOptions, null),
            new OperationResultHandler<Boolean>(handler) {
                @Override
                Boolean getResultValue(Result result) {
                    KVStoreImpl.resultSetPreviousValue(result, rvv);
                    initReturnRow(prevRowArg, rowKey, result, null);
                    return result.getSuccess();
                }
            });
    }

    @Override
    public boolean deleteIfVersion(PrimaryKey rowKeyArg,
                                   Version matchVersion,
                                   ReturnRow prevRowArg,
                                   WriteOptions writeOptions)
        throws FaultException {

        RowSerializer rowKey = (PrimaryKeyImpl)rowKeyArg;
        Result result = deleteIfVersionInternal(rowKey,
                                                matchVersion,
                                                prevRowArg,
                                                writeOptions,
                                                null);
        initReturnRow(prevRowArg, rowKey, result, null);
        return result.getSuccess();
    }

    /**
     * Internal use only
     *
     * Public for use by cloud proxy
     */
    public Result deleteIfVersionInternal(RowSerializer rowKey,
                                          Version matchVersion,
                                          ReturnRow prevRowArg,
                                          WriteOptions writeOptions,
                                          LogContext lc)
        throws FaultException {

        ReturnValueVersion rvv = makeRVV(prevRowArg);
        return store.executeRequestWithPrev(
            makeDeleteIfVersionRequest(rowKey, matchVersion, rvv,
                                       writeOptions, lc),
            rvv);
    }

    private Request makeDeleteIfVersionRequest(RowSerializer rowKey,
                                               Version matchVersion,
                                               ReturnValueVersion rvv,
                                               WriteOptions writeOptions,
                                               LogContext lc) {
        TableImpl table = (TableImpl) rowKey.getTable();
        Key key = table.createKeyInternal(rowKey, false);
        return store.makeDeleteIfVersionRequest(
            key, matchVersion, rvv,
            getDurability(writeOptions),
            getTimeout(writeOptions),
            getTimeoutUnit(writeOptions),
            table.getId(),
            lc);
    }

    @Override
    public void deleteIfVersionAsync(PrimaryKey key,
                                     Version matchVersion,
                                     final ReturnRow prevRowArg,
                                     WriteOptions writeOptions,
                                     ResultHandler<Boolean> handler) {
        checkNull("key", key);
        final PrimaryKeyImpl rowKey = (PrimaryKeyImpl) key;
        final ReturnValueVersion rvv = makeRVV(prevRowArg);
        store.executeRequest(
            makeDeleteIfVersionRequest(rowKey, matchVersion, rvv,
                                       writeOptions, null),
            new OperationResultHandler<Boolean>(handler) {
                @Override
                Boolean getResultValue(Result result) {
                    KVStoreImpl.resultSetPreviousValue(result, rvv);
                    initReturnRow(prevRowArg, rowKey, result, null);
                    return result.getSuccess();
                }
            });
    }

    @Override
    public int multiDelete(PrimaryKey rowKeyArg,
                           MultiRowOptions getOptions,
                           WriteOptions writeOptions)
        throws FaultException {

        RowSerializer rowKey = (PrimaryKeyImpl)rowKeyArg;
        Result result = multiDeleteInternal(rowKey, null, getOptions,
                                            writeOptions, null);
        return result.getNDeletions();
    }

    /**
     * Public for HTTP Proxy use only.
     * @hidden
     *
     * Deletes multiple rows from a table in an atomic operation, the con
     */
    public Result multiDeleteInternal(RowSerializer rowKey,
                                      byte[] continuationKey,
                                      MultiRowOptions getOptions,
                                      WriteOptions writeOptions,
                                      LogContext lc)
        throws FaultException {

        return store.executeRequest(
            makeMultiDeleteTableRequest(rowKey, continuationKey, getOptions,
                                        writeOptions, lc));
    }

    private Request makeMultiDeleteTableRequest(RowSerializer rowKey,
                                                byte[] continuationKey,
                                                MultiRowOptions getOptions,
                                                WriteOptions writeOptions,
                                                LogContext lc) {
        Table table = rowKey.getTable();
        TableKey key = TableKey.createKeyInternal(table, rowKey, true);
        if (!key.getMajorKeyComplete()) {
            throw new IllegalArgumentException
                ("Cannot perform multiDelete on a primary key without a " +
                 "complete major path.  Key: " + rowKey);
        }

        if (getOptions != null) {
            validateMultiRowOptions(getOptions, table, false);
        }
        final KeyRange keyRange = makeKeyRange(key, getOptions);

        final byte[] parentKeyBytes =
            store.getKeySerializer().toByteArray(key.getKey());
        final PartitionId partitionId =
            store.getDispatcher().getPartitionId(parentKeyBytes);
        final MultiDeleteTable del =
            new MultiDeleteTable(parentKeyBytes,
                                 makeTargetTables(table, getOptions),
                                 keyRange,
                                 continuationKey,
                                 getMaxWriteKB(writeOptions));
        return store.makeWriteRequest(del, partitionId,
                                      getDurability(writeOptions),
                                      getTimeout(writeOptions),
                                      getTimeoutUnit(writeOptions),
                                      lc);
    }

    @Override
    public void multiDeleteAsync(PrimaryKey key,
                                 MultiRowOptions getOptions,
                                 WriteOptions writeOptions,
                                 ResultHandler<Integer> handler) {
        checkNull("key", key);
        RowSerializer rowKey = (PrimaryKeyImpl)key;
        store.executeRequest(
            makeMultiDeleteTableRequest(rowKey, null /* continuationKey */,
                                        getOptions, writeOptions, null),
            new OperationResultHandler<Integer>(handler) {
                @Override
                Integer getResultValue(Result result) {
                    return result.getNDeletions();
                }
            });
    }

    /*
     * Index iterator operations
     */
    @Override
    public TableIterator<Row>
        tableIterator(IndexKey indexKeyArg,
                      MultiRowOptions getOptions,
                      TableIteratorOptions iterateOptions)
        throws FaultException {
        return tableIterator(indexKeyArg, getOptions, iterateOptions, null);
    }

    @Override
    public AsyncIterationHandle<Row>
        tableIteratorAsync(IndexKey key,
                           MultiRowOptions getOptions,
                           TableIteratorOptions iterateOptions)
        throws FaultException {

        checkNull("key", key);
        final AsyncIterationHandleImpl<Row> iterationHandle =
            new AsyncIterationHandleImpl<Row>(store.getLogger());
        iterationHandle.setIterator(
            tableIterator(key, getOptions, iterateOptions, null,
                          iterationHandle));
        return iterationHandle;
    }

    public TableIterator<Row>
        tableIterator(IndexKey indexKeyArg,
                      MultiRowOptions getOptions,
                      TableIteratorOptions iterateOptions,
                      Set<RepGroupId> shardSet)
        throws FaultException {

        return tableIterator(indexKeyArg, getOptions, iterateOptions,
                             shardSet, null);
    }

    private AsyncTableIterator<Row>
        tableIterator(IndexKey indexKeyArg,
                      MultiRowOptions getOptions,
                      TableIteratorOptions iterateOptions,
                      Set<RepGroupId> shardSet,
                      AsyncIterationHandleImpl<Row> iterationHandle)
        throws FaultException {

        final IndexKeyImpl indexKey = (IndexKeyImpl) indexKeyArg;
        if (getOptions != null) {
            validateMultiRowOptions(getOptions, indexKey.getTable(), true);
        }
        return IndexScan.createTableIterator(this,
                                             indexKey,
                                             getOptions,
                                             iterateOptions,
                                             shardSet,
                                             iterationHandle);
    }

    @Override
    public TableIterator<KeyPair>
        tableKeysIterator(IndexKey indexKeyArg,
                          MultiRowOptions getOptions,
                          TableIteratorOptions iterateOptions)
        throws FaultException {

        return tableKeysIterator(indexKeyArg, getOptions, iterateOptions,
                                 null);
    }

    @Override
    public AsyncIterationHandle<KeyPair>
        tableKeysIteratorAsync(IndexKey key,
                               MultiRowOptions getOptions,
                               TableIteratorOptions iterateOptions)
        throws FaultException {

        checkNull("key", key);
        final AsyncIterationHandleImpl<KeyPair> iterationHandle =
            new AsyncIterationHandleImpl<KeyPair>(store.getLogger());
        iterationHandle.setIterator(
            tableKeysIterator(key, getOptions, iterateOptions,
                              iterationHandle));
        return iterationHandle;
    }

    private AsyncTableIterator<KeyPair>
        tableKeysIterator(IndexKey indexKeyArg,
                          MultiRowOptions getOptions,
                          TableIteratorOptions iterateOptions,
                          AsyncIterationHandleImpl<KeyPair> iterationHandle)
        throws FaultException {

        final IndexKeyImpl indexKey = (IndexKeyImpl) indexKeyArg;
        if (getOptions != null) {
            validateMultiRowOptions(getOptions, indexKey.getTable(), true);
        }
        return IndexScan.createTableKeysIterator(
            this, indexKey, getOptions, iterateOptions, iterationHandle);
    }

    @Override
    public TableOperationFactory getTableOperationFactory() {
        return opFactory;
    }

    @Override
    public TableIterator<Row>
        tableIterator(Iterator<PrimaryKey> primaryKeyIterator,
                      MultiRowOptions getOptions,
                      TableIteratorOptions iterateOptions) {
        if (primaryKeyIterator == null) {
            throw new IllegalArgumentException("Parent key iterator should " +
                "not be null");
        }

        return tableIterator(singletonList(primaryKeyIterator), getOptions,
                             iterateOptions);
    }

    @Override
    public AsyncIterationHandle<Row>
        tableIteratorAsync(Iterator<PrimaryKey> primaryKeyIterator,
                           MultiRowOptions getOptions,
                           TableIteratorOptions iterateOptions) {

        checkNull("primaryKeyIterator", primaryKeyIterator);
        return tableIteratorAsync(singletonList(primaryKeyIterator),
                                  getOptions, iterateOptions);
    }

    @Override
    public TableIterator<PrimaryKey>
        tableKeysIterator(Iterator<PrimaryKey> primaryKeyIterator,
                          MultiRowOptions getOptions,
                          TableIteratorOptions iterateOptions) {

        if (primaryKeyIterator == null) {
            throw new IllegalArgumentException("Parent key iterator should " +
                "not be null");
        }

        return tableKeysIterator(singletonList(primaryKeyIterator), getOptions,
                                 iterateOptions);
    }

    @Override
    public AsyncIterationHandle<PrimaryKey> tableKeysIteratorAsync(
        Iterator<PrimaryKey> primaryKeyIterator,
        MultiRowOptions getOptions,
        TableIteratorOptions iterateOptions) {

        checkNull("primaryKeyIterator", primaryKeyIterator);
        return tableKeysIteratorAsync(singletonList(primaryKeyIterator),
                                      getOptions, iterateOptions);
    }

    @Override
    public TableIterator<Row>
        tableIterator(List<Iterator<PrimaryKey>> primaryKeyIterators,
                      MultiRowOptions getOptions,
                      TableIteratorOptions iterateOptions)
        throws FaultException {

        return tableIterator(primaryKeyIterators, getOptions, iterateOptions,
                             null);
    }

    @Override
    public AsyncIterationHandle<Row>
        tableIteratorAsync(List<Iterator<PrimaryKey>> primaryKeyIterators,
                           MultiRowOptions getOptions,
                           TableIteratorOptions iterateOptions)
        throws FaultException {

        checkNull("primaryKeyIterators", primaryKeyIterators);

        final AsyncIterationHandleImpl<Row> iterationHandle =
            new AsyncIterationHandleImpl<Row>(store.getLogger());
        iterationHandle.setIterator(
            tableIterator(primaryKeyIterators, getOptions, iterateOptions,
                          iterationHandle));
        return iterationHandle;
    }

    private AsyncTableIterator<Row>
        tableIterator(List<Iterator<PrimaryKey>> primaryKeyIterators,
                      MultiRowOptions getOptions,
                      TableIteratorOptions iterateOptions,
                      AsyncIterationHandleImpl<Row> iterationHandle)
        throws FaultException {

        if (primaryKeyIterators == null || primaryKeyIterators.isEmpty()) {
            throw new IllegalArgumentException("The key iterator list cannot " +
                "be null or empty");
        }

        if (primaryKeyIterators.contains(null)) {
            throw new IllegalArgumentException("The element of key iterator " +
                "list cannot be null.");
        }

        if (iterateOptions != null &&
            iterateOptions.getDirection() != Direction.UNORDERED) {
            throw new IllegalArgumentException("Direction must be " +
                "Direction.UNORDERED for this operation");
        }

        return new TableMultiGetBatch(this, primaryKeyIterators,
                                      getOptions, iterateOptions,
                                      iterationHandle)
            .createIterator();
    }

    @Override
    public TableIterator<PrimaryKey>
        tableKeysIterator(List<Iterator<PrimaryKey>> primaryKeyIterators,
                          MultiRowOptions getOptions,
                          TableIteratorOptions iterateOptions)
        throws FaultException {

        return tableKeysIterator(primaryKeyIterators, getOptions,
                                 iterateOptions, null);
    }

    @Override
    public AsyncIterationHandle<PrimaryKey>
        tableKeysIteratorAsync(List<Iterator<PrimaryKey>> primaryKeyIterators,
                               MultiRowOptions getOptions,
                               TableIteratorOptions iterateOptions)
        throws FaultException {

        checkNull("primaryKeyIterators", primaryKeyIterators);
        final AsyncIterationHandleImpl<PrimaryKey> iterationHandle =
            new AsyncIterationHandleImpl<PrimaryKey>(store.getLogger());
        iterationHandle.setIterator(
            tableKeysIterator(primaryKeyIterators, getOptions, iterateOptions,
                              iterationHandle));
        return iterationHandle;
    }

    private AsyncTableIterator<PrimaryKey>
        tableKeysIterator(List<Iterator<PrimaryKey>> primaryKeyIterators,
                          MultiRowOptions getOptions,
                          TableIteratorOptions iterateOptions,
                          AsyncIterationHandleImpl<PrimaryKey> iterationHandle)
        throws FaultException {

        if (primaryKeyIterators == null || primaryKeyIterators.isEmpty()) {
            throw new IllegalArgumentException("The key iterator list cannot " +
                "be null or empty");
        }

        if (primaryKeyIterators.contains(null)) {
            throw new IllegalArgumentException("The element of key iterator " +
                "list cannot be null.");
        }

        if (iterateOptions != null &&
            iterateOptions.getDirection() != Direction.UNORDERED) {
            throw new IllegalArgumentException("Direction must be " +
                "Direction.UNORDERED for this operation");
        }

        return new TableMultiGetBatch(this, primaryKeyIterators, getOptions,
                                      iterateOptions, iterationHandle)
            .createKeysIterator();
    }

    /**
     * @hidden
     */
    public TableIterator<KeyValueVersion>
        tableKVIterator(PrimaryKey rowKeyArg,
                        MultiRowOptions getOptions,
                        TableIteratorOptions iterateOptions)
        throws FaultException {

        return tableKVIterator(rowKeyArg, getOptions, iterateOptions, null);
    }

    /**
     * @hidden
     */
    public TableIterator<KeyValueVersion>
        tableKVIterator(PrimaryKey rowKeyArg,
                        MultiRowOptions getOptions,
                        TableIteratorOptions iterateOptions,
                        Set<Integer> partitions)
        throws FaultException {

        final PrimaryKeyImpl rowKey = (PrimaryKeyImpl) rowKeyArg;
        final Table table = rowKey.getTable();
        final TableKey key = TableKey.createKey(table, rowKey, true);

        if (getOptions != null) {
            throw new IllegalArgumentException("MultiRowOption currently " +
                "not supported by tableKVIterator");
        }

        return TableScan.createTableKVIterator(
            this, key, getOptions, iterateOptions, partitions);
    }

    /**
     * Returns an instance of Put (including PutIf*) if the internal operation
     * is a put.
     *
     * @return null if the operation is not a variant of Put.
     */
    private Put unwrapPut(Operation op) {
        InternalOperation iop = ((OperationImpl)op).getInternalOp();
        return (iop instanceof Put ? (Put) iop : null);
    }

    /**
     * All of the TableOperations can be directly mapped to simple KV operations
     * so do that.
     */
    @Override
    public List<TableOperationResult> execute(List<TableOperation> operations,
                                              WriteOptions writeOptions)
        throws TableOpExecutionException,
               DurabilityException,
               FaultException {

        Result result = executeInternal(operations, writeOptions, null);
        return createResultsFromExecuteResult(result, operations);
    }

    /**
     * Internal use only
     *
     * Public for use by cloud proxy
     */
    public Result executeInternal(List<TableOperation> operations,
                                  WriteOptions writeOptions, LogContext lc)
        throws TableOpExecutionException,
               DurabilityException,
               FaultException {

        final Table table = ((OpWrapper)operations.get(0)).getTable();
        final List<Operation> kvOperations =
            makeExecuteOps(operations, writeOptions, table);
        final Request req = store.makeExecuteRequest(
            kvOperations,
            ((TableImpl)table).getId(),
            getDurability(writeOptions),
            getTimeout(writeOptions),
            getTimeoutUnit(writeOptions),
            lc);
        return processExecuteResult(
            store.executeRequest(req), operations, kvOperations);
    }

    private Result processExecuteResult(Result result,
                                        List<TableOperation> operations,
                                        List<Operation> kvOperations)
        throws TableOpExecutionException {

        try {
            return KVStoreImpl.processExecuteResult(result, kvOperations);
        } catch (OperationExecutionException e) {
            /* Convert this to a TableOpExecutionException */
            int failedOpIndex = e.getFailedOperationIndex();
            PrimaryKey pkey = operations.get(failedOpIndex).getPrimaryKey();
            OperationResult opResult = e.getFailedOperationResult();
            TableOperationResult failedResult =
                    new OpResultWrapper(this, opResult, pkey);

            throw new TableOpExecutionException(operations.get(failedOpIndex),
                                                failedOpIndex,
                                                failedResult);
        }
    }

    public List<TableOperationResult>
        createResultsFromExecuteResult(Result result,
                                       List<TableOperation> operations) {

        List<OperationResult> results = result.getExecuteResult();
        List<TableOperationResult> tableResults =
                new ArrayList<TableOperationResult>(results.size());
        int index = 0;
        for (OperationResult opRes : results) {
            PrimaryKey pkey = operations.get(index).getPrimaryKey();
            tableResults.add(new OpResultWrapper(this, opRes, pkey));
            ++index;
        }
        return tableResults;
    }

    private List<Operation> makeExecuteOps(List<TableOperation> operations,
                                           WriteOptions writeOptions,
                                           Table table) {
        if (operations == null || operations.isEmpty()) {
            throw new IllegalArgumentException
                ("operations must be non-null and non-empty");
        }

        ArrayList<Operation> opList =
                new ArrayList<Operation>(operations.size());
        for (TableOperation op : operations) {
            Operation operation = ((OpWrapper)op).getOperation();
            opList.add(operation);

            Put putOp = unwrapPut(operation) ;
            if (putOp != null) {
                boolean updateTTL =
                    getUpdateTTL(writeOptions) || op.getUpdateTTL();
                putOp.setTTLOptions(getTTL(((OpWrapper)op).getTTL(), table),
                                    updateTTL);
            }
        }
        return opList;
    }

    @Override
    public void executeAsync(
        final List<TableOperation> operations,
        WriteOptions writeOptions,
        ResultHandler<List<TableOperationResult>> handler) {

        checkNull("operations", operations);
        final Table table = operations.get(0).getPrimaryKey().getTable();
        final List<Operation> kvOperations =
            makeExecuteOps(operations, writeOptions, table);
        store.executeRequest(
            store.makeExecuteRequest(kvOperations,
                                     ((TableImpl)table).getId(),
                                     getDurability(writeOptions),
                                     getTimeout(writeOptions),
                                     getTimeoutUnit(writeOptions),
                                     null),
            new OperationResultHandler<List<TableOperationResult>>(handler) {
                @Override
                List<TableOperationResult> getResultValue(Result result)
                    throws TableOpExecutionException {
                    return createResultsFromExecuteResult(
                        processExecuteResult(
                            result, operations, kvOperations),
                        operations);
                }
            });
    }

    /**
     * Creates a Row from the Value with a retry in the case of a
     * TableVersionException.
     *
     * The object passed in is used in-place and returned if all goes well.
     * If there is a TableVersionException a new object is created and
     * returned.
     */
    RowImpl getRowFromValueVersion(ValueVersion vv,
                                   RowImpl row,
                                   long expirationTime,
                                   boolean keyOnly) {
        ValueReader<RowImpl> reader = row.initRowReader();
        getRowFromValueVersion(vv, row, expirationTime, keyOnly, reader);
        return reader.getValue();
    }

    void getRowFromValueVersion(ValueVersion vv,
                                RowSerializer row,
                                long expirationTime,
                                boolean keyOnly,
                                ValueReader<?> reader) {

        final TableImpl table = (TableImpl) row.getTable();
        int requiredVersion = 0;
        assert(reader != null);

        try {
            if (keyOnly) {
                if (row instanceof RowImpl) {
                    ((RowImpl) row).removeValueFields();
                }
            }
            reader.setExpirationTime(expirationTime);
            if (!table.readRowFromValueVersion(reader, vv)) {
                reader.reset();
            }
            return;
        } catch (TableVersionException tve) {
            requiredVersion = tve.getRequiredVersion();
            assert requiredVersion > table.getTableVersion();
            reader.reset();
        }

        /*
         * Fetch the required table, create a new row from the existing
         * row and try again.  The fetch will throw if the table and version
         * can't be found.
         */
        TableImpl newTable = fetchTable(table.getFullName(),
                                        requiredVersion);
        assert requiredVersion == newTable.getTableVersion();

        /*
         * Set the version of the table to the original version to ensure that
         * deserialization does the right thing with added and removed fields.
         */
        newTable = (TableImpl)newTable.getVersion(table.getTableVersion());
        RowImpl newRow = newTable.createRow();
        if (reader instanceof RowReaderImpl) {
            RowReaderImpl rr = (RowReaderImpl)reader;
            rr.setValue(newRow);
        }
        newTable.readKeyFields(reader, row);
        reader.setExpirationTime(expirationTime);
        if (!newTable.readRowFromValueVersion(reader, vv)) {
            reader.reset();
        }
    }

    TableImpl fetchTable(String tableName, int tableVersion) {
        TableImpl table = fetchedTables.get(tableName);
        if (table != null && table.numTableVersions() >= tableVersion) {
            return (TableImpl) table.getVersion(tableVersion);
        }

        /*
         * Either the table is not in the cache or it is not sufficiently
         * recent.  Go to the server.
         */
        table = (TableImpl) getTable(tableName);
        if (table != null && table.numTableVersions() >= tableVersion) {

            /*
             * Cache the table.  If an intervening operation cached the
             * table, make sure that the cache has the lastest version.
             */
            TableImpl t = fetchedTables.putIfAbsent(tableName, table);
            if (t != null && table.numTableVersions() > t.numTableVersions()) {
                fetchedTables.put(tableName, table);
            }
            return (TableImpl) table.getVersion(tableVersion);
        }
        throw new IllegalArgumentException
            ("Table or version does not exist.  It may have been removed: " +
             tableName + ", version " + tableVersion);
    }

    public KVStoreImpl getStore() {
        return store;
    }

    /**
     * The next classes implement mapping of TableOperation and
     * TableOperationFactory to the KVStore Operation and OperationFactory.
     */
    private static class OpWrapper implements TableOperation {
        private final Operation op;
        private final TableOperation.Type type;
        private final RowSerializer record;
        private boolean updateTTL;

        private OpWrapper(Operation op, TableOperation.Type type,
                          final RowSerializer record) {
            this.op = op;
            this.type = type;
            this.record = record;
        }

        @Override
        public Row getRow() {
            if (record instanceof Row) {
                return (Row)record;
            }
            /* Return null if row is not RowImpl instance */
            return null;
        }

        @Override
        public PrimaryKey getPrimaryKey() {
            if (record instanceof PrimaryKey) {
                return (PrimaryKey) record;
            }

            TableImpl table = (TableImpl)record.getTable();
            PrimaryKeyImpl key = table.createPrimaryKey();
            table.readKeyFields(key.initRowReader(), record);
            return key;
        }

        @Override
        public TableOperation.Type getType() {
            return type;
        }

        @Override
        public boolean getAbortIfUnsuccessful() {
            return op.getAbortIfUnsuccessful();
        }

        private Operation getOperation() {
            return op;
        }

        @Override
        public void setUpdateTTL(boolean flag) {
            updateTTL = flag;
        }

        @Override
        public boolean getUpdateTTL() {
            return updateTTL;
        }

        TimeToLive getTTL() {
            return record.getTTL();
        }

        Table getTable() {
            return record.getTable();
        }
    }

    /**
     * Public for use by cloud proxy
     */
    public static class OpResultWrapper implements TableOperationResult {
        private final TableAPIImpl impl;
        private final OperationResult opRes;
        private final PrimaryKey key;

        private OpResultWrapper(TableAPIImpl impl,
                                OperationResult opRes, PrimaryKey key) {
            this.impl = impl;
            this.opRes = opRes;
            this.key = key;
        }

        @Override
        public Version getNewVersion() {
            return opRes.getNewVersion();
        }

        @Override
        public Row getPreviousRow() {
            ValueReader<RowImpl> reader =
                ((TableImpl)key.getTable()).createRow().initRowReader();
            return getPreviousRow(reader) ? reader.getValue() : null;
        }

        @Override
        public Version getPreviousVersion() {
            return opRes.getPreviousVersion();
        }

        @Override
        public boolean getSuccess() {
            return opRes.getSuccess();
        }

        @Override
        public long getPreviousExpirationTime() {
            return opRes.getPreviousExpirationTime();
        }

        public boolean getPreviousRow(ValueReader<?> reader) {
            Value value = opRes.getPreviousValue();
            /*
             * Put Version in the Row if it's available.
             */
            Version version = opRes.getPreviousVersion();
            if (value != null && key != null) {
                PrimaryKeyImpl rowKey = (PrimaryKeyImpl)key;
                ((TableImpl)key.getTable()).readKeyFields(reader, rowKey);
                impl.getRowFromValueVersion
                    (new ValueVersion(value, version),
                     rowKey,
                     opRes.getPreviousExpirationTime(),
                     false,
                     reader);
                return true;
            }
            return false;
        }
    }

    /**
     * Public for use by cloud proxy
     */
    public static class OpFactory implements TableOperationFactory {
        private final OperationFactoryImpl factory;

        private OpFactory(final OperationFactoryImpl factory) {
            this.factory = factory;
        }

        @Override
        public TableOperation createPut(Row rowArg,
                                        ReturnRow.Choice prevReturn,
                                        boolean abortIfUnsuccessful) {

            return createPutInternal((RowImpl)rowArg, prevReturn,
                                     abortIfUnsuccessful);
        }

        /**
         * Public for use by cloud proxy
         */
        public TableOperation createPutInternal
            (RowSerializer row,
             ReturnRow.Choice prevReturn,
             boolean abortIfUnsuccessful) {

            ReturnValueVersion.Choice choice =
                ReturnRowImpl.mapChoice(prevReturn);
            TableImpl table = (TableImpl)row.getTable();
            Key key = table.createKeyInternal(row, false);
            Value value = table.createValueInternal(row);
            Operation op = factory.createPut(key, value, choice,
                                             abortIfUnsuccessful,
                                             table.getId());
            return new OpWrapper(op, TableOperation.Type.PUT, row);
        }

        @Override
        public TableOperation createPutIfAbsent(Row rowArg,
                                                ReturnRow.Choice prevReturn,
                                                boolean abortIfUnsuccessful) {
            return createPutIfAbsentInternal((RowImpl)rowArg, prevReturn,
                                             abortIfUnsuccessful);
        }

        /**
         * Public for use by cloud proxy
         */
        public TableOperation createPutIfAbsentInternal
            (RowSerializer row,
             ReturnRow.Choice prevReturn,
             boolean abortIfUnsuccessful) {

            ReturnValueVersion.Choice choice =
                ReturnRowImpl.mapChoice(prevReturn);
            TableImpl table = (TableImpl)row.getTable();
            Key key = table.createKeyInternal(row, false);
            Value value = table.createValueInternal(row);
            Operation op = factory.createPutIfAbsent(key, value, choice,
                                                     abortIfUnsuccessful,
                                                     table.getId());
            return new OpWrapper(op, TableOperation.Type.PUT_IF_ABSENT, row);
        }

        @Override
        public TableOperation createPutIfPresent(Row rowArg,
                                                 ReturnRow.Choice prevReturn,
                                                 boolean abortIfUnsuccessful) {
            return createPutIfPresentInternal((RowImpl) rowArg, prevReturn,
                                              abortIfUnsuccessful);
        }

        /**
         * Public for use by cloud proxy
         */
        public TableOperation createPutIfPresentInternal
            (RowSerializer row,
             ReturnRow.Choice prevReturn,
             boolean abortIfUnsuccessful) {

            ReturnValueVersion.Choice choice =
                ReturnRowImpl.mapChoice(prevReturn);
            TableImpl table = (TableImpl)row.getTable();
            Key key = table.createKeyInternal(row, false);
            Value value = table.createValueInternal(row);
            Operation op = factory.createPutIfPresent(key, value, choice,
                                                     abortIfUnsuccessful,
                                                     table.getId());
            return new OpWrapper(op, TableOperation.Type.PUT_IF_PRESENT, row);
        }

        @Override
        public TableOperation createPutIfVersion(Row rowArg,
                                                 Version versionMatch,
                                                 ReturnRow.Choice prevReturn,
                                                 boolean abortIfUnsuccessful) {

            return createPutIfVersionInternal((RowImpl) rowArg, versionMatch,
                                              prevReturn, abortIfUnsuccessful);
        }

        /**
         * Public for use by cloud proxy
         */
        public TableOperation createPutIfVersionInternal
            (RowSerializer row,
             Version versionMatch,
             ReturnRow.Choice prevReturn,
             boolean abortIfUnsuccessful) {

            ReturnValueVersion.Choice choice =
                ReturnRowImpl.mapChoice(prevReturn);
            TableImpl table = (TableImpl)row.getTable();
            Key key = table.createKeyInternal(row, false);
            Value value = table.createValueInternal(row);
            Operation op = factory.createPutIfVersion(key, value,
                                                      versionMatch, choice,
                                                      abortIfUnsuccessful,
                                                      table.getId());
            return new OpWrapper(op, TableOperation.Type.PUT_IF_VERSION, row);
        }

        @Override
        public TableOperation createDelete
            (PrimaryKey keyArg,
             ReturnRow.Choice prevReturn,
             boolean abortIfUnsuccessful) {
            return createDeleteInternal((PrimaryKeyImpl) keyArg, prevReturn,
                                        abortIfUnsuccessful);
        }

        /**
         * Public for use by cloud proxy
         */
        public TableOperation createDeleteInternal(RowSerializer rowKey,
                                                   ReturnRow.Choice prevReturn,
                                                   boolean abortIfUnsuccessful) {
            ReturnValueVersion.Choice choice =
                ReturnRowImpl.mapChoice(prevReturn);
            TableImpl table = (TableImpl)rowKey.getTable();
            Key key = table.createKeyInternal(rowKey, false);
            Operation op = factory.createDelete(key, choice,
                                                abortIfUnsuccessful,
                                                table.getId());
            return new OpWrapper(op, TableOperation.Type.DELETE, rowKey);
        }

        @Override
        public TableOperation createDeleteIfVersion
            (PrimaryKey keyArg,
             Version versionMatch,
             ReturnRow.Choice prevReturn,
             boolean abortIfUnsuccessful) {

            return createDeleteIfVersionInternal((PrimaryKeyImpl)keyArg,
                                                 versionMatch, prevReturn,
                                                 abortIfUnsuccessful);
        }

        /**
         * Public for use by cloud proxy
         */
        public TableOperation createDeleteIfVersionInternal
            (RowSerializer rowKey,
             Version versionMatch,
             ReturnRow.Choice prevReturn,
             boolean abortIfUnsuccessful) {

            ReturnValueVersion.Choice choice =
                ReturnRowImpl.mapChoice(prevReturn);
            TableImpl table = (TableImpl)rowKey.getTable();
            Key key = table.createKeyInternal(rowKey, false);
            Operation op = factory.createDeleteIfVersion(key, versionMatch,
                                                         choice,
                                                         abortIfUnsuccessful,
                                                         table.getId());
            return new OpWrapper
                (op, TableOperation.Type.DELETE_IF_VERSION, rowKey);
        }
    }

    /************* end runtime methods **************/

    /*
     * Internal utilities
     */

    private ReturnValueVersion makeRVV(ReturnRow rr) {
        if (rr != null) {
            return ((ReturnRowImpl)rr).makeReturnValueVersion();
        }
        return null;
    }

    /**
     * Add expiration time to current and prior row
     * @param rr prior row
     * @param row current row
     * @param result the result of put or delete
     * @param reader the specified ValueReader used in deserialization.
     */
    private void initReturnRow(ReturnRow rr,
                               RowSerializer row,
                               Result result,
                               ValueReader<?> reader) {
        if (rr != null) {
            ReturnValueVersion rvv = makeRVV(rr);
            rvv.setValue(result.getPreviousValue());
            rvv.setVersion(result.getPreviousVersion());

            ValueReader<?> rowReader =
                (reader != null) ? reader : ((RowImpl)rr).initRowReader();
            ((ReturnRowImpl)rr).init(this, rvv, row,
                result.getPreviousExpirationTime(), rowReader);
        }
    }

    static KeyRange makeKeyRange(TableKey key, MultiRowOptions getOptions) {
        if (getOptions != null) {
            FieldRange range = getOptions.getFieldRange();
            if (range != null) {
                if (key.getKeyComplete()) {
                    throw new IllegalArgumentException
                        ("Cannot specify a FieldRange with a complete " +
                         "primary key");
                }
                key.validateFieldOrder(range);
                return createKeyRange(range);
            }
        } else {
            key.validateFields();
        }
        return null;
    }

    public static KeyRange createKeyRange(FieldRange range) {
        return createKeyRange(range, false);
    }

    public static KeyRange createKeyRange(FieldRange range, boolean forQuery) {

        if (range == null) {
            return null;
        }

        String start = null;
        String end = null;
        boolean startInclusive = true;
        boolean endInclusive = true;

        if (range.getStart() != null) {
            start = ((FieldValueImpl)range.getStart()).
                formatForKey(range.getDefinition(), range.getStorageSize());
            startInclusive = range.getStartInclusive();
        }

        if (range.getEnd() != null) {
            end = ((FieldValueImpl)range.getEnd()).
                formatForKey(range.getDefinition(), range.getStorageSize());
            endInclusive = range.getEndInclusive();
        }

        if (forQuery) {
            return new QueryKeyRange(start, startInclusive, end, endInclusive);
        }

        return new KeyRange(start, startInclusive, end, endInclusive);
    }

    /**
     * Turn a List<ResultKey> of keys into List<PrimaryKey>
     */
    private List<PrimaryKey>
        processMultiResults(PrimaryKey rowKey,
                            MultiRowOptions getOptions,
                            List<ResultKey> keys) {
        final List<PrimaryKey> list = new ArrayList<PrimaryKey>(keys.size());
        final boolean hasAncestorTables = (getOptions != null) &&
            (getOptions.getIncludedParentTables() != null);
        TableImpl t = (TableImpl) rowKey.getTable();
        if (hasAncestorTables) {
            t = t.getTopLevelTable();
        }
        for (ResultKey key : keys) {
            PrimaryKeyImpl pk = t.createPrimaryKeyFromResultKey(key);
            if (pk != null) {
                list.add(pk);
            }
        }
        return list;
    }

    /**
     * Turn a List<ResultKeyValueVersion> of results into List<Row>
     */
    private List<Row>
        processMultiResults(PrimaryKey rowKey,
                            MultiRowOptions getOptions,
                            Result result) {
        final List<ResultKeyValueVersion> resultList =
            result.getKeyValueVersionList();
        final List<Row> list = new ArrayList<Row>(resultList.size());
        final boolean hasAncestorTables = (getOptions != null) &&
            (getOptions.getIncludedParentTables() != null);
        TableImpl t = (TableImpl) rowKey.getTable();
        if (hasAncestorTables) {
            t = t.getTopLevelTable();
        }

        for (ResultKeyValueVersion rkvv : result.getKeyValueVersionList()) {
            RowImpl row = t.createRowFromKeyBytes(rkvv.getKeyBytes());
            if (row != null) {
                ValueVersion vv = new ValueVersion(rkvv.getValue(),
                                                   rkvv.getVersion());
                list.add(getRowFromValueVersion(vv,
                                                row,
                                                rkvv.getExpirationTime(),
                                                false));
            }
        }
        return list;
    }

    /**
     * Validate the ancestor and child tables, if set against the target table.
     */
    static void validateMultiRowOptions(MultiRowOptions mro,
                                        Table targetTable,
                                        boolean isIndex) {
        if (mro.getIncludedParentTables() != null) {
            for (Table t : mro.getIncludedParentTables()) {
                if (!((TableImpl)targetTable).isAncestor(t)) {
                    throw new IllegalArgumentException
                        ("Ancestor table \"" + t.getFullName() + "\" is not " +
                         "an ancestor of target table \"" +
                         targetTable.getFullName() + "\"");
                }
            }
        }
        if (mro.getIncludedChildTables() != null) {
            if (isIndex) {
                throw new UnsupportedOperationException
                    ("Child table returns are not supported for index " +
                     "scan operations");
            }
            for (Table t : mro.getIncludedChildTables()) {
                if (!((TableImpl)t).isAncestor(targetTable)) {
                    throw new IllegalArgumentException
                        ("Child table \"" + t.getFullName() + "\" is not a " +
                         "descendant of target table \"" +
                         targetTable.getFullName() + "\"");
                }

            }
        }
    }

    public static Consistency getConsistency(ReadOptions opts) {
        return (opts != null ? opts.getConsistency() : null);
    }

    public static long getTimeout(ReadOptions opts) {
        return (opts != null ? opts.getTimeout() : 0);
    }

    public static TimeUnit getTimeoutUnit(ReadOptions opts) {
        return (opts != null ? opts.getTimeoutUnit() : null);
    }

    static Direction getDirection(TableIteratorOptions opts,
                                  TableKey key) {
        if (opts == null) {
           return key.getMajorKeyComplete() ? Direction.FORWARD :
                                              Direction.UNORDERED;
        }
        return opts.getDirection();
    }

    public static int getBatchSize(TableIteratorOptions opts) {
        return ((opts != null && opts.getResultsBatchSize() != 0) ?
                opts.getResultsBatchSize():
                (opts != null && opts.getMaxReadKB() == 0 ?
                 KVStoreImpl.DEFAULT_ITERATOR_BATCH_SIZE : 0));
    }

    public static int getMaxReadKB(TableIteratorOptions opts) {
        return ((opts != null && opts.getMaxReadKB() != 0) ?
                opts.getMaxReadKB(): 0);
    }

    static Durability getDurability(WriteOptions opts) {
        return (opts != null ? opts.getDurability() : null);
    }

    static long getTimeout(WriteOptions opts) {
        return (opts != null ? opts.getTimeout() : 0);
    }

    static TimeUnit getTimeoutUnit(WriteOptions opts) {
        return (opts != null ? opts.getTimeoutUnit() : null);
    }

    static public TimeToLive getTTL(RowImpl row, Table table) {
        TimeToLive ttl = row.getTTLAndClearExpiration();
        return getTTL(ttl, table);
    }

    private static TimeToLive getTTL(TimeToLive ttl, Table table) {
        return ttl != null ? ttl : table.getDefaultTTL();
    }

    static boolean getUpdateTTL(WriteOptions opts) {
        return opts != null ? opts.getUpdateTTL() : false;
    }

    static int getMaxWriteKB(WriteOptions opts) {
        return ((opts != null && opts.getMaxWriteKB() != 0) ?
               opts.getMaxWriteKB(): 0);
    }

    static TargetTables makeTargetTables(Table target,
                                         MultiRowOptions getOptions) {
        List<Table> childTables =
            getOptions != null ? getOptions.getIncludedChildTables() : null;
        List<Table> ancestorTables =
            getOptions != null ? getOptions.getIncludedParentTables() : null;

        return new TargetTables(target, childTables, ancestorTables);
    }

    public TableMetadataHelper getTableMetadataHelper() {
        if (metadataHelper != null) {
            return metadataHelper;
        }
        return new MetadataHelper(this);
    }

    /*
     * Implementation of TableMetadataHelper for use in the client. Clients
     * should only call the single getTable() interface. This keeps
     * TableMetadata as a monolithic object out of the client.
     */
    private static class MetadataHelper implements TableMetadataHelper {

        private final TableAPIImpl tableAPI;

        MetadataHelper(TableAPIImpl tableAPI) {
            this.tableAPI = tableAPI;
        }

        @Override
        public TableImpl getTable(String namespace, String tableName) {
            return (TableImpl) tableAPI.getTable(namespace, tableName);
        }

        /*
         * This algorithm assumes that the top-level table has all of its
         * hierarchy in one piece to allow traversal into child tables.
         */
        @Override
        public TableImpl getTable(String namespace, String[] tablePath) {
            if (tablePath == null || tablePath.length == 0) {
                return null;
            }

            TableImpl targetTable =  getTable(namespace, tablePath[0]);
            if (tablePath.length > 1) {
                for (int i = 1; i < tablePath.length && targetTable != null;
                     i++) {
                    try {
                        targetTable = targetTable.getChildTable(tablePath[i]);
                    } catch (IllegalArgumentException ignored) {
                        targetTable = null;
                        break;
                    }
                }
            }
            return targetTable;
        }
    }

    /**
     * The MetadataCallback handler, it can be registered using
     * {@link #setTableMetadataCallback} method.
     *
     * The {@link TableMetadataCallback#metadataChanged} will be invoked when
     * it is detected that the table metadata has been changed.
     */
    public interface TableMetadataCallback {
        /**
         * The method is invoked after detected that the table metadata has
         * been changed, it should not block and do minimal processing,
         * delegating any blocking or time-consuming operations to a separate
         * thread and return back to the caller.
         *
         * @param oldSeqNum the old table metadata sequence number.
         * @param newSeqNum the new table metadata sequence number.
         */
        void metadataChanged(int oldSeqNum, int newSeqNum);
    }

}
