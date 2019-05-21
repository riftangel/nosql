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

import static oracle.kv.impl.api.ops.OperationHandler.CURSOR_READ_COMMITTED;

import java.util.Collections;
import java.util.List;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Get;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.Transaction;

import oracle.kv.Direction;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.TablePrivilege;
import oracle.kv.impl.topo.PartitionId;

/**
 * Base server handler for subclasses of MultiGetTableOperation.
 */
abstract class MultiGetTableOperationHandler<T extends MultiGetTableOperation>
        extends MultiTableOperationHandler<T> {

    MultiGetTableOperationHandler(OperationHandler handler,
                                  OpCode opCode,
                                  Class<T> operationType) {
        super(handler, opCode, operationType);
    }

    boolean iterateTable(T op,
                         Transaction txn,
                         PartitionId partitionId,
                         byte[] parentKey,
                         int batchSize,
                         byte[] resumeKey,
                         TableScanVisitor<T> visitor) {

        return iterateTable(op, txn, partitionId, parentKey,
                            true /*majorPathComplete*/, Direction.FORWARD,
                            batchSize, resumeKey, CURSOR_READ_COMMITTED,
                            LockMode.READ_UNCOMMITTED_ALL, visitor);
    }

    /**
     * The TableScanVisitor class implements an interface call by iteration
     * on successful acquisition of a new record.
     */
    static abstract class TableScanVisitor<T extends MultiGetTableOperation> {

        private final T operation;
        private final MultiTableOperationHandler<T> handler;
        final OperationTableInfo tableInfo = new OperationTableInfo();

        TableScanVisitor(T operation,
                         MultiTableOperationHandler<T> handler) {
            this.operation = operation;
            this.handler = handler;
        }

        T getOperation() {
            return operation;
        }

        OperationHandler getOperationHandler() {
            return handler.getOperationHandler();
        }

        OperationTableInfo getOperationTableInfo() {
            return tableInfo;
        }

        abstract int addResults(Cursor cursor,
                                DatabaseEntry keyEntry,
                                DatabaseEntry dataEntry);

        public int visit(Cursor cursor,
                         DatabaseEntry keyEntry,
                         DatabaseEntry dataEntry) {

            /*
             * 1.  check to see if key is part of table
             * 2.  if so:
             *    - fetch data
             *    - add to results
             */
            int match = handler.keyInTargetTable(operation,
                                                 tableInfo,
                                                 keyEntry,
                                                 dataEntry,
                                                 cursor,
                                                 true /* chargeReadCost */);
            if (match > 0) {
                final int nRecs = addResults(cursor, keyEntry, dataEntry);
                return (nRecs == 0) ? 0: match + nRecs;
            }
            return match;
        }
    }

    /**
     * A wrapper on Scanner used by the batch get methods. These have not been
     * refactored to use Scanner directly. There is no need for that at this
     * time.
     */
    boolean iterateTable(T op,
                         Transaction txn,
                         PartitionId partitionId,
                         byte[] parentKey,
                         boolean majorPathComplete,
                         Direction scanDirection,
                         int batchSize,
                         byte[] resumeKey,
                         CursorConfig cursorConfig,
                         LockMode lockMode,
                         TableScanVisitor<T> visitor) {

        initTableLists(op.getTargetTables(), visitor.getOperationTableInfo(),
                       txn, scanDirection, resumeKey);

        final Scanner scanner =
            new Scanner(op, txn,
                        partitionId,
                        getRepNode(),
                        parentKey,
                        majorPathComplete,
                        op.getSubRange(),
                        op.getDepth(),
                        scanDirection,
                        resumeKey,
                        true, /*moveAfterResumeKey*/
                        cursorConfig,
                        lockMode,
                        true);

        DatabaseEntry keyEntry = scanner.getKey();
        DatabaseEntry dataEntry = scanner.getData();
        Cursor cursor = scanner.getCursor();
        boolean moreElements;
        int nElements = 0;
        try {
            while ((moreElements = scanner.next()) == true) {
                int match = visitor.visit(cursor, keyEntry, dataEntry);
                if (match < 0) {
                    moreElements = false;
                    break;
                }

                nElements += match;
                if (batchSize > 0 && (nElements >= batchSize)) {
                    break;
                }
            }
        } finally {
            scanner.close();
        }
        return moreElements;
    }

    /**
     * An implementation of TableScanVisitor that is used for table rows scan.
     */
    static class TableScanValueVisitor<T extends MultiGetTableOperation>
            extends TableScanVisitor<T> {

        private final List<ResultKeyValueVersion> results;

        TableScanValueVisitor(T operation,
                              MultiTableOperationHandler<T> handler,
                              List<ResultKeyValueVersion> results) {

            super(operation, handler);
            this.results = results;
        }

        @Override
        int addResults(Cursor cursor,
                       DatabaseEntry keyEntry,
                       DatabaseEntry dataEntry) {

            final OperationHandler operationHandler = getOperationHandler();
            int nRecs =  0;
            /*
             * Because the iteration does not fetch the data, it is
             * necessary to lock the record and fetch the data now.
             * This is an optimization to avoid data fetches for
             * rows that are not in a target table.
             */
            assert dataEntry.getPartial();
            final DatabaseEntry dentry = new DatabaseEntry();
            OperationResult result =
                cursor.get(keyEntry, dentry, Get.CURRENT,
                           LockMode.DEFAULT.toReadOptions());

            T op = getOperation();
            if (result != null) {
                op.addReadBytes(getStorageSize(cursor));
                /*
                 * Filter out non-table data.
                 */
                if (!isTableData(dentry.getData(), null)) {
                    return 0;
                }

                /*
                 * Add ancestor table results.  These appear
                 * before targets, even for reverse iteration.
                 */
                short opSerialVersion = getOperation().getOpSerialVersion();
                nRecs = tableInfo.addAncestorValues(cursor, results, keyEntry,
                                                    opSerialVersion);
                addValueResult(operationHandler, results, cursor, keyEntry,
                               dentry, result, opSerialVersion);
            } else {
                op.addReadBytes(MIN_READ);
            }
            return nRecs;
        }
    }

    /**
     * An implementation of TableScanVisitor that is used table keyonly scan.
     */
    static class TableScanKeyVisitor<T extends MultiGetTableOperation>
            extends TableScanVisitor<T> {

        private final List<ResultKey> results;

        TableScanKeyVisitor(T operation,
                            MultiTableOperationHandler<T> handler,
                            List<ResultKey> results) {

            super(operation, handler);
            this.results = results;
        }

        @Override
        int addResults(Cursor cursor,
                       DatabaseEntry keyEntry,
                       DatabaseEntry dataEntry) {

            int nRecs =  0;

            /*
             * The iteration was done using READ_UNCOMMITTED_ALL so
             * it's necessary to call getCurrent() here to lock
             * the record.  The original DatabaseEntry is used
             * to avoid fetching data.  It had setPartial() called
             * on it.
             */
            assert dataEntry.getPartial();
            OperationResult result =
                cursor.get(keyEntry, dataEntry, Get.CURRENT,
                           LockMode.DEFAULT.toReadOptions());
            if (result != null) {

                /*
                 * Add ancestor table results.  These appear
                 * before targets, even for reverse iteration.
                 */
                nRecs = tableInfo.addAncestorKeys(cursor, results, keyEntry);
                addKeyResult(results,
                             keyEntry.getData(),
                             result.getExpirationTime());
            }
            return nRecs;
        }
    }

    @Override
    public List<? extends KVStorePrivilege>
        tableAccessPrivileges(long tableId) {

        return Collections.singletonList(new TablePrivilege.ReadTable(tableId));
    }
}
