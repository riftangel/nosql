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

import java.util.ArrayList;
import java.util.List;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.Transaction;

import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.topo.PartitionId;

/**
 * Server handler for {@link TableIterate}.
 */
public class TableIterateHandler
        extends TableIterateOperationHandler<TableIterate> {

    public TableIterateHandler(OperationHandler handler) {
        super(handler, OpCode.TABLE_ITERATE, TableIterate.class);
    }

    @Override
    Result execute(TableIterate op,
                   Transaction txn,
                   PartitionId partitionId) {

        verifyTableAccess(op);

        final List<ResultKeyValueVersion> results =
            new ArrayList<ResultKeyValueVersion>();

        final OperationTableInfo tableInfo = new OperationTableInfo();
        Scanner scanner = getScanner(op,
                                     tableInfo,
                                     txn,
                                     partitionId,
                                     CURSOR_READ_COMMITTED,
                                     LockMode.READ_UNCOMMITTED_ALL,
                                     true); // set keyOnly. Handle fetch here

        try {
            DatabaseEntry keyEntry = scanner.getKey();
            DatabaseEntry dataEntry = scanner.getData();

            /* this is used to do a full fetch of data when needed */
            DatabaseEntry dentry = new DatabaseEntry();
            Cursor cursor = scanner.getCursor();
            boolean moreElements;

            scanner.setChargeKeyRead(false);
            while ((moreElements = scanner.next()) == true) {
                int match = keyInTargetTable(op,
                                             tableInfo,
                                             keyEntry,
                                             dataEntry,
                                             cursor,
                                             false);
                if (match > 0) {

                    if (exceedsMaxReadKB(op, scanner.getCurrentStorageSize())) {
                        /* TODO: should the key size be subtracted? */
                        break;
                    }

                    /*
                     * The iteration was done using READ_UNCOMMITTED_ALL
                     * and with the cursor set to getPartial().  It is
                     * necessary to call getLockedData() here to both lock
                     * the record and fetch the data.
                     */
                    boolean ret = scanner.getLockedData(dentry);

                    if (ret) {
                        if (!isTableData(dentry.getData(), null)) {
                            continue;
                        }

                        /*
                         * Add ancestor table results.  These appear
                         * before targets, even for reverse iteration.
                         */
                        tableInfo.addAncestorValues(cursor, results, keyEntry,
                                                    op.getOpSerialVersion());
                        addValueResult(operationHandler, results,
                                       cursor, keyEntry, dentry,
                                       scanner.getResult(),
                                       op.getOpSerialVersion());
                    }
                } else if (match < 0) {
                    moreElements = false;
                    /* No matched key found, charge empty read */
                    op.addEmptyReadCharge();
                    break;
                }
                if (op.getBatchSize() != 0 &&
                    results.size() >= op.getBatchSize()) {
                    break;
                }
            }
            return new Result.IterateResult(getOpCode(),
                                            op.getReadKB(), op.getWriteKB(),
                                            results, moreElements);
        } finally {
            scanner.close();
        }
    }

    protected Scanner getScanner(TableIterate op,
                                 OperationTableInfo tableInfo,
                                 Transaction txn,
                                 PartitionId partitionId,
                                 CursorConfig cursorConfig,
                                 LockMode lockMode,
                                 boolean keyOnly) {
        return getScanner(op,
                          tableInfo,
                          txn,
                          partitionId,
                          op.getMajorComplete(),
                          op.getDirection(),
                          op.getResumeKey(),
                          true, /*moveAfterResumeKey*/
                          cursorConfig,
                          lockMode,
                          keyOnly);
    }

    /**
     * Gets the cursor configuration to be used during the iteration.
     * @return the cursor configuration
     */
    protected CursorConfig getCursorConfig() {
        return CURSOR_READ_COMMITTED;
    }
}
