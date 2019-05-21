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

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Transaction;

import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.ops.MultiTableOperationHandler.AncestorList;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.table.Table;
import oracle.kv.impl.topo.PartitionId;

/**
 * Server handler for {@link IndexKeysIterate}.
 */
public class IndexKeysIterateHandler
        extends IndexOperationHandler<IndexKeysIterate> {

    IndexKeysIterateHandler(OperationHandler handler) {
        super(handler, OpCode.INDEX_KEYS_ITERATE, IndexKeysIterate.class);
    }

    @Override
    Result execute(IndexKeysIterate op,
                   Transaction txn,
                   PartitionId partitionId  /* Not used */) {

        verifyTableAccess(op);

        final AncestorList ancestors =
            new AncestorList(operationHandler,
                             txn,
                             op.getResumePrimaryKey(),
                             op.getTargetTables().getAncestorTableIds());

        final Table table = getTable(op);
        final IndexImpl index = getIndex(op,
                                         table.getNamespace(),
                                         table.getFullName());
        IndexScanner scanner =
            new IndexScanner(op, txn,
                             getSecondaryDatabase(op,
                                                  table.getNamespace(),
                                                  table.getFullName()),
                             index,
                             op.getIndexRange(),
                             op.getResumeSecondaryKey(),
                             op.getResumePrimaryKey(),
                             true, // key-only, don't fetch data
                             true /* moveAfterResumeKey */);

        final List<ResultIndexKeys> results =  new ArrayList<>();

        boolean moreElements;

        /*
         * Cannot get the DatabaseEntry key objects from the scanner until it
         * has been initialized.
         */
        try {
            while ((moreElements = scanner.next()) == true) {

                final DatabaseEntry indexKeyEntry = scanner.getIndexKey();
                final DatabaseEntry primaryKeyEntry = scanner.getPrimaryKey();

                /*
                 * Data has not been fetched but the record is locked.
                 * Create the result from the index key and information
                 * available in the index record.
                 */
                List<ResultKey> ancestorKeys =
                    ancestors.addAncestorKeys(primaryKeyEntry);

                if (ancestorKeys != null) {

                    for (ResultKey key : ancestorKeys) {

                        byte[] indexKey =
                            reserializeToOldKeys(op,
                                                 index,
                                                 indexKeyEntry.getData());

                        if (indexKey != null) {
                            results.add(new ResultIndexKeys(
                                        key.getKeyBytes(),
                                        indexKey,
                                        key.getExpirationTime()));
                        }
                    }
                }

                byte[] indexKey = reserializeToOldKeys(op,
                                                       index,
                                                       indexKeyEntry.getData());
                if (indexKey != null) {
                    results.add(new ResultIndexKeys(
                                primaryKeyEntry.getData(),
                                indexKey,
                                scanner.getExpirationTime()));
                }

                /*
                 * Check the size limit after add the entry to result rather
                 * than throwing away the already-fetched entry.
                 */
                if (exceedsMaxReadKB(op)) {
                    break;
                }

                if (op.getBatchSize() > 0 &&
                    results.size() >= op.getBatchSize()) {
                    break;
                }
            }
            return new Result.IndexKeysIterateResult(getOpCode(),
                                                     op.getReadKB(),
                                                     op.getWriteKB(),
                                                     results,
                                                     moreElements);
        } finally {
            scanner.close();
        }
    }

    /*
     * This method translates, if needed, newer binary key format to older
     * binary key format. It is used to handle the following 2 cases:
     *
     * (a) The IndexKeysIterate op was sent by a pre-4.2 client to a 4.4+
     * server and the index was created by a 4.2+ server. In this case, the
     * binary index keys extracted from the index contain null or special-value
     * indicator bytes. The client that will receive these key will not be able
     * to deserilize them correctly, because the client does not expect these
     * indicator bytes.
     *
     * (b) The IndexKeysIterate op was sent by a 4.2 or 4.3 client to a 4.4+
     * server and the index was created by a 4.4+ server. In this case, the
     * client does know about the NULL indicator only. Furthermore, the value
     * of the NULL indicator has changed in 4.4. but the value of the null
     * indicator has
     *
     * To handle (a) and (b), this method deserializes the binary keys and
     * then reserialializes them again in the format expected by the client.
     */
    byte[] reserializeToOldKeys(IndexKeysIterate op,
                                IndexImpl index,
                                byte[] indexKey) {

        int indexVersion = index.getIndexVersion();
        short opVersion = op.getOpSerialVersion();

        if (// client is pre-4.2 and index is 4.2+
            opVersion < SerialVersion.V12 && index.supportsSpecialValues() ||
            // client is 4.2 or 4.3 and index is 4.4+
            (indexVersion > 0 &&
             (opVersion == SerialVersion.V12 ||
              opVersion == SerialVersion.V13))) {

            return index.reserializeToOldKey(indexKey, opVersion);
        }

        return indexKey;
    }
}
