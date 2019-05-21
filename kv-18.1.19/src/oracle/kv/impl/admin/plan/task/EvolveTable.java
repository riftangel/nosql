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

package oracle.kv.impl.admin.plan.task;

import static oracle.kv.impl.admin.plan.task.AddTable.lockTable;
import static oracle.kv.impl.admin.plan.task.AddTable.tableMetadataNotFound;

import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.TableNotFoundException;
import oracle.kv.impl.admin.plan.MetadataPlan;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.api.table.FieldMap;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.table.TimeToLive;

import com.sleepycat.je.Transaction;
import com.sleepycat.persist.model.Persistent;

/**
 * Evolve a table
 * version 0: original
 * version 1: added ttl
 * version 2: added namespace
 * version 3: added tableId
 * version 4: added description and systemTable
 */
@Persistent(version=4)
public class EvolveTable extends UpdateMetadata<TableMetadata> {
    private static final long serialVersionUID = 1L;

    private /*final*/ String namespace;
    private /*final*/ String tableName;
    private /*final*/ int tableVersion;
    private /*final*/ FieldMap fieldMap;
    private /*final*/ TimeToLive ttl;
    private /*final*/ String description;
    private /*final*/ boolean systemTable;

    /*
     * The table id of the target table when the task was created. It may be
     * 0 if the task was deserialized from an earlier version.
     */
    private /*final*/ long tableId;

    /**
     */
    public EvolveTable(MetadataPlan<TableMetadata> plan,
                       String namespace,
                       String tableName,
                       int tableVersion,
                       FieldMap fieldMap,
                       TimeToLive ttl,
                       String description,
                       boolean systemTable) {
        super(plan);

        /*
         * Caller verifies parameters
         */
        this.tableName = tableName;
        this.namespace = namespace;
        this.fieldMap = fieldMap;
        this.tableVersion = tableVersion;
        this.ttl = ttl;
        this.description = description;
        this.systemTable = systemTable;

        final TableMetadata md = getMetadata();
        if (md == null) {
            throw tableMetadataNotFound();
        }
        final TableImpl table = md.getTable(namespace, tableName);
        if (table == null) {
            throw tableDoesNotExist(namespace, tableName);
        }
        tableId = table.getId();
    }

    /*
     * No-arg ctor for use by DPL.
     */
    @SuppressWarnings("unused")
    private EvolveTable() {
    }

    @Override
    public void acquireLocks(Planner planner)
        throws PlanLocksHeldException {
        lockTable(planner, getPlan(), namespace, tableName);
    }

    @Override
    protected TableMetadata updateMetadata(TableMetadata md, Transaction txn) {

        /*
         * The table could have been removed and recreated since the task was
         * created. Note that tableId could be 0 if the task was from a
         * previous version. Skip the check if so.
         */
        final TableImpl table = md.getTable(namespace, tableName);
        if ((table == null) ||                                  /* gone */
            table.isDeleting() ||                               /* going */
            ((tableId != 0) && (tableId != table.getId()))) {   /* returned */
            throw tableDoesNotExist(namespace, tableName);
        }

        if (md.evolveTable(table, tableVersion, fieldMap, ttl,
                           description, systemTable)) {
            getPlan().getAdmin().saveMetadata(md, txn);
        }
        return md;
    }

    static TableNotFoundException tableDoesNotExist(String namespace,
                                                    String tableName) {
        return new TableNotFoundException(
                        "Table " +
                        TableMetadata.makeNamespaceName(namespace, tableName) +
                        " does not exist");
    }

    @Override
    public boolean logicalCompare(Task t) {
        if (this == t) {
            return true;
        }

        if (t == null) {
            return false;
        }

        if (getClass() != t.getClass()) {
            return false;
        }

        EvolveTable other = (EvolveTable) t;

        if (namespace == null) {
            if (other.namespace != null) {
                return false;
            }
        } else if (!namespace.equalsIgnoreCase(other.namespace)) {
            return false;
        }

        return (tableName.equalsIgnoreCase(other.tableName) &&
                (tableVersion == other.tableVersion)  &&
                (fieldMap.equals(other.fieldMap)));
    }
}
