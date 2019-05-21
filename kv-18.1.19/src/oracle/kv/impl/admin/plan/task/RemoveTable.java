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
import static oracle.kv.impl.admin.plan.task.EvolveTable.tableDoesNotExist;

import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.plan.MetadataPlan;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.admin.plan.TablePlanGenerator;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;

import com.sleepycat.je.Transaction;
import com.sleepycat.persist.model.Persistent;

/**
 * Remove/drop a table
 *
 * version 0: original
 * version 1: added namespace
 * version 2: added tableId
 */
@Persistent(version=2)
public class RemoveTable extends UpdateMetadata<TableMetadata> {
    private static final long serialVersionUID = 1L;

    protected /*final*/ String tableName;
    protected /*final*/ String namespace;
    protected /*final*/ boolean markForDelete;

    /**
     * The table id of the target table when the task was created. It may be
     * 0 if the task was deserialized from an earlier version.
     */
    private /*final*/ long tableId;

    public static RemoveTable newInstance(MetadataPlan<TableMetadata> plan,
                                          String namespace,
                                          String tableName,
                                          boolean markForDelete) {
        final RemoveTable removeTable =
            new RemoveTable(plan, namespace, tableName, markForDelete);
        removeTable.checkTableForRemove();
        return removeTable;
    }

    protected RemoveTable(MetadataPlan<TableMetadata> plan,
                          String namespace,
                          String tableName,
                          boolean markForDelete) {
        super(plan);

        /*
         * Caller verifies parameters
         */
        this.tableName = tableName;
        this.namespace = namespace;
        this.markForDelete = markForDelete;
    }

    /**
     * Checks if table to be removed can be found. This method must be called
     * once the table metadata is available.
     */
    protected void checkTableForRemove() {
        final TableMetadata md = getMetadata();

        if (md == null) {
            throw tableMetadataNotFound();
        }

        if (md.getTable(namespace, tableName, false) == null) {
            throw tableDoesNotExist(namespace, tableName);
        }

        /*
         * Any indexes should be removed by tasks run before this one, so
         * do fail if they are present at this point.
         */
        tableId = md.checkForRemove(namespace, tableName,
                                    true /* indexes allowed */).getId();
    }

    /*
     * No-arg ctor for use by DPL.
     */
    @SuppressWarnings("unused")
    private RemoveTable() {
    }

    @Override
    public void acquireLocks(Planner planner)
        throws PlanLocksHeldException {
        /*
         * We do not lock the indexes owned by this table because there is
         * a check in dropTable() that makes sure all indexes are gone.
         */
        lockTable(planner, getPlan(), namespace, tableName);
    }

    @Override
    protected TableMetadata updateMetadata(TableMetadata md, Transaction txn) {

        /*
         * See if the table is still present.  This will not throw if the
         * table is absent. Return the metadata so that it is broadcast, just
         * in case this is a re-execute.
         */
        final TableImpl table = md.getTable(namespace, tableName);
        if (table != null) {

            /*
             * If it is not the same table, then the original table was
             * removed and the metadata updated. In that case just exit.
             */
            if ((tableId != 0) && (tableId != table.getId())) {
                return null;
            }
            md.dropTable(namespace, tableName, markForDelete);
            getPlan().getAdmin().saveMetadata(md, txn);
        }
        return md;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
        return TablePlanGenerator.makeName(super.getName(sb),
                                           namespace,
                                           tableName,
                                           null);
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

        RemoveTable other = (RemoveTable) t;

        if (namespace == null) {
            if (other.namespace != null) {
                return false;
            }
        } else if (!namespace.equalsIgnoreCase(other.namespace)) {
            return false;
        }

        return (tableName.equalsIgnoreCase(other.tableName) &&
                (markForDelete == other.markForDelete));
    }
}
