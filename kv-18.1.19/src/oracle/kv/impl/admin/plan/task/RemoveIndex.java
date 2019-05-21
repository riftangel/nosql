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

import static oracle.kv.impl.admin.plan.task.AddTable.tableMetadataNotFound;
import static oracle.kv.impl.admin.plan.task.EvolveTable.tableDoesNotExist;
import static oracle.kv.impl.admin.plan.task.StartAddIndex.lockIndex;

import oracle.kv.impl.admin.IndexNotFoundException;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.plan.MetadataPlan;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.admin.plan.TablePlanGenerator;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;

import com.sleepycat.je.Transaction;
import com.sleepycat.persist.model.Persistent;

/**
 * Completes index addition, making the index publicly visible.
 *
 * version 0: original
 * version 1: added namespace
 * version 2: added tableId
 */
@Persistent(version=2)
public class RemoveIndex extends UpdateMetadata<TableMetadata> {
    private static final long serialVersionUID = 1L;

    protected /*final*/ String indexName;
    protected /*final*/ String tableName;
    protected /*final*/ String namespace;

    /*
     * The table id of the target table when the task was created. It may be
     * 0 if the task was deserialized from an earlier version.
     */
    private /*final*/ long tableId;

    public static RemoveIndex newInstance(MetadataPlan<TableMetadata> plan,
                                          String namespace,
                                          String indexName,
                                          String tableName) {
        final RemoveIndex removeIndex =
            new RemoveIndex(plan, namespace, indexName, tableName);
        removeIndex.checkIndexForRemove();
        return removeIndex;
    }

    protected RemoveIndex(MetadataPlan<TableMetadata> plan,
                          String namespace,
                          String indexName,
                          String tableName) {
        super(plan);

        /*
         * Caller verifies parameters
         */
        this.indexName = indexName;
        this.tableName = tableName;
        this.namespace = namespace;
    }

    /*
     * No-arg ctor for use by DPL.
     */
    @SuppressWarnings("unused")
    private RemoveIndex() {
    }

    /**
     * Check if index to be removed can be found.  This method must be called
     * once the table metadata is available.
     */
    protected void checkIndexForRemove() {
        final TableMetadata md = getMetadata();
        if (md == null) {
            throw tableMetadataNotFound();
        }
        final TableImpl table = md.getTable(namespace, tableName);
        if (table == null) {
            throw tableDoesNotExist(namespace, tableName);
        }
        if (table.getIndex(indexName) == null) {
            throw new IndexNotFoundException
                        ("Index " + indexName + " does not exists in table " +
                         TableMetadata.makeNamespaceName(namespace, tableName));
        }
        tableId = table.getId();
    }

    @Override
    public void acquireLocks(Planner planner)
        throws PlanLocksHeldException {
        /*
         * Note that we only lock the index and not the table. This is to allow
         * other table operations to take place.
         */
        lockIndex(planner, getPlan(), namespace, tableName, indexName);
    }

    @Override
    protected TableMetadata updateMetadata(TableMetadata md, Transaction txn) {

        /*
         * If the table or index is already gone, or the table is being
         * removed, we are done. In any case return the metadata so that it
         * is broadcast, just in case this is a re-execute.
         */
        final TableImpl table = md.getTable(namespace, tableName);
        if ((table != null) && !table.isDeleting() &&
            (table.getIndex(indexName) != null)) {

            /*
             * If this is a different table, then the index is gone and we do
             * not need to update the MD.
             */
            if ((tableId != 0) && (tableId != table.getId())) {
                return null;
            }
            md.dropIndex(namespace, indexName, tableName);
            getPlan().getAdmin().saveMetadata(md, txn);
        }
        return md;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
        return TablePlanGenerator.makeName(super.getName(sb),
                                           namespace,
                                           tableName,
                                           indexName);
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

        RemoveIndex other = (RemoveIndex) t;
        if (!tableName.equalsIgnoreCase(other.tableName)) {
            return false;
        }

        if (namespace == null) {
            if (other.namespace != null) {
                return false;
            }
        } else if (!namespace.equalsIgnoreCase(other.namespace)) {
            return false;
        }

        if (!indexName.equalsIgnoreCase(other.indexName)) {
            return false;
        }
        return true;
    }
}
