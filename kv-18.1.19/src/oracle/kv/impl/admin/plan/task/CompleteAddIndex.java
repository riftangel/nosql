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

import java.util.logging.Level;

import oracle.kv.impl.admin.plan.MetadataPlan;
import oracle.kv.impl.admin.plan.TablePlanGenerator;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.TableMetadata;

import com.sleepycat.je.Transaction;
import com.sleepycat.persist.model.Persistent;

/**
 * Completes index addition, making the index publicly visible.
 *
 * version 0: original
 * version 1: added namespace
 */
@Persistent(version=1)
public class CompleteAddIndex extends UpdateMetadata<TableMetadata> {
    private static final long serialVersionUID = 1L;

    private /*final*/ String indexName;
    private /*final*/ String tableName;
    private /*final*/ String namespace;

    /**
     */
    public CompleteAddIndex(MetadataPlan<TableMetadata> plan,
                            String namespace,
                            String indexName,
                            String tableName) {
        super(plan);
        this.indexName = indexName;
        this.tableName = tableName;
        this.namespace = namespace;
    }

    /*
     * No-arg ctor for use by DPL.
     */
    @SuppressWarnings("unused")
    private CompleteAddIndex() {
    }

    @Override
    protected TableMetadata updateMetadata(TableMetadata md, Transaction txn) {
        final IndexImpl index = (IndexImpl) md.getIndex(namespace,
                                                        tableName,
                                                        indexName);

        /*
         * The index should exist because the plan to create it started, but
         * it will not if, for example, the index population failed. Log this
         * situation and succeed;
         */
        if (index == null) {
            getPlan().getLogger().log(Level.INFO,
                                      "{0} index {1} does not exist in table," +
                                      " index population may have failed",
                                      new Object[]{this, indexName});
        } else {
            /* Use the TableMetadata method to bump the seq. number */
            md.updateIndexStatus(namespace, indexName, tableName,
                                 IndexImpl.IndexStatus.READY);
            getPlan().getAdmin().saveMetadata(md, txn);
        }
        return md;
    }

    @Override
    public boolean continuePastError() {
        return false;
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
        return true;
    }
}
