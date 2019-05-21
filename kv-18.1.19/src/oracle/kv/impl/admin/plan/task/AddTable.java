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

import java.util.List;
import java.util.concurrent.TimeUnit;

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.TableAlreadyExistsException;
import oracle.kv.impl.admin.plan.MetadataPlan;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.admin.plan.Planner.LockCategory;
import oracle.kv.impl.admin.plan.TablePlanGenerator;
import oracle.kv.impl.api.table.FieldMap;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableLimits;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.security.AccessCheckUtils;
import oracle.kv.impl.security.ExecutionContext;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.table.TimeToLive;

import com.sleepycat.je.Transaction;
import com.sleepycat.persist.model.Persistent;

/**
 * Adds a table
 *
 * version 0: original
 * version 1: added primaryKeySizes, ttl and ttlUnit fields
 * version 2: added namespace
 * version 3: added limits, systemTable
 */
@Persistent(version=3)
public class AddTable extends UpdateMetadata<TableMetadata> {
    private static final long serialVersionUID = 1L;

    private /*final*/ String tableName;
    private /*final*/ String namespace;
    private /*final*/ String parentName;
    private /*final*/ List<String> primaryKey;

    /*
     * The major key is the shard key. Since this is a DPL object, the
     * field cannot be renamed without upgrade issues, so we will
     * maintain the name.
     */
    private /*final*/ List<String> majorKey;
    private /*final*/ FieldMap fieldMap;
    private /*final*/ boolean r2compat;
    private /*final*/ int schemaId;
    private /*final*/ String description;
    /*
     * Note that we persist the base types instead of a TimeToLive instance
     * in order to avoid having to add DPL annotations to TimeToLive and its
     * superclass. If ttlUnit is null, no ttl was specified.
     */
    private /*final*/ int ttl;
    private /*final*/ TimeUnit ttlUnit;
    private /*final*/ List<Integer> primaryKeySizes;

    private /*final*/ TableLimits limits;

    /* true if adding a system table */
    private /*final*/ boolean sysTable;

    /**
     */
    public AddTable(MetadataPlan<TableMetadata> plan,
                    TableImpl table,
                    String parentName) {
        super(plan);

        /*
         * Caller verifies parameters
         */

        this.tableName = table.getName();
        this.parentName = parentName;
        this.namespace = table.getNamespace();

        this.primaryKey = table.getPrimaryKey();
        this.primaryKeySizes = table.getPrimaryKeySizes();

        /* Note that the major key is the shard key */
        this.majorKey = table.getShardKey();
        if (table.getDefaultTTL() != null) {
            this.ttl = (int) table.getDefaultTTL().getValue();
            this.ttlUnit = table.getDefaultTTL().getUnit();
        } else {
            this.ttl = 0;
            this.ttlUnit = null;
        }
        limits = (parentName == null) ? table.getTableLimits() : null;
        this.fieldMap = table.getFieldMap();
        this.r2compat = table.isR2compatible();
        this.schemaId = table.getSchemaId();
        this.description = table.getDescription();
        this.sysTable = table.isSystemTable();

        final TableMetadata md = plan.getMetadata();

        /*
         * If the table metadata is null we continue. The task will create
         * the metadata when it runs.
         */
        if ((md != null) &&
            (md.getTable(namespace, tableName, parentName) != null)) {
            throw tableAlreadyExists();
        }
        ensureCurrentUserOwnsParent(md);
    }

    /*
     * No-arg ctor for use by DPL.
     */
    @SuppressWarnings("unused")
    private AddTable() {
    }

    static IllegalCommandException tableMetadataNotFound() {
        return new IllegalCommandException("Table metadata not found");
    }

    /*
     * Checks if current user is the owner of a table.
     */
    private void ensureCurrentUserOwnsParent(TableMetadata md) {
        if (sysTable ||
            (parentName == null) ||
            (ExecutionContext.getCurrent() == null)) {
            return;
        }

        if (md == null) {
            throw tableMetadataNotFound();
        }

        final TableImpl parentTable = md.getTable(namespace, parentName);
        assert (parentTable != null);

        /* The parent table is a legacy table without owner, just return */
        if (parentTable.getOwner() == null) {
            return;
        }
        if (!AccessCheckUtils.currentUserOwnsResource(parentTable)) {
            throw new IllegalCommandException(
                "Only the owner of table " + parentName + " is able to " +
                "create child tables under it");
        }
    }

    @Override
    public void acquireLocks(Planner planner)
        throws PlanLocksHeldException {
        if (parentName != null) {
            lockTable(planner, getPlan(), namespace, parentName);
        }
        lockTable(planner, getPlan(), namespace, tableName);
    }

    static void lockTable(Planner planner, Plan plan,
                          String namespace, String tableName)
        throws PlanLocksHeldException {
        planner.lock(plan.getId(), plan.getName(), LockCategory.TABLE,
                     namespace, tableName);

    }

    @Override
    protected TableMetadata createMetadata() {
        return new TableMetadata(true);
    }

    @Override
    protected TableMetadata updateMetadata(TableMetadata md, Transaction txn) {

        /* If the table does not exist, add it */
        final TableImpl existing =
                md.getTable(namespace, tableName, parentName);
        if (existing == null) {
            md.addTable(namespace,
                        tableName,
                        parentName,
                        primaryKey,
                        primaryKeySizes,
                        majorKey,
                        fieldMap,
                        (ttlUnit == null) ? null :
                                    TimeToLive.createTimeToLive(ttl, ttlUnit),
                        limits,
                        r2compat,
                        schemaId,
                        description,
                        SecurityUtils.currentUserAsOwner(),
                        sysTable);
            getPlan().getAdmin().saveMetadata(md, txn);
            return md;
        }

        /*
         * Check to see if the existing table is the one we were adding. If
         * so this is a restart and we are done. Otherwise, some other table
         * was created with the same name.
         */
        if (!primaryKey.equals(existing.getPrimaryKey()) ||
            !majorKey.equals(existing.getShardKey()) ||
            !fieldMap.equals(existing.getFieldMap())) {
            throw tableAlreadyExists();
        }
        return md;
    }

    /**
     * Throws a "Table already exists" IllegalCommandException.
     */
    private TableAlreadyExistsException tableAlreadyExists() {
        return new TableAlreadyExistsException
            ("Table " +
             TableMetadata.makeQualifiedName(namespace, tableName, parentName) +
             " already exists");
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
                                           null);
    }

    /**
     * Returns true if this AddTable will end up creating the same table.
     * Checks that tableName, parentName, primaryKey, majorKey (shard key) and
     * fieldMap are the same. Intentionally excludes r2compat, schemId, and
     * description, since those don't directly affect the table metadata.
     */
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

        AddTable other = (AddTable) t;
        if (!tableName.equalsIgnoreCase(other.tableName)) {
            return false;
        }

        if (parentName == null) {
            if (other.parentName != null) {
                return false;
            }
        } else if (!parentName.equalsIgnoreCase(other.parentName)) {
            return false;
        }

        if (namespace == null) {
            if (other.namespace != null) {
                return false;
            }
        } else if (!namespace.equalsIgnoreCase(other.namespace)) {
            return false;
        }

        return (primaryKey.equals(other.primaryKey) &&
                majorKey.equals(other.majorKey) &&
                fieldMap.equals(other.fieldMap));
    }
}
