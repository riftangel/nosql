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

package oracle.kv.impl.security;

import java.util.EnumMap;
import java.util.HashSet;
import java.util.Set;

public abstract class TablePrivilege extends KVStorePrivilege {

    private static final long serialVersionUID = 1L;

    /**
     * Name of the specified table, used for display only.
     */
    private final String tableName;

    /**
     * Unique ID of the specified table, used together with privilege label
     * to distinguish one table privilege instance from another.
     */
    private final long tableId;

    /**
     * Table privilege creator.
     */
    interface CreatePrivilege {
        TablePrivilege createPrivilege(long tableId, String tableName);
    }

    /*
     * A convenient map of table privilege creators.
     */
    private static final EnumMap<KVStorePrivilegeLabel, CreatePrivilege>
    tablePrivCreateMap = new EnumMap<KVStorePrivilegeLabel, CreatePrivilege>(
        KVStorePrivilegeLabel.class);

    static {
        tablePrivCreateMap.put(
            KVStorePrivilegeLabel.DELETE_TABLE,
            new CreatePrivilege() {
                @Override
                public TablePrivilege createPrivilege(long id, String name) {
                    return new DeleteTable(id, name);
                }
            });
        tablePrivCreateMap.put(
            KVStorePrivilegeLabel.READ_TABLE,
            new CreatePrivilege() {
                @Override
                public TablePrivilege createPrivilege(long id, String name) {
                    return new ReadTable(id, name);
                }
            });
        tablePrivCreateMap.put(
            KVStorePrivilegeLabel.INSERT_TABLE,
            new CreatePrivilege() {
                @Override
                public TablePrivilege createPrivilege(long id, String name) {
                    return new InsertTable(id, name);
                }
            });
        tablePrivCreateMap.put(
            KVStorePrivilegeLabel.EVOLVE_TABLE,
            new CreatePrivilege() {
                @Override
                public TablePrivilege createPrivilege(long id, String name) {
                    return new EvolveTable(id, name);
                }
            });
        tablePrivCreateMap.put(
            KVStorePrivilegeLabel.CREATE_INDEX,
            new CreatePrivilege() {
                @Override
                public TablePrivilege createPrivilege(long id, String name) {
                    return new CreateIndex(id, name);
                }
            });
        tablePrivCreateMap.put(
            KVStorePrivilegeLabel.DROP_INDEX,
            new CreatePrivilege() {
                @Override
                public TablePrivilege createPrivilege(long id, String name) {
                    return new DropIndex(id, name);
                }
            });
    }

    private TablePrivilege(KVStorePrivilegeLabel privLabel,
                           long tableId,
                           String tableName) {
        super(privLabel);

        this.tableId = tableId;
        this.tableName = tableName;
    }

    private TablePrivilege(KVStorePrivilegeLabel privLabel,
                           long tableId) {
        this(privLabel, tableId, null /* tableName */);
    }

    /**
     * Gets a specific table privilege instance according to the specific
     * label and table information.  It is used in the case that builds a table
     * privilege instance according to user-input privilege name. In other
     * cases, it is recommend to directly get the instances via constructors
     * for efficiency.
     *
     * @param privLabel label of the privilege
     * @param tableId table id
     * @param tableName table name
     * @return table privilege instance specified by the label
     */
    public static TablePrivilege get(KVStorePrivilegeLabel privLabel,
                                     long tableId,
                                     String tableName) {
        if (privLabel.getType() != PrivilegeType.TABLE) {
            throw new IllegalArgumentException(
                "Could not obtain a table privilege with a non-table " +
                "privilege label " + privLabel);
        }

        final CreatePrivilege creator = tablePrivCreateMap.get(privLabel);
        if (creator == null) {
            throw new IllegalArgumentException(
                "Could not find a table privilege with label of " + privLabel);
        }
        return creator.createPrivilege(tableId, tableName);
    }

    /**
     * Return all table privileges on table with given name and id.
     */
    public static Set<TablePrivilege>
        getAllTablePrivileges(long tableId, String tableName) {

        final Set<TablePrivilege> tablePrivs = new HashSet<TablePrivilege>();
        for (KVStorePrivilegeLabel privLabel : tablePrivCreateMap.keySet()) {
            tablePrivs.add(get(privLabel, tableId, tableName));
        }
        return tablePrivs;
    }

    public long getTableId() {
        return tableId;
    }

    @Override
    public boolean equals(Object other) {
        if (!super.equals(other)) {
            return false;
        }
        final TablePrivilege otherTablePriv = (TablePrivilege) other;
        return tableId == otherTablePriv.tableId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 17 * prime + super.hashCode();
        result = result * prime + (int) (tableId ^ (tableId >>> 32));
        return result;
    }

    /**
     * A table privilege appears in the form of PRIV_NAME(TABLE_NAME)
     */
    @Override
    public String toString() {
        return String.format("%s(%s)", getLabel(), tableName);
    }

    public final static class ReadTable extends TablePrivilege {

        private static final long serialVersionUID = 1L;

        private static final KVStorePrivilege[] implyingPrivs =
            new KVStorePrivilege[] { SystemPrivilege.READ_ANY,
                                     SystemPrivilege.READ_ANY_TABLE };

        public ReadTable(long tableId, String tableName) {
            super(KVStorePrivilegeLabel.READ_TABLE, tableId, tableName);
        }

        public ReadTable(long tableId) {
            super(KVStorePrivilegeLabel.READ_TABLE, tableId);
        }

        @Override
        public KVStorePrivilege[] implyingPrivileges() {
            return implyingPrivs;
        }
    }

    public final static class InsertTable extends TablePrivilege {

        private static final long serialVersionUID = 1L;

        private static final KVStorePrivilege[] implyingPrivs =
            new KVStorePrivilege[] { SystemPrivilege.WRITE_ANY,
                                     SystemPrivilege.INSERT_ANY_TABLE };

        public InsertTable(long tableId, String tableName) {
            super(KVStorePrivilegeLabel.INSERT_TABLE, tableId, tableName);
        }

        public InsertTable(long tableId) {
            super(KVStorePrivilegeLabel.INSERT_TABLE, tableId);
        }

        @Override
        public KVStorePrivilege[] implyingPrivileges() {
            return implyingPrivs;
        }
    }

    public final static class DeleteTable extends TablePrivilege {

        private static final long serialVersionUID = 1L;

        private static final KVStorePrivilege[] implyingPrivs =
            new KVStorePrivilege[] { SystemPrivilege.WRITE_ANY,
                                     SystemPrivilege.DELETE_ANY_TABLE };

        public DeleteTable(long tableId, String tableName) {
            super(KVStorePrivilegeLabel.DELETE_TABLE, tableId, tableName);
        }

        public DeleteTable(long tableId) {
            super(KVStorePrivilegeLabel.DELETE_TABLE, tableId);
        }

        @Override
        public KVStorePrivilege[] implyingPrivileges() {
            return implyingPrivs;
        }
    }

    public final static class EvolveTable extends TablePrivilege {

        private static final long serialVersionUID = 1L;

        private static final KVStorePrivilege[] implyingPrivs =
            new KVStorePrivilege[] { SystemPrivilege.SYSDBA,
                                     SystemPrivilege.EVOLVE_ANY_TABLE };

        public EvolveTable(long tableId, String tableName) {
            super(KVStorePrivilegeLabel.EVOLVE_TABLE, tableId, tableName);
        }

        public EvolveTable(long tableId) {
            super(KVStorePrivilegeLabel.EVOLVE_TABLE, tableId);
        }

        @Override
        public KVStorePrivilege[] implyingPrivileges() {
            return implyingPrivs;
        }
    }

    public final static class CreateIndex extends TablePrivilege {

        private static final long serialVersionUID = 1L;

        private static final KVStorePrivilege[] implyingPrivs =
            new KVStorePrivilege[] { SystemPrivilege.SYSDBA,
                                     SystemPrivilege.CREATE_ANY_INDEX };

        public CreateIndex(long tableId, String tableName) {
            super(KVStorePrivilegeLabel.CREATE_INDEX, tableId, tableName);
        }

        public CreateIndex(long tableId) {
            super(KVStorePrivilegeLabel.CREATE_INDEX, tableId);
        }

        @Override
        public KVStorePrivilege[] implyingPrivileges() {
            return implyingPrivs;
        }
    }

    public final static class DropIndex extends TablePrivilege {

        private static final long serialVersionUID = 1L;

        private static final KVStorePrivilege[] implyingPrivs =
            new KVStorePrivilege[] { SystemPrivilege.SYSDBA,
                                     SystemPrivilege.DROP_ANY_INDEX };

        public DropIndex(long tableId, String tableName){
            super(KVStorePrivilegeLabel.DROP_INDEX, tableId, tableName);
        }

        public DropIndex(long tableId) {
            super(KVStorePrivilegeLabel.DROP_INDEX, tableId);
        }

        @Override
        public KVStorePrivilege[] implyingPrivileges() {
            return implyingPrivs;
        }
    }
}
