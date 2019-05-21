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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.api.table.IndexImpl.AnnotatedField;
import oracle.kv.impl.api.table.IndexImpl.IndexStatus;
import oracle.kv.impl.api.table.TableImpl.TableStatus;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.metadata.MetadataKey;
import oracle.kv.impl.security.ResourceOwner;
import oracle.kv.table.FieldDef;
import oracle.kv.table.Index;
import oracle.kv.table.Table;
import oracle.kv.table.TimeToLive;

/**
 * This is internal implementation that wraps Table and Index metadata
 * operations such as table/index creation, etc.
 *
 * TableMetadata stores tables in a tree.  The top level is a map from
 * name (String) to Table and contains top-level tables only.  Each top-level
 * table may or may not contain child tables.  When this class is serialized
 * the entire tree of Table objects, along with their contained Index objects,
 * is serialized.
 *
 * When a table lookup is performed it must be done top-down.  First the lookup
 * walks to the "root" of the metadata structure, which is the map contained in
 * this instance.  For top-level tables the lookup is a simple get.  For child
 * tables the code unwinds down the stack of parents to get the child.
 *
 * When a table is first inserted into TableMetadata it is assigned a
 * numeric id. Ids are allocated from the keyId member.
 *
 * Note that this implementation is not synchronized. If multiple threads
 * access a table metadata instance concurrently, and at least one of the
 * threads modifies the table metadata structurally, it must be synchronized
 * externally.
 */
public class TableMetadata implements TableMetadataHelper,
                                      Metadata<TableChangeList>,
                                      Serializable {

    private static final long serialVersionUID = 1L;

    private final Map<String, Table> tables =
        new TreeMap<String, Table>(FieldComparator.instance);

    private int seqNum = Metadata.EMPTY_SEQUENCE_NUMBER;

    private long keyId = INITIAL_KEY_ID;

    public static final int INITIAL_KEY_ID = 1;

    /*
     * Record of changes to the metadata. If null no changes will be kept.
     */
    private final List<TableChange> changeHistory;

    /**
     * Construct a table metadata object. If keepChanges is true any changes
     * made to are recorded and can be accessed through the getMetadataInfo()
     * interface.
     *
     * @param keepChanges
     */
    public TableMetadata(boolean keepChanges) {
        changeHistory = keepChanges ? new LinkedList<TableChange>() : null;
    }

    public TableImpl addTable(String namespace,
                              String name,
                              String parentName,
                              List<String> primaryKey,
                              List<Integer> primaryKeySizes,
                              List<String> shardKey,
                              FieldMap fieldMap,
                              TimeToLive ttl,
                              TableLimits limits,
                              boolean r2compat,
                              int schemaId,
                              String description,
                              ResourceOwner owner) {

        return addTable(namespace, name, parentName,
                        primaryKey, primaryKeySizes, shardKey, fieldMap, ttl,
                        limits, r2compat, schemaId, description, owner, false);
    }

    public TableImpl addTable(String namespace,
                              String name,
                              String parentName,
                              List<String> primaryKey,
                              List<Integer> primaryKeySizes,
                              List<String> shardKey,
                              FieldMap fieldMap,
                              TimeToLive ttl,
                              TableLimits limits,
                              boolean r2compat,
                              int schemaId,
                              String description,
                              ResourceOwner owner,
                              boolean sysTable) {
        final TableImpl table = insertTable(namespace, name, parentName,
                                            primaryKey, primaryKeySizes,
                                            shardKey,
                                            fieldMap,
                                            ttl, limits,
                                            r2compat, schemaId,
                                            description,
                                            owner, sysTable);
        addTableChange(table);
        return table;
    }

    private void addTableChange(TableImpl table) {
        bumpSeqNum();
        if (changeHistory != null) {
            changeHistory.add(new AddTable(table, seqNum));
        }
    }

    /**
     * Drops a table. If the table has indexes or child tables an
     * IllegalArgumentException is thrown. If markForDelete is true the table's
     * status is set to DELETING and is not removed.
     *
     * @param tableName the table name
     * @param markForDelete if true mark the table as DELETING
     */
    public void dropTable(String namespace,
                          String tableName,
                          boolean markForDelete) {
        removeTable(namespace, tableName, markForDelete);

        bumpSeqNum();
        if (changeHistory != null) {
            changeHistory.add(new DropTable(namespace, tableName,
                                            markForDelete, seqNum));
        }
    }

    /**
     * Evolves a table using new fields but only if it's not already been done
     * and if the supplied version indicates that the evolution started with
     * the latest table version.
     *
     * If this operation was retried the evolution may have already been
     * applied.  Check field equality and if equal, consider the evolution
     * done.
     *
     * @return true if the evolution happens, false otherwise
     *
     * @throws IllegalCommandException if an attempt is made to evolve a version
     * other than the latest table version
     */
    public boolean evolveTable(TableImpl table, int tableVersion,
                               FieldMap fieldMap, TimeToLive ttl,
                               String description,
                               boolean systemTable) {

        if (table.isSystemTable() != systemTable) {
            if (systemTable) {
                throw new IllegalCommandException
                    ("Table " + makeQualifiedName(null, null, table.getName()) +
                     " is not system table");
            }
            throw new IllegalCommandException
                ("Cannot evolve table " +
                 makeQualifiedName(null, null, table.getName()));
        }
        if (fieldMap.equals(table.getFieldMap())) {
            if (TableImpl.compareTTL(ttl, table.getDefaultTTL())) {
                return false;
            }
        }

        if (tableVersion != table.numTableVersions()) {
            throw new IllegalCommandException
                ("Table evolution must be performed on the latest version, " +
                 "version supplied is " + tableVersion + ", latest is " +
                 table.numTableVersions());
        }

        table.evolve(fieldMap, ttl, description);
        bumpSeqNum();
        if (changeHistory != null) {
            changeHistory.add(new EvolveTable(table, seqNum));
        }
        return true;
    }

    /**
     * Sets the table limits.
     */
    public void setLimits(TableImpl table, TableLimits newLimits) {
        table.setTableLimits(newLimits);
        bumpSeqNum();
        if (changeHistory != null) {
            changeHistory.add(new TableLimit(table, seqNum));
        }
    }
    
    public void addIndex(String namespace,
                         String indexName,
                         String tableName,
                         List<String> fields,
                         List<FieldDef.Type> types,
                         String description) {

        insertIndex(namespace, indexName, tableName,
                    fields, types, description);
        bumpSeqNum();
        if (changeHistory != null) {
            changeHistory.add(new AddIndex(namespace, indexName, tableName,
                                           fields, types, description, seqNum));
        }
    }

    public void addTextIndex(String namespace,
                             String indexName,
                             String tableName,
                             List<AnnotatedField> fields,
                             Map<String, String> properties,
                             String description) {
        final List<String> fieldNames = new ArrayList<String>(fields.size());
        final Map<String, String> annotations =
                new HashMap<String, String>(fields.size());
        IndexImpl.populateMapFromAnnotatedFields(fields,
                                                 fieldNames,
                                                 annotations);

        insertTextIndex(namespace, indexName, tableName, fieldNames,
                        annotations, properties, description);

        bumpSeqNum();
        if (changeHistory != null) {
            changeHistory.add(new AddIndex(namespace, indexName, tableName,
                                           fieldNames, annotations, properties,
                                           description, seqNum));
        }
    }

    public void dropIndex(String namespace,
                          String indexName,
                          String tableName) {
        if (removeIndex(namespace, indexName, tableName)) {
            bumpSeqNum();
            if (changeHistory != null) {
                changeHistory.add(new DropIndex(namespace,
                                                indexName,
                                                tableName,
                                                seqNum));
            }
        }
    }

    public boolean updateIndexStatus(String namespace,
                                     String indexName,
                                     String tableName,
                                     IndexStatus status) {
        final IndexImpl index = changeIndexStatus(namespace, indexName,
                                                  tableName, status);
        if (index != null) {
            bumpSeqNum();
            if (changeHistory != null) {
                changeHistory.add(new UpdateIndexStatus(index, seqNum));
            }
            return true;
        }
        return false;
    }

    /*
     * Add the table described.  It must not exist or an exception is thrown.
     * If it has a parent the parent must exist.
     */
    TableImpl insertTable(String namespace,
                          String name,
                          String parentName,
                          List<String> primaryKey,
                          List<Integer> primaryKeySizes,
                          List<String> shardKey,
                          FieldMap fields,
                          TimeToLive ttl,
                          TableLimits limits,
                          boolean r2compat,
                          int schemaId,
                          String description,
                          ResourceOwner owner,
                          boolean sysTable) {

        TableImpl table = null;

        if (r2compat) {
            verifyIdNotUsed(name);
        }

        if (parentName != null) {
            final TableImpl parent = getTable(namespace,
                                              parentName,
                                              true);
            if (parent.childTableExists(name)) {
                throw new IllegalArgumentException
                    ("Cannot create table.  Table exists: " +
                     makeQualifiedName(namespace, name, parentName));
            }

            if (parent.isSystemTable() != sysTable) {
                throw new IllegalArgumentException
                    ("Cannot create table " + name + ". It must" +
                     ((sysTable) ? " not" : "") + " be a system table, " +
                     "because its parent is " +
                     ((sysTable) ? "" : " not") + " a system table");
            }
            parent.checkChildLimit(name);
            table = TableImpl.createTable(namespace, name, parent,
                                          primaryKey, primaryKeySizes, shardKey,
                                          fields, r2compat, schemaId,
                                          description, true, owner, ttl, limits,
                                          sysTable);
            table.setId(allocateId());
            parent.getMutableChildTables().put(name, table);
        } else {
            final String namespaceName = makeNamespaceName(namespace, name);
            if (tables.containsKey(namespaceName)) {
                throw new IllegalArgumentException
                    ("Cannot create table.  Table exists: " + namespaceName);
            }
            table = TableImpl.createTable(namespace, name, null,
                                          primaryKey, primaryKeySizes, shardKey,
                                          fields, r2compat, schemaId,
                                          description, true, owner, ttl, limits,
                                          sysTable);
            table.setId(allocateId());
            tables.put(namespaceName, table);
        }
        return table;

    }

    /*
     * Evolve the table described.  It must not exist or an exception is thrown.
     */
    TableImpl evolveTable(String namespace, String tableName,
                          FieldMap fields, TimeToLive ttl, String description) {
        final TableImpl table = getTable(namespace, tableName, true);
        table.evolve(fields, ttl, description);
        return table;
    }

    /**
     * Removes a table. If the table has indexes or child tables an
     * IllegalArgumentException is thrown. If markForDelete is true the table's
     * status is set to DELETING and is not removed.
     *
     * @param tableName the table name
     * @param markForDelete if true mark the table as DELETING
     *
     * @return the removed table
     */
    Table removeTable(String namespace,
                      String tableName,
                      boolean markForDelete) {
        final TableImpl table = checkForRemove(namespace, tableName,
                                               false /* indexes allowed */);
        if (markForDelete) {
            table.setStatus(TableStatus.DELETING);
            return table;
        }
        final Table parent = table.getParent();

        if (parent != null) {
            ((TableImpl)parent).getMutableChildTables().remove(table.getName());
        } else {
            /* a top-level table */
            tables.remove(makeNamespaceName(namespace, table.getName()));
        }
        return table;
    }

    /**
     * Checks to see if it is ok to remove this table. Returns the table
     * instance if the table can be removed. Throws IllegalCommandException
     * if the table does not exist, it is a system table, or if the table is
     * referenced by child tables. If allowIndexes is false an
     * IllegalCommandException is thrown if the table contains indexes.
     */
    public TableImpl checkForRemove(String namespace,
                                    String tableName,
                                    boolean indexesAllowed) {
        final TableImpl table = getTable(namespace, tableName, true);
        final String qname = makeQualifiedName(namespace, null, tableName);
        /* getTable(..., true) can return null?? */
        if (table == null) {
            throw new IllegalCommandException
                ("Table " + qname + " does not exist");
        }

        if (table.isSystemTable()) {
            throw new IllegalCommandException
                ("Cannot remove system table: " + qname);
        }

        if (!table.getChildTables().isEmpty()) {
            throw new IllegalCommandException
                ("Cannot remove table " + qname +
                 ", it is still referenced by " +
                 "child tables");
        }

        if (!indexesAllowed && !table.getIndexes().isEmpty()) {
            throw new IllegalCommandException
                    ("Cannot remove table " + qname +
                     ", it still contains indexes");
        }

        return table;
    }

    IndexImpl insertIndex(String namespace,
                          String indexName,
                          String tableName,
                          List<String> fields,
                          List<FieldDef.Type> types,
                          String description) {
        final TableImpl table = getTable(namespace, tableName, true);
        if (table.isSystemTable()) {
            throw new IllegalCommandException
                ("Cannot add index " + indexName + " on system table: " +
                 makeQualifiedName(namespace, null, tableName));
        }
        if (table.isDeleting()) {
            throw new IllegalCommandException
                ("Cannot add index " + indexName + " on table: " +
                 makeQualifiedName(namespace, null, tableName) +
                 ", it is being removed");
        }
        if (table.getIndex(indexName) != null) {
            throw new IllegalArgumentException
                ("Index exists: " + indexName + " on table: " +
                 makeQualifiedName(namespace, null, tableName));
        }
        final IndexImpl index = new IndexImpl(indexName, table, fields,
                                              types, description);
        index.setStatus(IndexStatus.POPULATING);
        table.addIndex(index);
        return index;
    }

    boolean removeIndex(String namespace, String indexName, String tableName) {
        final TableImpl table = getTable(namespace, tableName, true);
        if (table.isSystemTable()) {
            throw new IllegalCommandException
                ("Cannot remove index " + indexName + " on system table: " +
                 makeQualifiedName(namespace, null, tableName));
        }

        final Index index = table.getIndex(indexName);
        if (index == null) {
            throw new IllegalArgumentException
                ("Index does not exist: " + indexName + " on table: " +
                 makeQualifiedName(namespace, null, tableName));
        }
        table.removeIndex(indexName);

        return true;
    }

    /*
     * Update the index status to the desired status.  If a change was made
     * return the Index, if the status is unchanged return null, allowing
     * this operation to be an idempotent no-op.
     */
    IndexImpl changeIndexStatus(String namespace,
                                String indexName,
                                String tableName,
                                IndexStatus status) {
        final TableImpl table = getTable(namespace, tableName, true);

        final IndexImpl index = (IndexImpl) table.getIndex(indexName);
        if (index == null) {
            throw new IllegalArgumentException
                ("Index does not exist: " + indexName + " on table: " +
                 makeQualifiedName(namespace, null, tableName));
        }
        if (index.getStatus() == status) {
            return null;
        }
        index.setStatus(status);
        return index;
    }

    IndexImpl insertTextIndex(String namespace,
                              String indexName,
                              String tableName,
                              List<String> fields,
                              Map<String, String> annotations,
                              Map<String, String> properties,
                              String description) {
        final TableImpl table = getTable(namespace, tableName, true);
        if (table.isSystemTable()) {
            throw new IllegalCommandException
                ("Cannot add text index " + indexName + " on table: " +
                 makeQualifiedName(namespace, null, tableName));
        }

        if (table.getTextIndex(indexName) != null) {
            throw new IllegalArgumentException
                ("Text Index exists: " + indexName + " on table: " +
                 makeQualifiedName(namespace, null, tableName));
        }
        final IndexImpl index = new IndexImpl(indexName, table, fields, null,
                                              annotations, properties,
                                              description);
        index.setStatus(IndexStatus.POPULATING);
        table.addIndex(index);
        return index;
    }

    /**
     * Return the named table.
     *
     * @param tableName is a "." separated path to the table name, e.g.
     * parent.child.target.  For top-level tables it is a single
     * component
     */
    public TableImpl getTable(String namespace,
                              String tableName,
                              boolean mustExist) {

        final String[] path = TableImpl.parseFullName(tableName);
        return getTable(namespace, path, mustExist);
    }

    /**
     * For compatibility only (used in KVProxy). Do not use internally.
     */
    public TableImpl getTable(String tableName) {
        return getTable(null, tableName);
    }

    public TableImpl getTable(String namespace,
                              String tableName,
                              String parentName) {
        final StringBuilder sb = new StringBuilder();
        if (parentName != null) {
            sb.append(parentName).append(TableImpl.SEPARATOR);
        }
        if (tableName != null) {
            sb.append(tableName);
        }
        return getTable(namespace, sb.toString(), false);
    }

    /**
     * @see TableMetadataHelper
     */
    @Override
    public TableImpl getTable(String namespace, String tableName) {
        return getTable(namespace, tableName, false);
    }

    /**
     * @see TableMetadataHelper
     */
    @Override
    public TableImpl getTable(String namespace, String[] tablePath) {
        return getTable(namespace, tablePath, false);
    }

    public TableImpl getTable(String namespace, String[] path,
                              boolean mustExist) {

        if (path == null || path.length == 0) {
            return null;      // TODO ??? thow exception if mustExist is true??
        }

        final String firstKey = makeNamespaceName(namespace, path[0]);
        TableImpl targetTable =  (TableImpl) tables.get(firstKey);

        if (path.length > 1) {
            for (int i = 1; i < path.length && targetTable != null; i++) {
                try {
                    targetTable = getChildTable(path[i], targetTable);
                } catch (IllegalArgumentException ignored) {
                    targetTable = null;
                    break;
                }
            }
        }

        if (targetTable == null && mustExist) {
            throw new IllegalArgumentException
                ("Table: " + makeQualifiedName(namespace, path) +
                 " does not exist in " + this);
        }
        return targetTable;
    }

    public boolean tableExists(String namespace,
                               String tableName,
                               String parentName) {
        return (getTable(namespace, tableName, parentName) != null);
    }

    /**
     * Returns the specified Index or null if it, or its containing table
     * does not exist.
     */
    public Index getIndex(String namespace,
                          String tableName,
                          String indexName) {
        final TableImpl table = getTable(namespace, tableName);
        if (table != null) {
            return table.getIndex(indexName);
        }
        return null;
    }

    public Index getTextIndex(String namespace,
                              String tableName,
                              String indexName) {
        final TableImpl table = getTable(namespace, tableName);
        if (table != null) {
            return table.getTextIndex(indexName);
        }
        return null;
    }

    private static String makeQualifiedName(String namespace,
                                            String[] pathName) {
        final StringBuilder sb = new StringBuilder();
        for (String step : pathName) {
            sb.append(step);
        }
        return makeQualifiedName(namespace, null, sb.toString());
    }

    /**
     * Create a string that uniquely identifies a table for use in error
     * messages.  Format is [parentName][.]name
     *
     * Public to be available to TableCommand
     */
    public static String makeQualifiedName(String namespace,
                                           String name,
                                           String parentName) {
        final StringBuilder sb = new StringBuilder();
        if (parentName != null) {
            if (namespace != null) {
                sb.append(namespace).append(":");
            }
            sb.append(parentName);
            if (name != null) {
                sb.append(TableImpl.SEPARATOR);
            }
        }
        if (name != null) {
            sb.append(name);
        }
        return sb.toString();
    }

    /**
     * Return the named child table.
     */
    public TableImpl getChildTable(String tableName, Table parent) {
        return (TableImpl) parent.getChildTable(tableName);
    }

    /*
     * Get a table from TableMetadataKey.  This is used by RepNodes to return
     * tables requested by clients.  In this path it's necessary to filter out
     * created, but not-yet-populated indexes.
     */
    public TableImpl getTable(TableMetadataKey mdKey) {
        final String tableName = stripNamespace(mdKey.getTableName());
        final String namespace = getNamespace(mdKey.getTableName());
        TableImpl table = getTable(namespace, tableName);
        if (table != null && table.getIndexes().size() > 0) {
            /* clone, filter */
            table = table.clone();

            for (final Iterator<Map.Entry<String, Index>> it =
                    table.getMutableIndexes().entrySet().iterator();
                it.hasNext();) {
                final Map.Entry<String, Index> entry = it.next();
                if (!((IndexImpl)entry.getValue()).getStatus().isReady()) {
                    it.remove();
                }
            }
        }
        return table;
    }

    /**
     * Return all top-level tables.
     */
    public Map<String, Table> getTables() {
        return tables;
    }

    /* public for access from ShowCommand (for now) */
    public Map<String, Table> getTables(String namespace) {

        if (namespace == null) {
            return tables;
        }

        final Map<String, Table> nsTables =
            new TreeMap<String, Table>(FieldComparator.instance);

        final String prefix = namespace + ":";

        for (Map.Entry<String, Table> entry : tables.entrySet()) {
            if (entry.getKey().toLowerCase().startsWith(prefix.toLowerCase())) {
                nsTables.put(stripNamespace(entry.getKey()), entry.getValue());
            }
        }
        return nsTables;
    }

    /**
     * Returns a sorted list of all tables. In this method parent and child
     * tables are listed independently, in alphabetical order, which means
     * parents first.
     */
    public List<String> listTables(String namespace) {
        final List<String> list = listTables(namespace, true);
        Collections.sort(list);
        return list;
    }

    /**
     * Adds all table names, parent and child to the list.  Child tables are
     * listed before parent tables because the iteration is depth-first.  This
     * simplifies code that does things like removing all tables or code that
     * depends on this order.  If other orders are desirable a parameter or
     * other method could be added to affect the iteration.
     *
     * @param namespace if non-null, only tables in the specified namespace are
     * returned and the names are full names (no namespace prefix). If null,
     * only tables not in a namespace are returned, unless the allTables
     * parameter is true.
     *
     * @param allTables if true, all tables are returned unless the namespace
     * parameter is non-null. In this case the names have the namespace prefix
     * added. The format is [namespace:]full-table-name.
     */
    public List<String> listTables(final String namespace,
                                   final boolean allTables) {
        final List<String> tableList = new ArrayList<String>();
        iterateTables(new TableMetadataIteratorCallback() {
                @Override
                public boolean tableCallback(Table table) {
                    if (namespace != null &&
                        namespace.equalsIgnoreCase(table.getNamespace())) {
                        tableList.add(table.getFullName());
                    } else if (namespace == null) {
                        if (allTables) {
                            tableList.add(table.getNamespaceName());
                        } else if (table.getNamespace() == null) {
                            tableList.add(table.getFullName());
                        }
                    }
                    return true;
                }
            });
        return tableList;
    }

    /**
     * Returns the number of tables in the structure, including
     * child tables.
     */
    private int numTables() {
        final int[] num = new int[1];
        iterateTables(new TableMetadataIteratorCallback() {
                @Override
                public boolean tableCallback(Table table) {
                    ++num[0];
                    return true;
                }
            });
        return num[0];
    }

    /**
     * Returns true if there are no tables defined.
     *
     * @return true if there are no tables defined
     */
    public boolean isEmpty() {
        return tables.isEmpty();
    }

    /**
     * Returns all text indexes.
     */
    public List<Index> getTextIndexes() {

        final List<Index> textIndexes = new ArrayList<Index>();

        iterateTables(new TableMetadataIteratorCallback() {
                @Override
                public boolean tableCallback(Table table) {
                    textIndexes.addAll
                        (table.getIndexes(Index.IndexType.TEXT).values());
                    return true;
                }
            });

        return textIndexes;
    }

    /**
     * Convenience method for getting all text index names.
     */
    public Set<String> getTextIndexNames() {
        final Set<String> textIndexNames = new HashSet<String>();
        for (Index ti : getTextIndexes()) {
            textIndexNames.add(ti.getName());
        }
        return textIndexNames;
    }

    private void bumpSeqNum() {
        seqNum++;
    }

    /*
     * Bump and return a new table id. Verify that the string version of
     * the id doesn't already exist as a table name.  If so, bump again.
     */
    private long allocateId() {
        while (true) {
            ++keyId;
            try {
                verifyIdNotUsed(TableImpl.createIdString(keyId));
                return keyId;
            } catch (IllegalArgumentException iae) {
                /* try the next id */
            }
        }
    }

    /* -- From Metadata -- */

    @Override
    public MetadataType getType() {
        return MetadataType.TABLE;
    }

    @Override
    public int getSequenceNumber() {
        return seqNum;
    }

    @Override
    public TableChangeList getChangeInfo(int startSeqNum) {
        return new TableChangeList(seqNum, getChanges(startSeqNum));
    }

    @Override
    public TableMetadata pruneChanges(int limitSeqNum, int maxChanges) {
        final int firstChangeSeqNum = getFirstChangeSeqNum();
        if (firstChangeSeqNum == -1) {
            /* No changes to prune. */
            return this;
        }

        final int firstRetainedChangeSeqNum =
                            Math.min(getSequenceNumber() - maxChanges + 1,
                                     limitSeqNum);
        if (firstRetainedChangeSeqNum <= firstChangeSeqNum) {
            /* Nothing to prune. */
            return this;
        }

        for (final Iterator<TableChange> itr = changeHistory.iterator();
             itr.hasNext() &&
             (itr.next().getSequenceNumber() < firstRetainedChangeSeqNum);) {
            itr.remove();
        }
        return this;
    }

    /* Not private for unit tests */
    int getFirstChangeSeqNum() {
        return (changeHistory == null) ? -1 :
                    changeHistory.isEmpty() ?
                                -1 : changeHistory.get(0).getSequenceNumber();
    }

    /* Unit tests */
    int getChangeHistorySize() {
        return (changeHistory == null) ? 0 : changeHistory.size();
    }

    /* -- Change support methods -- */

    private List<TableChange> getChanges(int startSeqNum) {

        /* Skip if we are out of date, or don't have changes */
        if ((startSeqNum >= seqNum) ||
            (changeHistory == null) ||
            changeHistory.isEmpty()) {
            return null;
        }

        /* Also skip if they are way out of date (or not initialized) */
        if (startSeqNum < changeHistory.get(0).getSequenceNumber()) {
            return null;
        }

        List<TableChange> list = null;

        for (TableChange change : changeHistory) {
            if (change.getSequenceNumber() > startSeqNum) {
                if (list == null) {
                    list = new LinkedList<TableChange>();
                }
                list.add(change);
            }
        }
        return list;
    }

    /**
     * Updates the metadata data from an info object. Returns true
     * if the table metadata was modified.
     *
     * @param metadataInfo info object to update from
     * @return true if the table metadata was modified
     */
    public boolean update(MetadataInfo metadataInfo) {

        if (metadataInfo instanceof TableChangeList) {
            return apply((TableChangeList)metadataInfo);
        }
        throw new IllegalArgumentException("Unknow metadata info: " +
                                           metadataInfo);
    }

    private boolean apply(TableChangeList changeList) {
        if (changeList.isEmpty()) {
            return false;
        }

        final int origSeqNum = seqNum;

        for (TableChange change : changeList) {
            if (change.getSequenceNumber() <= seqNum) {
                /* ignore older changes that will have been applied */
                continue;
            }
            if (change.getSequenceNumber() > (seqNum + 1)) {
                /*
                 * if there is an out of order change, fail. Changes should
                 * appear in a list from lower to higher. Generally it should
                 * not be possible to get here because changes are always sent
                 * relative to the current sequence number.
                 */
                break;
            }
            if (!change.apply(this)) {
                break;
            }
            seqNum = change.getSequenceNumber();
            if (changeHistory != null) {
                changeHistory.add(change);
            }
        }
        return origSeqNum != seqNum;
    }

    /**
     * Creates a copy of this TableMetadata object.
     *
     * @return the new TableMetadata instance
     */
    public TableMetadata getCopy() {
        try {
            final ByteArrayOutputStream bos = new ByteArrayOutputStream();
            final ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(this);
            oos.close();

            final ByteArrayInputStream bis =
                new ByteArrayInputStream(bos.toByteArray());
            final ObjectInputStream ois = new ObjectInputStream(bis);

            return (TableMetadata)ois.readObject();
        } catch (IOException ioe) {
            throw new IllegalStateException("Unexpected exception", ioe);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Unexpected exception", e);
        }
    }

    @Override
    public String toString() {
        return "TableMetadata[" + seqNum + ", " + tables.size() + ", " +
            ((changeHistory == null) ? "-" : changeHistory.size()) + "]";
    }

    /**
     * Compares two TableMetadata instances by comparing all tables, including
     * child tables.  This might logically be implemented as an override of
     * equals but that might mean adding hashCode() to avoid warnings and
     * that's not necessary.  If anyone ever needs a true equals() overload
     * then this can change.
     *
     * @return true if the objects have the same content, false otherwise.
     */
    public boolean compareMetadata(final TableMetadata omd) {
        final int num = numTables();
        if (num == omd.numTables()) {
            final int[] numCompared = new int[1];
                iterateTables(new TableMetadataIteratorCallback() {
                        @Override
                        public boolean tableCallback(Table table) {
                            if (!existsAndEqual((TableImpl) table, omd)) {
                                return false;
                            }
                            ++numCompared[0];
                            return true;
                        }
                    });
                return numCompared[0] == num;
        }
        return false;
    }

    /**
     * Iterates all tables and ensures that the string version of the
     * id for the table doesn't match the idString. Throws if it
     * exists.  This is called when creating a new table in r2compat mode.
     */
    private void verifyIdNotUsed(final String idString) {
        iterateTables(new TableMetadataIteratorCallback() {
                @Override
                public boolean tableCallback(Table table) {
                    final String tableId = ((TableImpl)table).getIdString();
                    if (tableId.equals(idString)) {
                        throw new IllegalArgumentException
                            ("Cannot create a table overlay with the name " +
                             idString + ", it exists as a table Id");
                    }
                    return true;
                }
            });
    }

    /**
     * Returns true if the table name exists in the TableMetadata and
     * the two tables are equal.
     */
    private static boolean existsAndEqual(TableImpl table,
                                          TableMetadata md) {
        final TableImpl otherTable = md.getTable(table.getNamespace(),
                                                 table.getFullName());
        if (otherTable != null && table.equals(otherTable)) {

            /*
             * Check child tables individually.  Table equality does not
             * consider children.
             */
            for (Table child : table.getChildTables().values()) {
                if (!existsAndEqual((TableImpl) child, md)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public static class TableMetadataKey implements MetadataKey, Serializable {
        private static final long serialVersionUID = 1L;
        private final String tableName;

        public TableMetadataKey(final String tableName) {
            this.tableName = tableName;
        }

        public TableMetadataKey(final String namespace,
                                final String tableName) {
            this.tableName = makeNamespaceName(namespace, tableName);
        }

        public String getTableName() {
            return tableName;
        }

        public MetadataKey getMetadataKey() {
            return this;
        }

        /*
         * For debugging
         */
        @Override
        public String toString() {
            return "TableMetadataKey[" +
                   (tableName != null ? tableName : "null") + "]";
        }
    }

    /**
     * Iterate over all tables, calling back to the callback for each.
     */
    public void iterateTables(TableMetadataIteratorCallback callback) {
        for (Table table : getTables().values()) {
            if (!iterateTables(table, callback)) {
                break;
            }
        }
    }

    /**
     * Implements iteration of all tables, depth-first (i.e. child tables are
     * visited before parents.
     */
    private static boolean
        iterateTables(Table table, TableMetadataIteratorCallback callback) {
        for (Table child : table.getChildTables().values()) {
            if (!iterateTables(child, callback)) {
                return false;
            }
        }
        if (!callback.tableCallback(table)) {
            return false;
        }
        return true;
    }

    public static String makeNamespaceName(String namespace, String name) {
        if (namespace == null) {
            return name;
        }
        final StringBuilder sb = new StringBuilder();
        sb.append(namespace).append(":").append(name);
        return sb.toString();
    }

    public static String stripNamespace(String name) {
        if (name == null || !name.contains(":")) {
            return name;
        }
        return name.substring(name.indexOf(":") + 1);
    }

    public static String getNamespace(String name) {
        if (name == null || !name.contains(":")) {
            return null;
        }
        return name.substring(0, name.indexOf(":"));
    }

    /**
     * An interface used for operations that need to iterate the entire tree of
     * metadata.
     */
    public interface TableMetadataIteratorCallback {

        /**
         * Returns true if the iteration should continue, false if not.
         */
        boolean tableCallback(Table t);
    }
}
