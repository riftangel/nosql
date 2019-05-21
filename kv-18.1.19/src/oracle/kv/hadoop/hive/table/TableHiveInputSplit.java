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

package oracle.kv.hadoop.hive.table;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import oracle.kv.PasswordCredentials;
import oracle.kv.hadoop.table.TableInputSplit;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

/**
 * Concrete implementation of the InputSplit interface required by version 1
 * of MapReduce to support Hive queries. A RecordReader will take instances
 * of this class, where each such instance corresponds to data stored in
 * an Oracle NoSQL Database store via the Table API, and use those instances
 * to retrieve that data when performing a given Hive query against the
 * store's data.
 * <p>
 * Note that the Hive infrastructure requires that even though the data
 * associated with instances of this class resides in a table in an Oracle
 * NoSQL Database store rather than an HDFS file, this class still must
 * subclass FileSplit. As a result, a Hadoop HDFS Path must be specified
 * for this class.
 * <p>
 * Also note that although this InputSplit is based on version 1 of MapReduce
 * (as requied by the Hive infrastructure), it wraps and delegates to a YARN
 * based (MapReduce version 2) InputSplit. This is done because the InputSplit
 * class Oracle NoSQL Database provides to support Hadoop integration is YARN
 * based, and this class wishes to exploit and reuse the functionality already
 * provided by the YARN based InputSplit class.
 */
public class TableHiveInputSplit extends FileSplit {

    private final TableInputSplit v2Split;

    private static final String[] EMPTY_STRING_ARRAY = new String[] { };

    public TableHiveInputSplit() {
        super((Path) null, 0, 0, EMPTY_STRING_ARRAY);
        this.v2Split = new TableInputSplit();
    }

    public TableHiveInputSplit(Path filePath, TableInputSplit v2Split) {
        super(filePath, 0, 0, EMPTY_STRING_ARRAY);
        this.v2Split = v2Split;
    }

    /**
     * Returns the HDFS Path associated with this split.
     *
     * @return the HDFS Path associated with this split
     */
    @Override
    public Path getPath() {
        return super.getPath();
    }

    /**
     * Get the size of the split, so that the input splits can be sorted by
     * size.
     *
     * @return the number of bytes in the split
     */
    @Override
    public long getLength() {

        return v2Split.getLength();
    }

    /**
     * Get the list of nodes by name where the data for the split would be
     * local.  The locations do not need to be serialized.
     *
     * @return a new array of the node nodes.
     * @throws IOException
     */
    @Override
    public String[] getLocations() throws IOException {

        return v2Split.getLocations();
    }

    /**
     * Serialize the fields of this object to <code>out</code>.
     *
     * @param out <code>DataOuput</code> to serialize this object into.
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        v2Split.write(out);
    }

    /**
     * Deserialize the fields of this object from <code>in</code>.
     *
     * <p>For efficiency, implementations should attempt to re-use storage in
     * the existing object where possible.</p>
     *
     * @param in <code>DataInput</code> to deseriablize this object from.
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        v2Split.readFields(in);
    }

    /*
     * A well-defined equals, hashCode, and toString method must be provided
     * so that instances of this class can be compared, uniquely identified,
     * and stored in collections.
     */

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TableHiveInputSplit)) {
            return false;
        }

        if (obj == this) {
            return true;
        }

        final TableHiveInputSplit obj1 = this;
        final TableHiveInputSplit obj2 = (TableHiveInputSplit) obj;

        final Path path1 = obj1.getPath();
        final Path path2 = obj2.getPath();

        if (path1 != null) {
            if (!path1.equals(path2)) {
                return false;
            }
        } else {
            if (path2 != null) {
                return false;
            }
        }
        return obj1.v2Split.equals(obj2.v2Split);
    }

    @Override
    public int hashCode() {

        int hc = 0;
        final Path filePath = getPath();
        if (filePath != null) {
            hc = filePath.hashCode();
        }

        return hc + v2Split.hashCode();
    }

    @Override
    public String toString() {
        if (v2Split == null) {
            return super.toString();
        }
        final StringBuilder buf =
            new StringBuilder(this.getClass().getSimpleName());
        buf.append(": [path=");
        buf.append(getPath());
        buf.append("], ");
        buf.append(v2Split.toString());

        return buf.toString();
    }

    public String getKVStoreName() {
        return v2Split.getKVStoreName();
    }

    public String[] getKVHelperHosts() {
        return v2Split.getKVHelperHosts();
    }

    public String getTableName() {
        return v2Split.getTableName();
    }

    /**
     * Returns the version 2 split. This method is called by the method
     * <code>TableHiveInputFormat.getRecordReader</code>; which uses the
     * version 2 split returned by this method to create the version 2
     * <code>RecordReader</code> that will be encapsulated by the version 1
     * <code>RecordReader</code> used in Hive queries.
     */
    TableInputSplit getV2Split() {
        return v2Split;
    }

    /**
     * Returns a <code>List</code> whose elements are <code>Set</code>s of
     * partitions; whose union is the set of all partitions in the store.
     */
    List<java.util.Set<Integer>> getPartitionSets() {
        return v2Split.getPartitionSets();
    }

    public int getQueryBy() {
        return v2Split.getQueryBy();
    }

    public String getWhereClause() {
        return v2Split.getWhereClause();
    }

    public String getSecurityLogin() {
        return v2Split.getSecurityLogin();
    }

    public PasswordCredentials getSecurityCredentials() {
        return v2Split.getSecurityCredentials();
    }

    public String getSecurityTrust() {
        return v2Split.getSecurityTrust();
    }
}
