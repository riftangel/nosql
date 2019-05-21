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

package oracle.kv.table;

import oracle.kv.KVStore;
import oracle.kv.Version;

/**
 * Row is a specialization of RecordValue to represent a single record,
 * or row, in a table.  It is a central object used in most table operations
 * defined in {@link TableAPI}.
 *<p>
 * Row objects are constructed explicitly using
 * {@link Table#createRow createRow} or implicitly when returned from table
 * access operations.
 *
 * @since 3.0
 */
public interface Row extends RecordValue {

    /**
     * Return the Table associated with this row.
     *
     * @return the Table
     */
    Table getTable();

    /**
     * Returns the Version for the row.  The description of {@link Version}
     * in its class description refers to its use in the {@link KVStore}
     * interface.  In {@link TableAPI} it it used as a return value for the
     * various put methods as well as {@link TableAPI#putIfVersion} and
     * {@link TableAPI#deleteIfVersion} to perform conditional updates to
     * allow an application to ensure that an update is occurring on the
     * desired version of a row.
     *
     * @return the Version object if it has been initialized, null
     * otherwise.  The Version will only be set if the row was retrieved from
     * the store.
     */
    Version getVersion();

    /**
     * Creates a PrimaryKey from this Row.  The non-key fields are
     * removed.
     *
     * @return the PrimaryKey
     */
    PrimaryKey createPrimaryKey();

    /**
     * Returns the version of the table used to create this row if it has been
     * deserialized from a get operation.  If the row has been created and
     * never been serialized the version returned is 0.  New Table versions
     * are created when a table is schema evolved.  Tables start out with
     * version 1 and it increments with each change.  This method can be used
     * by applications to help handle version changes.
     */
    int getTableVersion();

    /**
     * Equality comparison for Row instances is based on equality of the
     * individual field values and ignores the included {@link Version}, if
     * present.
     *
     * @return true if the objects are equal
     */
    @Override
    public boolean equals(Object other);

    /**
     * Returns a deep copy of this object.
     *
     * @return a deep copy of this object
     */
    @Override
    public Row clone();

    /**
     * Sets a time to live (TTL) value for the row to be used when the row
     * is inserted into a store. If not set, or if the value is null, the table
     * default value will be used. If no expiration time is desired, a
     * TimeToLive instance of duration 0 should be used. The constant, {@link
     * TimeToLive#DO_NOT_EXPIRE}, exists as a convenience for that purpose.
     * <p>
     * It is recommended that the TimeToLive be constructed using
     * {@link TimeToLive#ofDays} to use less space in the store. If hours are
     * needed then {@link TimeToLive#ofHours} can be used at the expense of
     * additional storage used.
     *
     * @param ttl the value to use, may be null
     *
     * @throws IllegalArgumentException if the TimeToLive is negative
     *
     * @since 4.0
     */
    public void setTTL(TimeToLive ttl);

    /**
     * Returns the time to live (TTL) value for this row or null if it
     * has not been set by a call to {@link #setTTL}. If this is null
     * it means that the table default will be used on a put operation.
     * The TTL property of a Row is used only on input. The expiration time
     * of a Row is available on output, or after a put operation using
     * {@link #getExpirationTime}.
     *
     * @return the TimeToLive for the row or null if not set
     *
     * @since 4.0
     */
    public TimeToLive getTTL();

    /**
     * Returns the expiration time of the row. A zero value indicates that the
     * row does not expire. If the row does not have a valid expiration time,
     * an exception is thrown. The expiration time is valid if this row
     * was returned by a get (e.g. {@link TableAPI#get}) or table iterator
     * (e.g. {@link TableAPI#tableIterator}) call. It will also be valid after
     * a successful put of the row (e.g. {@link TableAPI#put}.
     *
     * @return the expiration time in milliseconds since January 1, 1970,
     * or zero if the record never expires
     *
     * @throws IllegalStateException if called on a Row that has an undefined
     * expiration time, which is a Row that has not been put or retrieved via
     * a get or iterator operation
     *
     * @since 4.0
     */
   public long getExpirationTime();
}
