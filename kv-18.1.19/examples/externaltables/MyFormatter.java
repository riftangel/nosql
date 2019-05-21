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

package externaltables;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Vector;

import oracle.kv.KVStore;
import oracle.kv.Key;
import oracle.kv.KeyValueVersion;
import oracle.kv.Value;
import oracle.kv.exttab.Formatter;
import oracle.kv.exttab.TableFormatter;
import oracle.kv.table.FieldValue;
import oracle.kv.table.Row;

/**
 * A simple Formatter and TableFormatter implementation used by the External
 * Tables Cookbook example.
 * This class formats UserInfo records from the NoSQL Database to be
 * suitable for reading by the NOSQL_DATA Oracle Database table. The
 * {@link oracle.kv.exttab.Formatter#toOracleLoaderFormat} method accepts a
 * {@link oracle.kv.KeyValueVersion} and
 * {@link oracle.kv.exttab.TableFormatter#toOracleLoaderFormat} method accepts
 * {@link oracle.kv.table.Row}, both return a String which is formatted as a
 * list of key/values
 * (from {@link oracle.kv.exttab.Formatter#toOracleLoaderFormat} method),
 * or fields (from {@link oracle.kv.exttab.TableFormatter#toOracleLoaderFormat}
 * method) separated by delimiter "|". The return String can be interpreted
 * by the ACCESS PARAMETERS of the External Table definition.
 */
public class MyFormatter implements Formatter, TableFormatter {
    private static final String USER_OBJECT_TYPE = "user";
    private static final String INFO_PROPERTY_NAME = "info";

    /**
     * @hidden
     */
    public MyFormatter() { }

    @Override
    public String toOracleLoaderFormat(final KeyValueVersion kvv,
                                       final KVStore kvStore) {
        final Key key = kvv.getKey();
        final Value value = kvv.getValue();

        final List<String> majorPath = key.getMajorPath();
        final List<String> minorPath = key.getMinorPath();
        final String objectType = majorPath.get(0);

        if (!USER_OBJECT_TYPE.equals(objectType)) {
            throw new IllegalArgumentException("Unknown object type: " + key);
        }

        final String email = majorPath.get(1);
        final String propertyName =
            (minorPath.size() > 0) ? minorPath.get(0) : null;

        if (INFO_PROPERTY_NAME.equals(propertyName)) {
            final ByteArrayInputStream bais =
                new ByteArrayInputStream(value.getValue());
            final DataInputStream dis = new DataInputStream(bais);

            try {

                final String name = readString(dis);
                final String gender = readString(dis);
                final String address = readString(dis);
                final String phone = readString(dis);
                final StringBuilder sb = new StringBuilder();
                sb.append(email).append("|");
                sb.append(name).append("|");
                sb.append(gender).append("|");
                sb.append(address).append("|");
                sb.append(phone);
                return sb.toString();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return null;
    }

    /**
     * Converts Row from TableAPI to a string that will be consumed
     * by ORA process.
     */
    @Override
    public String toOracleLoaderFormat(final Row row,
                                       final KVStore kvStore) {
        String ret;
        if (row == null) {
            throw new Error("No matching row found");
        }

        try {
            final String delimiter = "|";
            final StringBuilder sb = new StringBuilder();
            boolean needDelimiter = false;
            final Vector<String> allCols = new Vector<String>();
            allCols.add("email");
            allCols.add("name");
            allCols.add("gender");
            allCols.add("address");
            allCols.add("phone");

            for (String col : allCols) {
                /* skip first delimiter */
                if (needDelimiter) {
                    sb.append(delimiter);
                } else {
                    needDelimiter = true;
                }

                /*
                 * For simple data type, it will convert to string
                 * automatically, and for other non-simple data types,
                 * it will convert to JSON. If the field is null, append
                 * nothing and Oracle External table will treat it as
                 * missing value and set it as null.
                 */
                final FieldValue v = row.get(col);
                if (!v.isNull()) {
                    sb.append(row.get(col));
                } else {
                    /* append nothing for null values */
                }
            }
            ret = sb.toString();
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        }

        return ret;
    }


    private String readString(final DataInput in)
        throws IOException {

        if (!in.readBoolean()) {
            return null;
        }

        return in.readUTF();
    }
}
