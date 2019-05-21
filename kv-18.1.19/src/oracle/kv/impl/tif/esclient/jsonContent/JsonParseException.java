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

package oracle.kv.impl.tif.esclient.jsonContent;

import com.fasterxml.jackson.core.JsonLocation;

public class JsonParseException extends Exception {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    protected static final int UNKNOWN_POSITION = -1;
    private final int lineNumber;
    private final int columnNumber;
    private final Object[] args;

    public JsonParseException(JsonLocation location,
            String msg,
            Object... args) {
        this(location, msg, null, args);
    }

    @SuppressWarnings("hiding")
    public JsonParseException(JsonLocation location,
            String msg,
            Throwable cause,
            Object... args) {
        super(msg, cause);
        int lineNumber = UNKNOWN_POSITION;
        int columnNumber = UNKNOWN_POSITION;
        if (location != null) {
            lineNumber = location.getLineNr();
            columnNumber = location.getColumnNr();
        }
        this.columnNumber = columnNumber;
        this.lineNumber = lineNumber;
        this.args = args;
    }

    public int getLineNumber() {
        return lineNumber;
    }

    public int getColumnNumber() {
        return columnNumber;
    }

    public Object[] getArgs() {
        return args;
    }

}
