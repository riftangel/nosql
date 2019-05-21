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

package oracle.kv.impl.query.compiler;

/**
 * A simple class to hold query expression and plan formatting information,
 * such as indent level. A new instance of this class is passed to
 * display() methods.
 *
 * theIndent:
 * The current number of space chars to be printed as indentation when
 * displaying the expression tree or the query execution plan.
 */
public class QueryFormatter {

    private final int theIndentIncrement;

    private int theIndent;

    public QueryFormatter(int increment) {
        theIndentIncrement = increment;
    }

    public QueryFormatter() {
        theIndentIncrement = 2;
    }

    public int getIndent() {
        return theIndent;
    }

    public int getIndentIncrement() {
        return theIndentIncrement;
    }

    public void setIndent(int v) {
        theIndent = v;
    }

    public void incIndent() {
        theIndent += theIndentIncrement;
    }

    public void decIndent() {
        theIndent -= theIndentIncrement;
    }

    public void indent(StringBuilder sb) {
        for (int i = 0; i < theIndent; ++i) {
            sb.append(' ');
        }
    }
}
