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

class IntegerRangeValue extends IntegerValueImpl {

    private static final long serialVersionUID = 1L;

    private final IntegerDefImpl theTypeDef;

    IntegerRangeValue(int value, IntegerDefImpl def) {
        super(value);
        theTypeDef = def;
        def.validateValue(value);
    }

    /**
     * This constructor creates IntegerValueImpl from the String format used for
     * sorted keys.
     */
    IntegerRangeValue(String keyValue, IntegerDefImpl def) {
        super(keyValue);
        theTypeDef = def;
        // No validation needed ????
    }

    @Override
    public IntegerRangeValue clone() {
        return new IntegerRangeValue(value, theTypeDef);
    }

    @Override
    public IntegerDefImpl getDefinition() {
        return theTypeDef;
    }
}
