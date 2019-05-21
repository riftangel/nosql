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

package oracle.kv.impl.api.query;

import java.util.HashMap;
import java.util.Map;

import oracle.kv.AsyncExecutionHandle;
import oracle.kv.StatementResult;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.MapValueImpl;
import oracle.kv.impl.api.table.RecordValueImpl;
import oracle.kv.query.BoundStatement;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.RecordDef;

/**
 * Implementation of BoundStatement
 */
public class BoundStatementImpl
    implements BoundStatement,
               InternalStatement {

    private final PreparedStatementImpl preparedStatement;

    private final Map<String, FieldValue> bindVariables;

    BoundStatementImpl(PreparedStatementImpl preparedStatement) {
        this.preparedStatement = preparedStatement;
        bindVariables = new HashMap<String, FieldValue>();
    }

    PreparedStatementImpl getPreparedStmt() {
        return preparedStatement;
    }

    @Override
    public String toString() {
        return preparedStatement.toString();
    }

    @Override
    public RecordDef getResultDef() {
        return preparedStatement.getResultDef();
    }

    @Override
    public Map<String, FieldDef> getVariableTypes() {
        return preparedStatement.getExternalVarsTypes();
    }

    @Override
    public FieldDef getVariableType(String name) {
        return preparedStatement.getExternalVarType(name);
    }

    @Override
    public Map<String, FieldValue> getVariables() {
        return bindVariables;
    }

    @Override
    public BoundStatement createBoundStatement() {
        return preparedStatement.createBoundStatement();
    }

    @Override
    public BoundStatement setVariable(String name, FieldValue value) {
        validate(name, value);
        bindVariables.put(name, value);
        return this;
    }

    @Override
    public BoundStatement setVariable(String name, int value) {
        FieldValue val = FieldDefImpl.integerDef.createInteger(value);
        setVariable(name, val);
        return this;
    }

    @Override
    public BoundStatement setVariable(String name, boolean value) {
        FieldValue val = FieldDefImpl.booleanDef.createBoolean(value);
        setVariable(name, val);
        return this;
    }

    @Override
    public BoundStatement setVariable(String name, double value) {
        FieldValue val = FieldDefImpl.doubleDef.createDouble(value);
        setVariable(name, val);
        return this;
    }

    @Override
    public BoundStatement setVariable(String name, float value) {
        FieldValue val = FieldDefImpl.floatDef.createFloat(value);
        setVariable(name, val);
        return this;
    }

    @Override
    public BoundStatement setVariable(String name, long value) {
        FieldValue val = FieldDefImpl.longDef.createLong(value);
        setVariable(name, val);
        return this;
    }

    @Override
    public BoundStatement setVariable(String name, String value) {
        FieldValue val = FieldDefImpl.stringDef.createString(value);
        setVariable(name, val);
        return this;
    }

    @Override
    public BoundStatement setVariable(String name, byte[] value) {
        FieldValue val = FieldDefImpl.binaryDef.createBinary(value);
        setVariable(name, val);
        return this;
    }


    private void validate(String varName, FieldValue value) {

        FieldDefImpl def = (FieldDefImpl)getVariableType(varName);

        if (def == null) {
            throw new IllegalArgumentException(
                "Variable " + varName + " has not been declared in the query");
        }

        if (!((FieldDefImpl)value.getDefinition()).isSubtype(def)) {
            throw new IllegalArgumentException(
                "Variable " + varName + " does not have an expected type. " +
                "Expected " + def.getType() + " or subtype, got " +
                value.getType());
        }

        checkRecordsContainAllFields(varName, value);
    }

    /*
     * Check if record values have all the fields defined in the type.
     */
    private void checkRecordsContainAllFields(
        String varName,
        FieldValue value) {

        if (value.isNull() ) {
            return;
        }

        FieldDef def = value.getDefinition();

        if (def.isRecord()) {

            RecordValueImpl rec = (RecordValueImpl)value.asRecord();

            /*
             * The various RecordValue.put() methods forbid adding a field
             * whose name or type does not comform to the record def.
             */
            for (int i = 0; i < rec.getNumFields(); ++i) {

                FieldValue fval = rec.get(i);

                if (fval == null) {
                    String fname = rec.getFieldName(i);
                    throw new IllegalArgumentException(
                        "Value for variable " + varName +
                            " not conforming to type definition: there is no" +
                            " value for field: '" + fname + "'.");
                }

                checkRecordsContainAllFields(varName, fval);
            }
        } else if (def.isArray()) {
            for (FieldValue v : value.asArray().toList()) {
                checkRecordsContainAllFields(varName, v);
            }
        } else if (def.isMap()) {
            for (FieldValue v :
                ((MapValueImpl)value.asMap()).getFieldsInternal().values()) {
                checkRecordsContainAllFields(varName, v);
            }
        }
    }

    @Override
    public StatementResult executeSync(
        KVStoreImpl store,
        ExecuteOptions options) {

        if (options == null) {
            options = new ExecuteOptions();
        }

        return new QueryStatementResultImpl(
            store.getTableAPIImpl(), options, this, false /* async */);
    }

    @Override
    public AsyncExecutionHandle executeAsync(KVStoreImpl store,
                                             ExecuteOptions options) {

        if (options == null) {
            options = new ExecuteOptions();
        }

        final QueryStatementResultImpl result =
            new QueryStatementResultImpl(store.getTableAPIImpl(), options,
                                         this, true /* async */);
        return result.getExecutionHandle();
    }
}
