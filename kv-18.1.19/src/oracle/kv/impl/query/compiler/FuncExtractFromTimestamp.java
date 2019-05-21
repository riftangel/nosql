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

import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.runtime.FuncExtractFromTimestampIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.types.TypeManager;

/**
 * Function to extract element for a TIMESTAMP value.
 */
public class FuncExtractFromTimestamp extends Function {

    /*
     * The temporal fields to extract from Timestamp.
     *
     * Don't change the ordering of the below values they are mapped to
     * FN_YEAR to FN_ISOWEEK in FuncCode and should have the same ordering.
     */
    public static enum Unit {
        YEAR,
        MONTH,
        DAY,
        HOUR,
        MINUTE,
        SECOND,
        MILLISECOND,
        MICROSECOND,
        NANOSECOND,
        WEEK,
        ISOWEEK
    }

    FuncExtractFromTimestamp(FuncCode code, String name) {
        super(code, name,
              TypeManager.TIMESTAMP_QSTN(),
              TypeManager.INT_QSTN()); /* RetType */
    }

    @Override
    boolean mayReturnNULL(ExprFuncCall caller) {
        return  caller.getArg(0).mayReturnNULL();
    }

    @Override
    PlanIter codegen(CodeGenerator codegen,
                     ExprFuncCall caller,
                     PlanIter[] argIters) {

        int resultReg = codegen.allocateResultReg(caller);

        Unit unit =
            Unit.values()[theCode.ordinal() - FuncCode.FN_YEAR.ordinal()];

        return new FuncExtractFromTimestampIter(caller, resultReg,
                                                unit, argIters[0]);
    }
}
