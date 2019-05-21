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
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.runtime.FuncCurrentTimeMillisIter;
import oracle.kv.impl.query.types.TypeManager;

/**
 * Function to return the current system time in millisecods since the epoch
 */
public class FuncCurrentTimeMillis extends Function {

    FuncCurrentTimeMillis() {
        super(FuncCode.FN_CURRENT_TIME_MILLIS, "current_time_millis",
              TypeManager.LONG_ONE());
    }

    @Override
    boolean mayReturnNULL(ExprFuncCall caller) {
        return false;
    }

    @Override
    PlanIter codegen(CodeGenerator codegen,
                     ExprFuncCall caller,
                     PlanIter[] argIters) {

        int resultReg = codegen.allocateResultReg(caller);

        return new FuncCurrentTimeMillisIter(caller, resultReg);
    }
}

