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

import java.util.ArrayList;

/**
 * There is a single instance of the FunctionLib class, created during static
 * initialization as a static member of the CompilerAPI class. This single
 * instance  creates and stores the Function objs for all builtin functions.
 * It also registers these Function objs in the root static context.
 */
public class FunctionLib {

    /*
     * This enum defines a unique code for each builtin function.
     *
     * WARNING!!
     * ADD ALL NEW CODES AT THE END AND  DO NOT REORDER CODES. This is because
     * function codes may appear in the query plan that gets send over the wire
     * from clients to servers.
     */
    public static enum FuncCode {

        OP_AND,
        OP_OR,

        OP_EQ,
        OP_NEQ,
        OP_GT,
        OP_GE,
        OP_LT,
        OP_LE,

        OP_EQ_ANY,
        OP_NEQ_ANY,
        OP_GT_ANY,
        OP_GE_ANY,
        OP_LT_ANY,
        OP_LE_ANY,

        OP_ADD_SUB,
        OP_MULT_DIV,
        OP_ARITH_UNARY,

        FN_SEQ_CONCAT,

        OP_EXISTS,
        OP_NOT_EXISTS,

        OP_NOT,

        FN_SIZE,

        OP_IS_NULL,
        OP_IS_NOT_NULL,

        FN_YEAR,
        FN_MONTH,
        FN_DAY,
        FN_HOUR,
        FN_MINUTE,
        FN_SECOND,
        FN_MILLISECOND,
        FN_MICROSECOND,
        FN_NANOSECOND,
        FN_WEEK,
        FN_ISOWEEK,

        FN_CURRENT_TIME,
        FN_CURRENT_TIME_MILLIS,

        FN_EXPIRATION_TIME,
        FN_EXPIRATION_TIME_MILLIS,
        FN_REMAINING_HOURS,
        FN_REMAINING_DAYS,
        FN_ROW_VERSION,

        FN_COUNT_STAR,
        FN_COUNT,
        FN_COUNT_NUMBERS,
        FN_SUM,
        FN_AVG,
        FN_MIN,
        FN_MAX;

        private static final FuncCode[] VALUES = values();

        public static FuncCode valueOf(int ordinal) {
            return VALUES[ordinal];
        }
    }

    ArrayList<Function> theFunctions;

    FunctionLib(StaticContext sctx) {

        theFunctions = new ArrayList<Function>(64);

        theFunctions.add(new FuncAndOr(FuncCode.OP_AND, "AND"));
        theFunctions.add(new FuncAndOr(FuncCode.OP_OR, "OR"));

        theFunctions.add(new FuncCompOp(FuncCode.OP_EQ, "EQ"));
        theFunctions.add(new FuncCompOp(FuncCode.OP_NEQ, "NEQ"));
        theFunctions.add(new FuncCompOp(FuncCode.OP_GT, "GT"));
        theFunctions.add(new FuncCompOp(FuncCode.OP_GE, "GE"));
        theFunctions.add(new FuncCompOp(FuncCode.OP_LT, "LT"));
        theFunctions.add(new FuncCompOp(FuncCode.OP_LE, "LE"));

        theFunctions.add(new FuncAnyOp(FuncCode.OP_EQ_ANY, "EQ_ANY"));
        theFunctions.add(new FuncAnyOp(FuncCode.OP_NEQ_ANY, "NEQ_ANY"));
        theFunctions.add(new FuncAnyOp(FuncCode.OP_GT_ANY, "GT_ANY"));
        theFunctions.add(new FuncAnyOp(FuncCode.OP_GE_ANY, "GE_ANY"));
        theFunctions.add(new FuncAnyOp(FuncCode.OP_LT_ANY, "LT_ANY"));
        theFunctions.add(new FuncAnyOp(FuncCode.OP_LE_ANY, "LE_ANY"));

        theFunctions.add(new FuncArithOp(FuncCode.OP_ADD_SUB, "+-"));
        theFunctions.add(new FuncArithOp(FuncCode.OP_MULT_DIV, "*/"));
        theFunctions.add(new FuncArithUnaryOp(FuncCode.OP_ARITH_UNARY, "-"));

        theFunctions.add(new FuncConcat());

        theFunctions.add(new FuncExists(FuncCode.OP_EXISTS, "EXISTS"));
        theFunctions.add(new FuncExists(FuncCode.OP_NOT_EXISTS, "NOT_EXISTS"));

        theFunctions.add(new FuncNot());

        theFunctions.add(new FuncSize());

        theFunctions.add(new FuncIsNull(FuncCode.OP_IS_NULL, "IS_NULL"));
        theFunctions.add(new FuncIsNull(FuncCode.OP_IS_NOT_NULL, "IS_NOT_NULL"));

        theFunctions.add(new FuncExtractFromTimestamp(FuncCode.FN_YEAR,
                                                      "year"));
        theFunctions.add(new FuncExtractFromTimestamp(FuncCode.FN_MONTH,
                                                      "month"));
        theFunctions.add(new FuncExtractFromTimestamp(FuncCode.FN_DAY,
                                                      "day"));
        theFunctions.add(new FuncExtractFromTimestamp(FuncCode.FN_HOUR,
                                                      "hour"));
        theFunctions.add(new FuncExtractFromTimestamp(FuncCode.FN_MINUTE,
                                                      "minute"));
        theFunctions.add(new FuncExtractFromTimestamp(FuncCode.FN_SECOND,
                                                      "second"));
        theFunctions.add(new FuncExtractFromTimestamp(FuncCode.FN_MILLISECOND,
                                                      "millisecond"));
        theFunctions.add(new FuncExtractFromTimestamp(FuncCode.FN_MICROSECOND,
                                                      "microsecond"));
        theFunctions.add(new FuncExtractFromTimestamp(FuncCode.FN_NANOSECOND,
                                                      "nanosecond"));
        theFunctions.add(new FuncExtractFromTimestamp(FuncCode.FN_WEEK,
                                                      "week"));
        theFunctions.add(new FuncExtractFromTimestamp(FuncCode.FN_ISOWEEK,
                                                      "isoweek"));

        theFunctions.add(new FuncCurrentTime());
        theFunctions.add(new FuncCurrentTimeMillis());

        theFunctions.add(new FuncExpirationTime());
        theFunctions.add(new FuncExpirationTimeMillis());
        theFunctions.add(new FuncRemainingHours());
        theFunctions.add(new FuncRemainingDays());
        theFunctions.add(new FuncRowVersion());

        theFunctions.add(new FuncCountStar());
        theFunctions.add(new FuncCount(FuncCode.FN_COUNT, "count"));
        theFunctions.add(new FuncCount(FuncCode.FN_COUNT_NUMBERS,
                                       "count_numbers"));
        theFunctions.add(new FuncSum());
        theFunctions.add(new FuncAvg());
        theFunctions.add(new FuncMinMax(FuncCode.FN_MIN, "min"));
        theFunctions.add(new FuncMinMax(FuncCode.FN_MAX, "max"));

        for (Function func : theFunctions) {
            sctx.addFunction(func);
        }
    }

    Function getFunc(FuncCode c) {
        return theFunctions.get(c.ordinal());
    }
}
