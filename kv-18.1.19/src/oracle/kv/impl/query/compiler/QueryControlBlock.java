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

import java.util.HashSet;

import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.query.PreparedStatementImpl.DistributionKind;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.PrimaryKeyImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableMetadataHelper;
import oracle.kv.impl.query.compiler.Expr.ExprKind;
import oracle.kv.impl.query.compiler.parser.KVParser;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.runtime.ReceiveIter;
import oracle.kv.impl.util.CommonLoggerUtils;
import oracle.kv.query.PrepareCallback;
import oracle.kv.table.FieldRange;

/**
 * The query control block.
 *
 * theTableMetaHelper:
 *
 * theInitSctx:
 * The top-level static context for the query (for now, there is actually no
 * nesting of static contexts (no need for scoping within a query)), so
 * theInitSctx is the only sctx obj used by the query.
 *
 * theException:
 *
 * generatedNames:
 * A set of names generated internally for use in otherwise unnamed maps and
 * arrays that need them (for Avro schema generation). This set guarantees
 * uniqueness, which is also required by Avro.
 */
public class QueryControlBlock {

    private final KVStoreImpl theStore;

    private final char[] theQueryString;

    private final TableMetadataHelper theTableMetaHelper;

    private final StatementFactory theStatementFactory;

    private final StaticContext theInitSctx;

    private final String theNamespace;

    private final PrepareCallback thePrepareCallback;

    private boolean theStrictMode;

    private int theInternalVarCounter = 0;

    private int theNumRegs = 0;

    private int theNumPlanIters = 0;

    private FieldDefImpl theResultDef;

    private boolean theWrapResultInRecord;

    private Expr theRootExpr;

    private PlanIter theRootPlanIter;

    /* this may, or may not be the same as theRootPlanIter */
    private ReceiveIter theReceiveIter;

    private RuntimeException theException = null;

    private final HashSet<String> generatedNames = new HashSet<String>();

    PrimaryKeyImpl thePushedPrimaryKey;

    DistributionKind thePushedDistributionKind;

    TableImpl theTargetTable;

    /* For unit testing only */
    FieldRange thePushedRange;

    boolean theHaveJsonConstructors;

    boolean hasSort;

    QueryControlBlock(
        TableAPIImpl tableAPI,
        char[] queryString,
        StaticContext sctx,
        String namespace,
        PrepareCallback prepareCallback) {

        theStore = tableAPI != null ?
            (KVStoreImpl) tableAPI.getStore() : null;
        theQueryString = queryString;
        theTableMetaHelper = tableAPI != null ?
            tableAPI.getTableMetadataHelper() :
            (prepareCallback != null ? prepareCallback.getMetadataHelper() :
             null);
        theInitSctx = sctx;
        theStatementFactory = null;
        theNamespace = namespace;
        thePrepareCallback = prepareCallback;
    }

    QueryControlBlock(
        TableMetadataHelper metadataHelper,
        StatementFactory statementFactory,
        char[] queryString,
        StaticContext sctx,
        String namespace,
        PrepareCallback prepareCallback) {

        theStore = null;
        theQueryString = queryString;
        theTableMetaHelper = metadataHelper;
        theInitSctx = sctx;
        theStatementFactory = statementFactory;
        theNamespace = namespace;
        thePrepareCallback = prepareCallback;
    }

    public KVStoreImpl getStore() {
        return theStore;
    }

    TableMetadataHelper getTableMetaHelper() {
        return theTableMetaHelper;
    }

    public StaticContext getInitSctx() {
        return theInitSctx;
    }

    public String getNamespace() {
        return theNamespace;
    }

    public PrepareCallback getPrepareCallback() {
        return thePrepareCallback;
    }

    boolean strictMode() {
        return theStrictMode;
    }

    StatementFactory getStatementFactory() {
        return theStatementFactory;
    }

    public RuntimeException getException() {
        return theException;
    }

    public boolean succeeded() {
        return theException == null;
    }

    public String getErrorMessage() {
        return CommonLoggerUtils.getStackTrace(theException);
    }

    public Expr getRootExpr() {
        return theRootExpr;
    }

    void setRootExpr(Expr e) {
        theRootExpr = e;
    }

    public PrimaryKeyImpl getPushedPrimaryKey() {
        return thePushedPrimaryKey;
    }

    public DistributionKind getPushedDistributionKind() {
        return thePushedDistributionKind;
    }

    public long getTargetTableId() {
        if (theTargetTable != null) {
            return theTargetTable.getId();
        }
        return 0L;
    }

    public String getTargetTableName() {
        if (theTargetTable != null) {
            return theTargetTable.getFullName();
        }
        return null;
    }

    public void setPushedPrimaryKey(PrimaryKeyImpl key) {
        thePushedPrimaryKey = key;
    }

    public void setPushedDistributionKind(DistributionKind kind) {
        thePushedDistributionKind = kind;
    }

    public void setTargetTable(TableImpl table) {
        theTargetTable = table;
    }

    public FieldDefImpl getResultDef() {
        return theResultDef;
    }

    public boolean wrapResultInRecord() {
        return theWrapResultInRecord;
    }

    void setWrapResultInRecord(boolean v) {
        theWrapResultInRecord = v;
    }

    public String getResultColumnName() {

        if (theRootExpr.getKind() == ExprKind.SFW) {

            ExprSFW sfw = (ExprSFW)theRootExpr;
            return sfw.getFieldName(0);

        } else if (theRootExpr.getKind() == ExprKind.RECEIVE) {

            ExprReceive rcv = (ExprReceive)theRootExpr;

            if (rcv.getInput().getKind() == ExprKind.SFW) {
                ExprSFW sfw = (ExprSFW)rcv.getInput();
                return sfw.getFieldName(0);
            }
        }

        return null;
    }

    void setHasSort(boolean hasSort) {
        this.hasSort = hasSort;
    }

    public boolean hasSort() {
        return hasSort;
    }

    /**
     * The caller is responsible for determining success or failure by
     * calling QueryControlBlock.succeeded(). On failure there may be
     * an exception which can be obtained using
     * QueryControlBlock.getException().
     */
    void compile() {

        KVParser parser = new KVParser();
        parser.parse(theQueryString);

        if (!parser.succeeded()) {
            theException = parser.getParseException();
            return;
        }

        Translator translator = new Translator(this);
        translator.translate(parser.getParseTree());
        theException = translator.getException();

        if (theException != null) {
            return;
        }

        if (translator.isQuery()) {
            theRootExpr = translator.getRootExpr();
            theResultDef = theRootExpr.getType().getDef();

            OptRulePushIndexPreds rule = new OptRulePushIndexPreds();
            rule.apply(theRootExpr);
            theException = rule.getException();

            if (theException != null) {
                return;
            }

            Distributer distributer = new Distributer(this);
            distributer.distributeQuery();

            CodeGenerator codegen = new CodeGenerator(this);
            codegen.generatePlan(theRootExpr);
            theException = codegen.getException();

            if (theException == null) {
                theRootPlanIter = codegen.getRootIter();

                /*
                 * The type of the root expr may be EMPTY, if the optimizer
                 * found out that the query will return nothing. In this case,
                 * used the result type after transalation, because the users
                 * expect a record type as the result type.
                 */
                FieldDefImpl resDef = theRootExpr.getType().getDef();
                if (!resDef.isEmpty()) {
                    theResultDef = theRootExpr.getType().getDef();
                }
            }
        }

        return;
    }

    void parse() {

        KVParser parser = new KVParser();
        parser.parse(theQueryString);

        if (!parser.succeeded()) {
            theException = parser.getParseException();
            return;
        }

        Translator translator = new Translator(this);
        translator.translate(parser.getParseTree());
        theException = translator.getException();

        if (theException != null) {
            return;
        }

        if (translator.isQuery()) {
            theRootExpr = translator.getRootExpr();

            OptRulePushIndexPreds rule = new OptRulePushIndexPreds();
            rule.apply(theRootExpr);
            theException = rule.getException();
        }

        return;
    }

    String createInternalVarName(String prefix) {

        if (prefix == null) {
            return "$internVar-" + theInternalVarCounter++;
        }

        return "$" + prefix + "-" + theInternalVarCounter++;
    }

    void incNumRegs(int num) {
        theNumRegs += num;
    }

    public int getNumRegs() {
        return theNumRegs;
    }

    public int incNumPlanIters() {
        return theNumPlanIters++;
    }

    public int getNumIterators() {
        return theNumPlanIters;
    }

    public PlanIter getQueryPlan() {
        return theRootPlanIter;
    }

    public void setReceiveIter(ReceiveIter receiveIter) {
        theReceiveIter = receiveIter;
    }

    public ReceiveIter getReceiveIter() {
        return theReceiveIter;
    }

    public String displayExprTree() {
        return theRootExpr.display();
    }

    public String displayQueryPlan() {
        return theRootPlanIter.display();
    }

    /**
     * Use the generatedNames set to generate a unique name based on the
     * prefix, which is unique per-type (record, enum, binary).  Avro
     * requires generated names for some data types that otherwise do not
     * need them in the DDL.
     */
    String generateFieldName(String prefix) {
        final String gen = "_gen";
        int num = 0;
        StringBuilder sb = new StringBuilder(prefix);
        sb.append(gen);
        String name = sb.toString();
        while (generatedNames.contains(name)) {
            sb.append(num++);
            name = sb.toString();
        }
        generatedNames.add(name);
        return name;
    }
}
