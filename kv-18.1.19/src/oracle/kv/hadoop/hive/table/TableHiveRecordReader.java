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

package oracle.kv.hadoop.hive.table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import oracle.kv.hadoop.table.TableRecordReader;
import oracle.kv.table.FieldRange;
import oracle.kv.table.Index;
import oracle.kv.table.IndexKey;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

/**
 * Concrete implementation of RecordReader; used to read PrimaryKey/Row pairs
 * from an InputSplit.
 * <p>
 * Note that this RecordReader is based on version 1 of MapReduce (as
 * required by the Hive infrastructure), but wraps and delegates to a YARN
 * based (MapReduce version 2) RecordReader. This is done because the
 * RecordReader provided by Oracle NoSQL Database to support Hadoop
 * integration is YARN based, and this class wishes to exploit and reuse
 * the functionality already provided by the YARN based RecordReader class.
 */
public class TableHiveRecordReader implements RecordReader<Text, Text> {

    private static final Log LOG =
        LogFactory.getLog("oracle.kv.hadoop.hive.table.TableHiveRecordReader");

    private TableRecordReader v2RecordReader;

    public TableHiveRecordReader(JobConf jobConf,
                                 TableRecordReader v2RecordReader) {
        initialize(jobConf, v2RecordReader);
    }

    @Override
    public void close() throws IOException {
        /* Close and null out for GC */
        if (v2RecordReader != null) {
            v2RecordReader.close();
            v2RecordReader = null;
        }
        V1V2TableUtil.resetInputJobInfoForNewQuery();
        LOG.trace("close");
    }

    @Override
    public long getPos() throws IOException {
        LOG.trace("getPos");
        return 0L;
    }

    @Override
    public float getProgress() {
        LOG.trace("getProgress");
        return v2RecordReader.getProgress();
    }

    @Override
    public boolean next(Text key, Text value) throws IOException {

        LOG.trace("next [key = " + key + ", value = " + value + "]");

        if (key == null || value == null) {
            return false;
        }
        boolean ret = false;
        try {
            key.clear();
            value.clear();
            ret = v2RecordReader.nextKeyValue();
            if (ret) {
                final Row curRow = v2RecordReader.getCurrentValue();
                assert curRow != null;
                key.set(curRow.createPrimaryKey().toString());
                value.set(curRow.toString());
            }
        } catch (IOException ioe) {
            LOG.error("TableHiveRecordReader " + this + " caught: " + ioe, ioe);
            /* IOE from v2 record reader operation, no need to wrap */
            throw ioe;
        } catch (Exception e) {
            LOG.error("TableHiveRecordReader " + this + " caught: " + e, e);
            /*
             * Wrap to IOE to match the exception in Hadoop record reader
             * interface.
             */
            throw new IOException(e);
        }
        return ret;
    }

    /**
     * Get the current key.
     *
     * @return the current key or null if there is no current key
     */
    @Override
    public Text createKey() {
        return new Text();
    }

    /**
     * Get the current value.
     * @return the object that was read
     */
    @Override
    public Text createValue() {
        return new Text();
    }

    private void initialize(final JobConf jobConf,
                            final TableRecordReader v2RecRdr) {
        LOG.trace("open");

        /*
         * Initialize any information related to predicate pushdown.
         *
         * Note that this class is instantiated, and its methods are invoked,
         * on the server side of a given Hive query; from a process running
         * on a MapReduce job's DataNode in a Hadoop cluster. When that
         * query includes a predicate (a WHERE clause) the predicate is
         * parsed and analyzed on the client side. If the predicate consists
         * of components that can be pushed to the KVStore for backend
         * filtering, then the client side decomposes the predicate into
         * two disjoint sets; one set containing the search conditions that
         * will be pushed and the other containing the remaining ("residual")
         * parts of the original predicate. When the query is successfully
         * decomposed, the search conditions are serialized and sent to the
         * server side (the MapReduce job) for further processing here;
         * whereas the residual predicate remains on the client side and
         * will be applied to the results produced by the filtering performed
         * by the MapReduce job on the backend.
         *
         * To communicate the search conditions (the predicate to be pushed)
         * from the client side to the server side, the client side Hive
         * infrastructure sets a property with name FILTER_EXPR_CONF_STR
         * to a value consisting of the SERIALIZED form of the search
         * conditions. The value of that property is retrieved below in
         * this method, and then deserialized to recover the search
         * conditions to push to the KVStore server side so that filtering
         * can be performed there.
         *
         * Note that because the residual predicate will not be pushed, Hive
         * only sends the search conditions; never the residual. Thus, the
         * residual predicate will always be null (and ignored) in this
         * class.
         */
        final String filterExprPropVal =
            jobConf.get(TableScanDesc.FILTER_EXPR_CONF_STR);

        if (filterExprPropVal != null) {
            final ExprNodeGenericFuncDesc filterExpr =
                      Utilities.deserializeExpression(filterExprPropVal);

            if (LOG.isTraceEnabled()) {
                LOG.trace("Hive query = " + HiveConf.getVar(
                              jobConf, HiveConf.ConfVars.HIVEQUERYSTRING));
                LOG.trace("deserialized predicate = " + filterExpr);
                final StringBuilder whereClause = new StringBuilder();
                TableHiveInputFormat.buildPushPredicate(
                                         filterExpr, whereClause);
                LOG.trace("where clause = " + whereClause.toString());
            }

            final List<String> hiveFilterCols = filterExpr.getCols();

            if (hiveFilterCols != null) {
                final IndexPredicateAnalyzer analyzer =
                   TableHiveInputFormat.basicPredicateAnalyzer(hiveFilterCols);

                final List<IndexSearchCondition> hiveSearchConditions =
                    new ArrayList<IndexSearchCondition>();

                /*
                 * The analyzePredicate method will use the predicate info
                 * retrieved from the deserialized filterExpr to populate the
                 * hiveSearchConditions ArrayList with the desired search
                 * conditions. That method also returns the residual predicate.
                 * But in this case, since the residual will always be null
                 * (as explained above), the return value is ignored below.
                 */
                analyzer.analyzePredicate(filterExpr, hiveSearchConditions);

                if (LOG.isTraceEnabled()) {
                    LOG.trace("search conditions to validate = " +
                              hiveSearchConditions);
                }

                final Table table = v2RecRdr.getKvTable();
                if (table != null) {

                    /*
                     * If the search conditions retrieved from the deserialized
                     * property can be pushed (are valid), then construct the
                     * appropriate key (IndexKey or PrimaryKey) and/or
                     * FieldRange.
                     */
                    if (TableHiveInputFormat.searchConditionsValid(
                            hiveSearchConditions, table)) {

                        /* Give index priority over primaryKey. */
                        FieldRange fieldRange = null;
                        PrimaryKey primaryKey = null;
                        final IndexKey indexKey =
                            TableHiveInputFormat.
                                indexKeyFromSearchConditionsNoRange(
                                                 hiveSearchConditions, table);
                        if (indexKey != null) {

                            v2RecRdr.setIndexKey(indexKey);
                            fieldRange = indexFieldRangeFromSearchConditions(
                                hiveSearchConditions, indexKey.getIndex());
                        } else {

                            primaryKey =
                                primaryKeyFromSearchConditionsNoRange(
                                    hiveSearchConditions, table);
                            if (primaryKey != null) {

                                v2RecRdr.setPrimaryKey(primaryKey);
                                fieldRange =
                                    primaryFieldRangeFromSearchConditions(
                                        hiveSearchConditions, table);
                            }
                        }
                        v2RecRdr.setMultiRowOptions(fieldRange);

                        if (LOG.isTraceEnabled()) {
                            LOG.trace("primaryKey = " + primaryKey);
                            LOG.trace("indexKey = " + indexKey);
                        }
                    } else {
                        LOG.trace("search conditions not valid for " +
                                  "PrimaryKey or IndexKey iteration. " +
                                  "Convert predicate to native query string");
                    }
                }
            }
        }
        this.v2RecordReader = v2RecRdr;
    }

    /**
     * Assumes the columns in the given search conditions correspond to the
     * fields of a <em>valid</em> <code>PrimaryKey</code> of the given table,
     * and uses that information to construct and return a partial key
     * that can be "pushed" to the store for scanning and filtering the
     * table in the backen server; or returns <code>null</code> (or an
     * "empty" key) if those search conditions do not satisfy the necessary
     * criteria for constructing the key. The criteria used when constructing
     * the key is as follows:
     *
     * For the set of columns in the search conditions that are associated
     * with the '=' operation, if those columns form a valid key (that is,
     * the first 'N' fields of the table's key, with no "gaps"), then those
     * fields are used to construct the key to return. Additionally, if the
     * search conditions reference the 'N+1st' field of the table's key as
     * a <code>FieldRange</code>, then that field is <em>not</em> included
     * in the returned key; so that the <code>FieldRange</code> can be
     * handled ("pushed") separately.
     */
    private PrimaryKey primaryKeyFromSearchConditionsNoRange(
                       final List<IndexSearchCondition> searchConditions,
                       final Table table) {

        if (searchConditions == null || table == null) {
            return null;
        }

        final List<String> tableFields =
            TableHiveInputFormat.getPrimaryColumnsLower(table);

        if (tableFields == null || tableFields.isEmpty()) {
            return null;
        }

        /*
         * If there is a column in the search conditions that is associated
         * with a field range, than exclude it from the key construction
         * process; so that the field range can be handled separately.
         */
        final Map<String, IndexSearchCondition> searchCondMap =
                                  new HashMap<String, IndexSearchCondition>();
        for (IndexSearchCondition cond : searchConditions) {
            final String colName =
                             cond.getColumnDesc().getColumn().toLowerCase();
            if ((GenericUDFOPEqual.class.getName()).equals(
                                                    cond.getComparisonOp())) {
                searchCondMap.put(colName, cond);
            }
        }

        /*
         * Because this method is called only after the search conditions
         * have been validated, there is no need to check for a valid
         * PrimaryKey. Additionally, if the search conditions contain
         * no '=' operators (searchCondMap.size == 0), then those search
         * conditions must represent a FieldRange on the first field of
         * the PrimaryKey; in which case, this method returns an empty
         * PrimaryKey (so that filtering will be performed on that range,
         * and all remaining fields are 'wildcarded').
         */
        final PrimaryKey primaryKey = table.createPrimaryKey();

        if (searchCondMap.isEmpty()) {
            return primaryKey;
        }

        /*
         * Reaching this point means that there must be at least one column
         * (field) in the search conditions corresponding to the '=' operator.
         * In that case, return the partial PrimaryKey containing the fields
         * corresponding to the '=' operators.
         */
        if (tableFields.containsAll(searchCondMap.keySet())) {

            final List<String> fieldNames = primaryKey.getFields();

            for (String fieldName : fieldNames) {
                final IndexSearchCondition searchCond =
                              searchCondMap.get(fieldName.toLowerCase());

                if (searchCond == null) {
                    /* null ==> no more elements in searchCondMap. Done. */
                    return primaryKey;
                }
                TableHiveInputFormat.populateKey(
                    fieldName, searchCond.getConstantDesc(), primaryKey);
            }
        }
        return primaryKey;
    }

    private FieldRange indexFieldRangeFromSearchConditions(
                       final List<IndexSearchCondition> searchConditions,
                       final Index index) {

        if (searchConditions == null || index == null) {
            return null;
        }

        /*
         * Retrieve the search conditions (column name, operator, value)
         * associated with the '<', '<=', '>', or '>=' operator. That is,
         * if a column name in the search conditions is NOT associated
         * with a field range (a start and end condition), exclude that
         * column name from the list.
         *
         * Note that this method assumes that the search conditions were
         * validated; that is, it is guaranteed that if the search conditions
         * contain one inequality (ex. '>' or >='), then they must contain
         * exactly one other inequality; each with the same column name,
         * but 'opposite' inequality operator ('>' and '<' or '<=', or
         * '>=' and '<' or '<='). Nevertheless, some defensive coding is
         * employed.
         */
        final List<IndexSearchCondition> rangeSearchConds =
                                  new ArrayList<IndexSearchCondition>();
        for (IndexSearchCondition cond : searchConditions) {
            final String op = cond.getComparisonOp();

            if ((GenericUDFOPGreaterThan.class.getName()).equals(op) ||
                (GenericUDFOPEqualOrGreaterThan.class.getName()).equals(op) ||
                (GenericUDFOPLessThan.class.getName()).equals(op) ||
                (GenericUDFOPEqualOrLessThan.class.getName()).equals(op)) {

                rangeSearchConds.add(cond);
            }
        }

        if (rangeSearchConds.isEmpty()) {
            return null;
        }

        /*
         * Only create the desired fieldRange if the column names in
         * rangeSearchConds correspond to (lower case) fields in the
         * given index. But note that rangeSearchConds should contain
         * only 2 elements -- one corresponding to the start value
         * ('>' or '<=') of a column in the index, and the other
         * corresponding to the end value ('<' or '<=') of that SAME
         * column. This means that rangeSearchConds should containd
         * only 2 elements, with the same column name; which will be
         * verified by creageFieldRange itself. As a result, when
         * determining whether the column names in rangeSearchConds
         * correspond to fields in the given index, it is only necessary
         * to retrieve the first column name in rangeSearchConds; that
         * is, rangeSearchConds.get(0).
         */
        final String colNameLower =
            rangeSearchConds.get(0).getColumnDesc().getColumn().toLowerCase();

        for (String fieldName : index.getFields()) {
            if (fieldName.toLowerCase().equals(colNameLower)) {
                return createFieldRange(fieldName, rangeSearchConds, index);
            }
        }
        return null;
    }

    /**
     * Convenience method that analyzes the contents of the given search
     * conditions consisting of column (field) names and corresponding
     * comparison operation(s) and uses that information to construct a
     * <code>FieldRange</code> that can be input to a <code>TableScan</code>
     * iterator.
     * <p>
     * Note that this method assumes that the given search conditions have
     * already been validated (for example, via a method such as,
     * <code>TableHiveInputFormat.searchConditionsValid</code>).
     */
    private FieldRange primaryFieldRangeFromSearchConditions(
                       final List<IndexSearchCondition> searchConditions,
                       final Table table) {

        if (searchConditions == null || table == null) {
            return null;
        }

        /*
         * Retrieve the search conditions (column name, operator, value)
         * associated with the '<', '<=', '>', or '>=' operator. That is,
         * if a column name in the search conditions is NOT associated
         * with a field range (a start and end condition), exclude that
         * column name from the list.
         *
         * Note that this method assumes that the search conditions were
         * validated; that is, it is guaranteed that if the search conditions
         * contain one inequality (ex. '>' or >='), then they must contain
         * exactly one other inequality; each with the same column name,
         * but 'opposite' inequality operator ('>' and '<' or '<=', or
         * '>=' and '<' or '<='). Nevertheless, some defensive coding is
         * employed.
         */
        final List<IndexSearchCondition> rangeSearchConds =
                                  new ArrayList<IndexSearchCondition>();
        for (IndexSearchCondition cond : searchConditions) {
            final String op = cond.getComparisonOp();

            if ((GenericUDFOPGreaterThan.class.getName()).equals(op) ||
                (GenericUDFOPEqualOrGreaterThan.class.getName()).equals(op) ||
                (GenericUDFOPLessThan.class.getName()).equals(op) ||
                (GenericUDFOPEqualOrLessThan.class.getName()).equals(op)) {

                rangeSearchConds.add(cond);
            }
        }

        if (rangeSearchConds.isEmpty()) {
            return null;
        }

        final List<String> tableFields =
            TableHiveInputFormat.getPrimaryColumnsLower(table);

        if (tableFields == null || tableFields.isEmpty()) {
            return null;
        }

        /* Column name from both conditions should be equal, so get(0). */
        final String colNameLower =
            rangeSearchConds.get(0).getColumnDesc().getColumn().toLowerCase();

        /* Make sure the field range columns belong to the primary key. */
        final PrimaryKey primaryKey = table.createPrimaryKey();
        final List<String> fieldNames = primaryKey.getFields();

        for (String fieldName : fieldNames) {
            if (fieldName.toLowerCase().equals(colNameLower)) {
                return createFieldRange(fieldName, rangeSearchConds, table);
            }
        }
        return null;
    }

    private FieldRange createFieldRange(
                           final String fieldName,
                           final List<IndexSearchCondition> conditions,
                           final Index index) {

        return createFieldRange(fieldName, conditions, index, null);
    }

    private FieldRange createFieldRange(
                           final String fieldName,
                           final List<IndexSearchCondition> conditions,
                           final Table table) {

        return createFieldRange(fieldName, conditions, null, table);
    }

    private FieldRange createFieldRange(
                           final String fieldName,
                           final List<IndexSearchCondition> conditions,
                           final Index index,
                           final Table table) {

        if (!rangeConditionsValid(conditions)) {
            return null;
        }

        final IndexSearchCondition cond0 = conditions.get(0);
        final IndexSearchCondition cond1 = conditions.get(1);
        final String op0 = cond0.getComparisonOp();
        final String op1 = cond1.getComparisonOp();
        final ExprNodeConstantDesc constantDesc0 = cond0.getConstantDesc();
        final ExprNodeConstantDesc constantDesc1 = cond1.getConstantDesc();

        FieldRange fieldRange = null;
        if (index != null) {
            fieldRange = index.createFieldRange(fieldName);
        } else if (table != null) {
            fieldRange = table.createFieldRange(fieldName);
        } else {
            return null;
        }

        /* If op0 is '>' then set BEGIN point, NOT INCLUSIVE. */
        if ((GenericUDFOPGreaterThan.class.getName()).equals(op0)) {
            setStart(fieldRange, constantDesc0, false);
        }
        /* If op0 is '>=' then set BEGIN point, INCLUSIVE. */
        if ((GenericUDFOPEqualOrGreaterThan.class.getName()).equals(op0)) {
            setStart(fieldRange, constantDesc0, true);
        }
        /* If op0 is '<' then set END point, NOT INCLUSIVE. */
        if ((GenericUDFOPLessThan.class.getName()).equals(op0)) {
            setEnd(fieldRange, constantDesc0, false);
        }
        /* If op0 is '<=' then set END point, INCLUSIVE. */
        if ((GenericUDFOPEqualOrLessThan.class.getName()).equals(op0)) {
            setEnd(fieldRange, constantDesc0, true);
        }

        /* If op1 is '>' then set BEGIN point, NOT INCLUSIVE. */
        if ((GenericUDFOPGreaterThan.class.getName()).equals(op1)) {
            setStart(fieldRange, constantDesc1, false);
        }
        /* If op1 is '>=' then set BEGIN point, INCLUSIVE. */
        if ((GenericUDFOPEqualOrGreaterThan.class.getName()).equals(op1)) {
            setStart(fieldRange, constantDesc1, true);
        }
        /* If op1 is '<' then set END point, NOT INCLUSIVE. */
        if ((GenericUDFOPLessThan.class.getName()).equals(op1)) {
            setEnd(fieldRange, constantDesc1, false);
        }
        /* If op1 is '<=' then set END point, INCLUSIVE. */
        if ((GenericUDFOPEqualOrLessThan.class.getName()).equals(op1)) {
            setEnd(fieldRange, constantDesc1, true);
        }

        return fieldRange;
    }

    private boolean rangeConditionsValid(
                        final List<IndexSearchCondition> conds) {
        /*
         * For valid field range, must be exactly 2 ('opposite') inequalities
         * with same column name.
         */
        if (conds.size() != 2) {
            return false;
        }

        final IndexSearchCondition cond0 = conds.get(0);
        final IndexSearchCondition cond1 = conds.get(1);

        final String colName0 = cond0.getColumnDesc().getColumn();
        final String colName1 = cond1.getColumnDesc().getColumn();

        if (!colName0.equals(colName1)) {
            return false;
        }

        final String op0 = cond0.getComparisonOp();
        final String op1 = cond1.getComparisonOp();

        if ((GenericUDFOPGreaterThan.class.getName()).equals(op0) ||
            (GenericUDFOPEqualOrGreaterThan.class.getName()).equals(op0)) {

            /* If op0 is '>' or '>=', then op1 must be '<' or '<='. */
            if (!(GenericUDFOPLessThan.class.getName()).equals(op1) &&
                !(GenericUDFOPEqualOrLessThan.class.getName()).equals(op1)) {
                return false;
            }

        } else if ((GenericUDFOPLessThan.class.getName()).equals(op0) ||
                   (GenericUDFOPEqualOrLessThan.class.getName()).equals(op0)) {

            /* If op0 is '<' or '<=', then op1 must be '>' or '>='. */
            if (!(GenericUDFOPGreaterThan.class.getName()).equals(op1) &&
               !(GenericUDFOPEqualOrGreaterThan.class.getName()).equals(op1)) {
                return false;
            }
        }
        return true;
    }

    private void setStart(final FieldRange fieldRange,
                          final ExprNodeConstantDesc constantDesc,
                          final boolean isInclusive) {

        final String typeName = constantDesc.getTypeInfo().getTypeName();
        final Object value = constantDesc.getValue();

        /* Currently supports only the following primitive types . */
        if (serdeConstants.INT_TYPE_NAME.equals(typeName)) {
            fieldRange.setStart(((Integer) value).intValue(), isInclusive);
        } else if (serdeConstants.BIGINT_TYPE_NAME.equals(typeName)) {
            fieldRange.setStart(((Long) value).longValue(), isInclusive);
        } else if (serdeConstants.FLOAT_TYPE_NAME.equals(typeName)) {
            fieldRange.setStart(((Float) value).floatValue(), isInclusive);
        } else if (serdeConstants.DECIMAL_TYPE_NAME.equals(typeName)) {
            fieldRange.setStart(((Float) value).floatValue(), isInclusive);
        } else if (serdeConstants.DOUBLE_TYPE_NAME.equals(typeName)) {
            fieldRange.setStart(((Double) value).doubleValue(), isInclusive);
        } else if (serdeConstants.STRING_TYPE_NAME.equals(typeName)) {
            fieldRange.setStart((String) value, isInclusive);
        }
    }

    private void setEnd(final FieldRange fieldRange,
                        final ExprNodeConstantDesc constantDesc,
                        final boolean isInclusive) {

        final String typeName = constantDesc.getTypeInfo().getTypeName();
        final Object value = constantDesc.getValue();

        /* Currently supports only the following primitive types . */
        if (serdeConstants.INT_TYPE_NAME.equals(typeName)) {
            fieldRange.setEnd(((Integer) value).intValue(), isInclusive);
        } else if (serdeConstants.BIGINT_TYPE_NAME.equals(typeName)) {
            fieldRange.setEnd(((Long) value).longValue(), isInclusive);
        } else if (serdeConstants.FLOAT_TYPE_NAME.equals(typeName)) {
            fieldRange.setEnd(((Float) value).floatValue(), isInclusive);
        } else if (serdeConstants.DECIMAL_TYPE_NAME.equals(typeName)) {
            fieldRange.setEnd(((Float) value).floatValue(), isInclusive);
        } else if (serdeConstants.DOUBLE_TYPE_NAME.equals(typeName)) {
            fieldRange.setEnd(((Double) value).doubleValue(), isInclusive);
        } else if (serdeConstants.STRING_TYPE_NAME.equals(typeName)) {
            fieldRange.setEnd((String) value, isInclusive);
        }
    }
}
