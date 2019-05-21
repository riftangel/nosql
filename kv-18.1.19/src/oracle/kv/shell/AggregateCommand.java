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

package oracle.kv.shell;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.codehaus.jackson.JsonNode;

import oracle.kv.Direction;
import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.KeyValueVersion;
import oracle.kv.ParallelScanIterator;
import oracle.kv.StoreIteratorConfig;
import oracle.kv.StoreIteratorException;
import oracle.kv.Value;
import oracle.kv.impl.admin.client.CommandShell;
import oracle.kv.shell.CommandUtils.RunTableAPIOperation;
import oracle.kv.table.FieldValue;
import oracle.kv.table.MultiRowOptions;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.util.shell.CommandWithSubs;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellException;

/**
 * Aggregate command, a simple data aggregation command to count, sum, or
 * average numeric fields that match the input filter condition and are of
 * the appropriate type.
 */
public class AggregateCommand extends CommandWithSubs {

    final static String COMMAND_OVERVIEW =
        "The Aggregate command encapsulates commands that performs simple " +
        "data" + eol + "aggregation operations on numeric fields of values " +
        "from a store or rows" + eol + "from a table.";

    private static final
        List<? extends SubCommand> subs =
                   Arrays.asList(new AggregateKVSub(),
                                 new AggregateTableSub());

    public AggregateCommand() {
        super(subs, "aggregate", 3, 2);
        overrideJsonFlag = true;
    }

    @Override
    protected String getCommandOverview() {
        return COMMAND_OVERVIEW;
    }

    /**
     * Base abstract class for AggregateSubCommands.  This class extracts
     * the generic flags "-count", "-sum" and "-avg" from the command line
     * and includes some common methods for sub commands.
     *
     * The extending classes should implements the abstract method exec().
     */
    abstract static class AggregateSubCommand extends SubCommand {

        final static String COUNT_FLAG = "-count";
        final static String COUNT_FLAG_DESC = COUNT_FLAG;
        final static String SUM_FLAG = "-sum";
        final static String SUM_FLAG_DESC = SUM_FLAG +  " <field[,field]+>";
        final static String AVG_FLAG = "-avg";
        final static String AVG_FLAG_DESC = AVG_FLAG +  " <field[,field]+>";
        final static String START_FLAG = "-start";
        final static String END_FLAG = "-end";

        static final String genericFlags =
            "[" + COUNT_FLAG_DESC + "] [" + SUM_FLAG_DESC + "] " +
            "[" + AVG_FLAG_DESC + "]";

        boolean doCounting;
        List<String> sumFields;
        List<String> avgFields;
        AggResult result;

        AggregateSubCommand(String name, int prefixLength) {
            super(name, prefixLength);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            doCounting = false;
            sumFields = new ArrayList<String>();
            avgFields = new ArrayList<String>();
            result = new AggResult();
            exec(args, shell, result);
            return genStatsSummary(result);
        }

        abstract void exec(String[] args, Shell shell, AggResult aggResult)
            throws ShellException;

        int checkGenericArg(Shell shell, String arg, String[] args, int i)
            throws ShellException {

            int rval = i;
            if (COUNT_FLAG.equals(arg)) {
                doCounting = true;
            } else if (SUM_FLAG.equals(arg)) {
                String str = Shell.nextArg(args, rval++, this);
                String[] fields = str.split(",");
                for (String field: fields) {
                    if (!sumFields.contains(field)) {
                        sumFields.add(field);
                    }
                }
            } else if (AVG_FLAG.equals(arg)) {
                String str = Shell.nextArg(args, rval++, this);
                String[] fields = str.split(",");
                for (String field: fields) {
                    if (!avgFields.contains(field)) {
                        avgFields.add(field);
                    }
                }
            } else {
                shell.unknownArgument(arg, this);
            }
            return rval;
        }

        List<String> getAggFields() {
            if (sumFields.isEmpty() && avgFields.isEmpty()) {
                return null;
            }
            final List<String> fields = new ArrayList<String>();
            if (!sumFields.isEmpty()) {
                for (String field: sumFields) {
                    if (!fields.contains(field)) {
                        fields.add(field);
                    }
                }
            }
            if (!avgFields.isEmpty()) {
                for (String field: avgFields) {
                    if (!fields.contains(field)) {
                        fields.add(field);
                    }
                }
            }
            return fields;
        }

        void validateAggArgs(Shell shell)
            throws ShellException {

            if (!doCounting && sumFields.isEmpty() && avgFields.isEmpty()) {
                shell.requiredArg(COUNT_FLAG + " or " + SUM_FLAG +
                    " or " + AVG_FLAG, this);
            }
        }

        /* Generate output result. */
        String genStatsSummary(AggResult aggResult) {
            StringBuilder sb = new StringBuilder();
            long count = aggResult.getCount();
            if (doCounting) {
                sb.append("Row count: ");
                sb.append(count);
                if (sumFields.isEmpty() && avgFields.isEmpty()) {
                    return sb.toString();
                }
            }

            Formatter fmt = new Formatter(sb);
            if (!sumFields.isEmpty()) {
                sb.append(eol);
                sb.append("Sum:");
                for (String field: sumFields) {
                    if (count == 0) {
                        fmt.format(eolt + "%s: No numerical value", field);
                        continue;
                    }
                    Number sum = aggResult.getSum(field);
                    if (sum == null) {
                        fmt.format(eolt + "%s: No numerical value", field);
                        continue;
                    }
                    String fieldInfo = getFieldInfo(aggResult, field);
                    if (sum instanceof Double) {
                        if (((Double)sum).isInfinite()) {
                            fmt.format(eolt + "%s: numeric overflow",
                                       fieldInfo);
                            continue;
                        }
                        fmt.format(eolt + "%s: %.2f", fieldInfo, sum);
                    } else {
                        fmt.format(eolt + "%s: %d", fieldInfo, sum);
                    }
                }
            }
            if (!avgFields.isEmpty()) {
                sb.append(eol);
                sb.append("Average:");
                for (String field: avgFields) {
                    if (count == 0) {
                        fmt.format(eolt + "%s: No numerical value", field);
                        continue;
                    }

                    Double avg = aggResult.getAvg(field);
                    String fieldInfo = getFieldInfo(aggResult, field);
                    if (avg == null) {
                        fmt.format(eolt + "%s: No numerical value", field);
                        continue;
                    } else if (avg.isInfinite()) {
                        fmt.format(eolt + "%s: numeric overflow", fieldInfo);
                        continue;
                    }
                    fmt.format(eolt + "%s: %.2f", fieldInfo, avg.doubleValue());
                }
            }

            fmt.close();
            return sb.toString();
        }

        private String getFieldInfo(AggResult aggResult, String field) {
            int cnt = aggResult.getCount(field);
            return field + "(" + cnt + ((cnt > 1) ? " values" : " value") + ")";
        }

        /**
         * A class used to tally the fields value to calculate aggregated
         * sum or average value.
         */
        static class AggResult {

            private long count;
            private final HashMap<String, CalcSum<?>> sums;

            AggResult() {
                sums = new HashMap<String, CalcSum<?>>();
                count = 0;
            }

            void tallyCount() {
                count++;
            }

            long getCount() {
                return count;
            }

            Number getSum(String field) {
                CalcSum<?> sum = sums.get(field);
                if (sum == null) {
                    return null;
                }
                return sum.getValue();
            }

            int getCount(String field) {
                CalcSum<?> sum = sums.get(field);
                if (sum == null) {
                    return 0;
                }
                return sum.getCount();
            }

            Double getAvg(String field) {
                Number sum = getSum(field);
                int cnt = getCount(field);
                if (sum == null || cnt == 0) {
                    return null;
                }
                return sum.doubleValue()/cnt;
            }

            void tallyInt(String field, Integer value) {
                tallyLong(field, value.longValue());
            }

            void tallyLong(String field, Long value) {
                CalcSum<?> sum = sums.get(field);
                if (sum == null) {
                    sum = new CalcSumLong(value);
                    sums.put(field, sum);
                    return;
                }
                if (sum instanceof CalcSumLong) {
                    try {
                        ((CalcSumLong)sum).doSum(value);
                    } catch (ArithmeticException ae) {
                        sum = new CalcSumDouble(sum);
                        ((CalcSumDouble)sum).doSum(value);
                        sums.put(field, sum);
                    }
                } else {
                    ((CalcSumDouble)sum).doSum(value);
                }
            }

            void tallyFloat(String field, Float value) {
                tallyDouble(field, value.doubleValue());
            }

            void tallyDouble(String field, Double value) {
                CalcSum<?> sum = sums.get(field);
                if (sum == null) {
                    sum = new CalcSumDouble(value);
                    sums.put(field, sum);
                } else {
                    ((CalcSumDouble)sum).doSum(value);
                }
            }
        }

        /**
         * Abstract CalcSum class is to calculates a sum generically
         * and count the values, the extending classes should implements
         * the methods: zero(), add(Number v) and valueOf(Number v).
         */
        static abstract class CalcSum<T extends Number> {
            private int count;
            private T sum;

            CalcSum() {
                sum = zero();
                count = 0;
            }

            CalcSum(Number value) {
                sum = valueOf(value);
                count = 1;
            }

            CalcSum(CalcSum<?> cs) {
                sum = valueOf(cs.getValue());
                this.count = cs.getCount();
            }

            void doSum(Number value) {
                sum = add(sum, value);
                count++;
            }

            T getValue() {
                return sum;
            }

            int getCount() {
                return count;
            }

            abstract T zero();
            abstract T valueOf(Number v);
            abstract T add(T v1, Number v2);
        }

        /**
         * CalsSumLong extends CalcSum to represent a sum operator, whose
         * result is Long value.
         */
        static class CalcSumLong extends CalcSum<Long> {

            CalcSumLong(Number value) {
                super(value);
            }

            CalcSumLong(CalcSum<?> sum) {
                super(sum);
            }

            @Override
            Long zero() {
                return 0L;
            }

            @Override
            Long valueOf(Number v) {
                return v.longValue();
            }

            @Override
            Long add(Long v1, Number v2) {
                return longAddAndCheck(v1.longValue(), v2.longValue());
            }

            /* Add two long integers, checking for overflow. */
            private long longAddAndCheck(long a, long b) {
                if (a > b) {
                    /* use symmetry to reduce boundary cases */
                    return longAddAndCheck(b, a);
                }
                /* assert a <= b */
                if (a < 0) {
                    if (b < 0) {
                        /* check for negative overflow */
                        if (Long.MIN_VALUE - b <= a) {
                            return a + b;
                        }
                        throw new ArithmeticException("Add failed: underflow");
                    }
                    /* Opposite sign addition is always safe */
                    return a + b;
                }
                /* check for positive overflow */
                if (a <= Long.MAX_VALUE - b) {
                    return a + b;
                }
                throw new ArithmeticException("Add failed: overflow");
            }
        }

        /**
         * CalsSumDouble extends CalcSum to represent a sum operator, whose
         * result is Double value.
         */
        static class CalcSumDouble extends CalcSum<Double> {

            CalcSumDouble(Number value) {
                super(value);
            }

            CalcSumDouble(CalcSum<?> sum) {
                super(sum);
            }

            @Override
            Double zero() {
                return 0.0d;
            }

            @Override
            Double valueOf(Number v) {
                return v.doubleValue();
            }

            @Override
            Double add(Double v1, Number v2) {
                return v1.doubleValue() + v2.doubleValue();
            }
        }
    }

    /**
     * Performs simple data aggregation operations on numeric fields.
     * Records found must be Avro for sum and avg to function properly.
     * Sum and avg match on any Avro recording containing the named numeric
     * fields.
     */
    static class AggregateKVSub extends AggregateSubCommand {
        final static String COMMAND_NAME = "kv";
        final static String KEY_FLAG = "-key";
        final static String KEY_FLAG_DESC = KEY_FLAG + " <key>";
        final static String START_FLAG_DESC = START_FLAG + " <prefixString>";
        final static String END_FLAG_DESC = END_FLAG + " <prefixString>";
        final static String SCHEMA_FLAG = "-schema";
        final static String SCHEMA_FLAG_DESC = SCHEMA_FLAG + " <name>";

        final static String COMMAND_SYNTAX =
            "aggregate " + COMMAND_NAME + " " + genericFlags + eolt +
            "[" + SCHEMA_FLAG_DESC + "] [" + KEY_FLAG_DESC + "] " + eolt +
            "[" + START_FLAG_DESC + "] [" + END_FLAG_DESC +	"]";

        final static String COMMAND_DESCRIPTION =
            "Performs simple data aggregation operations on numeric fields." +
            eolt +
            COUNT_FLAG + " returns the count of matching records" + eolt +
            SUM_FLAG + " returns the sum of the values of matching " +
            "fields." + eolt +
            "     All records with the specified schema with the named field" +
            eolt +
            "     are matched.  Unmatched records are ignored." + eolt +
            AVG_FLAG + " returns the average of the values of matching " +
            "fields." + eolt +
            "     All records with the specified schema with the named field" +
            eolt +
            "     are matched.  Unmatched records are ignored." + eolt +
            SCHEMA_FLAG + " specifies the avro schema name." + eolt +
            KEY_FLAG + " specifies the key (prefix) to use." + eolt +
            START_FLAG + " and " + END_FLAG + " flags can be used for " +
            "restricting the range used" + eolt + "for iteration." ;

        private static final Type[] SCHEMA_NUMERIC_TYPES = {
            Type.INT, Type.LONG, Type.FLOAT, Type.DOUBLE
        };

        public AggregateKVSub() {
            super(COMMAND_NAME, 2);
        }

        @SuppressWarnings("deprecation")
        @Override
        void exec(String[] args, Shell shell, AggResult aggResult)
            throws ShellException {

            Shell.checkHelp(args, this);
            Key key = null;
            String rangeStart = null;
            String rangeEnd = null;
            String schemaName = null;
            KVStore store = ((CommandShell) shell).getStore();

            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if (KEY_FLAG.equals(arg)) {
                    String keyString = Shell.nextArg(args, i++, this);
                    try {
                        key = CommandUtils.createKeyFromURI(keyString);
                    } catch (IllegalArgumentException iae) {
                        invalidArgument(iae.getMessage());
                    }
                } else if (START_FLAG.equals(arg)) {
                    rangeStart = Shell.nextArg(args, i++, this);
                } else if (END_FLAG.equals(arg)) {
                    rangeEnd = Shell.nextArg(args, i++, this);
                } else if (SCHEMA_FLAG.equals(arg)) {
                    schemaName = Shell.nextArg(args, i++, this);
                } else {
                    i = checkGenericArg(shell, arg, args, i);
                }
            }

            validateAggArgs(shell);

            /* Initialize the KeyRange object if start or end is specified. */
            KeyRange kr = null;
            if (rangeStart != null || rangeEnd != null) {
                try {
                    kr = new KeyRange(rangeStart, true, rangeEnd, true);
                } catch (IllegalArgumentException iae) {
                    invalidArgument(iae.getMessage());
                }
            }

            List<String> fields = getAggFields();
            if (fields != null && schemaName == null) {
                shell.requiredArg(SCHEMA_FLAG, this);
            }

            oracle.kv.avro.JsonAvroBinding binding = null;
            if (schemaName != null) {
                binding = getAvroBinding(store, schemaName);
            }
            /* Perform aggregation operation. */
            execAgg(store, key, kr, fields, binding, aggResult);
        }

        @SuppressWarnings("deprecation")
        private oracle.kv.avro.JsonAvroBinding getAvroBinding(KVStore store,
                                                              String schemaName)
            throws ShellException {

            oracle.kv.avro.AvroCatalog catalog = store.getAvroCatalog();
            catalog.refreshSchemaCache(null);
            Map<String, Schema> schemaMap = catalog.getCurrentSchemas();
            Schema schema = schemaMap.get(schemaName);
            if (schema == null) {
                throw new ShellException("Schema does not exist or " +
                    "is disabled: " + schemaName);
            }
            try {
                return catalog.getJsonBinding(schema);
            } catch (oracle.kv.avro.UndefinedSchemaException  use) {
                throw new ShellException("Schema does not exist or " +
                    "is disabled: " + schemaName);
            }
        }

        /**
         * execAgg is the heart of this command, it iterates the matched
         * records and get specified field value and tally them.
         */
        @SuppressWarnings("deprecation")
        private void execAgg(KVStore store, Key key, KeyRange kr,
                             List<String> fields,
                             oracle.kv.avro.JsonAvroBinding binding,
                             AggResult aggResult)
            throws ShellException {

            Iterator<?> it = null;
            try {
                if (binding == null) {
                    /* Count only without schema specified, use keysIterator. */
                    if (keyContainsMinorPath(key)) {
                        /* There's a minor key path, use it to advantage */
                        it = store.multiGetKeysIterator(Direction.FORWARD,
                                                        100, key, kr, null);
                    } else {
                        /* A generic store iteration */
                        it = store.storeKeysIterator(Direction.UNORDERED, 100,
                                                     key, kr, null, null, 0,
                                                     null, getIteratorConfig());
                        if (!it.hasNext() && key != null) {
                            closeIterator(it);
                            /*
                             * A complete major path won't work with store
                             * iterator and we can't distinguish between
                             * a complete major path or not, so if store
                             * iterator fails entire, try the key as a
                             * complete major path.
                             */
                            it = store.multiGetKeysIterator(Direction.FORWARD,
                                                            100, key, kr, null);
                        }
                    }
                } else {
                    if (keyContainsMinorPath(key)) {
                        /* There's a minor key path, use it to advantage */
                        it = store.multiGetIterator(Direction.FORWARD,
                                                    100, key, kr, null);
                    } else {
                        /* A generic store iteration */
                        it = store.storeIterator(Direction.UNORDERED, 100,
                                                 key, kr, null, null, 0,
                                                 null, getIteratorConfig());
                        if (!it.hasNext() && key != null) {
                            closeIterator(it);
                            /*
                             * A complete major path won't work with store
                             * iterator and we can't distinguish between
                             * a complete major path or not, so if store
                             * iterator fails entire, try the key as a
                             * complete major path.
                             */
                            it = store.multiGetIterator(Direction.FORWARD,
                                                        100, key, kr, null);
                        }
                    }
                }

                while (it.hasNext()) {
                    if (binding != null) {
                        Value value = ((KeyValueVersion)it.next()).getValue();
                        oracle.kv.avro.JsonRecord jsonRec =
                            getJsonRec(binding, value);
                        if (jsonRec == null) {
                            continue;
                        }
                        if (fields != null) {
                            for (String field: fields) {
                                tallyFieldValue(aggResult, jsonRec, field);
                            }
                        }
                    } else {
                        it.next();
                    }
                    aggResult.tallyCount();
                }
            } catch (Exception e) {
                throw new ShellException(e.getMessage(), e);
            } finally {
                if (it != null) {
                    closeIterator(it);
                }
            }
        }

        private boolean keyContainsMinorPath(Key key) {
            return (key != null &&
                    key.getMinorPath() != null &&
                    key.getMinorPath().size() > 0);
        }

        private StoreIteratorConfig getIteratorConfig() {
            /**
             * Setting it to 0 lets the KV Client determine the number of
             * threads based on topology information.
             */
            return new StoreIteratorConfig().setMaxConcurrentRequests(0);
        }

        private void closeIterator(Iterator<?> iterator) {
            if (iterator instanceof ParallelScanIterator) {
                ((ParallelScanIterator<?>)iterator).close();
            }
        }

        @SuppressWarnings("deprecation")
        private oracle.kv.avro.JsonRecord getJsonRec(
            oracle.kv.avro.JsonAvroBinding binding,
            Value value) {

            if (value == null || value.getFormat() != Value.Format.AVRO) {
                return null;
            }
            try {
                return binding.toObject(value);
            } catch (oracle.kv.avro.SchemaNotAllowedException ignored) {
                /**
                 * Return null if specified schema is not the
                 * schema associated with the value.
                 */
            } catch (IllegalArgumentException ignored) {
                /* Return null if deserialization failed. */
            }
            return null;
        }

        @SuppressWarnings("deprecation")
        private void tallyFieldValue(AggResult aggResult,
                                     oracle.kv.avro.JsonRecord jsonRec,
                                     String field) {

            Type type = getScalarType(jsonRec.getSchema(), field);
            if (type == null) {
                return;
            }
            JsonNode jsonNode = jsonRec.getJsonNode().get(field);
            if (jsonNode.isNull() || !jsonNode.isNumber()) {
                return;
            }
            switch (type) {
            case INT:
                aggResult.tallyInt(field, jsonNode.getIntValue());
                break;
            case LONG:
                aggResult.tallyLong(field, jsonNode.getLongValue());
                break;
            case FLOAT:
            case DOUBLE:
                aggResult.tallyDouble(field, jsonNode.getDoubleValue());
                break;
            default:
                break;
            }
        }

        /* Check if the field is numeric type. */
        private Type getScalarType(Schema schema, String fieldName) {
            Field field = schema.getField(fieldName);
            if (field == null) {
                return null;
            }
            Type fieldType = field.schema().getType();
            for (Type type: SCHEMA_NUMERIC_TYPES){
                if (fieldType.equals(type)) {
                    return fieldType;
                }
            }
            return null;
        }

        @Override
        protected String getCommandSyntax() {
            return COMMAND_SYNTAX;
        }

        @Override
        protected String getCommandDescription() {
            return COMMAND_DESCRIPTION;
        }
    }

    /**
     * Performs simple data aggregation operations on numeric fields of
     * a table.
     */
    static class AggregateTableSub extends AggregateSubCommand {
        final static String COMMAND_NAME = "table";
        final static String TABLE_FLAG = "-name";
        final static String NAMESPACE_FLAG = "-namespace";
        final static String TABLE_FLAG_DESC = TABLE_FLAG + " <name>";
        final static String INDEX_FLAG = "-index";
        final static String INDEX_FLAG_DESC = INDEX_FLAG + " <name>";
        final static String FIELD_FLAG = "-field";
        final static String FIELD_FLAG_DESC = FIELD_FLAG + " <name>";
        final static String VALUE_FLAG = "-value";
        final static String VALUE_FLAG_DESC = VALUE_FLAG + " <value>";
        final static String START_FLAG_DESC = START_FLAG + " <value>";
        final static String END_FLAG_DESC = END_FLAG + " <value>";
        final static String JSON_FLAG = "-json";
        final static String JSON_FLAG_DESC = JSON_FLAG + " <string>";

        final static String COMMAND_SYNTAX =
            "aggregate " + COMMAND_NAME + " " + TABLE_FLAG_DESC + eolt +
            genericFlags + eolt +
            "[" + INDEX_FLAG_DESC + "] [" + FIELD_FLAG_DESC + " " +
            VALUE_FLAG_DESC + "]+" + eolt +
            "[" + FIELD_FLAG_DESC + " [" + START_FLAG_DESC + "] [" +
            END_FLAG_DESC + "]]" + eolt +
            "[" + JSON_FLAG_DESC + "]";

        final static String COMMAND_DESCRIPTION =
            "Performs simple data aggregation operations on numeric fields " +
            "of a table." + eolt +
            COUNT_FLAG + " returns the count of matching records." + eolt +
            SUM_FLAG + " returns the sum of the values of matching " +
            "fields." + eolt +
            AVG_FLAG + " returns the average of the values of matching " +
            "fields." + eolt +
            FIELD_FLAG + " and " + VALUE_FLAG + " pairs are used to " +
            "used to specify fields of the" + eolt + "primary key or " +
            "index key used for the operation.  If no fields are" + eolt +
            "specified an iteration of the entire table or index is " +
            "performed." + eolt +
            FIELD_FLAG + "," + START_FLAG + " and " + END_FLAG + " flags " +
            "can be used to define a value range for" + eolt +
            "the last field specified." +  eolt +
            JSON_FLAG + " indicates that the key field values are in " +
            "JSON format.";

        public AggregateTableSub() {
            super(COMMAND_NAME, 3);
        }

        @Override
        void exec(String[] args, Shell shell, AggResult aggResult)
            throws ShellException {

            Shell.checkHelp(args, this);

            String namespace = null;
            String tableName = null;
            HashMap<String, String> mapVals = new HashMap<String, String>();
            String frFieldName = null;
            String rgStart = null;
            String rgEnd = null;
            String indexName = null;
            String jsonString = null;

            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if (TABLE_FLAG.equals(arg)) {
                    tableName = Shell.nextArg(args, i++, this);
                } else if (FIELD_FLAG.equals(arg)) {
                    String fname = Shell.nextArg(args, i++, this);
                    if (++i < args.length) {
                        arg = args[i];
                        if (VALUE_FLAG.equals(arg)) {
                            String fVal = Shell.nextArg(args, i++, this);
                            mapVals.put(fname, fVal);
                        } else {
                            while (i < args.length) {
                                arg = args[i];
                                if (START_FLAG.equals(arg)) {
                                    rgStart = Shell.nextArg(args, i++, this);
                                } else if (END_FLAG.equals(arg)) {
                                    rgEnd = Shell.nextArg(args, i++, this);
                                } else {
                                    break;
                                }
                                i++;
                            }
                            if (rgStart == null && rgEnd == null) {
                                invalidArgument(arg + ", " +
                                    VALUE_FLAG + " or " +
                                    START_FLAG + " | " + END_FLAG +
                                    " is reqired");
                            }
                            frFieldName = fname;
                            i--;
                        }
                    } else {
                        shell.requiredArg(VALUE_FLAG + " or " +
                            START_FLAG + " | " + END_FLAG, this);
                    }
                } else if (NAMESPACE_FLAG.equals(arg)) {
                    namespace = Shell.nextArg(args, i++, this);
                } else if (INDEX_FLAG.equals(arg)) {
                    indexName = Shell.nextArg(args, i++, this);
                } else if (JSON_FLAG.equals(arg)) {
                    jsonString = Shell.nextArg(args, i++, this);
                } else {
                    i = checkGenericArg(shell, arg, args, i);
                }
            }

            if (tableName == null) {
                shell.requiredArg(TABLE_FLAG, this);
            }
            validateAggArgs(shell);

            final CommandShell cmdShell = (CommandShell) shell;
            final TableAPI tableImpl = cmdShell.getStore().getTableAPI();
            if (namespace == null) {
                namespace = cmdShell.getNamespace();
            }
            final Table table = CommandUtils.findTable(tableImpl,
                                                       namespace,
                                                       tableName);

            List<String> fields = getAggFields();
            if (fields != null) {
                for (String field: fields) {
                    if (table.getField(field) == null) {
                        invalidArgument("Field does not exist in " +
                                "table: " + field);
                    }
                }
            }
            /**
             * Create the key object.
             *  - Create the key from JSON string if it is specified.
             *  - Create key and set field values accordingly.
             */
            RecordValue key = null;
            if (jsonString != null) {
                key = CommandUtils.createKeyFromJson(table, indexName,
                                                     jsonString);
            } else {
                if (indexName == null) {
                    key = table.createPrimaryKey();
                } else {
                    key = CommandUtils.findIndex(table, indexName)
                          .createIndexKey();
                }
                /* Set field values to key. */
                for (Map.Entry<String, String> entry: mapVals.entrySet()) {
                    CommandUtils.putIndexKeyValues(key, entry.getKey(),
                                                   entry.getValue());
                }
            }

            /**
             * Initialize MultiRowOptions if -start and -end are specified
             * for a field.
             */
            MultiRowOptions mro = null;
            if (rgStart != null || rgEnd != null) {
                mro = CommandUtils.createMultiRowOptions(tableImpl,
                    table, key, null, null, frFieldName, rgStart, rgEnd);
            }

            /* Perform aggregation operation. */
            execAgg(tableImpl, key, mro, fields, aggResult);
        }

        /**
         * It iterates the matched rows, get value of specified fields with
         * numeric types and tally them.
         */
        private void execAgg(final TableAPI tableImpl,
                             final RecordValue key,
                             final MultiRowOptions mro,
                             final List<String> fields,
                             final AggResult aggResult)
            throws ShellException {

            new RunTableAPIOperation() {

                @Override
                void doOperation() throws ShellException {
                    TableIterator<?> iter = null;
                    try {
                        if (fields == null) {
                            if (key.isPrimaryKey()) {
                                iter = tableImpl.tableKeysIterator(
                                    key.asPrimaryKey(), mro, null);
                            } else {
                                iter = tableImpl.tableKeysIterator(
                                    key.asIndexKey(), mro, null);
                            }
                        } else {
                            if (key.isPrimaryKey()) {
                                iter = tableImpl.tableIterator(
                                    key.asPrimaryKey(), mro, null);
                            } else {
                                iter = tableImpl.tableIterator(
                                    key.asIndexKey(), mro, null);
                            }
                        }
                        while (iter.hasNext()) {
                            if (fields != null) {
                                Row row = (Row)iter.next();
                                for (String field: fields) {
                                    tallyFieldValue(aggResult, row, field);
                                }
                            } else {
                                iter.next();
                            }
                            aggResult.tallyCount();
                        }
                    } catch (StoreIteratorException sie) {
                        Throwable t = sie.getCause();
                        if (t != null && t instanceof FaultException) {
                            throw (FaultException)t;
                        }
                        throw new ShellException(
                            t != null ? t.getMessage() : sie.getMessage());
                    } finally {
                        if (iter != null) {
                            iter.close();
                        }
                    }
                }
            }.run();
        }

        private void tallyFieldValue(AggResult aggResult,
                                     Row row, String field) {
            FieldValue fv = row.get(field);
            if (fv.isNull()) {
                return;
            }
            if (fv.isInteger()) {
                aggResult.tallyInt(field, fv.asInteger().get());
            } else if (fv.isLong()) {
                aggResult.tallyLong(field, fv.asLong().get());
            } else if (fv.isFloat()) {
                aggResult.tallyFloat(field, fv.asFloat().get());
            } else if (fv.isDouble()) {
                aggResult.tallyDouble(field, fv.asDouble().get());
            }
        }

        @Override
        protected String getCommandSyntax() {
            return COMMAND_SYNTAX;
        }

        @Override
        protected String getCommandDescription() {
            return COMMAND_DESCRIPTION;
        }
    }
}
