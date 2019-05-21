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

import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import oracle.kv.impl.query.compiler.EscapeUtil;

/**
 * This class contains the parsed representation of a stringified path to a
 * potentially nested field inside data (a FieldValue) or the table schema
 * (a FieldDef).
 *
 * fieldMap:
 * The FieldMap of the containing object that provides context for navigations.
 * In most cases it will be the FieldMap associated with a TableImpl. In some
 * cases it is the FieldMap of a RecordValueImpl.
 *
 * steps:
 * List containing parsed info for each step of the path. The steps include
 * identifiers, quoted strings, and the "special" steps keys(), values(),
 * and []. See method parsePathName() for the details of how a stringified
 * path is parsed to create this list.
 *
 * pathName:
 * The full string name of the path. It is computed on-the-fly when needed (see
 * getPathName() method). The string has the "external" format, i.e., it looks
 * like a DML path expr. Specifically: 
 * - it uses .keys() for the keys of map fields,
 * - it uses .values() for the values of map fields,
 * - it uses [] for the values of array fields.
 *
 * isComplex:
 * True if this is a complex path.
 */
public class TablePath {

    public static enum StepKind {
        BRACKETS,
        VALUES,
        KEYS,
        REC_FIELD,
        MAP_FIELD
    }

    static class StepInfo {
        String step;
        StepKind kind;
        boolean isQuoted;

        StepInfo(String step, boolean isQuoted) {
            this.step = step;
            this.isQuoted = isQuoted;

            if (!isQuoted) {
                if (step.equals(TableImpl.BRACKETS)) {
                    kind = StepKind.BRACKETS;
                } else if (step.equalsIgnoreCase(TableImpl.VALUES)) {
                    kind = StepKind.VALUES;
                } else if (step.equalsIgnoreCase(TableImpl.KEYS)) {
                    kind = StepKind.KEYS;
                } else {
                    kind = StepKind.REC_FIELD;
                }
            } else {
                kind = StepKind.REC_FIELD;
            }

            /*
             * Note: if kind is set to REC_FIELD_NAME, it may actually be
             * MAP_FIELD_NAME instead. This is not known at construction
             * time. The code that constructs this TableInfo is responsible
             * for resetting the kind later, if needed.
             */
        }

        StepInfo(StepInfo si) {
            step = si.step;
            kind = si.kind;
            isQuoted = si.isQuoted;
        }
    }
 
    final private FieldMap fieldMap;

    private List<StepInfo> steps;

    private boolean isComplex;

    public TablePath(TableImpl table, String path) {
        this(table.getFieldMap(), path);
    }

    protected TablePath(FieldMap fieldMap, String path) {

        this.fieldMap = fieldMap;

        if (path != null) {
            steps = parsePathName(path);
        } else {
            steps = new ArrayList<StepInfo>();
        }

        isComplex = (steps.size() > 1);
    }

    /*
     * This constructor set the isQuoted flag of each step to false.
     * Currently this is OK, because all callers of this constructor
     * are not using quoted path steps.
     */
    public TablePath(FieldMap fieldMap, List<String> steps) {
        this.fieldMap = fieldMap;
        this.steps = new ArrayList<StepInfo>(steps.size());
        for (int i = 0; i < steps.size(); ++i) {
            this.steps.add(new StepInfo(steps.get(i), false));
        }
        isComplex = (steps.size() > 1);
    }

    @Override
    public String toString() {
        return getPathName(steps.size());
    }

    @Override
    public int hashCode() {
        return getPathName(steps.size()).hashCode();
    }

    /*
     * Comparing the steps in order is sufficient to distinguish TablePath
     * instances. Comparisons are never made across tables, and all paths
     * within the same table are unique.
     */
    @Override
    public boolean equals(Object o) {

        if (!(o instanceof TablePath)) {
            return false;
        }

        TablePath other = (TablePath)o;

        if (steps.size() != other.steps.size()) {
            return false;
        }

        for (int i = 0; i < steps.size(); ++i) {

            StepInfo si1 = steps.get(i);
            StepInfo si2 = other.steps.get(i);

            if (si1.kind == StepKind.MAP_FIELD) {
                if (!si1.step.equals(si2.step)) {
                    return false;
                }
            } else {
                if (!si1.step.equalsIgnoreCase(si2.step)) {
                    return false;
                }
            }

            if (si1.kind != si2.kind) {
                return false;
            }
        }

        return true;
    }

    public void clear() {
        steps.clear();
        isComplex = false;
    }

    final FieldMap getFieldMap() {
        return fieldMap;
    }

    public final boolean isComplex() {
        return isComplex;
    }

    public boolean isEmpty() {
        return steps.isEmpty();
    }

    public int numSteps() {
        return steps.size();
    }

    final StepInfo getStepInfo(int pos) {
        return steps.get(pos);
    }

    final void addStepInfo(StepInfo si) {
        steps.add(si);
        if (steps.size() > 1) {
            isComplex = true;
        }
    }

    public final String getStep(int pos) {
        return steps.get(pos).step;
    }

    final void setStep(int pos, String step) {
        steps.get(pos).step = step;
    }

    final void add(int pos, String step, boolean isQuoted) {
        steps.add(pos, new StepInfo(step, isQuoted));
        if (steps.size() > 1) {
            isComplex = true;
        }
    }

    public final String remove(int pos) {
        StepInfo si = steps.remove(pos);
        if (steps.size() <= 1) {
            isComplex = false;
        }
        return si.step;
    }

    public StepKind getStepKind(int pos) {
        return steps.get(pos).kind;
    }

    public boolean isMapKeyStep(int pos) {
        return steps.get(pos).kind == StepKind.MAP_FIELD;
    }

    void setIsMapKeyStep(int pos) {
        steps.get(pos).kind = StepKind.MAP_FIELD;
    }

    public boolean isKeysStep(int pos) {
        return steps.get(pos).kind == StepKind.KEYS;
    }

    public boolean isValuesStep(int pos) {
        return steps.get(pos).kind == StepKind.VALUES;
    }

    void setIsValuesStep(int pos) {
        StepInfo si = steps.get(pos);
        si.kind = StepKind.VALUES;
        si.step = TableImpl.VALUES;
        si.isQuoted = false;
    }

    public boolean isBracketsStep(int pos) {
        return steps.get(pos).kind == StepKind.BRACKETS;
    }

    public void reverseSteps() {
        Collections.reverse(steps);
    }

    public final String getLastStep() {
        return steps.get(steps.size() - 1).step;
    }

    /**
     * Returns the FieldDef associated with the first (and maybe only)
     * component of the field.
     */
    public FieldDefImpl getFirstDef() {
        return fieldMap.getFieldDef(steps.get(0).step);
    }

    /**
     * Returns a list of field names components in a complex field name,
     * or a single name if the field name is not complex (this is for
     * simplicity in use).
     */
    static List<StepInfo> parsePathName(String pathname) {

        List<StepInfo> list = new ArrayList<StepInfo>();
        StringBuilder sb = new StringBuilder();
        boolean inQuotedStep = false;
        boolean isQuoted = false;
        boolean done = false;

        for (int i = 0; i < pathname.length(); ++i) {

            char ch = pathname.charAt(i);

            /*
             * We must keep track of steps that appear as quoted strings.
             * Such steps are used primarily for paths that cross a specific
             * entry of a map; the entry is identified by its field name, which
             * can be any string. For such steps, the delimiting quotes must be
             * removed before adding them to the list (because the quotes are
             * also removed from the map data). However, we must remember
             * whether a step was a quoted string in order to compare TablePaths
             * correctly (for example, we must be able to recognize that the
             * steps ."values()" and .values() are not the same).
             */
            if (ch == '"') {
                if (inQuotedStep) {
                    if (i > 0 && pathname.charAt(i-1) == '\\') {
                        sb.append(ch);
                    } else {
                        inQuotedStep = false;
                    }
                } else {
                    inQuotedStep = true;
                    isQuoted = true;
                }
                continue;
            }

            if (!inQuotedStep && (ch == '.' || ch == '[')) {

                /*
                 * We just finished parsing a step and we are about to start
                 * parsing the next step. Create a StepInfo for the just
                 * finished step and add it to the list.
                 */
                String step = sb.toString();
                step = EscapeUtil.inlineEscapedChars(step);

                list.add(new StepInfo(step, isQuoted));
                sb.delete(0, sb.length());
                isQuoted = false;

                /*
                 * If the next step starts with '[', it must be a brackets
                 * step ([]). We process this step right here, i.e. we create
                 * a StepInfo for it, add it to the list, and proceed to the
                 * next step, if any.
                 */
                if (ch == '[') {
                    ++i;
                    if (i == pathname.length() || pathname.charAt(i) != ']') {
                        throw new IllegalArgumentException(
                            "Malformed path name: " + pathname);
                    }

                    list.add(new StepInfo("[]", false));
                    ++i;
                    if (i == pathname.length()) {
                        done = true;
                    }
                }
            } else {
                sb.append(ch);
            }
        }

        if (!done) {
            String step = sb.toString();
            step = EscapeUtil.inlineEscapedChars(step);
            list.add(new StepInfo(step, isQuoted));
        }

        return list;
    }

    public final String getPathName() {
        return getPathName(steps.size());
    }

    public final String getPathName(int pos) {

        StringBuilder sb = new StringBuilder();

        int numSteps = steps.size();

        for (int i = 0; i < pos; ++i) {

            StepInfo si = steps.get(i);

            /* Delete the dot that was added after the previous step */
            if (isBracketsStep(i)) {
                sb.delete(sb.length() - 1, sb.length());
            }

            if (si.isQuoted) {
                sb.append("\"").append(si.step).append("\"");
            } else {
                sb.append(si.step);
            }

            if (i < numSteps - 1) {
                sb.append(TableImpl.SEPARATOR);
            }
        }

        return sb.toString();
    }
}
