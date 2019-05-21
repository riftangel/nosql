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

package oracle.kv.impl.query.runtime;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import oracle.kv.table.FieldValue;
import oracle.kv.impl.api.table.ArrayValueImpl;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.MapValueImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.api.table.RecordValueImpl;

import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.Expr.UpdateKind;
import oracle.kv.impl.query.compiler.QueryFormatter;

/**
 * theUpdateKind:
 * The update kind (one of SET, ADD, PUT, or REMOVE)
 *
 * theInputIter:
 * Computes the target expression
 *
 * thePosIter:
 * Computes the position expression of an ADD clause, if present.
 *
 * theNewValueIter:
 * Computes the replacement values, in case of SET, or the new values, in
 * case of ADD/PUT. It's null for REMOVE.
 *
 * theTargetItemReg:
 * The register to store the value of the $ variable.
 */
public class UpdateFieldIter extends PlanIter {

    static private class UpdateFieldState extends PlanIterState {

        ParentItemContext theParentItemContext;

        final ArrayList<String> theKeysToRemove;

        final ArrayList<Integer> thePositionsToRemove;

        UpdateFieldState() {
            theParentItemContext = new ParentItemContext();
            theKeysToRemove = new ArrayList<String>(32);
            thePositionsToRemove = new ArrayList<Integer>(32);
        }

        @Override
        public void reset(PlanIter iter) {
            super.reset(iter);
            theParentItemContext.reset();
            theKeysToRemove.clear();
            thePositionsToRemove.clear();
        }
    }

    private final UpdateKind theUpdateKind;

    private final PlanIter theInputIter;

    private final PlanIter thePosIter;

    private final PlanIter theNewValueIter;

    private final int theTargetItemReg;

    private final boolean theCloneNewValues;

    public UpdateFieldIter(
        Expr e,
        UpdateKind kind,
        PlanIter inputIter,
        PlanIter posIter,
        PlanIter newValueIter,
        int targetItemReg,
        boolean cloneNewValues) {

        super(e, -1);
        theUpdateKind = kind;
        theInputIter = inputIter;
        thePosIter = posIter;
        theNewValueIter = newValueIter;
        theTargetItemReg = targetItemReg;
        theCloneNewValues = cloneNewValues;
    }

    public UpdateFieldIter(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        short ordinal = readOrdinal(in, UpdateKind.values().length);
        theUpdateKind = UpdateKind.values()[ordinal];
        theInputIter = deserializeIter(in, serialVersion);
        thePosIter = deserializeIter(in, serialVersion);
        theNewValueIter = deserializeIter(in, serialVersion);
        theTargetItemReg = readPositiveInt(in, true);
        theCloneNewValues = in.readBoolean();
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        out.writeShort(theUpdateKind.ordinal());
        serializeIter(theInputIter, out, serialVersion);
        serializeIter(thePosIter, out, serialVersion);
        serializeIter(theNewValueIter, out, serialVersion);
        out.writeInt(theTargetItemReg);
        out.writeBoolean(theCloneNewValues);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.UPDATE_FIELD;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new UpdateFieldState());
        theInputIter.open(rcb);
        if (thePosIter != null) {
            thePosIter.open(rcb);
        }
        if (theNewValueIter != null) {
            theNewValueIter.open(rcb);
        }
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        theInputIter.close(rcb);
        if (thePosIter != null) {
            thePosIter.close(rcb);
        }
        if (theNewValueIter != null) {
            theNewValueIter.close(rcb);
        }
        state.close();
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {
        theInputIter.reset(rcb);
        if (thePosIter != null) {
            thePosIter.reset(rcb);
        }
        if (theNewValueIter != null) {
            theNewValueIter.reset(rcb);
        }
        PlanIterState state = rcb.getState(theStatePos);
        state.reset(this);
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        UpdateFieldState state = (UpdateFieldState)rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        boolean more = theInputIter.next(rcb);

        if (!more) {
            state.done();
            return false;
        }

        boolean updated = false;

        while (more) {

            int inputReg = theInputIter.getResultReg();
            FieldValueImpl targetItem = rcb.getRegVal(inputReg);

            switch (theUpdateKind) {
            case SET: {
                if (doSet(rcb, state, targetItem)) {
                    updated = true;
                }
                break;
            }
            case ADD: {
                if (doAdd(rcb, targetItem)) {
                    updated = true;
                }
                break;
            }
            case PUT: {
                if (doPut(rcb, targetItem)) {
                    updated = true;
                }
                break;
            }
            case REMOVE: {
                /*
                 * For each parent item, we must collect all the items to
                 * remove, before actually removing them. This is because
                 * removing the items immediately would invalide the
                 * "iterator" in the path expr that produces these items.
                 */
                FieldValueImpl savedParentItem =
                    state.theParentItemContext.theParentItem;

                theInputIter.getParentItemContext(rcb,
                                                  state.theParentItemContext);

                FieldValueImpl parentItem =
                    state.theParentItemContext.theParentItem;
                int targetPos = state.theParentItemContext.theTargetPos;
                String targetKey = state.theParentItemContext.theTargetKey;

                if (rcb.getTraceLevel() >= 3) {
                    rcb.trace("Removing item :\n" + targetItem +
                              "\nfrom parent item :\n" + parentItem);
                }

                if (parentItem.isRecord()) {
                    throw new QueryException(
                        "Cannot remove fields from records.\n" +
                        "Field " + targetKey + "\nRecord:\n" + parentItem,
                        theLocation);
                }

                if (parentItem.isNull()) {
                    continue;
                }

                if (savedParentItem == null ||
                    savedParentItem == parentItem) {

                    if (targetKey != null) {
                        assert(parentItem.isMap());
                        state.theKeysToRemove.add(targetKey);
                    } else {
                        assert(parentItem.isArray());
                        state.thePositionsToRemove.add(targetPos);
                    }

                } else {
                    if (doRemove(state, savedParentItem)) {
                        updated = true;
                    }

                    state.theKeysToRemove.clear();
                    state.thePositionsToRemove.clear();

                    if (targetKey != null) {
                        assert(parentItem.isMap());
                        state.theKeysToRemove.add(targetKey);
                    } else {
                        assert(parentItem.isArray());
                        state.thePositionsToRemove.add(targetPos);
                    }
                }

                break;
            }
            default:
                throw new QueryStateException(
                    "Unexpected kind of update clause: " + theUpdateKind);
            }

            more = theInputIter.next(rcb);
        }

        if (theUpdateKind == UpdateKind.REMOVE &&
            state.theParentItemContext.theParentItem != null) {
            if (doRemove(state, state.theParentItemContext.theParentItem)) {
                updated = true;
            }

        }

        state.done();
        return updated;
    }


    boolean doSet(
        RuntimeControlBlock rcb,
        UpdateFieldState state,
        FieldValueImpl targetItem) {

        theInputIter.getParentItemContext(rcb, state.theParentItemContext);

        FieldValueImpl parentItem = state.theParentItemContext.theParentItem;
        int targetPos = state.theParentItemContext.theTargetPos;
        String targetKey = state.theParentItemContext.theTargetKey;

        if (parentItem.isNull()) {
            return false;
        }

        if (theTargetItemReg >= 0) {
            rcb.setRegVal(theTargetItemReg, targetItem);
        }

        /*
         * No need to call theNewValueIter.next(rcb) more than once, because
         * the new-value expr is wrapped by a conditional array constructor
         */
        boolean more = theNewValueIter.next(rcb);

        if (!more) {
            theNewValueIter.reset(rcb);
            return false;
        }

        int inputReg = theNewValueIter.getResultReg();
        FieldValueImpl newTargetItem = rcb.getRegVal(inputReg);

        if (theCloneNewValues && !newTargetItem.isAtomic()) {
            newTargetItem = newTargetItem.clone();
        }

        if (rcb.getTraceLevel() >= 1) {
            rcb.trace("SET:\nParentItem =\n" + parentItem + "\nTargetItem:\n" +
                      targetItem + "\nNewValue:\n" + newTargetItem +
                      "\ntarget pos = " + targetPos);
        }

        try {
            switch (parentItem.getType()) {
            case RECORD: {
                RecordValueImpl rec = (RecordValueImpl)parentItem;
                FieldDefImpl targetType;

                if (newTargetItem.isJsonNull()) {
                    newTargetItem = NullValueImpl.getInstance();
                }

                if (targetPos >= 0) {
                    targetType = rec.getFieldDef(targetPos);
                } else {
                    targetType = rec.getFieldDef(targetKey);
                }

                newTargetItem = CastIter.castValue(newTargetItem,
                                                   targetType,
                                                   theLocation);

                if (targetPos >= 0) {
                    rec.put(targetPos, newTargetItem);
                } else {
                    rec.put(targetKey, newTargetItem);
                }

                if (rcb.getTraceLevel() >= 1) {
                    rcb.trace("SET DONE:\nParentItem after update =\n" +
                              parentItem);
                }
                break;
            }
            case MAP: {
                MapValueImpl map = (MapValueImpl)parentItem;

                FieldDefImpl targetType = map.getElementDef();

                newTargetItem = CastIter.castValue(newTargetItem,
                                                   targetType,
                                                   theLocation);
                map.put(targetKey, newTargetItem);
                break;
            }
            case ARRAY: {
                ArrayValueImpl arr = (ArrayValueImpl)parentItem;
                FieldDefImpl targetType = arr.getElementDef();

                newTargetItem = CastIter.castValue(newTargetItem,
                                                   targetType,
                                                   theLocation);

                arr.set(targetPos, newTargetItem);
                break;
            }
            default:
                throw new QueryStateException(
                    "Field to SET is not contained in a record, map, or " +
                    "array");
            }
        } catch (IllegalArgumentException e) {
            throw new QueryException(
                "SET operation failed. Cause: " + e.getMessage(),
                theLocation);
        }

        theNewValueIter.reset(rcb);
        return true;
    }

    boolean doAdd(RuntimeControlBlock rcb, FieldValueImpl targetItem) {

        if (!targetItem.isArray()) {
            return false;
        }

        ArrayValueImpl arr = (ArrayValueImpl)targetItem;
        FieldDefImpl elemDef = arr.getElementDef();

        if (theTargetItemReg >= 0) {
            rcb.setRegVal(theTargetItemReg, targetItem);
        }

        int pos = -1;

        if (thePosIter != null) {

            boolean more = thePosIter.next(rcb);

            if (more) {
                FieldValueImpl posVal =
                    rcb.getRegVal(thePosIter.getResultReg());

                if (!posVal.isNumeric()) {
                    throw new QueryException(
                        "ADD operation failed. Cause: The position " +
                        "expression does not return a numeric item",
                        theLocation);
                }

                if (thePosIter.next(rcb)) {
                    throw new QueryException(
                        "ADD operation failed. Cause: The position " +
                        "expression returns more than one items",
                        theLocation);
                }

                if (!posVal.isInteger()) {
                    posVal = CastIter.castValue(posVal,
                                                FieldDefImpl.integerDef,
                                                theLocation);
                }

                pos = posVal.getInt();

                if (pos < 0) {
                    pos = 0;
                }

                if (pos >= arr.size()) {
                    pos = -1;
                }
            }
        }

        if (rcb.getTraceLevel() >= 2) {
            rcb.trace("Adding item at position " + pos + " of array\n" + arr);
        }

        boolean more = theNewValueIter.next(rcb);

        if (!more) {
            theNewValueIter.reset(rcb);
            return false;
        }

        /*
         * The new value expr may be referencing the target array (e.g., via
         * the $ var). So, we must collect all the new elements from the new
         * value expr BEFORE updating the target array. Otherwise, if we
         * update the target array immediately, an iterator over the target
         * array that is maintained by the new value expr will be
         * invalidated, and as a result, calling theNewValueIter.next(rcb)
         * below may fail or result in an erroneous addition.
         */
        ArrayList<FieldValueImpl> values = new ArrayList<FieldValueImpl>();

        while (more) {

            FieldValueImpl val = rcb.getRegVal(theNewValueIter.getResultReg());

            if (theCloneNewValues && !val.isAtomic()) {
                val = val.clone();
            }
            val = CastIter.castValue(val, elemDef, theLocation);

            if (rcb.getTraceLevel() >= 2) {
                rcb.trace("Item to add:\n" + val);
            }

            values.add(val);

            more = theNewValueIter.next(rcb);
        }

        try {
            for (int i = 0; i < values.size(); ++i) {
                if (pos < 0) {
                    arr.add(values.get(i));
                } else {
                    arr.add(pos, values.get(i));
                    ++pos;
                }
            }

            if (rcb.getTraceLevel() >= 2) {
                rcb.trace("Target array after update:\n" + arr);
            }

        } catch (IllegalArgumentException e) {
            throw new QueryException(
                "ADD operation failed. Cause: " + e.getMessage(),
                theLocation);
        }

        if (thePosIter != null) {
            thePosIter.reset(rcb);
        }

        theNewValueIter.reset(rcb);
        return true;
    }

    boolean doPut(RuntimeControlBlock rcb, FieldValueImpl targetItem) {

        if (!targetItem.isMap()) {
            return false;
        }

        MapValueImpl map = (MapValueImpl)targetItem;
        FieldDefImpl elemDef = map.getElementDef();

        if (theTargetItemReg >= 0) {
            rcb.setRegVal(theTargetItemReg, targetItem);
        }

        boolean more = theNewValueIter.next(rcb);

        if (!more) {
            theNewValueIter.reset(rcb);
            return false;
        }

        /*
         * The new value expr may be referencing the target map (e.g., via the
         * $ var). So, we must collect all the new fields from the new
         * value expr BEFORE updating the target map. Otherwise, if we
         * update the target map immediately, an iterator over the target
         * map that is maintained by the new value expr will be invalidated,
         * and as a result, calling theNewValueIter.next(rcb) below will fail.
         */
        ArrayList<String> keys = new ArrayList<String>();
        ArrayList<FieldValueImpl> values = new ArrayList<FieldValueImpl>();

        while (more) {

            FieldValueImpl val = rcb.getRegVal(theNewValueIter.getResultReg());

            if (val.isMap()) {
                MapValueImpl fromMap = (MapValueImpl)val;

                for (Map.Entry<String, FieldValue> entry :
                         fromMap.getFields().entrySet()) {

                    String fkey = entry.getKey();
                    FieldValueImpl fval = (FieldValueImpl)entry.getValue();
                    if (theCloneNewValues && !fval.isAtomic()) {
                        fval = fval.clone();
                    }
                    fval = CastIter.castValue(fval, elemDef, theLocation);

                    keys.add(fkey);
                    values.add(fval);
                }

            } else if (val.isRecord()) {
                RecordValueImpl fromRec = (RecordValueImpl)val;
                int numFields = fromRec.getNumFields();

                for (int i = 0; i < numFields; ++i) {
                    String fkey = fromRec.getFieldName(i);
                    FieldValueImpl fval = fromRec.get(i);
                    if (!fval.isAtomic()) {
                        fval = fval.clone();
                    }
                    fval = CastIter.castValue(fval, elemDef, theLocation);

                    keys.add(fkey);
                    values.add(fval);
                }
            }

            more = theNewValueIter.next(rcb);
        }

        theNewValueIter.reset(rcb);

        try {
            for (int i = 0; i < keys.size(); ++i) {
                map.put(keys.get(i), values.get(i));
            }
        } catch (IllegalArgumentException e) {
            throw new QueryException(
                "PUT operation failed. Cause: " + e.getMessage(),
                theLocation);
        }

        return !keys.isEmpty();
    }

    boolean doRemove(
        UpdateFieldState state,
        FieldValueImpl parentItem) {

        if (parentItem.isMap()) {
            MapValueImpl map = (MapValueImpl)parentItem;

            if (state.theKeysToRemove.isEmpty()) {
                return false;
            }

            for (String key : state.theKeysToRemove) {
                map.remove(key);
            }
        } else {
            ArrayValueImpl arr = (ArrayValueImpl)parentItem;

            int numRemoved = 0;

            for (Integer pos : state.thePositionsToRemove) {
                int adjustedPos = pos.intValue() - numRemoved;
                arr.remove(adjustedPos);
                ++numRemoved;
            }

            if (numRemoved == 0) {
                return false;
            }
        }

        return true;
    }

    @Override
    protected void displayContent(StringBuilder sb, QueryFormatter formatter) {

        theInputIter.display(sb, formatter);

        if (thePosIter != null) {
            sb.append(",\n");
            thePosIter.display(sb, formatter);
        }

        if (theTargetItemReg >= 0) {
            sb.append(",\n");
            formatter.indent(sb);
            sb.append("targetItemReg : ").append(theTargetItemReg);
        }

        if (theCloneNewValues) {
            sb.append(",\n");
            formatter.indent(sb);
            sb.append("cloneNewValues : ").append(theCloneNewValues);
        }

        if (theNewValueIter != null) {
            sb.append(",\n");
            theNewValueIter.display(sb, formatter);
        }
    }

    @Override
    void displayName(StringBuilder sb) {
        sb.append(theUpdateKind);
    }
}
