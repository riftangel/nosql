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

package oracle.kv.impl.api.ops;

import static oracle.kv.impl.util.ObjectUtil.checkNull;
import static oracle.kv.impl.util.SerialVersion.TTL_SERIAL_VERSION;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.ReturnValueVersion;
import oracle.kv.ReturnValueVersion.Choice;
import oracle.kv.Value;
import oracle.kv.impl.api.lob.KVLargeObjectImpl;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.table.TimeToLive;

/**
 * A Put operation puts a value in the KV Store.
 * <br>
 * The operation is transmitted over the wire. Options related to operation
 * such as table identifier or TTL parameters are parts of wire format.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class Put extends SingleKeyOperation {

    /**
     * The value to write
     */
    private final RequestValue requestValue;

    /**
     * Whether to return previous value/version.
     */
    private final Choice prevValChoice;

    /**
     * Table operations include the table id.  0 means no table.
     */
    private final long tableId;

    private TimeToLive ttl;
    private boolean updateTTL;

    /**
     * Constructs a put operation.
     */
    public Put(byte[] keyBytes, Value value, Choice prevValChoice) {
        this(OpCode.PUT, keyBytes, value, prevValChoice, 0);
    }

    /**
     * Constructs a put operation with a table id.
     */
    public Put(byte[] keyBytes,
               Value value,
               Choice prevValChoice,
               long tableId) {
        this(OpCode.PUT, keyBytes, value, prevValChoice, tableId);
    }

    /**
     * Constructs a put operation with a table id and TTL-related arguments.
     */
    public Put(byte[] keyBytes,
            Value value,
            Choice prevValChoice,
            long tableId, TimeToLive ttl, boolean updateTTL) {
     this(OpCode.PUT, keyBytes, value, prevValChoice, tableId,
          ttl, updateTTL);
    }

    /**
     * For subclasses, allows passing OpCode.
     */
    Put(OpCode opCode,
        byte[] keyBytes,
        Value value,
        Choice prevValChoice,
        long tableId) {

        super(opCode, keyBytes);
        checkNull("prevValChoice", prevValChoice);
        this.requestValue = new RequestValue(value);
        this.prevValChoice = prevValChoice;
        this.tableId = tableId;
    }

    Put(OpCode opCode,
        byte[] keyBytes,
        Value value,
        Choice prevValChoice,
        long tableId,
        TimeToLive ttl, boolean updateTTL) {

        this(opCode, keyBytes, value, prevValChoice, tableId);
        this.ttl = ttl;
        this.updateTTL = updateTTL;
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    Put(DataInput in, short serialVersion)
        throws IOException {
        this(OpCode.PUT, in, serialVersion);
    }

    /**
     * For subclasses, allows passing OpCode.
     */
    Put(OpCode opCode, DataInput in, short serialVersion)
        throws IOException {

        super(opCode, in, serialVersion);

        requestValue = new RequestValue(in, serialVersion);
        checkNull("requestValue.bytes", requestValue.getBytes());
        prevValChoice = Choice.readFastExternal(in, serialVersion);

        /*
         * Read table id.  If there is no table the value is 0.
         */
        tableId = in.readLong();
        if (serialVersion >= TTL_SERIAL_VERSION) {
            ttl = TimeToLive.readFastExternal(in, serialVersion);
            updateTTL = in.readBoolean();
        }
    }

    public void setTTLOptions(TimeToLive ttl, boolean updateTTL) {
        this.ttl = ttl;
        this.updateTTL = updateTTL;
    }

    /**
     * Writes this object to the output stream.  Format for {@code
     * serialVersion} {@link SerialVersion#STD_UTF8_VERSION} or greater:
     * <ol>
     * <li> ({@link SingleKeyOperation}) {@code super}
     * <li> ({@link RequestValue}) {@link #getValueBytes requestValue}
     * <li> ({@link Choice}) {@link #getReturnValueVersionChoice prevValChoice}
     * <li> ({@link DataOutput#writeLong long}) {@link #getTableId tableId}
     * <li> ({@link #writeTimeToLive(DataOutput, short, TimeToLive, String)
     *      TimeToLive}) {@link #getTTL ttl}
     * <li> ({@link DataOutput#writeBoolean boolean}) {@link #getUpdateTTL
     *      updateTTL}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        requestValue.writeFastExternal(out, serialVersion);
        prevValChoice.writeFastExternal(out, serialVersion);

        /*
         * Write the table id.  If this is not a table operation the id will be
         * 0.
         */
        out.writeLong(tableId);
        if (serialVersion >= TTL_SERIAL_VERSION) {
            writeTimeToLive(out, serialVersion, ttl, getOpCode().toString());
            out.writeBoolean(updateTTL);
        }
    }

    /**
     * Gets the value to be put
     */
    public byte[] getValueBytes() {
        return requestValue.getBytes();
    }

    public ReturnValueVersion.Choice getReturnValueVersionChoice() {
        return prevValChoice;
    }

    @Override
    public boolean performsRead() {
        /* Puts that do not require a return do not incur a read */
        return !prevValChoice.equals(Choice.NONE);
    }

    @Override
    public boolean performsWrite() {
        assert !isDelete();
        return true;
    }

    /**
     * Returns the tableId, which is 0 if this is not a table operation.
     */
    @Override
    public long getTableId() {
        return tableId;
    }

    /**
     * Returns expiry duration
     */
    TimeToLive getTTL() {
        return ttl;
    }

    /**
     * Returns whether to update expiry
     */
    boolean getUpdateTTL() {
        return updateTTL;
    }

    @Override
    public byte[] checkLOBSuffix(byte[] lobSuffixBytes) {
        return KVLargeObjectImpl.hasLOBSuffix(getKeyBytes(), lobSuffixBytes) ?
            getKeyBytes() :
            null;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toString());
        if (tableId != 0) {
            sb.append(" Table Id ");
            sb.append(tableId);
        }
        sb.append(" Value: ");
        sb.append(requestValue);

        return sb.toString();
    }
}
