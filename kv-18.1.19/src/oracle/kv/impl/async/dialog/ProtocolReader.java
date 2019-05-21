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

package oracle.kv.impl.async.dialog;

import java.util.Arrays;

import oracle.kv.impl.async.BytesUtil;

public class ProtocolReader {

    static final String ERROR_UNKNOWN_IDENTIFIER = "Unknown identifier: ";
    static final String ERROR_INVALID_MAGIC_NUMBER = "Invalid magic number: ";
    static final String ERROR_INVALID_FIELD = "Invalid field value: ";
    static final String ERROR_MAX_LENGTH_EXCEEDED = "Max length exceeded: ";

    private final ChannelInput input;
    private final int maxMesgLen;

    public ProtocolReader(ChannelInput input, int maxLength) {
        this.input = input;
        this.maxMesgLen = maxLength;
    }

    /**
     * Reads a message from ChannelInput.
     *
     * @return the message, null if not enough data
     */
    public ProtocolMesg read() {
        if (input.readableBytes() < 1) {
            return null;
        }
        input.mark();
        byte ident = input.readByte();
        switch (ProtocolMesg.getType(ident)) {
        case ProtocolMesg.PROTOCOL_VERSION_MESG:
            return readProtocolVersion();
        case ProtocolMesg.PROTOCOL_VERSION_RESPONSE_MESG:
            return readProtocolVersionResponse();
        case ProtocolMesg.CONNECTION_CONFIG_MESG:
            return readConnectionConfig();
        case ProtocolMesg.CONNECTION_CONFIG_RESPONSE_MESG:
            return readConnectionConfigResponse();
        case ProtocolMesg.NO_OPERATION_MESG:
            return readNoOperation();
        case ProtocolMesg.CONNECTION_ABORT_MESG:
            return readConnectionAbort();
        case ProtocolMesg.PING_MESG:
            return readPing();
        case ProtocolMesg.PINGACK_MESG:
            return readPingAck();
        case ProtocolMesg.DIALOG_START_MESG:
            return readDialogStart(ident);
        case ProtocolMesg.DIALOG_FRAME_MESG:
            return readDialogFrame(ident);
        case ProtocolMesg.DIALOG_ABORT_MESG:
            return readDialogAbort();
        default:
            throw new ProtocolViolationException(
                    false,
                    String.format("%s%x",
                        ProtocolViolationException.
                        ERROR_UNKNOWN_IDENTIFIER,
                        ident));
        }
    }

    private ProtocolMesg readProtocolVersion() {
        /* At least 4 bytes with magic number and a packed long */
        if (input.readableBytes() < 4) {
            input.reset();
            return null;
        }

        byte[] magicNumber = new byte[4];
        magicNumber[0] = (byte) 0x01;
        magicNumber[1] = input.readByte();
        magicNumber[2] = input.readByte();
        magicNumber[3] = input.readByte();
        if (!Arrays.equals(magicNumber, ProtocolMesg.MAGIC_NUMBER)) {
            throw new ProtocolViolationException(
                    false,
                    String.format(
                        "%sExpected %s, but got %s",
                        ProtocolViolationException.
                        ERROR_INVALID_MAGIC_NUMBER,
                        BytesUtil.toString(magicNumber, 1, 3),
                        BytesUtil.toString(ProtocolMesg.MAGIC_NUMBER, 1, 3)));
        }

        if (!input.canReadPackedLong()) {
            input.reset();
            return null;
        }

        return new ProtocolMesg.ProtocolVersion((int) input.readPackedLong());
    }

    private ProtocolMesg readProtocolVersionResponse() {
        /* At least 4 bytes with magic number and a packed long */
        if (input.readableBytes() < 4) {
            input.reset();
            return null;
        }

        byte[] magicNumber = new byte[4];
        magicNumber[0] = (byte) 0x01;
        magicNumber[1] = input.readByte();
        magicNumber[2] = input.readByte();
        magicNumber[3] = input.readByte();
        if (!Arrays.equals(magicNumber, ProtocolMesg.MAGIC_NUMBER)) {
            throw new ProtocolViolationException(
                    false,
                    String.format(
                        "%sExpected %s, but got %s",
                        ProtocolViolationException.
                        ERROR_INVALID_MAGIC_NUMBER,
                        BytesUtil.toString(magicNumber, 1, 3),
                        BytesUtil.toString(ProtocolMesg.MAGIC_NUMBER, 1, 3)));
        }

        if (!input.canReadPackedLong()) {
            input.reset();
            return null;
        }
        return new ProtocolMesg.ProtocolVersionResponse(
                (int) input.readPackedLong());
    }

    private ProtocolMesg readConnectionConfig() {
        /* At least 5 bytes with 5 packed long */
        if (input.readableBytes() < 5) {
            input.reset();
            return null;
        }
        if (!input.canReadPackedLong()) {
            input.reset();
            return null;
        }
        long uuid = input.readPackedLong();
        if (!input.canReadPackedLong()) {
            input.reset();
            return null;
        }
        long maxDialogs = input.readPackedLong();
        if (!input.canReadPackedLong()) {
            input.reset();
            return null;
        }
        long maxLength = input.readPackedLong();
        if (!input.canReadPackedLong()) {
            input.reset();
            return null;
        }
        long maxTotLen = input.readPackedLong();
        if (!input.canReadPackedLong()) {
            input.reset();
            return null;
        }
        long interval = input.readPackedLong();

        if ((maxDialogs <= 0) ||
            (maxLength <= 0) ||
            (maxTotLen <= 0) ||
            (interval <= 0)) {
            throw new ProtocolViolationException(
                    false,
                    String.format(
                        "%s Received ConnectionConfig(" +
                        "uuid=%x " +
                        "maxDialogs=%d maxLength=%d " +
                        "maxTotLen=%d interval=%d)",
                        ProtocolViolationException.
                        ERROR_INVALID_FIELD,
                        uuid, maxDialogs, maxLength, maxTotLen, interval));
        }

        return new ProtocolMesg.ConnectionConfig(
                uuid, maxDialogs, maxLength, maxTotLen, interval);
    }

    private ProtocolMesg readConnectionConfigResponse() {
        /* At least 4 bytes with 4 packed long */
        if (input.readableBytes() < 4) {
            input.reset();
            return null;
        }
        if (!input.canReadPackedLong()) {
            input.reset();
            return null;
        }
        long maxDialogs = input.readPackedLong();
        if (!input.canReadPackedLong()) {
            input.reset();
            return null;
        }
        long maxLength = input.readPackedLong();
        if (!input.canReadPackedLong()) {
            input.reset();
            return null;
        }
        long maxTotLen = input.readPackedLong();
        if (!input.canReadPackedLong()) {
            input.reset();
            return null;
        }
        long interval = input.readPackedLong();

        if ((maxDialogs <= 0) ||
            (maxLength <= 0) ||
            (maxTotLen <= 0) ||
            (interval <= 0)) {
            throw new ProtocolViolationException(
                    false,
                    String.format(
                        "%s Received ConnectionConfig(" +
                        "maxDialogs=%d maxLength=%d " +
                        "maxTotLen=%d interval=%d)",
                        ProtocolViolationException.
                        ERROR_INVALID_FIELD,
                        maxDialogs, maxLength, maxTotLen, interval));
        }

        return new ProtocolMesg.ConnectionConfigResponse(
                maxDialogs, maxLength, maxTotLen, interval);
    }

    private ProtocolMesg readNoOperation() {
        return new ProtocolMesg.NoOperation();
    }

    private ProtocolMesg readConnectionAbort() {
        /* At least 2 bytes with errno and a packed long */
        if (input.readableBytes() < 2) {
            input.reset();
            return null;
        }
        int errno = input.readByte();
        if ((errno < 0) ||
            (errno >= ProtocolMesg.ConnectionAbort.CAUSES.length)) {
            errno = 0;
        }
        if (!input.canReadPackedLong()) {
            input.reset();
            return null;
        }
        int length = (int) input.readPackedLong();

        if (length < 0) {
            throw new ProtocolViolationException(
                    false,
                    String.format(
                        "%s Received negative length in ConnectionAbort, " +
                        "length=%d",
                        ProtocolViolationException.
                        ERROR_INVALID_FIELD,
                        length));
        }

        if (length > maxMesgLen) {
            /* Discard the detail field */
            length = 0;
        }

        if (input.readableBytes() < length) {
            input.reset();
            return null;
        }

        return new ProtocolMesg.ConnectionAbort(
                ProtocolMesg.ConnectionAbort.CAUSES[errno],
                input.readUTF8(length));
    }

    private ProtocolMesg readPing() {
        if (!input.canReadPackedLong()) {
            input.reset();
            return null;
        }
        return new ProtocolMesg.Ping(input.readPackedLong());
    }

    private ProtocolMesg readPingAck() {
        if (!input.canReadPackedLong()) {
            input.reset();
            return null;
        }
        return new ProtocolMesg.PingAck(input.readPackedLong());
    }

    private ProtocolMesg readDialogStart(byte ident) {
        boolean sampled = ((ident & 0x04) != 0);
        boolean finish = ((ident & 0x02) != 0);
        boolean cont = ((ident & 0x01) != 0);
        if (finish && cont) {
            throw new ProtocolViolationException(
                    false,
                    ProtocolViolationException.
                    ERROR_INVALID_FIELD +
                    "Both finish and cont are true in DialogStart");
        }

        /* At least 4 bytes with 4 packed long */
        if (input.readableBytes() < 4) {
            input.reset();
            return null;
        }
        if (!input.canReadPackedLong()) {
            input.reset();
            return null;
        }
        int typeno = (int) input.readPackedLong();
        if (!input.canReadPackedLong()) {
            input.reset();
            return null;
        }
        long dialogId = input.readPackedLong();
        if (!input.canReadPackedLong()) {
            input.reset();
            return null;
        }
        long timeout = input.readPackedLong();
        if (!input.canReadPackedLong()) {
            input.reset();
            return null;
        }
        int length = (int) input.readPackedLong();

        if ((dialogId == 0) || (timeout <= 0) || (length < 0)) {
            throw new ProtocolViolationException(
                    false,
                    String.format("%s Received DialogStart " +
                        "(dialogId=%s timeout=%d length=%d)",
                        ProtocolViolationException.
                        ERROR_INVALID_FIELD,
                        Long.toString(dialogId, 16), timeout, length));
        }

        checkMaxLength(length, "DialogStart");

        if (input.readableBytes() < length) {
            input.reset();
            return null;
        }
        return new ProtocolMesg.DialogStart(
                sampled, finish, cont, typeno, dialogId, timeout,
                input.readBytes(length));
    }

    private ProtocolMesg readDialogFrame(byte ident) {
        boolean finish = ((ident & 0x02) != 0);
        boolean cont = ((ident & 0x01) != 0);
        if (finish && cont) {
            throw new ProtocolViolationException(
                    false,
                    ProtocolViolationException.
                    ERROR_INVALID_FIELD +
                    "Both finish and cont are true in DialogStart");
        }

        /* At least 2 bytes with 2 packed long */
        if (input.readableBytes() < 2) {
            input.reset();
            return null;
        }
        if (!input.canReadPackedLong()) {
            input.reset();
            return null;
        }
        long dialogId = input.readPackedLong();
        if (!input.canReadPackedLong()) {
            input.reset();
            return null;
        }
        int length = (int) input.readPackedLong();

        if ((dialogId == 0) || (length < 0)) {
            throw new ProtocolViolationException(
                    false,
                    String.format("%s Received DialogFrame " +
                        "(dialogId=%s length=%d)",
                        ProtocolViolationException.
                        ERROR_INVALID_FIELD,
                        Long.toString(dialogId, 16), length));
        }

        checkMaxLength(length, "DialogFrame");

        if (input.readableBytes() < length) {
            input.reset();
            return null;
        }
        return new ProtocolMesg.DialogFrame(
                finish, cont, dialogId, input.readBytes(length));
    }

    private ProtocolMesg readDialogAbort() {
        /* At least 3 bytes with errno and 2 packed long */
        if (input.readableBytes() < 3) {
            input.reset();
            return null;
        }
        int errno = input.readByte();
        if ((errno < 0) ||
            (errno >= ProtocolMesg.DialogAbort.CAUSES.length)) {
            errno = 0;
        }
        if (!input.canReadPackedLong()) {
            input.reset();
            return null;
        }
        long dialogId = input.readPackedLong();
        if (!input.canReadPackedLong()) {
            input.reset();
            return null;
        }
        int length = (int) input.readPackedLong();

        if ((dialogId == 0) || (length < 0)) {
            throw new ProtocolViolationException(
                    false,
                    String.format("%s Received DialogAbort " +
                        "(dialogId=%s length=%d)",
                        ProtocolViolationException.
                        ERROR_INVALID_FIELD,
                        Long.toString(dialogId, 16), length));
        }

        checkMaxLength(length, "DialogAbort");

        if (input.readableBytes() < length) {
            input.reset();
            return null;
        }
        return new ProtocolMesg.DialogAbort(
               ProtocolMesg.DialogAbort.CAUSES[errno],
               dialogId,
               input.readUTF8(length));
    }

    private void checkMaxLength(int length, String mesg) {
        if (length > maxMesgLen) {
            throw new ProtocolViolationException(
                    false,
                    String.format(
                        "%s Received %s with length=%d, but the limit is %d",
                        ProtocolViolationException.
                        ERROR_MAX_LENGTH_EXCEEDED,
                        mesg, length, maxMesgLen));
        }
    }
}
