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

package oracle.kv.impl.rep.migration;

import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.Response;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import oracle.kv.impl.topo.RepNodeId;

/**
 * The partition migration transfer protocol. There are three components in the
 * protocol, the initial transfer request sent by the target to the source node,
 * the transfer response, and the database operations, both sent from the source
 * to the target.
 */
public class TransferProtocol {

    /*
     * Version 2 as of release 4.0, with the introduction of TTL
     */
    static final int VERSION = 2;

    /* Constant used to indicate a transfer only request */
    static final RepNodeId TRANSFER_ONLY_TARGET = new RepNodeId(0, 0);

    /* -- Transfer request -- */

    /*
     * Transfer request size
     *      4 int protocol version
     *      4 int partition ID
     *      4 int target group ID
     *      4 int target node number
     *      4 int 0 (unused - must be zero)
     */
    private static final int REQUEST_SIZE = 4 + 4 + 4 + 4 + 4;

    /**
     * Object encapsulating a transfer request.
     */
    public static class TransferRequest {

        final int partitionId;
        final RepNodeId targetRNId;

        private TransferRequest(int partitionId, RepNodeId targetRnId) {
            this.partitionId = partitionId;
            this.targetRNId = targetRnId;
        }

        /*
         * Writes a transfer only request.
         */
        public static void write(DataChannel channel, int partitionId)
                           throws IOException {
            write(channel, partitionId, TRANSFER_ONLY_TARGET);
        }

        /*
         * Writes a transfer request.
         */
        static void write(DataChannel channel, int partitionId,
                          RepNodeId targetRNId)
            throws IOException {

            final ByteBuffer buffer = ByteBuffer.allocate(REQUEST_SIZE);
            buffer.putInt(VERSION);
            buffer.putInt(partitionId);
            buffer.putInt(targetRNId.getGroupId());
            buffer.putInt(targetRNId.getNodeNum());
            buffer.putInt(0);
            buffer.flip();
            channel.write(buffer);
        }

        /*
         * Reads a transfer request.
         */
        public static TransferRequest read(DataChannel channel) throws
            IOException {
            ByteBuffer readBuffer =
                        ByteBuffer.allocate(TransferProtocol.REQUEST_SIZE);
            read(readBuffer, channel);

            final int version = readBuffer.getInt();

            if (version != TransferProtocol.VERSION) {
                final StringBuilder sb = new StringBuilder();
                sb.append("Protocol version mismatch, received ");
                sb.append(version);
                sb.append(" expected ");
                sb.append(TransferProtocol.VERSION);
                throw new IOException(sb.toString());
            }
            final int partitionId = readBuffer.getInt();
            final int targetGroupId = readBuffer.getInt();
            final int targetNodeNum = readBuffer.getInt();

            /* Unused, mbz */
            readBuffer.getInt();

            return new TransferRequest(partitionId,
                                       (targetGroupId == 0) ?
                                           TRANSFER_ONLY_TARGET :
                                           new RepNodeId(targetGroupId,
                                                         targetNodeNum));
        }

        private static void read(ByteBuffer bb, DataChannel channel)
            throws IOException {
            while (bb.remaining() > 0) {
                if (channel.read(bb) < 0) {
                    throw new IOException("Unexpected EOF");
                }
            }
            bb.flip();
        }

        /* -- Request response -- */

        /*
         * ACK Response
         *
         *  byte Response.OK
         */
        static void writeACKResponse(DataChannel channel) throws IOException {
            ByteBuffer buffer = ByteBuffer.allocate(1);
            buffer.put((byte)Response.OK.ordinal());
            buffer.flip();

            // TODO - Should this check be != buffer size?
            if (channel.write(buffer) == 0) {
                throw new IOException("Failed to write response. " +
                                      "Send buffer size: " +
                                      channel.socket().getSendBufferSize());
            }
        }

        /*
         * Busy Response
         *
         *  byte   Response.Busy
         *  int    numStreams - the number of migration streams the source
         *                      currently supports (may change, may be 0)
         *  int    reason message length
         *  byte[] reason message bytes
         */
        static void writeBusyResponse(DataChannel channel,
                                      int numStreams,
                                      String message) throws IOException {

            byte[] mb = message.getBytes();
            ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + 4 + mb.length);
            buffer.put((byte)Response.BUSY.ordinal());
            buffer.putInt(numStreams);
            buffer.putInt(mb.length);
            buffer.put(mb);
            buffer.flip();
            if (channel.write(buffer) == 0) {
                throw new IOException("Failed to write response. " +
                                      "Send buffer size: " +
                                      channel.socket().getSendBufferSize());
            }
        }

        /*
         * Error Response
         *
         *  byte   Response.Busy
         *  int    reason message length
         *  byte[] reason message bytes
         */
        static void writeErrorResponse(DataChannel channel,
                                       Response response,
                                       String message) throws IOException {

            byte[] mb = message.getBytes();
            ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + mb.length);
            buffer.put((byte)response.ordinal());
            buffer.putInt(mb.length);
            buffer.put(mb);
            buffer.flip();
            if (channel.write(buffer) == 0) {
                throw new IOException("Failed to write response. " +
                                      "Send buffer size: " +
                                      channel.socket().getSendBufferSize());
            }
        }

        public static Response readResponse(DataInputStream stream)
            throws IOException {

            int ordinal = stream.read();
            if ((ordinal < 0) || (ordinal >= Response.values().length)) {
                throw new IOException("Error reading response= " + ordinal);
            }
            return Response.values()[ordinal];
        }

        public static int readNumStreams(DataInputStream stream)
            throws IOException {
            return stream.readInt();
        }

        public static String readReason(DataInputStream stream) {
            try {
                int size = stream.readInt();
                byte[] bytes = new byte[size];
                stream.readFully(bytes);
                return new String(bytes);
            } catch (IOException ioe) {
                return "";
            }
        }
    }

    /* -- DB OPs -- */

    /**
     * Operation messages. These are the messages sent from the source to the
     * target node during the partition data transfer.
     *
     * WARNING: To avoid breaking serialization compatibility, the order of the
     * values must not be changed and new values must be added at the end.
     */
    public static enum OP {

        /**
         * A DB read operation. A COPY is generated by the key-ordered reads
         * of the source DB.
         */
        COPY(0),

        /**
         * A client put operation.
         */
        PUT(1),

        /**
         * A client delete operation.
         */
        DELETE(2),

        /**
         * Indicates that client transaction is about to be committed. No
         * further PUT or DELETE messages should be sent for the transaction.
         */
        PREPARE(3),

        /**
         * The client transaction has been successfully committed.
         */
        COMMIT(4),

        /**
         * The client transaction has been aborted.
         */
        ABORT(5),

        /**
         * End of Data. The partition migration data transfer is complete and
         * no further messages will be sent from the source.
         */
        EOD(6);

        private static OP[] VALUES = values();

        private OP(int ordinal) {
            if (ordinal != ordinal()) {
                throw new IllegalArgumentException("Wrong ordinal");
            }
        }

        /*
         * Gets the OP corresponding to the specified ordinal.
         */
        public static OP get(int ordinal) {
            if ((ordinal >= 0) && (ordinal < VALUES.length)) {
                return VALUES[ordinal];
            }
            return null;
        }
    }
}
