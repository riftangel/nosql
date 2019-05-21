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

package oracle.kv.util.expimp;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import oracle.kv.util.expimp.AbstractStoreExport.RecordStreamMap.RecordEntityInfo;

/**
 * Custom Input/Output stream is used by the export utility to stream the data
 * from KVStore to the export store.
 *
 * Producer ----> CustomOutputStream ----> CustomInputStream ----> Consumer
 * (KVStore)                                                     (Export Store)
 *
 * The data bytes read from the Producer are placed in a buffer in
 * CustomOutputStream. CustomInputStream reads bytes from the buffer in
 * CustomOutputStream and transfers them to the Consumer.
 *
 * An ArrayBlockingQueue is used to synchronize producer (CustomOutputStream)
 * and consumer (CustomInputStream). The size of the queue is fixed to 1000.
 * Data bytes retrieved from the KVStore are placed in data blocks (byte[]) of
 * size 1000. When the data block becomes full, its fed into the queue.
 * CustomInputStream reads the bytes from the queue one data block (1000 bytes)
 * at a time. Hence the max capacity of the queue at any time is 1000 * 1000 =
 * 1MB.
 */
public class CustomStream {

    /**
     * Producer of DataBlocks. The data blocks are placed in ArrayBlockingQueue
     */
    public static class CustomOutputStream extends ByteArrayOutputStream {

        /*
         * Size of the ArrayBlockingQueue fixed to 1000
         */
        private final int queueSize = 1000;

        /*
         * Blocking queue holding 1MB data blocks
         */
        BlockingQueue<byte[]> dataBytes =
            new ArrayBlockingQueue<byte[]>(queueSize);

        /*
         * Size of the data block fed into the queue is fixed to 1000 bytes
         */
        private int dataBlockSize = 1000;
        private byte[] dataBlock;

        /*
         * Offset of a data block is initially 0. As bytes are fed into it,
         * offset is incremented.
         */
        private int offset = 0;
        
        private RecordEntityInfo recEntityInfo;

        public CustomOutputStream() {
            dataBlock = new byte[dataBlockSize];
        }

        /**
         * Checks if the data block has reached its max capacity
         */
        public boolean dataBlockFull() {
            return offset == dataBlockSize;
        }

        public void setRecordEntityInfo(RecordEntityInfo recEntityInfo) {
            this.recEntityInfo = recEntityInfo;
        }

        /**
         * Write the bytes into the data block.
         */
        public void customWrite(byte[] recordBytes) {

            for (int i = 0; i < recordBytes.length; i++) {

                dataBlock[offset++] = recordBytes[i];

                /*
                 * If the data block reaches its maximum capacity, feed it into
                 * ArrayBlockingQueue which can be retrieved by the Consumer.
                 * Create a new data block and continue writing the bytes into
                 * the new data block
                 */
                if (dataBlockFull()) {

                    try {
                        dataBytes.put(dataBlock);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    /*
                     * Create a new data block and set its offset to 0
                     */
                    dataBlock = new byte[dataBlockSize];
                    offset = 0;
                }
            }
        }

        /**
         * Override the write method to make use of customWrite method
         */
        @Override
        public synchronized void write(byte[] recordBytes,
                                       int off,
                                       int length) {

            byte[] b = new byte[length];
            System.arraycopy(recordBytes, off, b, 0, length);
            customWrite(b);
        }

        /**
         * Called by the Consumer (CustomInputStream) to retrieve a data block
         * from the ArrayBlockingQueue
         */
        public byte[] readDataBlock() {

            try {
                byte[] item = dataBytes.poll(50, TimeUnit.SECONDS);

                if (item == null) {
                    recEntityInfo.flushStream();
                    item = dataBytes.poll(50, TimeUnit.SECONDS);

                    /*
                     * If item is null, return empty byte signifying end of
                     * read
                     */ 
                    if (item == null) {
                        return new byte[0];
                    }

                    return item;
                }

                return item;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            return null;
        }

        /**
         * Put the last data block (need not be full) into the queue. Place an
         * empty data block at the end of the queue to signify that all the
         * bytes have been transferred.
         */
        public void customFlush() {

            if (offset > 0) {
                /*
                 * Size of the last data block is equal to offset
                 */
                byte[] lastBlock = new byte[offset];
                System.arraycopy(dataBlock, 0, lastBlock, 0, offset);

                try {
                    dataBytes.put(lastBlock);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            /*
             * Place an empty data block to signify that write has ended
             */
            try {
                dataBytes.put(new byte[0]);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Consumer of DataBlocks. The data blocks are taken from ArrayBlockingQueue
     */
    public static class CustomInputStream extends InputStream {

        /*
         * Producer of DataBlocks which are placed in the BlockingQueue
         */
        CustomOutputStream out;

        /*
         * Data Block read from the BlockingQueue
         */
        byte[] dataBlock = null;

        /*
         * Offset of the data block is initially 0. As bytes are read from the
         * data block, the offset is incremented.
         */
        int offset = 0;

        /*
         * Signifies all the bytes have been read from the producer
         */
        boolean readDone = false;

        public CustomInputStream(CustomOutputStream out) {
            this.out = out;
        }

        @Override
        public int read() {

            if (readDone) {
                return -1;
            }

            /*
             * Read a data block from the queue if reading for the first time
             * or if the previous data block has been completely exhausted.
             */
            if (dataBlock == null || offset == dataBlock.length) {
                dataBlock = out.readDataBlock();
                offset = 0;
            }

            /*
             * Read a zero length data block. Signifies end of read.
             */
            if (dataBlock.length == 0) {
                readDone = true;
                return -1;
            }

            /*
             * Read a byte from the data block
             */
            byte data = dataBlock[offset++];

            if (data < 0) {
                return 256 - (data * -1);
            }

            return data;
        }
    }
}
