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

package oracle.kv.hadoop;

import java.io.IOException;

import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import oracle.kv.Key;
import oracle.kv.Value;
import oracle.kv.avro.AvroCatalog;
import oracle.kv.avro.GenericAvroBinding;

/**
 * @hidden
 * @deprecated as of 4.0, use the table API instead.
 */
@Deprecated
public class KVAvroRecordReader
    extends KVRecordReaderBase<Key, IndexedRecord> {

    private Class<?> formatterClass;
    private AvroFormatter formatter = null;
    private GenericAvroBinding binding;

    /**
     * Called once at initialization.
     * @param split the split that defines the range of records to read
     * @param context the information about the task
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {

        KVInputSplit kvInputSplit = (KVInputSplit) split;
        super.initialize(split, context);

        String formatterClassName = kvInputSplit.getFormatterClassName();
        if (formatterClassName != null &&
            !"".equals(formatterClassName)) {
            try {
                formatterClass = Class.forName(formatterClassName);
                formatter = (AvroFormatter) formatterClass.newInstance();
            } catch (Exception E) {
                IllegalArgumentException iae = new IllegalArgumentException
                    ("Couldn't find formatter class: " +
                     formatterClassName);
                iae.initCause(E);
                throw iae;
            }
        }
        AvroCatalog catalog = kvstore.getAvroCatalog();
        binding = catalog.getGenericMultiBinding(catalog.getCurrentSchemas());
    }

    /**
     * Get the current value
     * @return the object that was read
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public IndexedRecord getCurrentValue()
        throws IOException, InterruptedException {

        if (current == null) {
            return null;
        }

        if (formatter != null) {
            return formatter.toAvroRecord(current, kvstore);
        }

        /* Key unusedKey = current.getKey(); */
        Value value = current.getValue();

        IndexedRecord record = null;
        if (value != null) {
            record = binding.toObject(value);
        }

        return record;
    }

    /**
     * Get the current key.
     * @return the current key or null if there is no current key
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public Key getCurrentKey()
        throws IOException, InterruptedException {

        if (current == null) {
            return null;
        }

        return current.getKey();
    }
}
