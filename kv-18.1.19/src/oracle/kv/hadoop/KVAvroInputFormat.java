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
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import oracle.kv.Key;

/**
 * A Hadoop InputFormat class for reading data from Oracle NoSQL Database and
 * returning Avro IndexedRecords as the map-reduce value and the record's Key
 * as the map-reduce key.
 * <p>
 * One use for this class is to read NoSQL Database records into Oracle Loader
 * for Hadoop.
 * <p>
 * Refer to the javadoc for {@link KVInputFormatBase} for information on the
 * parameters that may be passed to this class.
 *
 * @since 2.0
 *
 * @deprecated as of 4.0, use the table API instead.
 */
@Deprecated
public class KVAvroInputFormat
    extends KVInputFormatBase<Key, IndexedRecord> {

    /**
     * @hidden
     */
    @Override
    public RecordReader<Key, IndexedRecord>
        createRecordReader(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {

        KVAvroRecordReader ret = new KVAvroRecordReader();
        ret.initialize(split, context);
        return ret;
    }
}
