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

import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;

/**
 * Concrete implementation of TableStorageHandlerBase; which assumes that the
 * data accessed from the desired Oracle NoSQL Database has been stored, and
 * will be accessed, via the Table API.
 * <p>
 *
 * @since 3.1
 */
@SuppressWarnings({"deprecation", "rawtypes"})
public class TableStorageHandler extends TableStorageHandlerBase<Text, Text> {

    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return TableHiveInputFormat.class;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        return HiveIgnoreKeyTextOutputFormat.class;
    }

    @Override
    public Class<? extends SerDe> getSerDeClass() {
        return TableSerDe.class;
    }
}
