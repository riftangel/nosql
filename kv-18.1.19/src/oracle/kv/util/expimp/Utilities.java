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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.logging.LogRecord;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * General Export/Import utility class
 */
public class Utilities {

    /**
     * Generate a CRC checksum given the record key and value bytes
     */
    public static long getChecksum(byte[] keyBytes,
                                   byte[] valueBytes) {

        byte[] recordBytes =
            new byte[keyBytes.length + valueBytes.length];

        System.arraycopy(keyBytes, 0, recordBytes,
                         0, keyBytes.length);

        System.arraycopy(valueBytes, 0, recordBytes,
                         keyBytes.length, valueBytes.length);

        Checksum checksum = new CRC32();
        checksum.update(recordBytes, 0, recordBytes.length);

        return checksum.getValue();
    }

    /**
     * Format the LogRecord needed for export and import
     *
     * @param record LogRecord
     */
    public static String format(LogRecord record) {

        Throwable throwable = record.getThrown();
        String throwableException = "";

        if (throwable != null) {
            throwableException = "\n" + throwable.toString();
        }

        Date date=new Date(record.getMillis());
        SimpleDateFormat simpleDateFormat =
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        String dateFormat = simpleDateFormat.format(date);

        return dateFormat + " " + record.getLevel().getName() + ": " +
               record.getMessage() + throwableException + "\n";
    }
}
