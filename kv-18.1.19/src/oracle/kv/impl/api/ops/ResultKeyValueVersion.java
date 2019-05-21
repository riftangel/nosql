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
import static oracle.kv.impl.util.SerializationUtil.readNonNullByteArrayOldShortLength;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullByteArrayOldShortLength;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.Value;
import oracle.kv.Version;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SerializationUtil;

/**
 * Holds key and value as byte arrays to avoid conversion to Key and Value
 * objects on the service side.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class ResultKeyValueVersion implements FastExternalizable {

    private final byte[] keyBytes;
    private final ResultValue resultValue;
    private final Version version;
    private final long expirationTime;

    public ResultKeyValueVersion(byte[] keyBytes,
                                 byte[] valueBytes,
                                 Version version,
                                 long expirationTime) {
        checkNull("keyBytes", keyBytes);
        checkNull("version", version);
        this.keyBytes = keyBytes;
        this.resultValue = new ResultValue(valueBytes);
        this.version = version;
        this.expirationTime = expirationTime;
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor
     * first to read common elements.
     */
    public ResultKeyValueVersion(DataInput in, short serialVersion)
        throws IOException {

        keyBytes = readNonNullByteArrayOldShortLength(in, serialVersion);
        resultValue = new ResultValue(in, serialVersion);
        version = Version.createVersion(in, serialVersion);
        expirationTime = Result.readExpirationTime(in, serialVersion);
    }

    /**
     * Writes this object to the output stream.  Format for {@code
     * serialVersion} {@link SerialVersion#STD_UTF8_VERSION} and greater:
     * <ol>
     * <li> ({@link SerializationUtil#writeNonNullByteArray non-null byte
     *      array}) {@link #getKeyBytes keyBytes}
     * <li> ({@link ResultValue}) <i>resultValue</i>
     * <li> ({@link Version}) {@link #getVersion version}
     * <li> ({@link DataOutput#writeBoolean boolean}) <i>expirationTime
     *      non-zero</i>
     * <li> <i>[Optional]</i> ({@link DataOutput#writeLong long}) {@link
     *      #getExpirationTime expirationTime} // if non-zero
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        writeNonNullByteArrayOldShortLength(out, serialVersion, keyBytes);
        resultValue.writeFastExternal(out, serialVersion);
        version.writeFastExternal(out, serialVersion);
        Result.writeExpirationTime(out, expirationTime, serialVersion);
    }

    public byte[] getKeyBytes() {
        return keyBytes;
    }

    public Value getValue() {
        return resultValue.getValue();
    }

    public byte[] getValueBytes() {
        return resultValue.getBytes();
    }

    public Version getVersion() {
        return version;
    }

    public long getExpirationTime() {
        return expirationTime;
    }
}
