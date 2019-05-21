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

package oracle.kv.impl.util.contextlogger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.logging.Level;

import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.JsonUtils;
import oracle.kv.impl.util.SerializationUtil;

/**
 * A LogContext establishes a logging context for a SkLogger.log call.
 * The logging context contains information about the original API request
 * that resulted in the execution of the code that produced a log record.
 * This allows the log records that are associated with the same request to
 * be grouped together during postprocessing of the log.
 *
 * The LogContext also establishes the current environmental log level for the
 * logging call, overriding the logger's current log level.
 * @see #writeFastExternal FastExternalizable format
 *
 */
public class LogContext extends JsonUtils
    implements Serializable, FastExternalizable {

    private static final long serialVersionUID = 1;

    private String id;      /* Keep as a String to avoid conversions. */
    private String entry;   /* API entrypoint that created this context. */
    private String origin;  /* User that invoked the entrypoint. */
    private int logLevel;   /* Environmental log level to apply. */
    private boolean logged; /* Indicates whether this context was emitted. */

    /* No-arg constructor for Json serialization. */
    public LogContext() {
    }

    public LogContext(CorrelationId id, /* Must be provided. */
                      String entry,     /* Can be null. */
                      String origin,    /* Can be null. */
                      Level logLevel) { /* Can be null. */
        this(id, entry, origin, logLevel, false);
    }

    private LogContext(CorrelationId id,
                       String entry,
                       String origin,
                       Level logLevel,
                       boolean logged) {
        if (id == null) {
            throw new IllegalStateException("CorrelationId cannot be null.");
        }
        this.id = id.toString();
        this.entry = entry;
        this.origin = origin;
        this.logLevel = logLevel.intValue();
        this.logged = logged;
    }

    /* for FastExternalizable */
    public LogContext(DataInput in, short serialVersion)
        throws IOException {

        id = SerializationUtil.readNonNullString(in, serialVersion);
        entry = SerializationUtil.readString(in, serialVersion);
        origin = SerializationUtil.readString(in, serialVersion);
        logLevel = in.readInt();
        logged = in.readBoolean();
    }

    public String getId() {
        return id;
    }

    public String getEntry() {
        return entry;
    }

    public String getOrigin() {
        return origin;
    }

    public void putOrigin(String newOrigin) {
        origin = newOrigin;
    }

    public int getLogLevel() {
        return logLevel;
    }

    public void putLogLevel(Level newLevel) {
        logLevel = newLevel.intValue();
    }

    public boolean isLogged() {
        return logged;
    }

    public void setLogged() {
        logged = true;
    }

    public String toJsonString() {
        try {
            return mapper.writeValueAsString(this);
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public String toString() {
        return toJsonString();
    }

    static public LogContext fromJsonString(String js) {
        if (js == null) {
            return null;
        }
        try {
            return mapper.readValue(js, LogContext.class);
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    /* boolean logged does not participate. */
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((entry == null) ? 0 : entry.hashCode());
        result = prime * result + ((origin == null) ? 0 : origin.hashCode());
        result = prime * result + logLevel;
        return result;
    }

    @Override
    /* boolean logged does not participate. */
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        LogContext other = (LogContext) obj;
        if (entry == null) {
            if (other.entry != null) {
                return false;
            }
        } else if (!entry.equals(other.entry)) {
            return false;
        }
        if (id == null) {
            if (other.id != null) {
                return false;
            }
        } else if (!id.equals(other.id)) {
            return false;
        }
        if (logLevel != other.logLevel) {
            return false;
        }
        if (origin == null) {
            if (other.origin != null) {
                return false;
            }
        } else if (!origin.equals(other.origin)) {
            return false;
        }
        return true;
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link SerializationUtil#writeNonNullString non-null String})
     *      {@link #getId id}
     * <li> ({@link SerializationUtil#writeString String}) {@link #getEntry
     *      entry}
     * <li> ({@link SerializationUtil#writeString String}) {@link #getOrigin
     *      origin}
     * <li> ({@link DataOutput#writeInt int}) {@link #getLogLevel logLevel}
     * <li> ({@link DataOutput#writeBoolean boolean}) {@link #isLogged logged}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        SerializationUtil.writeNonNullString(out, serialVersion, id);
        SerializationUtil.writeString(out, serialVersion, entry);
        SerializationUtil.writeString(out, serialVersion, origin);
        out.writeInt(logLevel);
        out.writeBoolean(logged);
    }
}
