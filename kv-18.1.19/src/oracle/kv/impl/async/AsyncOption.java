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

package oracle.kv.impl.async;

/**
 * A collection of options to configure for socket channels, a listening
 * channels and async connections.
 */
public class AsyncOption<T> {

    /* Socket options */
    /**
     * Keep connection alive.
     */
    public static final AsyncOption<Boolean> SO_KEEPALIVE =
        new AsyncOption<Boolean>("SO_KEEPALIVE", Boolean.class);
    /**
     * Linger on close if data is present.
     */
    public static final AsyncOption<Integer> SO_LINGER =
        new AsyncOption<Integer>("SO_LINGER", Integer.class);
    /**
     * The size of the socket receive buffer.
     */
    public static final AsyncOption<Integer> SO_RCVBUF =
        new AsyncOption<Integer>("SO_RCVBUF", Integer.class);
    /**
     * Re-use address.
     */
    public static final AsyncOption<Boolean> SO_REUSEADDR =
        new AsyncOption<Boolean>("SO_REUSEADDR", Boolean.class);
    /**
     * The size of the socket send buffer.
     */
    public static final AsyncOption<Integer> SO_SNDBUF =
        new AsyncOption<Integer>("SO_SNDBUF", Integer.class);
    /**
     * Disable the Nagle algorithm.
     */
    public static final AsyncOption<Boolean> TCP_NODELAY =
        new AsyncOption<Boolean>("TCP_NODELAY", Boolean.class);

    /* Server socket option */

    /**
     * The value of backlog of server socket.
     */
    public static final AsyncOption<Integer> SSO_BACKLOG =
        new AsyncOption<Integer>("SSO_BACKLOG", Integer.class);

    /* Dialog options */

    /**
     * Max number of active dialogs local wants to start concurrently.
     *
     * The actual max num of concurrent dialogs local can start is the min of
     * DLG_LOCAL_MAXDLGS of local and DLG_REMOTE_MAXDLGS of remote.
     */
    public static final AsyncOption<Integer> DLG_LOCAL_MAXDLGS =
        new AsyncOption<Integer>("DLG_LOCAL_MAXDLGS", Integer.class);
    /**
     * Max value for the length field in all protocol messages local wants to
     * send.
     *
     * The actual max length local can send is the min of DLG_LOCAL_MAXLEN of
     * local and DLG_REMOTE_MAXLEN of remote.
     */
    public static final AsyncOption<Integer> DLG_LOCAL_MAXLEN =
        new AsyncOption<Integer>("DLG_LOCAL_MAXLEN", Integer.class);
    /**
     * Max total length of a continuation of frames in a request/response local
     * wants to send .
     *
     * The actual max totoal length local can send is the min of
     * DLG_LOCAL_MAXTOTOLEN of local and DLG_REMOTE_MAXTOTLEN of remote.
     */
    public static final AsyncOption<Integer> DLG_LOCAL_MAXTOTLEN =
        new AsyncOption<Integer>("DLG_LOCAL_MAXTOTLEN", Integer.class);
    /**
     * Max number of active dialogs remote can start concurrently.
     *
     * The actual max num of concurrent dialogs remote can send is the min of
     * DLG_LOCAL_MAXDLGS of remote and DLG_LOCAL_MAXDLGS of local.
     */
    public static final AsyncOption<Integer> DLG_REMOTE_MAXDLGS =
        new AsyncOption<Integer>("DLG_REMOTE_MAXDLGS", Integer.class);
    /**
     * Max value for the length field in all protocol messages remote can send.
     *
     * The actual max length remote can send is the min of DLG_LOCAL_MAXLEN of
     * remote and DLG_REMOTE_MAXLEN of local.
     */
    public static final AsyncOption<Integer> DLG_REMOTE_MAXLEN =
        new AsyncOption<Integer>("DLG_REMOTE_MAXLEN", Integer.class);
    /**
     * Max total length of a continuation of frames in a request/response sent
     * by remote.
     *
     * The actual max totoal length remote can send is the min of
     * DLG_LOCAL_MAXTOTOLEN of remote and DLG_REMOTE_MAXTOTLEN of local.
     */
    public static final AsyncOption<Integer> DLG_REMOTE_MAXTOTLEN =
        new AsyncOption<Integer>("DLG_REMOTE_MAXTOTLEN", Integer.class);
    /**
     * The timeout in milliseconds for the underlying connection to be ready
     * for dialogs (including connection establishment and handshake).
     */
    public static final AsyncOption<Integer> DLG_CONNECT_TIMEOUT =
        new AsyncOption<Integer>("DLG_CONNECT_TIMEOUT", Integer.class);
    /**
     * The number of heartbeat messages that must be detected as missing
     * during an otherwise idle period before the connection shuts down.
     *
     * This value provides the basis for timeout used to detect
     * unresponsiveness of the connection or remote endpoint. The timeout is
     * calculated as DLG_HEARTBEAT_TIMEOUT * DLG_HEARTBEAT_INTERVAL. Upon a
     * timeout the connection is closed.
     *
     * Reducing this value permits the discovery of unresponsiveness faster.
     * However, it increases the chances of false positives as well, if the
     * network is experiencing transient problems from which it might just
     * recover.
     */
    public static final AsyncOption<Integer> DLG_HEARTBEAT_TIMEOUT =
        new AsyncOption<Integer>("DLG_HEARTBEAT_TIMEOUT", Integer.class);
    /**
     * The interval in milliseconds for sending heartbeat to the remote.
     *
     * This value provides the basis for timeout used to detect
     * unresponsiveness of the connection or remote endpoint. The timeout is
     * calculated as DLG_HEARTBEAT_TIMEOUT * DLG_HEARTBEAT_INTERVAL. Upon a
     * timeout the connection is closed.
     *
     * The actual interval is the max of both local and remote.
     */
    public static final AsyncOption<Integer> DLG_HEARTBEAT_INTERVAL =
        new AsyncOption<Integer>("DLG_HEARTBEAT_INTERVAL", Integer.class);
    /**
     * The timeout interval in milliseconds for deciding the connection is
     * idle.
     *
     * If there is no started dialog on the connection during the interval, the
     * connection is deemed idle and therefore discarded.
     *
     * The actual timeout is the max of both local and remote.
     */
    public static final AsyncOption<Integer> DLG_IDLE_TIMEOUT =
        new AsyncOption<Integer>("DLG_IDLE_TIMEOUT", Integer.class);
    /**
     * The maximum number of dialogs included in each batch when flushing
     * dialog frames.
     */
    public static final AsyncOption<Integer> DLG_FLUSH_BATCHSZ =
        new AsyncOption<Integer>("DLG_FLUSH_BATCHSZ", Integer.class);
    /**
     * The maximum number of batches for each flush operation.
     */
    public static final AsyncOption<Integer> DLG_FLUSH_NBATCH =
        new AsyncOption<Integer>("DLG_FLUSH_NBATCH", Integer.class);


    private final String name;
    private final Class<T> type;

    private AsyncOption(String name, Class<T> type) {
        this.name = name;
        this.type = type;
    }

    public String name() {
        return name;
    }

    public Class<T> type() {
        return type;
    }

    /**
     * Parses a string as a value of this option's type.
     */
    T parseStringValue(String s) {
        if (type == Integer.class) {
            return type.cast(Integer.parseInt(s));
        }
        if (type == Boolean.class) {
            return type.cast(Boolean.parseBoolean(s));
        }
        throw new UnsupportedOperationException(
            "Need to implement string parser for type " + type);
    }

    @Override
    public String toString() {
        return name;
    }
}
