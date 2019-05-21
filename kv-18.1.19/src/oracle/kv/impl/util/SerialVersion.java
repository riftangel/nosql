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

package oracle.kv.impl.util;

import java.util.HashMap;
import java.util.Map;

import oracle.kv.KVVersion;

/**
 * Defines the previous and current serialization version for services and
 * clients.
 *
 * <p>As features that affect serialized formats are introduced constants
 * representing those features should be added here, associated with the
 * versions in which they are introduced. This creates a centralized location
 * for finding associations of features, serial versions, and release
 * versions. Existing constants (as of release 4.0) are spread throughout the
 * source and can be moved here as time permits.
 *
 * @see oracle.kv.impl.util.registry.VersionedRemote
 */
public class SerialVersion {

    public static final short UNKNOWN = -1;

    private static final Map<Short, KVVersion> kvVersions =
        new HashMap<Short, KVVersion>();

    /* Unsupported versions */

    /* R1 version */
    public static final short V1 = 1;
    static { init(V1, KVVersion.R1_2_123); }

    /* Introduced at R2 (2.0.23) */
    public static final short V2 = 2;
    static { init(V2, KVVersion.R2_0_23); }

    /* Introduced at R2.1 (2.1.8) */
    public static final short V3 = 3;
    static { init(V3, KVVersion.R2_1); }

    /* Supported versions */

    /*
     * Introduced at R3.0 (3.0.5)
     *  - secondary datacenters
     *  - table API
     */
    public static final short V4 = 4;
    static { init(V4, KVVersion.R3_0); }

    public static final short TABLE_API_VERSION = V4;

    /*
     * The default earliest supported serial version.
     */
    private static final short DEFAULT_MINIMUM = V4;

    /*
     * Check that the default minimum version matches the KVVersion
     * prerequisite version, since that is the first supported KV version.
     */
    static {
        assert KVVersion.PREREQUISITE_VERSION == getKVVersion(DEFAULT_MINIMUM);
    }

    /*
     * The earliest supported serial version.  Clients and servers should both
     * reject connections from earlier versions.
     */
    public static final short MINIMUM = Integer.getInteger(
        "oracle.kv.minimum.serial.version", DEFAULT_MINIMUM).shortValue();

    static {
        if (MINIMUM != DEFAULT_MINIMUM) {
            System.err.println("Setting SerialVersion.MINIMUM=" + MINIMUM);
        }
    }

    /* Introduced at R3.1 (3.1.0) for role-based authorization */
    public static final short V5 = 5;
    static { init(V5, KVVersion.R3_1); }

    /*
     * Introduced at R3.2 (3.2.0):
     * - real-time session update
     * - index key iteration
     */
    public static final short V6 = 6;
    static { init(V6, KVVersion.R3_2); }

    public static final short RESULT_INDEX_ITERATE_VERSION = V6;

    /*
     * Introduced at R3.3 (3.3.0) for secondary Admin type and JSON flag to
     * verifyConfiguration, and password expiration.
     */
    public static final short V7 = 7;
    static { init(V7, KVVersion.R3_2); }

    /*
     * Introduced at R3.4 (3.4.0) for the added replica threshold parameter on
     * plan methods, and the CommandService.getAdminStatus,
     * repairAdminQuorum, and createFailoverPlan methods.
     * Also added MetadataNotFoundException.
     *
     * Added bulk get APIs to Key/Value and Table interface.
     */
    public static final short V8 = 8;
    static { init(V8, KVVersion.R3_4); }

    public static final short BATCH_GET_VERSION = V8;

    /*
     * Introduced at R3.5 (3.5.0) for Admin automation V1 features, including
     * json format output, error code, and Kerberos authentication.
     *
     * Added bulk put APIs to Key/Value and Table interface.
     */
    public static final short V9 = 9;
    static { init(V9, KVVersion.R3_5); }

    public static final short BATCH_PUT_VERSION = V9;

    /*
     * The first version that support admin CLI output in JSON string.
     */
    public static final short ADMIN_CLI_JSON_V1_VERSION = V9;

    /*
     * Introduced at R4.0/V10:
     * - new query protocol operations. These were added in V10, but because
     *   they were "preview" there is no attempt to handle V10 queries in
     *   releases > V10. Because the serialization format of queries has changed
     *   such operations will fail with an appropriate message.
     * - time to live
     * - Arbiters
     * - Full text search
     */
    public static final short V10 = 10;
    static { init(V10, KVVersion.R4_0); }

    public static final short TTL_SERIAL_VERSION = V10;

    /*
     * Introduced at R4.1/V11
     * - SN/topology contraction
     * - query protocol change (not compatible with V10)
     * - new SNA API for mount point sizes
     */
    public static final short V11 = 11;
    static { init(V11, KVVersion.R4_1); }

    public static final short QUERY_VERSION = V11;

    /*
     * Introduced at R4.2/V12
     * - query protocol change (compatible with V11)
     * - getStorageNodeInfo added to SNA
     * - indicator bytes in indexes
     */
    public static final short V12 = 12;
    static { init(V12, KVVersion.R4_2); }

    public static final short QUERY_VERSION_2 = V12;

    /*
     * Introduced at R4.3/V13
     * - new SNI API for checking parameters
     */
    public static final short V13 = 13;
    static { init(V13, KVVersion.R4_3); }

    /* query protocol change (compatible with V11) */
    public static final short QUERY_VERSION_3 = V13;

    /*
     * Introduced at R4.4/V14
     * - Standard UTF-8 encoding
     * - typed indexes for JSON, affecting index plans
     * - SFWIter.theDoNullOnEmpty field
     * - Snapshot command is executed on the server side, using the admin to
     *   coordinate operations and locking.
     * - add TableQuery.mathContext field
     * - changed the value of the NULL indicator in index keys and
     *   added IndexImpl.serialVersion.
     */
    public static final short V14 = 14;
    static { init(V14, KVVersion.R4_4); }

    /** Use standard UTF-8 encoding rather than the modified Java encoding. */
    public static final short STD_UTF8_VERSION = V14;

    /* JSON index change */
    public static final short JSON_INDEX_VERSION = V14;
    /* Namespaces */
    public static final short NAMESPACE_VERSION = V14;

    public static final short QUERY_VERSION_4 = V14;

    /* Snapshot command change */
    public static final short SNAPSHOT_ON_SERVER_VERSION = V14;

    /* Network restore utility introduced at R4.4 */
    public static final short NETWORK_RESTORE_UTIL_VERSION = V14;

    /* Introduced at R4.5/V15
     * - Switched query and DDL statements to char[] from String.
     * - Added currentIndexRange field in TableQuery op
     * - BaseTableIter may carry more than 1 keys and ranges
     * - Add TABLE_V1 to Value.Format
     * - added theIsUpdate field in BaseTableIter and ReceiveIter
     */
    public static final short V15 = 15;

    static { init(V15, KVVersion.R4_5); }

    /* Switched statements to char[] for avoiding passwords in Strings */
    public static final short CHAR_ARRAY_STATEMENTS_VERSION = V15;

    public static final short QUERY_VERSION_5 = V15;

    /* Add TABLE_V1 to Value.Format */
    public static final short VALUE_FORMAT_TABLE_V1_VERSION = V15;

    /*
     * - Added members theIndexTupleRegs and theIndexResultReg in BaseTableIter
     * - Master affinity zone feature.
     * - Added failed shard removal.
     * - Added verify data feature.
     * - Added table limits
     * - Added LogContext string in Request.
     * - Check running subscription feeder before running elasticity operation
     * - Add maxReadKB to Table/Index iterate operation
     */
    public static final short V16 = 16;
    static { init(V16, KVVersion.R18_1); }

    public static final short QUERY_VERSION_6 = V16;

    /* Add metadataSeqNum to Result */
    public static final short RESULT_WITH_METADATA_SEQNUM = V16;

    /* Add Admin API to set table limits */
    public static final short TABLE_LIMITS_VERSION = V16;

    /* Added methods to support failed shard removal */
    public static final short TOPOLOGY_REMOVESHARD_VERSION = V16;

    /* Add APIs and additional result fields for resource tracking */
    public static final short RESOURCE_TRACKING_VERSION = V16;

    /* Master affinity zone feature */
    public static final short MASTER_AFFINITY_VERSION = V16;

    /*Add Admin API to verify data*/
    public static final short VERIFY_DATA_VERSION = V16;

    /* Support get table by id */
    public static final short GET_TABLE_BY_ID_VERSION = V16;

    /* Support all admin CLI for JSON output v2 */
    public static final short ADMIN_CLI_JSON_V2_VERSION = V16;

    /* Enable request type command */
    public static final short ENABLE_REQUEST_TYPE_VERSION = V16;

    /* Add emptyReadFactor to Table/Index iterate operation */
    public static final short EMPTY_READ_FACTOR_VERSION = V16;

    /* Pass a LogContext in the Request object. */
    public static final short LOGCONTEXT_REQUEST_VERSION = V16;

    /* Add maxReadKB to Table/Index iterate operation */
    public static final short MAXKB_ITERATE_VERSION = V16;

    /* Add tableId to execute operation */
    public static final short EXECUTE_OP_TABLE_ID = V16;

    /* Add maxWriteKB and resumeKey to MultiDeleteTable operation */
    public static final short MULTIDELTBL_WRITEKB_RESUMEKEY = V16;

    /*
     * When adding a new version and updating DEFAULT_CURRENT, be sure to make
     * corresponding changes in KVVersion as well as the files referenced from
     * there to add a new release version.
     */
    private static final short DEFAULT_CURRENT = V16;

    /**
     * The current serial version, with a system property override for use in
     * testing.
     */
    public static final short CURRENT = Integer.getInteger(
        "oracle.kv.test.currentserialversion", DEFAULT_CURRENT).shortValue();

    static {
        if (CURRENT != DEFAULT_CURRENT) {
            System.err.println("Setting SerialVersion.CURRENT=" + CURRENT);
        }
    }

    private static void init(short version, KVVersion kvVersion) {
        kvVersions.put(version, kvVersion);
    }

    public static KVVersion getKVVersion(short serialVersion) {
        return kvVersions.get(serialVersion);
    }

    /**
     * Creates an appropriate exception for a client that does not meet the
     * minimum required version.
     *
     * @param clientSerialVersion the serial version of the client
     * @param requiredSerialVersion the minimum required version
     * @return an appropriate exception
     */
    public static UnsupportedOperationException clientUnsupportedException(
        short clientSerialVersion, short requiredSerialVersion) {

        return new UnsupportedOperationException(
            "The client is incompatible with this service. " +
            "Client version is " +
            getKVVersion(clientSerialVersion).getNumericVersionString() +
            ", but the minimum required version is " +
            getKVVersion(requiredSerialVersion).getNumericVersionString());
    }

    /**
     * Creates an appropriate exception for a server that does not meet the
     * minimum required version.
     *
     * @param serverSerialVersion the serial version of the server
     * @param requiredSerialVersion the minimum required version
     * @return an appropriate exception
     */
    public static UnsupportedOperationException serverUnsupportedException(
        short serverSerialVersion, short requiredSerialVersion) {

        return new UnsupportedOperationException(
            "The server is incompatible with this client.  " +
            "Server version is " +
            getKVVersion(serverSerialVersion).getNumericVersionString() +
            ", but the minimum required version is " +
            getKVVersion(requiredSerialVersion).getNumericVersionString());
    }
}
