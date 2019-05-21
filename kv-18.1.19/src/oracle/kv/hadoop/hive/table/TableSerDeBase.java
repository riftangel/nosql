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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import oracle.kv.FaultException;
import oracle.kv.KVSecurityConstants;
import oracle.kv.KVSecurityException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.ParamConstant;
import oracle.kv.PasswordCredentials;
import oracle.kv.impl.security.PasswordManager;
import oracle.kv.impl.security.PasswordStore;
import oracle.kv.impl.security.util.KVStoreLogin;
import oracle.kv.table.FieldDef;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

/**
 * Abstract Hive SerDe class that performs deserialization and/or
 * serialization of data loaded into a KVStore via the Table API.
 */
@SuppressWarnings("deprecation")
abstract class TableSerDeBase implements SerDe {

    private static final String THIS_CLASSNAME =
                                    TableSerDeBase.class.getName();
    private static final String FILE_SEP =
        System.getProperty("file.separator");
    private static final String USER_SECURITY_DIR =
        System.getProperty("user.dir") + FILE_SEP + "TABLE_SERDE_SECURITY_DIR";

    private static final Log LOG = LogFactory.getLog(THIS_CLASSNAME);

    /* Fields shared by sub-classes */
    private KVStore kvStore = null;
    private TableAPI kvTableApi = null;
    private Table kvTable = null;
    private List<String> kvFieldNames = null;
    private List<FieldDef> kvFieldDefs = null;
    private List<FieldDef.Type> kvFieldTypes = null;

    private String hiveTableName = null;

    private LazySerDeParameters serdeParams;

    private ObjectInspector objInspector;

    /* Holds results of deserialization. */
    protected List<Object> hiveRow;

    /* Holds results of serialization. */
    protected MapWritable kvMapWritable;

    /*
     * Transient fields that do not survive serialization/deserialization.
     * The initial values for these fields are obtained from the TBLPROPERTIES
     * that are specified when the table is created for the first time on the
     * Hive client side. Since neither the values of these fields, nor the
     * value of TBLPROPERTIES will survive serialization/deserialization during
     * Hive query MapReduce processing, a static counterpart is specified for
     * each of these fields; where, during initialization, the value of each
     * such static field is set to the same initial value as its transient
     * counterpart. Then, when this class is reinitialized on the server side,
     * since the static fields will survive serialization/deserialization,
     * each such transient field uses its static counterpart (instead of
     * TBLPROPERTIES) to recover its corresponding value from the client
     * side.
     */
    private String kvStoreName = null;
    private String[] kvHelperHosts = null;
    private String[] kvHadoopHosts = null;
    private String kvTableName = null;

    private String loginFile = null;
    private String trustFile = null;
    private String username = null;
    private String password = null;

    /*
     * Static fields that survive serialization/deserialization. These fields
     * are the counterparts to the transient fields above.
     */
    private static String staticKvStoreName = null;
    private static String[] staticKvHelperHosts = null;
    private static String[] staticKvHadoopHosts = null;
    private static String staticKvTableName = null;

    private static String staticLoginFile = null;
    private static String staticTrustFile = null;
    private static String staticUsername = null;
    private static String staticPassword = null;

    /* Implementation-specific methods each sub-class must provide. */

    protected abstract void validateParams(Properties tbl)
                                throws SerDeException;

    protected abstract ObjectInspector createObjectInspector()
                                           throws SerDeException;

    /*
     * Methods required by BOTH the org.apache.hadoop.hive.serde2.Deserializer
     * interface and the org.apache.hadoop.hive.serde2.Serializer interface.
     * See org.apache.hadoop.hive.serde2.SerDe.
     */

    @Override
    public void initialize(Configuration job, Properties tbl)
                    throws SerDeException {
        initKvStoreParams(tbl);
        serdeParams = initSerdeParams(job, tbl);
        /* For degugging. */
        displayInitParams(serdeParams);
        validateParams(tbl);
        objInspector = createObjectInspector();
        hiveRow = new ArrayList<Object>();
        kvMapWritable = new MapWritable();
    }

    /**
     * Returns statistics collected when deserializing and/or serializing.
     */
    @Override
    public SerDeStats getSerDeStats() {
        return null;
    }

    /* Required by the org.apache.hadoop.hive.serde2.Deserializer interface. */

    /**
     * Deserializes the given Writable parameter and returns a Java Object
     * representing the contents of that parameter. The field parameter
     * of this method references a field from a row of the KVStore table
     * having the name specified by the tableName field of this class. Thus,
     * the Object returned by this method references the contents of that
     * table field.
     *
     * @param field The Writable object containing a serialized object from
     *        a row of the KVStore table with name specified by tableName.
     * @return A Java object representing the contents in the given table
     *         field parameter.
     */
    @Override
    public abstract Object deserialize(Writable field) throws SerDeException;

    /**
     * Returns the ObjectInspector that can be used to navigate through the
     * internal structure of the Object returned from the deserialize method.
     */
    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return objInspector;
    }


    /* Required by the org.apache.hadoop.hive.serde2.Serializer interface. */

    /**
     * Return the Writable class returned by the serialize method; which is
     * used to initialize the SequenceFile header.
     */
    @Override
    public Class<? extends Writable> getSerializedClass() {
        return MapWritable.class;
    }

    /**
     * Serialize the given Object by navigating inside the Object with the
     * given ObjectInspector. The given Object references a Hive row and
     * the return value is an instance of Writable that references a
     * KVStore table fieldName:fieldValue pair.
     *
     * @param obj The Object whose contents are examined and from which the
     *            return value is constructed.
     * @param objectInspector The object to use to navigate the given Object's
     *                        contents.
     * @return A Writable object representing the contents in the given
     *         Object to seriaize.
     */
    @Override
    public abstract Writable serialize(Object obj,
                                       ObjectInspector objectInspector)
                                           throws SerDeException;

    @Override
    public String toString() {

        /* Perform defensive coding to avoid NullPoinerException. */

        final String[] helperHosts = getKvHelperHosts();
        String helperHostsStr = null;
        if (helperHosts != null) {
            helperHostsStr = (Arrays.asList(helperHosts)).toString();
        }

        final String[] hadoopHosts = getKvHadoopHosts();
        String hadoopHostsStr = null;
        if (hadoopHosts != null) {
            hadoopHostsStr = (Arrays.asList(hadoopHosts)).toString();
        }

        String hiveColumnNamesStr = null;
        String hiveColumnTypesStr = null;
        if (serdeParams != null) {
            final TypeInfo typeInfo = serdeParams.getRowTypeInfo();
            if (typeInfo != null) {
                hiveColumnNamesStr =
                    (((StructTypeInfo) typeInfo).getAllStructFieldNames())
                                                    .toString();
                hiveColumnTypesStr =
                    (((StructTypeInfo) typeInfo).getAllStructFieldTypeInfos())
                                                    .toString();
            }
        }

        final StringBuilder buf = new StringBuilder("[");

        buf.append("kvStoreName=" + getKvStoreName());
        buf.append(":");
        buf.append("kvHelperHosts=" + helperHostsStr);
        buf.append(":");
        buf.append("kvHadoopHosts=" + hadoopHostsStr);
        buf.append(":");
        buf.append("kvTableName=" + getKvTableName());
        buf.append(":");
        buf.append("kvFieldNames=" + getKvFieldNames());
        buf.append(":");
        buf.append("kvFieldTypes=" + getKvFieldTypes());
        buf.append(":");
        buf.append("hiveTableName=" + getHiveTableName());
        buf.append(":");
        buf.append("hiveSeparators=" + getSeparatorsStr(serdeParams));
        buf.append(":");
        buf.append("hiveColumnNames=" + hiveColumnNamesStr);
        buf.append(":");
        buf.append("hiveColumnTypes=" + hiveColumnTypesStr);
        buf.append("]");

        return buf.toString();
    }

    String getKvStoreName() {
        return kvStoreName;
    }

    String[] getKvHelperHosts() {
        return kvHelperHosts;
    }

    String[] getKvHadoopHosts() {
        return kvHadoopHosts;
    }

    String getKvTableName() {
        return kvTableName;
    }

    TableAPI getKvTableApi() {
        return kvTableApi;
    }

    Table getKvTable() {
        return kvTable;
    }

    List<String> getKvFieldNames() {
        return kvFieldNames;
    }

    List<FieldDef> getKvFieldDefs() {
        return kvFieldDefs;
    }

    List<FieldDef.Type> getKvFieldTypes() {
        return kvFieldTypes;
    }

    LazySerDeParameters getSerdeParams() {
        return serdeParams;
    }

    String getHiveTableName() {
        return hiveTableName;
    }

    /**
     * Convenience method that return the byte value of the given number
     * String; where if the given defaultVal is returned if the given number
     * String is not a number.
     */
    static byte getByte(String altValue, byte defaultVal) {
        if (altValue != null && altValue.length() > 0) {
            try {
                return Byte.valueOf(altValue).byteValue();
            } catch (NumberFormatException e) {
                return (byte) altValue.charAt(0);
            }
        }
        return defaultVal;
      }

    /**
     * Convenience method that returns a comma-separated String consisting of
     * the Hive column separators specified in the given LazySerDeParameters.
     */
    String getSeparatorsStr(LazySerDeParameters params) {
        final StringBuilder buf = new StringBuilder("[");
        if (params != null) {
            final byte[] seps = params.getSeparators();
            for (int i = 0; i < seps.length; i++) {
                buf.append(seps[i]);
                if (i < seps.length - 1) {
                    buf.append(",");
                }
            }
        }
        buf.append("]");
        return buf.toString();
    }

    private void initKvStoreParams(Properties tbl) throws SerDeException {

        kvStoreName = tbl.getProperty(ParamConstant.KVSTORE_NAME.getName());
        if (kvStoreName == null) {
            kvStoreName = staticKvStoreName;
            if (kvStoreName == null) {
                final String msg =
                    "No KV Store name specified. Specify the store name via " +
                    "the '" + ParamConstant.KVSTORE_NAME.getName() +
                    "' property in the TBLPROPERTIES clause when creating " +
                    "the Hive table";
                LOG.error(msg);
                throw new SerDeException(new IllegalArgumentException(msg));
            }
        } else {
            staticKvStoreName = kvStoreName;
        }

        final String helperHosts =
                tbl.getProperty(ParamConstant.KVSTORE_NODES.getName());
        if (helperHosts != null) {
            kvHelperHosts = helperHosts.trim().split(",");
            staticKvHelperHosts = helperHosts.trim().split(",");
        } else {
            if (staticKvHelperHosts != null) {
                kvHelperHosts = new String[staticKvHelperHosts.length];
                for (int i = 0; i < staticKvHelperHosts.length; i++) {
                    kvHelperHosts[i] = staticKvHelperHosts[i];
                }
            } else {
                final String msg =
                    "No KV Store helper hosts specified. Specify the helper " +
                    "hosts via the '" + ParamConstant.KVSTORE_NODES.getName() +
                    "' property in the TBLPROPERTIES clause when creating " +
                    "the Hive table";
                LOG.error(msg);
                throw new SerDeException(new IllegalArgumentException(msg));
            }
        }

        final String hadoopHosts =
                tbl.getProperty(ParamConstant.KVHADOOP_NODES.getName());
        if (hadoopHosts != null) {
            kvHadoopHosts = hadoopHosts.trim().split(",");
            staticKvHadoopHosts = hadoopHosts.trim().split(",");
        } else {
            if (staticKvHadoopHosts != null) {
                kvHadoopHosts = new String[staticKvHadoopHosts.length];
                for (int i = 0; i < staticKvHadoopHosts.length; i++) {
                    kvHadoopHosts[i] = staticKvHadoopHosts[i];
                }
            } else {
                kvHadoopHosts = new String[kvHelperHosts.length];
                staticKvHadoopHosts = new String[staticKvHelperHosts.length];
                for (int i = 0; i < kvHelperHosts.length; i++) {
                    /* Strip off the ':port' suffix */
                    final String[] hostPort =
                        (kvHelperHosts[i]).trim().split(":");
                    final String[] staticHostPort =
                        (staticKvHelperHosts[i]).trim().split(":");
                    kvHadoopHosts[i] = hostPort[0];
                    staticKvHadoopHosts[i] = staticHostPort[0];
                }
            }
        }

        kvTableName = tbl.getProperty(ParamConstant.TABLE_NAME.getName());
        if (kvTableName == null) {
            kvTableName = staticKvTableName;
            if (kvTableName == null) {
                final String msg =
                    "No KV Table name specified. Specify the table name via " +
                    "the '" + ParamConstant.TABLE_NAME.getName() +
                    "' property in the TBLPROPERTIES clause when creating " +
                    "the Hive table";
                LOG.error(msg);
                throw new SerDeException(new IllegalArgumentException(msg));
            }
        } else {
            staticKvTableName = kvTableName;
        }

        /* Handle the params related to security. */
        final String loginFileVal =
            tbl.getProperty(KVSecurityConstants.SECURITY_FILE_PROPERTY);
        if (loginFileVal != null) {
            loginFile = loginFileVal;
            staticLoginFile = loginFileVal;
        } else {
            if (staticLoginFile != null) {
                loginFile = staticLoginFile;
            }
        }

        final String trustFileVal =
            tbl.getProperty(KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY);
        if (trustFileVal != null) {
            trustFile = trustFileVal;
            staticTrustFile = trustFileVal;
        } else {
            if (staticTrustFile != null) {
                trustFile = staticTrustFile;
            }
        }

        final String localLoginFile =
                         createLocalKVSecurity(loginFile, trustFile);

        final String usernameVal =
            tbl.getProperty(KVSecurityConstants.AUTH_USERNAME_PROPERTY);
        if (usernameVal != null) {
            username = usernameVal;
            staticUsername = usernameVal;
        } else {
            if (staticUsername != null) {
                username = staticUsername;
            }
        }

        final String passwordVal =
            tbl.getProperty(ParamConstant.AUTH_USER_PWD_PROPERTY.getName());
        if (passwordVal != null) {
            password = passwordVal;
            staticPassword = passwordVal;
        } else {
            if (staticPassword != null) {
                password = staticPassword;
            }
        }

        PasswordCredentials passwordCreds = null;
        if (username != null && password != null) {
            final char[] userPassword = password.toCharArray();
            passwordCreds =
                new PasswordCredentials(username, userPassword);
        } else {
            passwordCreds = createPasswordCredentials(tbl, username);
        }

        final KVStoreConfig kvStoreConfig =
                new KVStoreConfig(kvStoreName, kvHelperHosts);
        kvStoreConfig.setSecurityProperties(
              KVStoreLogin.createSecurityProperties(localLoginFile));

        /*
         * If the same Hive CLI session is used to run queries that must
         * connect to different KVStores where one store is non-secure and
         * the other is secure, then since the security information is stored
         * in the state of this class, if an attempt is made to connect to
         * the non-secure store and that security information is non-null,
         * then a FaultException will be thrown when attempting to connect
         * to the store. This is because that non-null security information
         * will cause the connection mechanism to attempt a secure connection
         * instead of a non-secure connection. To address this, FaultException
         * is caught and the connection attempt is retried with no security
         * information.
         *
         * Also, to support testing using a KVStore mock, the kvstore is
         * tested for null. Where if kvstore is NOT null, then it is assumed
         * that a mock of the kvstore has been created by a test, and the
         * attempt to find and connect to the real kvstore is by-passed.
         */
        RuntimeException cause = null;
        if (kvStore == null) {
            try {
                kvStore = KVStoreFactory.getStore(
                              kvStoreConfig, passwordCreds, null);
            } catch (FaultException fe) {
                cause = fe;
            } catch (KVSecurityException kse) {
                cause = kse;
            }

            if (cause != null) {
                if (passwordCreds != null) {
                    final KVStoreConfig kvStoreConfigNonSecure =
                        new KVStoreConfig(kvStoreName, kvHelperHosts);
                    kvStoreConfigNonSecure.setSecurityProperties(
                        KVStoreLogin.createSecurityProperties(null));
                    kvStore = KVStoreFactory.getStore(kvStoreConfigNonSecure);
                } else {
                    throw cause;
                }
            }
        }

        kvTableApi = kvStore.getTableAPI();
        kvTable = kvTableApi.getTable(kvTableName);
        if (kvTable == null) {
            final String msg =
                "Store does not contain table [name=" + kvTableName + "]";
            LOG.error(msg);
            throw new SerDeException(new IllegalArgumentException(msg));
        }

        kvFieldNames = kvTable.getFields();
        kvFieldDefs = new ArrayList<FieldDef>();
        kvFieldTypes = new ArrayList<FieldDef.Type>();
        for (String fieldName : kvFieldNames) {
            final FieldDef fieldDef = kvTable.getField(fieldName);
            kvFieldDefs.add(fieldDef);
            kvFieldTypes.add(fieldDef.getType());
        }
    }

    private LazySerDeParameters initSerdeParams(
                Configuration job, Properties tbl) throws SerDeException {
        hiveTableName =
            tbl.getProperty(hive_metastoreConstants.META_TABLE_NAME);
        /*
         * The above returns a value of the form: 'databaseName.tableName'.
         * For the logging output below, strip off the database name prefix.
         */
        final int indx = hiveTableName.indexOf(".");
        if (indx >= 0) {
            hiveTableName = hiveTableName.substring(indx + 1);
        }
        return new LazySerDeParameters(job, tbl, THIS_CLASSNAME);
    }

    /*
     * Convenience method for debugging. Performs defensive coding to avoid
     * NullPoinerException.
     */
    private void displayInitParams(final LazySerDeParameters params) {

        final String[] helperHosts = getKvHelperHosts();
        String helperHostsStr = null;
        if (helperHosts != null) {
            helperHostsStr = (Arrays.asList(helperHosts)).toString();
        }

        final String[] hadoopHosts = getKvHadoopHosts();
        String hadoopHostsStr = null;
        if (hadoopHosts != null) {
            hadoopHostsStr = (Arrays.asList(hadoopHosts)).toString();
        }

        String hiveColumnNamesStr = null;
        String hiveColumnTypesStr = null;
        String nullSequenceStr = null;
        String isLastColumnTakesRestStr = null;
        String isEscapedStr = null;
        String escapeCharStr = null;
        if (params != null) {
            hiveColumnNamesStr = (params.getColumnNames()).toString();
            hiveColumnTypesStr = (params.getColumnTypes()).toString();
            nullSequenceStr = (params.getNullSequence()).toString();
            isLastColumnTakesRestStr =
                Boolean.toString(params.isLastColumnTakesRest());
            isEscapedStr = Boolean.toString(params.isEscaped());
            escapeCharStr = Byte.toString(params.getEscapeChar());
        }

        LOG.debug("kvStoreName = " + getKvStoreName());
        LOG.debug("kvHelperHosts = " + helperHostsStr);
        LOG.debug("kvHadoopHosts = " + hadoopHostsStr);
        LOG.debug("kvTableName = " + getKvTableName());
        LOG.debug("kvFieldNames = " + getKvFieldNames());
        LOG.debug("kvFieldTypes = " + getKvFieldTypes());
        LOG.debug("hiveTableName = " + getHiveTableName());
        LOG.debug("hiveSeparators = " + getSeparatorsStr(params));
        LOG.debug("hiveColumnNames = " + hiveColumnNamesStr);
        LOG.debug("hiveColumnTypes = " + hiveColumnTypesStr);
        LOG.debug("nullSequence = " + nullSequenceStr);
        LOG.debug("lastColumnTakesRest = " + isLastColumnTakesRestStr);
        LOG.debug("isEscaped = " + isEscapedStr);
        LOG.debug("escapeChar = " + escapeCharStr);
    }

    private String createLocalKVSecurity(final String loginFlnm,
                                         final String trustFlnm)
                              throws SerDeException {

        if (loginFlnm == null) {
            return null;
        }

        if (trustFlnm == null) {
            return null;
        }

        String localLoginFile = loginFlnm;
        String localTrustFile = trustFlnm;

        final File localLoginFileFd = new File(localLoginFile);
        final File localTrustFileFd = new File(localTrustFile);

        if (!localLoginFileFd.exists() || !localTrustFileFd.exists()) {

            final ClassLoader cl = TableSerDeBase.class.getClassLoader();
            final File userSecurityDirFd = new File(USER_SECURITY_DIR);
            if (!userSecurityDirFd.exists()) {
                if (!userSecurityDirFd.mkdirs()) {
                    throw new SerDeException(
                                  new IOException(
                                     "failed to create " + userSecurityDirFd));
                }
            }

            try {
                if (!localTrustFileFd.exists()) {
                    if (localTrustFileFd.isAbsolute()) {
                        localTrustFile = localTrustFileFd.getName();
                    }
                    InputStream trustStream = null;
                    if (cl != null) {
                        trustStream = cl.getResourceAsStream(localTrustFile);
                    } else {
                        trustStream =
                        ClassLoader.getSystemResourceAsStream(localTrustFile);
                    }

                    final File trustFd =
                       new File(USER_SECURITY_DIR + FILE_SEP + localTrustFile);
                    final FileOutputStream trustFlnmFos =
                                           new FileOutputStream(trustFd);

                    if (trustStream != null) {
                        int nextByte = trustStream.read();
                        while (nextByte != -1) {
                            trustFlnmFos.write(nextByte);
                            nextByte = trustStream.read();
                        }
                    }
                    trustFlnmFos.close();
                }

                if (!localLoginFileFd.exists()) {
                    String loginFileNoPath = localLoginFile;
                    if (localLoginFileFd.isAbsolute()) {
                        loginFileNoPath = localLoginFileFd.getName();
                    }
                    InputStream loginStream = null;
                    if (cl != null) {
                        loginStream = cl.getResourceAsStream(loginFileNoPath);
                    } else {
                        loginStream =
                        ClassLoader.getSystemResourceAsStream(loginFileNoPath);
                    }
                    final Properties loginProps = new Properties();
                    if (loginStream != null) {
                        loginProps.load(loginStream);
                    }

                    final String trustFlnmFromLogin =
                        loginProps.getProperty(
                            KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY);
                    if (trustFlnmFromLogin != null &&
                        !trustFlnmFromLogin.equals(localTrustFile)) {

                        /* Replace <path>/trustFlnm with existing trustFlnm. */
                        loginProps.setProperty(
                            KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY,
                            localTrustFile);
                    }
                    localLoginFile =
                        USER_SECURITY_DIR + FILE_SEP + loginFileNoPath;
                    final File loginFd = new File(localLoginFile);
                    final FileOutputStream loginFos =
                               new FileOutputStream(loginFd);
                    loginProps.store(loginFos, null);
                    loginFos.close();
                }
            } catch (IOException e) {
                throw new SerDeException(e);
            }
        }
        return localLoginFile;
    }

    private PasswordCredentials createPasswordCredentials(
                                    final Properties tblProps,
                                    final String user)
                              throws SerDeException {
        if (user == null) {
            return null;
        }

        String passwordLoc = null;
        Integer passwordOrWallet = null; /* 0=file, 1=wallet, null=no pwd */
        PasswordCredentials passwordCredentials = null;

        String passwordLocVal = tblProps.getProperty(
                                     KVSecurityConstants.AUTH_WALLET_PROPERTY);
        if (passwordLocVal != null) {
            passwordLoc = passwordLocVal;
            passwordOrWallet = 1;
        } else {
            passwordLocVal = tblProps.getProperty(
                                 KVSecurityConstants.AUTH_PWDFILE_PROPERTY);

            if (passwordLocVal != null) {
                passwordLoc = passwordLocVal;
                passwordOrWallet = 0;
            }
        }

        if (passwordLoc != null) {
            PasswordStore passwordStore = null;

            /* Retrieve the password from the wallet or password file. */
            if (passwordOrWallet != null) {

                PasswordManager storeMgr = null;
                if (passwordOrWallet == 1) {
                    final File walletDirFd = new File(passwordLoc);
                    if (walletDirFd.exists()) {
                        try {
                            storeMgr = PasswordManager.load(
                                PasswordManager.WALLET_MANAGER_CLASS);
                        } catch (Exception e) {
                            e.printStackTrace(); /* Send to DataNode stderr. */
                            throw new SerDeException(e); /* Send to Hive CLI */
                        }
                        passwordStore = storeMgr.getStoreHandle(walletDirFd);
                    }
                } else {
                    final File passwordFileFd = new File(passwordLoc);
                    if (passwordFileFd.exists()) {
                        try {
                            storeMgr = PasswordManager.load(
                                PasswordManager.FILE_STORE_MANAGER_CLASS);
                        } catch (Exception e) {
                            e.printStackTrace(); /* Send to DataNode stderr. */
                            throw new SerDeException(e); /* Send to Hive CLI */
                        }
                        passwordStore =
                            storeMgr.getStoreHandle(passwordFileFd);
                    }
                }
            }

            /* Create the PasswordCredentials from user & password. */
            if (passwordStore != null) {
                try {
                    passwordStore.open(null);
                    final Collection<String> secretAliases =
                                    passwordStore.getSecretAliases();
                    final Iterator<String> aliasItr = secretAliases.iterator();
                    final char[] userPassword = (aliasItr.hasNext() ?
                        passwordStore.getSecret(aliasItr.next()) : null);
                    username = user;
                    staticUsername = username;
                    password = String.valueOf(userPassword);
                    staticPassword = password;
                    passwordCredentials =
                        new PasswordCredentials(user, userPassword);
                } catch (IOException e) {
                    throw new SerDeException(e);
                }
            }
        }
        return passwordCredentials;
    }

    /**
     * For testing only; to support the use of a mocked store.
     */
    protected void setStore(final KVStore testStore) {
        this.kvStore = testStore;
    }
}
