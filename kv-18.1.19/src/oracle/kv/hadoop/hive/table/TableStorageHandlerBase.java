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
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.KVSecurityConstants;
import oracle.kv.ParamConstant;
import oracle.kv.hadoop.table.TableInputSplit;
import oracle.kv.impl.security.PasswordManager;
import oracle.kv.impl.security.PasswordStore;
import oracle.kv.impl.util.ExternalDataSourceUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;

/**
 * Abstract class which is used as the base class for Oracle NoSQL Database
 * Hive StorageHandler classes designed to work with data stored via the
 * Table API.
 * <p>
 *
 * For a list of parameters that are recognized, see the javadoc for
 * {@link oracle.kv.hadoop.table.TableInputFormatBase}.
 *
 * @since 3.1
 */
abstract class TableStorageHandlerBase<K extends WritableComparable<?>,
                                       V extends Writable>
                          extends DefaultStorageHandler
                          implements HiveStoragePredicateHandler {

    private static final Log LOG = LogFactory.getLog(
                       "oracle.kv.hadoop.hive.table.TableStorageHandlerBase");

    protected String kvStoreName = null;
    protected String[] kvHelperHosts = null;
    protected String[] kvHadoopHosts = null;
    protected String tableName = null;

    protected String primaryKeyProperty = null;

    /* For MultiRowOptions */
    protected String fieldRangeProperty = null;

    /* For TableIteratorOptions */
    protected Direction direction = Direction.UNORDERED;
    protected Consistency consistency = null;
    protected long timeout = 0;
    protected TimeUnit timeoutUnit = TimeUnit.MILLISECONDS;
    protected int maxRequests = 0;
    protected int batchSize = 0;
    protected int maxBatches = 0;

    protected JobConf jobConf = new JobConf();

    /**
     * @hidden
     */
    protected TableStorageHandlerBase() {
    }

    /**
     * Creates a configuration for job input. This method provides the
     * mechanism for populating this StorageHandler's configuration (returned
     * by <code>JobContext.getConfiguration</code>) with the properties that
     * may be needed by the handler's bundled artifacts; for example, the
     * <code>InputFormat</code> class, the <code>SerDe</code> class, etc.
     * returned by this handler. Any key value pairs set in the
     * <code>jobProperties</code> argument are guaranteed to be set in the
     * job's configuration object; and any "context" information associated
     * with the job can be retrieved from the given <code>TableDesc</code>
     * parameter.
     * <p>
     * Note that implementations of this method must be idempotent. That
     * is, when this method is invoked more than once with the same
     * <code>tableDesc</code> values for a given job, the key value pairs
     * in <code>jobProperties</code>, as well as any external state set
     * by this method, should be the same after each invocation. How this
     * invariant guarantee is achieved is left as an implementation detail;
     * although to support this guarantee, changes should only be made
     * to the contents of <code>jobProperties</code>, but never to
     * <code>tableDesc</code>.
     */
    @Override
    public void configureInputJobProperties(
        TableDesc tableDesc, Map<String, String> jobProperties) {

        configureJobProperties(tableDesc, jobProperties);
    }

    /**
     * Using semantics identical to the semantics of the
     *<code>configureInputJobProperties</code> method, creates a
     * configuration for job output. For more detail, refer to the
     * description of the <code>configureInputJobProperties</code> method.
     */
    @Override
    public void configureOutputJobProperties(
        TableDesc tableDesc, Map<String, String> jobProperties) {

        configureJobProperties(tableDesc, jobProperties);
    }

    /**
     * Although this method was originally intended to configure
     * properties for a job based on the definition of the source or
     * target table the job accesses, this method is now deprecated
     * in Hive. The methods <code>configureInputJobProperties</code>
     * and <code>configureOutputJobProperties</code> should be used
     * instead.
     */
    @Override
    public void configureTableJobProperties(
        TableDesc tableDesc, Map<String, String> jobProperties) {

        configureJobProperties(tableDesc, jobProperties);
    }

    private void configureJobProperties(TableDesc tableDesc,
                                        Map<String, String> jobProperties) {

        /* Retrieve all properties specified when Hive table was created. */
        final Properties tableProperties = tableDesc.getProperties();

        /* REQUIRED properties. */
        kvStoreName =
            tableProperties.getProperty(ParamConstant.KVSTORE_NAME.getName());
        if (kvStoreName == null) {
            throw new IllegalArgumentException
                ("No KV Store Name provided via the '" +
                 ParamConstant.KVSTORE_NAME.getName() + "' property in the " +
                 "TBLPROPERTIES clause.");
        }
        LOG.debug("kvStoreName = " + kvStoreName);

        jobProperties.put(ParamConstant.KVSTORE_NAME.getName(), kvStoreName);
        jobConf.set(ParamConstant.KVSTORE_NAME.getName(), kvStoreName);

        final String kvHelperHostsStr =
            tableProperties.getProperty(ParamConstant.KVSTORE_NODES.getName());

        if (kvHelperHostsStr != null) {
            kvHelperHosts = kvHelperHostsStr.trim().split(",");
        } else {
            throw new IllegalArgumentException
                ("No comma-separated list of hostname:port pairs (KV Helper " +
                 "Hosts) provided via the '" +
                 ParamConstant.KVSTORE_NODES.getName() + "' property in the " +
                 "TBLPROPERTIES clause.");
        }
        jobProperties.put(
            ParamConstant.KVSTORE_NODES.getName(), kvHelperHostsStr);
        jobConf.set(ParamConstant.KVSTORE_NODES.getName(), kvHelperHostsStr);
        LOG.debug("kvHelperHosts = " + kvHelperHostsStr);

        /* Hadoop Hosts are required only for BigData SQL. */
        String kvHadoopHostsStr =
           tableProperties.getProperty(ParamConstant.KVHADOOP_NODES.getName());

        if (kvHadoopHostsStr != null) {
            kvHadoopHosts = kvHadoopHostsStr.trim().split(",");
        } else {
            kvHadoopHosts = new String[kvHelperHosts.length];
            final StringBuilder hadoopBuf = new StringBuilder();
            for (int i = 0; i < kvHelperHosts.length; i++) {
                /* Strip off the ':port' suffix */
                final String[] hostPort = (kvHelperHosts[i]).trim().split(":");
                kvHadoopHosts[i] = hostPort[0];
                if (i != 0) {
                    hadoopBuf.append(",");
                }
                hadoopBuf.append(kvHadoopHosts[i]);
            }
            kvHadoopHostsStr = hadoopBuf.toString();
        }
        jobProperties.put(
            ParamConstant.KVHADOOP_NODES.getName(), kvHadoopHostsStr);
        jobConf.set(ParamConstant.KVHADOOP_NODES.getName(), kvHadoopHostsStr);
        LOG.debug("kvHadoopHosts = " + kvHadoopHostsStr);

        tableName =
            tableProperties.getProperty(ParamConstant.TABLE_NAME.getName());
        if (tableName == null) {
            throw new IllegalArgumentException
                ("No KV Store Table Name provided via the '" +
                 ParamConstant.TABLE_NAME.getName() + "' property in the " +
                 "TBLPROPERTIES clause.");
        }
        jobProperties.put(ParamConstant.TABLE_NAME.getName(), tableName);
        jobConf.set(ParamConstant.TABLE_NAME.getName(), tableName);
        LOG.debug("tableName = " + tableName);

        /* OPTIONAL properties. */
        primaryKeyProperty = tableProperties.getProperty(
            ParamConstant.PRIMARY_KEY.getName());
        if (primaryKeyProperty != null) {
            jobProperties.put(
                ParamConstant.PRIMARY_KEY.getName(), primaryKeyProperty);
            jobConf.set(
                ParamConstant.PRIMARY_KEY.getName(), primaryKeyProperty);
        }
        LOG.trace("primaryKeyProperty = " + primaryKeyProperty);

        /* For MultiRowOptions. */
        fieldRangeProperty = tableProperties.getProperty(
            ParamConstant.FIELD_RANGE.getName());
        if (fieldRangeProperty != null) {
            jobProperties.put(
                ParamConstant.FIELD_RANGE.getName(), fieldRangeProperty);
            jobConf.set(
                ParamConstant.FIELD_RANGE.getName(), fieldRangeProperty);
        }
        LOG.trace("fieldRangeProperty = " + fieldRangeProperty);

        /*
         * For TableIteratorOptions. Note that when doing PrimaryKey iteration,
         * Direction must be UNORDERED.
         */
        final String consistencyStr = tableProperties.getProperty(
            ParamConstant.CONSISTENCY.getName());
        if (consistencyStr != null) {
            consistency = ExternalDataSourceUtils.parseConsistency(
                                                      consistencyStr);
            jobProperties.put(
                ParamConstant.CONSISTENCY.getName(), consistencyStr);
            jobConf.set(ParamConstant.CONSISTENCY.getName(), consistencyStr);
        }
        LOG.trace("consistency = " + consistencyStr);

        final String timeoutStr = tableProperties.getProperty(
            ParamConstant.TIMEOUT.getName());
        if (timeoutStr != null) {
            timeout = ExternalDataSourceUtils.parseTimeout(timeoutStr);
            timeoutUnit = TimeUnit.MILLISECONDS;
            jobProperties.put(ParamConstant.TIMEOUT.getName(), timeoutStr);
            jobConf.set(ParamConstant.TIMEOUT.getName(), timeoutStr);
        }
        LOG.trace("timeout = " + timeout);

        final String maxRequestsStr = tableProperties.getProperty(
            ParamConstant.MAX_REQUESTS.getName());
        if (maxRequestsStr != null) {
            try {
                maxRequests = Integer.parseInt(maxRequestsStr);
                jobProperties.put(
                    ParamConstant.MAX_REQUESTS.getName(), maxRequestsStr);
                jobConf.set(
                    ParamConstant.MAX_REQUESTS.getName(), maxRequestsStr);
            } catch (NumberFormatException NFE) {
                LOG.warn("Invalid value for " +
                     ParamConstant.MAX_REQUESTS.getName() + " [" +
                     maxRequestsStr + "]: proceeding with value determined " +
                     "by system");
            }
        }
        LOG.trace("maxRequests = " + maxRequests);

        final String batchSizeStr = tableProperties.getProperty(
            ParamConstant.BATCH_SIZE.getName());
        if (batchSizeStr != null) {
            try {
                batchSize = Integer.parseInt(batchSizeStr);
                jobProperties.put(
                    ParamConstant.BATCH_SIZE.getName(), batchSizeStr);
                jobConf.set(
                    ParamConstant.BATCH_SIZE.getName(), batchSizeStr);
            } catch (NumberFormatException NFE) {
                LOG.warn("Invalid value for " +
                     ParamConstant.BATCH_SIZE.getName() + " [" +
                     batchSizeStr + "]: proceeding with value determined " +
                     "by system");
            }
        }
        LOG.trace("batchSize = " + batchSize);

        final String maxBatchesStr = tableProperties.getProperty(
            ParamConstant.MAX_BATCHES.getName());
        if (maxBatchesStr != null) {
            try {
                maxBatches = Integer.parseInt(maxBatchesStr);
                jobProperties.put(
                    ParamConstant.MAX_BATCHES.getName(), maxBatchesStr);
                jobConf.set(
                    ParamConstant.MAX_BATCHES.getName(), maxBatchesStr);
            } catch (NumberFormatException NFE) {
                LOG.warn("Invalid value for " +
                     ParamConstant.MAX_BATCHES.getName() + " [" +
                     maxBatchesStr + "]: proceeding with value determined " +
                     "by system");
            }
        }
        LOG.trace("maxBatches = " + maxBatches);

        /* Handle properties related to security. */
        configureKVSecurityProperties(tableProperties, jobProperties);

        setConf(new Configuration(jobConf));
    }

    private void configureKVSecurityProperties(
                     Properties tblProperties,
                     Map<String, String> jobProperties) {

        final String loginFile = tblProperties.getProperty(
                         KVSecurityConstants.SECURITY_FILE_PROPERTY);
        if (loginFile != null) {
            jobProperties.put(
                KVSecurityConstants.SECURITY_FILE_PROPERTY,
                loginFile);
            jobConf.set(KVSecurityConstants.SECURITY_FILE_PROPERTY,
                        loginFile);
        }

        final String trustFile = tblProperties.getProperty(
                         KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY);
        if (trustFile != null) {
            jobProperties.put(
                KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY,
                trustFile);
            jobConf.set(KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY,
                        trustFile);
        }

        final String username = tblProperties.getProperty(
                         KVSecurityConstants.AUTH_USERNAME_PROPERTY);
        if (username != null) {
            jobProperties.put(KVSecurityConstants.AUTH_USERNAME_PROPERTY,
                              username);
            jobConf.set(KVSecurityConstants.AUTH_USERNAME_PROPERTY,
                        username);
        }

        /* Determine if wallet or password file and get file/dir name. */
        Integer passwordOrWallet = null; /* 0=file, 1=wallet, null=no pwd */

        String passwordLocProp = KVSecurityConstants.AUTH_WALLET_PROPERTY;
        String passwordLoc = tblProperties.getProperty(passwordLocProp);

        if (passwordLoc != null) {
            passwordOrWallet = 1;
        } else {
            passwordLocProp = KVSecurityConstants.AUTH_PWDFILE_PROPERTY;
            passwordLoc = tblProperties.getProperty(passwordLocProp);
            if (passwordLoc != null) {
                passwordOrWallet = 0;
            }
        }

        /*
         * The tblProperties from tableDesc are populated from the various
         * system property values specified via the TBLPROPERTIES directive
         * when the Hive external table is created. If the query is to be
         * run against a secure store, then the username and either a password
         * file or a wallet must be specified; but the password itself is not
         * specified in TBLPROPERTIES. If the query is the type of query that
         * Hive executes by running a MapReduce job (rather than the type in
         * which the metadatastore is employed to execute the query from only
         * the Hive client), then the backend of the MapReduce job will not be
         * able to access the password file or wallet; and thus, will not be
         * able to obtain the password by reading the file or wallet, as is
         * done from the Hive client. This is because the file and the wallet
         * are not transferred from the frontend client side of the query to
         * the backend MapReduce side (the DataNodes) of the query. As a
         * result, the password must be retrieved here on the client side,
         * from the password file or wallet located on the client's local file
         * system, and then be directly placed in tblProperties (as well as
         * the jobProperties and jobConf). Because the Hive infrastructure
         * transfers the tblProperties to the backend, the MapReduce job uses
         * the transferred tblProperties it receives to obtain the password.
         */
        if (passwordLoc != null) {
            jobProperties.put(passwordLocProp, passwordLoc);
            jobConf.set(passwordLocProp, passwordLoc);

            PasswordStore passwordStore = null;

            if (passwordOrWallet != null) {

                PasswordManager storeMgr = null;
                if (passwordOrWallet == 1) {
                    final File walletDirFd = new File(passwordLoc);
                    if (walletDirFd.exists()) {
                        try {
                            storeMgr = PasswordManager.load(
                                PasswordManager.WALLET_MANAGER_CLASS);
                        } catch (Exception e) {
                            e.printStackTrace(); /* Send to Hive log file. */
                            throw new SecurityException(e); /* Send to CLI. */
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
                            e.printStackTrace(); /* Send to Hive log file. */
                            throw new SecurityException(e); /* Send to CLI. */
                        }
                        passwordStore =
                            storeMgr.getStoreHandle(passwordFileFd);
                    }
                }
            }

            if (passwordStore != null) {
                try {
                    passwordStore.open(null);
                    final Collection<String> secretAliases =
                                    passwordStore.getSecretAliases();
                    final Iterator<String> aliasItr = secretAliases.iterator();
                    final char[] userPassword = (aliasItr.hasNext() ?
                        passwordStore.getSecret(aliasItr.next()) : null);
                    final String password = String.valueOf(userPassword);

                    tblProperties.put(
                        ParamConstant.AUTH_USER_PWD_PROPERTY.getName(),
                        password);
                    jobProperties.put(
                        ParamConstant.AUTH_USER_PWD_PROPERTY.getName(),
                        password);
                    jobConf.set(
                        ParamConstant.AUTH_USER_PWD_PROPERTY.getName(),
                        password);
                } catch (IOException e) {
                    throw new SecurityException(e);
                } finally {
                    passwordStore.discard();
                }
            }
        }
    }

    /**
     * Method required by the HiveStoragePredicateHandler interface.
     * <p>
     * This method validates the components of the given predicate and
     * ultimately produces the following artifacts:
     *
     * <ul>
     *   <li>a Hive object representing the predicate that will be pushed to
     *       the backend for server side filtering
     *   <li>the String form of the computed predicate to push; which can be
     *       passed to the server via the ONSQL query mechanism
     *   <li>a Hive object consisting of the remaining components of the
     *       original predicate input to this method -- referred to as the
     *       'residual' predicate; which represents the criteria the Hive
     *       infrastructure will apply (on the client side) to the results
     *       returned after server side filtering has been performed
     * </ul>
     *
     * The predicate analysis model that Hive employs is basically a two
     * step process. First, an instance of the Hive IndexPredicateAnalyzer
     * class is created and its analyzePredicate method is invoked, which
     * returns a Hive class representing the residual predicate, and also
     * populates a Collection whose contents is dependent on the particular
     * implementation of IndexPredicateAnalyzer that is used. After
     * analyzePredicate is invoked, the analyzer's translateSearchConditions
     * method is invoked to convert the contents of the populated Collection
     * to a Hive object representing the predicate that can be pushed to
     * the server side. Finally, the object that is returned is an instance
     * of the Hive DecomposedPredicate class; which contains the computed
     * predicate to push and the residual predicate.
     * <p>
     * Note that because the Hive built-in IndexPredicateAnalyzer produces
     * only predicates that consist of 'AND' statements, and which correspond
     * to PrimaryKey based or IndexKey based predicates, if the Hive built-in
     * analyzer does not produce a predicate to push, then a custom analyzer
     * that extends the capabilities of the Hive built-in analyzer is
     * employed. This extended analyzer handles statements that the built-in
     * analyzer does not handle. Additionally, whereas the built-in analyzer
     * populates a List of Hive IndexSearchConditions corresponding to the
     * filtering criteria of the predicate to push, the extended analyzer
     * populates an ArrayDeque in which the top (first element) of the
     * Deque is a Hive object consisting of all the components of the original
     * input predicate, but with 'invalid' operators replaced with 'valid'
     * operators; for example, with 'IN <i>list</i>' replaced with 'OR'
     * statements.
     * <p>
     * In each case, translateSearchConditions constructs the appropriate
     * Hive predicate to push from the contents of the given Collection;
     * either List of IndexSearchCondition, or ArrayDeque.
     */
    @Override
    @SuppressWarnings("deprecation")
    public DecomposedPredicate decomposePredicate(
               JobConf jobConfig,
               org.apache.hadoop.hive.serde2.Deserializer deserializer,
               ExprNodeDesc predicate) {

        /* Reset query state to default values. */
        TableHiveInputFormat.resetQueryInfo();
        DecomposedPredicate decomposedPredicate = null;

        /*
         * Try the Hive built-in analyzer first; which will validate the
         * components of the given predicate and separate them into two
         * disjoint sets: a set of search conditions that correspond to
         * either a valid PrimaryKey or IndexKey (and optional FieldRange)
         * that can be scanned (in the backend KVStore server) using one
         * of the TableIterators; and a set containing the remaining components
         * of the predicate (the 'residual' predicate), which Hive will
         * apply to the results returned after the search conditions have
         * first been applied on (pushed to) the backend.
         */
        final IndexPredicateAnalyzer analyzer =
            TableHiveInputFormat.sargablePredicateAnalyzer(
                predicate, (TableSerDe) deserializer);

        if (analyzer != null) { /* Use TableScan or IndexScan */

            /* Decompose predicate into search conditions and residual. */
            final List<IndexSearchCondition> searchConditions =
                new ArrayList<IndexSearchCondition>();

            final ExprNodeGenericFuncDesc residualPredicate =
                (ExprNodeGenericFuncDesc) analyzer.analyzePredicate(
                    predicate, searchConditions);

            decomposedPredicate = new DecomposedPredicate();

            decomposedPredicate.pushedPredicate =
                analyzer.translateSearchConditions(searchConditions);
            decomposedPredicate.residualPredicate = residualPredicate;

            /*
             * Valid search conditions and residual have been obtained.
             * Determine whether the search conditions are index based or
             * based on the table's primary key. If index based, then tell
             * the InputFormat to build splits and scan (iterate) based on
             * shards; otherwise, tell the InputFormat to base the iterator
             * on partition sets.
             */
            final StringBuilder whereBuf = new StringBuilder();
            TableHiveInputFormat.buildPushPredicate(
                decomposedPredicate.pushedPredicate, whereBuf);
            final String whereStr = whereBuf.toString();

            TableHiveInputFormat.setQueryInfo(
                searchConditions, (TableSerDe) deserializer, whereStr);

            if (LOG.isDebugEnabled()) {
                LOG.debug("-----------------------------");
                LOG.debug("residual = " +
                          decomposedPredicate.residualPredicate);
                LOG.debug("predicate = " +
                          decomposedPredicate.pushedPredicate);
                LOG.debug("search conditions = " + searchConditions);

                switch (TableHiveInputFormat.getQueryBy()) {
                    case TableInputSplit.QUERY_BY_INDEX:
                        LOG.debug("push predicate to secondary index [" +
                                  "WHERE " + whereStr + "]");
                        break;

                    case TableInputSplit.QUERY_BY_PRIMARY_ALL_PARTITIONS:
                    case TableInputSplit.QUERY_BY_PRIMARY_SINGLE_PARTITION:
                        LOG.debug("push predicate to primary index [" +
                                  "WHERE " + whereStr + "]");
                        break;
                    default:
                        break;
                }
                LOG.debug("-----------------------------");
            }

        } else { /* IndexPredicateAnalyzer == null ==> Use native query */

            /*
             * The given predicate does not consist of search conditions that
             * correspond to either a valid PrimaryKey or IndexKey (or
             * FieldRange). Thus, employ the extended analyzer to handle
             * statements the built-in analyzer cannot handle.
             */
            final TableHiveInputFormat.ExtendedPredicateAnalyzer
                extendedAnalyzer =
                    TableHiveInputFormat.createPredicateAnalyzerForOnql(
                        (TableSerDe) deserializer);

            if (extendedAnalyzer == null) {
                LOG.debug("extended predicate analyzer = null ... " +
                          "NO PREDICATE PUSHDOWN");
                return null;
            }

            final ArrayDeque<ExprNodeDesc> pushPredicateDeque =
                new ArrayDeque<ExprNodeDesc>();

            final ExprNodeGenericFuncDesc residualPredicate =
                (ExprNodeGenericFuncDesc) extendedAnalyzer.analyzePredicate(
                                              predicate, pushPredicateDeque);
            if (LOG.isTraceEnabled()) {
                final ExprNodeDesc[] qElements =
                    pushPredicateDeque.toArray(
                        new ExprNodeDesc[pushPredicateDeque.size()]);
                LOG.trace("-----------------------------");
                LOG.trace("push predicate queue elements:");
                for (int i = 0; i < qElements.length; i++) {
                    LOG.trace("element[" + i + "] = " + qElements[i]);
                }
                LOG.trace("-----------------------------");
            }

            decomposedPredicate = new DecomposedPredicate();
            final StringBuilder whereBuf = new StringBuilder();

            decomposedPredicate.residualPredicate = residualPredicate;
            decomposedPredicate.pushedPredicate =
                extendedAnalyzer.translateSearchConditions(
                                     pushPredicateDeque, whereBuf);

            if (decomposedPredicate.pushedPredicate != null) {

                if (LOG.isTraceEnabled()) {
                    TableHiveInputFormat.
                        ExtendedPredicateAnalyzer.displayNodeTree(
                            decomposedPredicate.pushedPredicate);
                }

                final String whereStr = whereBuf.toString();

                if (LOG.isDebugEnabled()) {
                    LOG.debug("-----------------------------");
                    LOG.debug("residual = " +
                              decomposedPredicate.residualPredicate);
                    LOG.debug("predicate = " +
                              decomposedPredicate.pushedPredicate);
                    LOG.debug("push predicate via native query [" +
                              "WHERE " + whereStr + "]");
                    LOG.debug("-----------------------------");
                }

                TableHiveInputFormat.setQueryInfo(
                    (TableSerDe) deserializer, whereStr);

            } else {
                LOG.debug("Extended predicate analyzer found no predicate " +
                          "to push. Will use all of residual for filtering.");
            }

        } /* endif: IndexPredicteAnalyzer != null or == null */

        return decomposedPredicate;
    }
}
