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

package oracle.kv.impl.tif;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Set;
import java.util.logging.Logger;

import javax.net.ssl.SSLContext;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.IndexImpl.IndexField;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableKey;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.tif.TransactionAgenda.Commit;
import oracle.kv.impl.tif.esclient.esRequest.BulkRequest;
import oracle.kv.impl.tif.esclient.esRequest.CATRequest;
import oracle.kv.impl.tif.esclient.esRequest.ClusterHealthRequest;
import oracle.kv.impl.tif.esclient.esRequest.CreateIndexRequest;
import oracle.kv.impl.tif.esclient.esRequest.DeleteIndexRequest;
import oracle.kv.impl.tif.esclient.esRequest.DeleteRequest;
import oracle.kv.impl.tif.esclient.esRequest.GetHttpNodesRequest;
import oracle.kv.impl.tif.esclient.esRequest.GetMappingRequest;
import oracle.kv.impl.tif.esclient.esRequest.GetRequest;
import oracle.kv.impl.tif.esclient.esRequest.IndexDocumentRequest;
import oracle.kv.impl.tif.esclient.esRequest.IndexExistRequest;
import oracle.kv.impl.tif.esclient.esRequest.MappingExistRequest;
import oracle.kv.impl.tif.esclient.esRequest.PutMappingRequest;
import oracle.kv.impl.tif.esclient.esResponse.BulkResponse;
import oracle.kv.impl.tif.esclient.esResponse.CATResponse;
import oracle.kv.impl.tif.esclient.esResponse.ClusterHealthResponse;
import oracle.kv.impl.tif.esclient.esResponse.ClusterHealthResponse.ClusterHealthStatus;
import oracle.kv.impl.tif.esclient.esResponse.CreateIndexResponse;
import oracle.kv.impl.tif.esclient.esResponse.DeleteIndexResponse;
import oracle.kv.impl.tif.esclient.esResponse.DeleteResponse;
import oracle.kv.impl.tif.esclient.esResponse.GetHttpNodesResponse;
import oracle.kv.impl.tif.esclient.esResponse.GetMappingResponse;
import oracle.kv.impl.tif.esclient.esResponse.GetResponse;
import oracle.kv.impl.tif.esclient.esResponse.IndexAlreadyExistsException;
import oracle.kv.impl.tif.esclient.esResponse.IndexDocumentResponse;
import oracle.kv.impl.tif.esclient.esResponse.IndexExistResponse;
import oracle.kv.impl.tif.esclient.esResponse.MappingExistResponse;
import oracle.kv.impl.tif.esclient.esResponse.PutMappingResponse;
import oracle.kv.impl.tif.esclient.httpClient.ESHttpClient;
import oracle.kv.impl.tif.esclient.httpClient.ESHttpClientBuilder;
import oracle.kv.impl.tif.esclient.httpClient.ESHttpClientBuilder.SecurityConfigCallback;
import oracle.kv.impl.tif.esclient.httpClient.SSLContextException;
import oracle.kv.impl.tif.esclient.jsonContent.ESJsonBuilder;
import oracle.kv.impl.tif.esclient.jsonContent.ESJsonUtil;
import oracle.kv.impl.tif.esclient.restClient.ESAdminClient;
import oracle.kv.impl.tif.esclient.restClient.ESDMLClient;
import oracle.kv.impl.tif.esclient.restClient.ESRestClient;
import oracle.kv.impl.tif.esclient.restClient.RestResponse;
import oracle.kv.impl.tif.esclient.restClient.RestStatus;
import oracle.kv.impl.tif.esclient.restClient.monitoring.ESNodeMonitor;
import oracle.kv.impl.tif.esclient.restClient.monitoring.MonitorClient;
import oracle.kv.impl.tif.esclient.restClient.utils.ESLatestResponse;
import oracle.kv.impl.tif.esclient.restClient.utils.ESRestClientUtil;
import oracle.kv.impl.tif.esclient.security.TIFSSLContext;
import oracle.kv.impl.util.HostPort;
import oracle.kv.impl.util.JsonUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.table.ArrayDef;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.MapDef;
import oracle.kv.table.MapValue;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Table;
import oracle.kv.table.TimestampValue;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import org.apache.http.HttpHost;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;

/**
 * Object representing an Elastic Search (ES) handler with all ES related
 * operations including ES index and mapping management and data operations.
 */
public class ElasticsearchHandler {

    /*
     * Property names of interest to index creation.
     */
    final static String SHARDS_PROPERTY = "ES_SHARDS";
    final static String REPLICAS_PROPERTY = "ES_REPLICAS";

    /* The name of fields used in mapping type for TIMESTAMP */
    private static final String DATE = "date";
    private static final String NANOS = "nanos";

    /* The JSONs of mapping type for TIMESTAMP */
    private static final String SIMPLE_TIMSTAMP_TYPE_JSON =
        "{\"type\":\"date\"}";
    private static final String TIMESTAMP_TYPE_JSON =
        "{\"" + DATE + "\": " + SIMPLE_TIMSTAMP_TYPE_JSON +
         ", " + "\"" + NANOS + "\":{\"type\":\"integer\"}}";

    private final Logger logger;
    private final ESRestClient esRestClient;
    private final ESAdminClient adminClient;
    private final ESDMLClient client;
    private final MonitorClient esMonitoringClient;

    /* Timeout values for the http client this ElasticsearchHandler uses 
     * This timeout is used by ESSyncResponseListener and is based on
     * the expected bulk request processing time.
     */
    private static final int maxRetryTimeoutMillis = 120000;

    /*
     * These variables are set at Construction time. AdminClient instantiation
     * does a call to ES to get the values for this.
     * 
     * Note that one JVM process can connect to only one ES Cluster.
     */
    public static String ES_VERSION;
    public static String ES_CLUSTER_NAME;

    /*
     * This variable is set at the time of Register-ES plan.
     * ElasticsearchHandler is per store.
     * 
     * This is the store name which is prefixed to all FTS index.
     * 
     * One effect of STORE_NAME being null is that ensureCommit() will refresh
     * all indices on the ES Cluster.
     */
    public static String STORE_NAME = null;

    private boolean doEnsureCommit;

    ElasticsearchHandler(ESRestClient esRestClient,
            MonitorClient esMonitoringClient,
            Logger logger) {
        this.esRestClient = esRestClient;
        this.client = esRestClient.dml();
        this.adminClient = esRestClient.admin();
        this.esMonitoringClient = esMonitoringClient;
        this.logger = logger;
        ES_VERSION = this.adminClient.getESVersion();
        ES_CLUSTER_NAME = this.adminClient.getClusterName();

    }

    /**
     * Closes the ES handler
     */
    public void close() {
        if (esMonitoringClient != null) {
            esMonitoringClient.close();
        }
        if (esRestClient != null) {
            esRestClient.close();
        }
    }

    /*
     * Typically FTS would use only one instance of ElasticsearchHandler.
     * However, this single instance restriction, if required should be at the
     * TextIndexFeeder Layer and not here.
     */
    public static ElasticsearchHandler newInstance(
                                                   final String clusterName,
                                                   final String esMembers,
                                                   final boolean isSecure,
                                                   SecurityParams esSecurityParams,
                                                   final int monitoringFixedDelay,
                                                   Logger logger)
        throws IOException {

        ESRestClient esRestClient = null;
        MonitorClient esMonitoringClient = null;
        ESNodeMonitor esNodeMonitor = null;

        ESHttpClient baseRestClient = null;
        ESHttpClient baseMonitoringClient = null;

        if (!isSecure) {
            esSecurityParams = null;
        }
        /*
         * Create ES Http Clients for restClient and monitoring Client. No need
         * to check connections as that is done during register-es-plan before
         * setting the SN parameters.
         * 
         * For now, using same logger for low level httpclient and higher level
         * clients.
         */
        baseRestClient =
            ElasticsearchHandler.createESHttpClient(clusterName, esMembers,
                                                    esSecurityParams, logger);

        baseMonitoringClient =
            ElasticsearchHandler.createESHttpClient(clusterName, esMembers,
                                                    esSecurityParams, logger);

        esRestClient = new ESRestClient(baseRestClient, logger);
        esMonitoringClient = new MonitorClient(baseMonitoringClient,
                                               new ESLatestResponse(), logger);

        List<ESHttpClient> registeredESHttpClients =
            new ArrayList<ESHttpClient>();
        registeredESHttpClients.add(esMonitoringClient.getESHttpClient());
        registeredESHttpClients.add(esRestClient.getESHttpClient());
        esNodeMonitor =
            new ESNodeMonitor(esMonitoringClient, monitoringFixedDelay,
                              registeredESHttpClients, isSecure, logger);
        esRestClient.getESHttpClient().setFailureListener(esNodeMonitor);

        esNodeMonitor.start();

        return new ElasticsearchHandler(esRestClient, esMonitoringClient,
                                        logger);

    }

    public ESRestClient getEsRestClient() {
        return esRestClient;
    }

    public ESAdminClient getAdminClient() {
        return adminClient;
    }

    public ESDMLClient getClient() {
        return client;
    }

    public MonitorClient getEsMonitoringClient() {
        return esMonitoringClient;
    }

    /**
     * Enables ensure commit
     */
    void enableEnsureCommit() {
        doEnsureCommit = true;
    }

    /**
     * Checks if an ES index exists
     *
     * @param indexName  name of ES index
     *
     * @return true if an ES index exists
     * @throws IOException
     */
    boolean existESIndex(String indexName) throws IOException {
        return (existESIndex(indexName, adminClient));
    }

    /**
     * Checks if an ES index mapping exists
     *
     * @param esIndexName  name of ES index
     * @param esIndexType  type of mapping in ES index
     *
     * @return true if a mapping exits
     * @throws IOException
     */
    boolean existESIndexMapping(String esIndexName,
                                String esIndexType)
        throws IOException {

        /* if no index, no mapping */
        if (!existESIndex(esIndexName)) {
            return false;
        }

        MappingExistRequest request =
            new MappingExistRequest(esIndexName, esIndexType,
                                    adminClient.getESVersion());
        MappingExistResponse response = adminClient.mappingExists(request);

        /*
         * Create Index API may create an empty mapping. 
         */
        if (response.exists()) {
            GetMappingRequest getMappingReq =
                new GetMappingRequest(esIndexName, esIndexType);
            
            GetMappingResponse getMappingResp =
                adminClient.getMapping(getMappingReq);
            if (getMappingResp.mapping() != null) {
                return true;
            }
            return false;

        }

        /* check if mapping exists in index */
        return response.exists();
    }

    /**
     *
     * Gets a json string representation of a mapping.
     *
     * @param esIndexName  name of ES index
     * @param esIndexType  type of mapping in ES index
     *
     * @return json string
     * @throws IOException
     */
    String getESIndexMapping(String esIndexName, String esIndexType)
        throws IOException {

        /* if no index, no mapping */
        if (!existESIndex(esIndexName)) {
            return null;
        }
        GetMappingRequest request = new GetMappingRequest(esIndexName,
                                                          esIndexType);

        GetMappingResponse response = adminClient.getMapping(request);

        return response.mapping();

    }

    /**
     * Creates an ES index with default property, the default number of shards
     * and replicas would be applied by ES.
     *
     * @param esIndexName  name of ES index
     * @throws Exception
     * @throws IllegalStateException
     */
    void createESIndex(String esIndexName) throws IOException {
        createESIndex(esIndexName, (Map<String, String>) null);
    }

    /**
     * Creates an ES index
     *
     * @param esIndexName  name of ES index
     * @param properties   Map of index properties, can be null
     * @throws Exception
     */
    void createESIndex(String esIndexName, Map<String, String> properties)
        throws IOException {

        Map<String, String> indexSettings = new LinkedHashMap<String, String>();

        if (properties != null) {
            final String shards = properties.get(SHARDS_PROPERTY);
            final String replicas = properties.get(REPLICAS_PROPERTY);

            if (shards != null) {
                if (Integer.parseInt(shards) < 1) {
                    throw new IllegalStateException
                        ("The " + SHARDS_PROPERTY + " value of " + shards +
                         " is not allowed.");
                }
                indexSettings.put("number_of_shards", shards);
            }
            if (replicas != null) {
                if (Integer.parseInt(replicas) < 0) {
                    throw new IllegalStateException
                        ("The " + REPLICAS_PROPERTY + " value of " + replicas +
                         " is not allowed.");
                }
                indexSettings.put("number_of_replicas", replicas);
            }
        }

        CreateIndexResponse createResponse = null;
        try {
            CreateIndexRequest createIndex =
                new CreateIndexRequest(esIndexName).settings(indexSettings);
            createResponse = adminClient.createIndex(createIndex);

        } catch (IndexAlreadyExistsException iae) {

            /*
             * That is OK; multiple repnodes will all try to create the index
             * at the same time, only one of them can win.
             * 
             * Or this could be a restart of TIF case, so index could already be
             * existing.
             */

            logger.fine("ES index " + esIndexName + " has already been" +
                        "created");
            return;

        } catch (IOException e) {

            logger.log(java.util.logging.Level.SEVERE,
                       " index could not be created due to:" + e);

            throw e;

        }

        if (!createResponse.isAcknowledged()) {
            throw new IllegalStateException("Fail to create ES index "
                    + esIndexName);
        }

        logger.info("ES index " + esIndexName + " created");
    }

    /**
     * Deletes an ES index
     *
     * @param esIndexName  name of ES index
     *
     * @throws IllegalStateException
     */
    void deleteESIndex(String esIndexName) throws IllegalStateException {

        if (!deleteESIndex(esIndexName, adminClient, logger)) {
            logger.info("nothing to delete, ES index " + esIndexName +
                        " does not exist.");
        }

        logger.info("ES index " + esIndexName + " deleted");
    }

    /**
     * Returns all ES indices corresponding to text indices in the kvstore
     *
     * @param storeName  name of kv store
     *
     * @return list of all ES index names
     * @throws IOException
     */
    Set<String> getAllESIndexNamesInStore(final String storeName)
        throws IOException {

        return getAllESIndexInStoreInternal(storeName, adminClient);
    }

    /**
     * Creates an ES index mapping
     *
     * @param esIndexName  name of ES index
     * @param esIndexType  ES index type
     * @param mappingSpec  mapping specification
     *
     * @throws IllegalStateException
     */
    void createESIndexMapping(String esIndexName,
                              String esIndexType,
                              JsonGenerator mappingSpec)
        throws IllegalStateException {

        PutMappingResponse mresp = null;

        try {
            /* ensure the ES index exists */
            if (!existESIndex(esIndexName)) {
                throw new IllegalStateException("ES Index " + esIndexName
                        + " " + "does not exist");
            }

            /* ensure no pre-existing conflicting mapping */
            if (existESIndexMapping(esIndexName, esIndexType)) {
                /*
                 * It is not entirely create how create index API behaves.
                 * Create Index API can add empty mapping
                 */
                String existingMapping = getESIndexMapping(esIndexName,
                                                           esIndexType);

                if (!JsonUtils.getMapFromJsonStr(existingMapping).isEmpty()) {

                    if (ESRestClientUtil.isMappingResponseEqual
                                                        (existingMapping,
                                                         mappingSpec,
                                                         esIndexName,
                                                         esIndexType)) {
                        return;
                    }

                    throw new IllegalStateException
                    ("Mapping " + esIndexType + " already exists in index " +
                     esIndexName + ", but differs from new mapping." +
                     "\nexisting mapping: " + existingMapping +
                     "\nnew mapping: " + mappingSpec);
                }
            }

            PutMappingRequest mappingReq = new PutMappingRequest(esIndexName,
                                                                 esIndexType,
                                                                 mappingSpec);
            mresp = adminClient.createMapping(mappingReq);
        } catch (IOException ioe) {
            logger.severe("Exception occured while trying to create mapping:"
                    + ioe);
        }
        if (mresp == null || !mresp.isAcknowledged()) {
            String msg = "Cannot install ES mapping for ES index "
                    + esIndexName + ", type " + esIndexType;
            logger.info(msg);
            throw new IllegalStateException(msg);
        }


        logger.info("Mapping created for ES index: " + esIndexName +
                    ", index type: " + esIndexType +
                    ", mapping spec: " + mappingSpec);
    }

    /**
     * Fetches an entry from ES index
     *
     * @param esIndexName  name of ES index
     * @param esIndexType  type of index mapping
     * @param key          key of entry to get
     *
     * @return response from ES index
     * @throws IOException
     */
    GetResponse get(String esIndexName, String esIndexType, String key)
        throws IOException {

        GetRequest req = new GetRequest(esIndexName, esIndexType, key);
        return client.get(req);

    }

    /**
     * Sends a document to ES for indexing
     *
     * @param document  document to index
     * @throws IOException
     */
    IndexDocumentResponse index(IndexOperation document) throws IOException {

        IndexDocumentRequest req =
            new IndexDocumentRequest(document.getESIndexName(),
                                     document.getESIndexType(),
                                     document.getPkPath()).source(document.getDocument());

        IndexDocumentResponse response = client.index(req);

        ensureCommit();

        return response;
    }

    /**
     * Deletes an entry from ES index
     *
     * @param esIndexName  name of ES index
     * @param esIndexType  type of index mapping
     * @param key          key of entry to delete
     * @throws IOException
     */
    DeleteResponse del(String esIndexName, String esIndexType, String key)
        throws IllegalStateException,
        IOException {

        DeleteRequest delReq = new DeleteRequest(esIndexName, esIndexType, key);
        DeleteResponse response = null;
        try {
            response = client.delete(delReq);
        } catch (IOException e) {
            throw new IllegalStateException("Could not delete document:" +
                    esIndexName + ":" + esIndexType + ":" + key + " due to:" +
                    e.getCause());
        }

        ensureCommit();

        return response;
    }

    /**
     * Send a bulk operation to Elasticsearch
     *
     * @param batch  a batch of operations
     * @return  response from ES cluster, or null if the batch is empty.
     * @throws IOException
     */
    BulkResponse doBulkOperations(List<TransactionAgenda.Commit> batch)
        throws IOException {

        if (batch.size() == 0) {
            return null;
        }

        BulkRequest bulkRequest = new BulkRequest();

        /* If operations were purged by an index deletion, the batch will
         * contain empty transactions.  If every transaction in the batch is
         * empty, then the bulkRequest will be empty.  We don't want to send an
         * empty bulkRequest to ES -- it will throw
         * ActionRequestValidationException.  So we keep track of the actual
         * number of operations here, and skip this request, declaring it
         * successful so that the empty transactions will be cleaned up.
         */
        int numberOfOperations = 0;
        for (Commit commit : batch) {

            /* apply each operation to ES index */
            for (IndexOperation op : commit.getOps()) {
                IndexOperation.OperationType type = op.getOperation();
                if (type == IndexOperation.OperationType.PUT) {
                    bulkRequest.add(new IndexDocumentRequest(op.getESIndexName(),
                                                             op.getESIndexType(),
                                                             op.getPkPath())
                                                               .source
                                                               (op.getDocument()));
                } else if (type == IndexOperation.OperationType.DEL) {
                    bulkRequest.add(new DeleteRequest(op.getESIndexName(),
                                                      op.getESIndexType(),
                                                      op.getPkPath()));
                } else {
                    throw new IllegalStateException("illegal op to ES index "
                            + op.getOperation());
                }
                numberOfOperations++;
            }
        }

        if (numberOfOperations == 0) {
            return null;
        }

        /* Default timeout is one minute, which seems proper. */
        return client.bulk(bulkRequest);
    }

    /* sync ES to ensure commit */
    private void ensureCommit() throws IOException {
        if (doEnsureCommit) {
            /*
             * First get all indices in the ES for this store. Refresh them all.
             */
            Set<String> indices = getAllESIndexNamesInStore(STORE_NAME);

            client.refresh(indices.toArray(new String[0]));
        }
    }

    /*------------------------------------------------------*/
    /* static functions, start with public static functions */
    /*------------------------------------------------------*/

    /**
     * Verify that the given Elasticsearch node exists by connecting to it.
     * This is a transient connection used only during configuration. This
     * method is called in the context of the Admin during plan construction.
     *
     * If the node doesn't exist/the connection fails, throw
     * IllegalCommandException, because the user provided an incorrect address.
     *
     * If the node exists/the connection succeeds, ask the node for a list of
     * its peers, and return that list as a String of hostname:port[,...].
     *
     * If storeName is not null, then we expect that an ES Index corresponding
     * to the store name should NOT exist. If such does exist, then
     * IllegalCommandException is thrown, unless the forceClear flag is set. If
     * forceClear is true, then the offending ES index will be summarily
     * removed.
     * 
     * This method will check the cluster state of ES and verify the cluster
     * name matches the provided cluster name.
     *
     * @param clusterName  name of ES cluster
     * @param transportHp  host and port of ES node to connect
     * @param storeName    name of the NoSQL store, or null as described above
     * @param secure       user configuration requirement. 
     *                     This value comes from the Plan command.
     *                     Based on this value SSLContext
     *                     can be null or not null.
     * @param secParams    SecurityParams configured on SN.
     *                     This will be used to create the
     *                     SSLContext used in SSLEngine.
     * @param forceClear   if true, allows deletion of the existing ES index
     * @param logger       caller's logger, if any. Null is allowed in tests.
     *
     * @return list of discovered ES node and port
     */
    public static String getAllTransports(String clusterName,
                                          HostPort transportHp,
                                          String storeName,
                                          boolean secure,
                                          SecurityParams secParams,
                                          boolean forceClear,
                                          Logger logger) {

        final String errorMsg =
            "Can't connect to an Elasticsearch cluster at ";

        /*
         * Create a Monitoring Client. Get All nodes in the cluster. Make sure
         * that clusterName given in the argument matches. Create a string out
         * of HttpHosts in the format: hostname:port[,...] Close the monitoring
         * client, as this is a one time connection for configuration.
         */

        StringBuilder sb = new StringBuilder();
        MonitorClient monitorClient = null;
        ESRestClient restClient = null;

        List<HttpHost> availableNodes = null;

        GetHttpNodesResponse resp = null;
        if (!secure) {
            secParams = null;
        }
        try {

            ESHttpClient baseHttpClient =
                createESHttpClient(clusterName, transportHp, secParams,
                                   logger);

            monitorClient = new MonitorClient(baseHttpClient,
                                              new ESLatestResponse(), logger);
            resp =
                monitorClient.getHttpNodesResponse(new GetHttpNodesRequest());

            if (resp != null &&
                    !resp.getClusterName().equals(clusterName.trim())) {

                throw new IllegalCommandException(errorMsg + transportHp +
                        " Given Cluster Name does not match the" +
                        " cluster name on ES side.");
            }

            if (resp != null) {
                if (!ESRestClientUtil.isEmpty(resp.getClusterName()) &&
                        resp.getClusterName().equals(clusterName)) {
                    availableNodes = resp.getHttpHosts(
                                                       secure ? ESHttpClient.Scheme.HTTPS
                                                               : ESHttpClient.Scheme.HTTP,
                                                       logger);
                }
            }

            if (availableNodes == null || availableNodes.isEmpty()) {
                throw new IllegalCommandException(errorMsg + transportHp +
                        " {" + clusterName + "}");
            }

            for (HttpHost node : availableNodes) {
                if (sb.length() != 0) {
                    sb.append(ParameterUtils.HELPER_HOST_SEPARATOR);
                }
                sb.append(node.getHostName()).append(":")
                  .append(node.getPort());
            }

            /*
             * since each es index corresponds to a text index, we do not
             * know exactly the es index name, but we know all es indices
             * starts with a prefix derived from store name, which can
             * be used to check if there are pre-existing es indices for
             * the particular store
             */
            if (storeName != null) {
                /* fetch list of all indices in ES under the store */

                restClient = new ESRestClient(baseHttpClient, logger);

                Set<String> allIndices =
                    getAllESIndexInStoreInternal(storeName,
                                                 restClient.admin());

                /* delete each existing ES index if force clear */
                String offendingIndexes = "";
                for (String indexName : allIndices) {
                    if (forceClear) {
                        deleteESIndex(indexName, restClient.admin(), logger);
                    } else {
                        offendingIndexes += "  " + indexName + "\n";
                    }
                }
                if (! "".equals(offendingIndexes)) {
                    throw new IllegalCommandException
                        ("The Elasticsearch cluster \"" + clusterName +
                         "\" already contains indexes\n" +
                         "corresponding to the NoSQL Database " +
                         "store \"" + storeName + "\".\n" +
                         "Here is a list of them:\n" +
                         offendingIndexes +
                         "This situation might occur if you " +
                         "register an ES cluster simultaneously with\n" +
                         "two NoSQL Database stores that have the same " +
                         "store name, which is not allowed;\n" +
                         "or if you have removed a NoSQL store " +
                         "to which the ES cluster was registered\n" +
                         "(which makes the ES indexes orphans), " +
                         "and then created the store again with \n" +
                         "the same name. If the offending indexes " +
                         "are no longer needed, you can remove\n" +
                         "them by re-issuing the plan register-es " +
                         "command with the -force option.");
                }
            }
        } catch (IOException e) {
            if (e.getCause() instanceof SSLContextException) {
                throw new IllegalCommandException("Could not set up Security" +
                        "Context based on the current security configurations" +
                        "for ESNode:" + transportHp);
            }
            // TODO: more granularity in exceptions required..especially
            // connection refused.
            throw new IllegalCommandException(errorMsg + transportHp, e);

        } catch (Exception e) {
            throw new IllegalCommandException(errorMsg + transportHp, e);
        } finally {
            /*
             * Both restClient and monitorClient will end up closing same
             * httpClient.
             */
            if (monitorClient != null) {
                monitorClient.close();
            }
            if (restClient != null) {
                restClient.close();
            }
        }

        return sb.toString();

    }


    /**
     * Return an indication of whether the ES cluster is considered "healthy".
     * 
     * @param esMembers
     */
    public static boolean isClusterHealthy(String esMembers,
                                           ESAdminClient esAdminClient) {

        ClusterHealthRequest req = new ClusterHealthRequest();
        ClusterHealthResponse resp = null;
        try {
            resp = esAdminClient.clusterHealth(req);
        } catch (IOException e) {
            /*
             * Could not execute an API call after few retries means cluster is
             * not healthy.
             */
            return false;
        }

        ClusterHealthStatus status = resp.getClusterHealthStatus();

        /*
         * We want to insist on a GREEN status for the cluster when operations
         * such as creating or deleting an index are performed.  This is
         * because there are weird cases where a deletion would be undone
         * later, if some nodes are down when the deletion took place.
         * cf. https://github.com/elastic/elasticsearch/issues/13298
         *
         * The problem with requiring GREEN status is that ES's out-of-the-box
         * defaults result in a single-node cluster's status always being
         * YELLOW.  Requiring GREEN for index deletion would cause problems for
         * tire-kickers, and we don't want that.
         *
         * The compromise is to allow YELLOW status for single-node clusters,
         * but insist on GREEN for every other situation.
         */

        if (ClusterHealthStatus.GREEN.equals(status)) {
            return true;
        }

        /*
         * Turns out it's not so easy to distinguish between a "single-node
         * cluster" and a multi-node cluster that has only one node available.
         * I am not finding a reliable way to do it.
         *
         * It seems that ES lacks a notion of a persistent topology.  If a node
         * is not present, it's as if it never existed!
         *
         * Things I tried:
         *  - NodesInfoRequest will not reliably return information
         *    about nodes that aren't running.
         *
         *  - Looking at the number of unassigned shards - this only tells
         *    you that there aren't enough nodes to satisfy the number of
         *    replicas specified.
         *
         *  - for an unassigned shard there's a "reason" it is unassigned,
         *    which can be any of the values of the enum
         *    org.elasticsearch.cluster.routing.Reason.  This looked promising,
         *    as newly created indexes that can't satisfy their replica
         *    requirements give the reason value of INDEX_CREATED; but
         *    after a restart of the single ES node, this value changed to
         *    CLUSTER_RECOVERED, which is the same as for shards that were
         *    previously assigned to a missing node.  Dang it.
         *    The reason value NODE_LEFT seems promising, but it's also
         *    transient.
         *
         * So, for now we will user kvstore's knowledge of the number of nodes
         * in the cluster at register-es time.  This isn't 100% reliable because
         * the number of nodes might have changed since register-es, but that
         * should be uncommon.  We do advise users to issue register-es after
         * changing the ES cluster's topology, to update kvstore's list of
         * nodes.
         *
         */
        final int nRegisteredNodes = esMembers.split(",").length;
        if (ClusterHealthStatus.YELLOW.equals(status) &&
            resp.getNumberOfDataNodes() == 1 &&
            nRegisteredNodes == 1) {
            return true;
        }

        return false;
    }

    /**
     * Static version of existESIndex for use by getAllTransports, when no
     * ElasticsearchHandler object exists.  Called during es registration.
     *
     * @param indexName     The name of the ES index to check
     * @param esAdminClient The ES Admin client handle
     * @return              True if the index exists
     * @throws IOException
     */
    public static boolean existESIndex(String indexName,
                                       ESAdminClient esAdminClient)
        throws IOException {
        IndexExistRequest existsRequest =
            new IndexExistRequest(indexName);
        IndexExistResponse existResponse = esAdminClient.indexExists(existsRequest);

        return existResponse.exists();
    }

    /**
     * Static version of addingESIndex
     *
     * @param esIndexName   name of ES index
     * @param esAdminClient ES Admin client handle
     *
     * @throws IllegalStateException, IndexAlreadyExistsException
     */
    public static void createESIndex(String esIndexName,
                                     ESAdminClient esAdminClient)
        throws IndexAlreadyExistsException,
        IllegalStateException {

        CreateIndexResponse createResponse = null;
        try {
            createResponse =
                esAdminClient.createIndex(new CreateIndexRequest(esIndexName));
        } catch (IOException e) {

            throw new IllegalStateException("Fail to create ES index " +
                    esIndexName + " due to:" + e);
        }

        if (createResponse == null || !createResponse.isAcknowledged()) {
            throw new IllegalStateException("Fail to create ES index " +
                    esIndexName);
        }
    }

    /**
     * Static version of deleteEsIndex
     *
     * @param indexName      name of the ES index to remove
     * @param esAdminClient  ES Admin client handle
     * @return               True if the index existed and was deleted
     */
    public static boolean deleteESIndex(String indexName,
                                        ESAdminClient esAdminClient,
                                        Logger logger) {
        DeleteIndexResponse deleteIndexResponse = null;
        try {
            if (!existESIndex(indexName, esAdminClient)) {
                return false;
            }

            DeleteIndexRequest deleteIndexRequest =
                new DeleteIndexRequest(indexName);

            deleteIndexResponse =
                esAdminClient.deleteIndex(deleteIndexRequest);
            if (!deleteIndexResponse.exists()) {
                logger.warning("Index:" + indexName +
                        " could not get deleted because it did not exist.");
            }

        } catch (IOException e) {
            logger.severe("Delete Request Failed due to:" + e);
            throw new IllegalStateException("Fail to delete ES index " +
                    indexName + " due to:" + e);
        }
        if (!deleteIndexResponse.isAcknowledged()) {
            throw new IllegalStateException("Fail to delete ES index " +
                                            indexName);
        }
        return true;
    }

    /**
     * Returns all ES indices corresponding to text indices in the kvstore
     *
     * @param storeName  name of kv store
     * @param adminClient es client
     * @return  list of all ES index names
     * @throws IOException
     */
    static Set<String> getAllESIndexInStoreInternal
                                   (final String storeName,
                                    final ESAdminClient adminClient)
        throws IOException {
        final Set<String> ret;
        String prefix = null;
        if (storeName != null) {
            prefix = TextIndexFeeder.deriveESIndexPrefix(storeName);
        }

        /* fetch list of all indices in ES with prefix */
        CATResponse resp = adminClient.catInfo(CATRequest.API.INDICES, null,
                                               prefix, null, null);

        ret = resp.getIndices();
        return ret;
    }

    static String constructMapping(IndexImpl index) {

        try {
            JsonGenerator jsonGen = generateMapping(index);
            jsonGen.flush();
            return new String(
                              ((ByteArrayOutputStream) jsonGen
                                      .getOutputTarget()).toByteArray(),
                              "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(
                                            "Unable to serialize ES mapping" +
                                            " for text index due to UTF-8" +
                                            " enconding not supported " +
                                            index.getName(), e);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Unable to serialize ES mapping for text index due to" +
                    " json generation issues " +
                    index.getName(), e);
       }
    }

    /*
     * Creates the JSON to describe an ES type mapping for this index.
     */
    static JsonGenerator generateMapping(IndexImpl index) {

        final Table table = index.getTable();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try {
            ESJsonBuilder jsonBuilder = new ESJsonBuilder(baos);
            JsonGenerator jsonGen = jsonBuilder.jsonGenarator();
            jsonGen.writeStartObject(); // Mapping Start
            jsonGen.writeBooleanField("dynamic", false);
            jsonBuilder.startStructure("properties").startStructure("_pkey");
            jsonGen.writeBooleanField("enabled", false);
            jsonBuilder.startStructure("properties").startStructure("_table");
            /*
             * For unit tests, this static method is used directly without
             * any esHandler instantiation.
             * ES_VERSION will be null in that case, as there is no
             * ES Cluster started in these tests. Default ES_VERSION.
             */
            if (ES_VERSION == null) {
                ES_VERSION = "2.4.4";
            }
            if (ES_VERSION.compareTo("5") > 0) {
                jsonGen.writeStringField("type", "keyword");
            } else {
                jsonGen.writeStringField("type", "string");
            }
            jsonGen.writeEndObject(); // end _table structure

            for (String keyField : table.getPrimaryKey()) {
                String type = defaultESTypeFor(table.getField(keyField));
                if ("string".equals(type)) {
                    if (ES_VERSION.compareTo("5") > 0) {
                        jsonBuilder.field(keyField,
                                          getMappingTypeInfo("keyword"));
                    } else {
                        jsonBuilder.field(keyField,
                                          getMappingTypeInfo("string"));
                    }
                } else {
                    jsonBuilder.field(keyField, getMappingTypeInfo(type));
                }
            }
            jsonGen.writeEndObject(); // end pkey properties
            jsonGen.writeEndObject(); // end pkey

            /*
             * We want to preserve the letter case of field names in the ES
             * document type, but the name of the path in IndexField is
             * lower-cased. The field names in IndexImpl have their original
             * case intact. So we iterate over the list of String field names
             * and the list of IndexFields in parallel, so that we have the
             * unmolested name in hand when it's needed.
             */
            List<IndexField> indexFields = index.getIndexFields();
            int indexFieldCounter = 0;
            for (String field : index.getFields()) {
                IndexField indexField = indexFields.get(indexFieldCounter++);

                /*
                 * We have to parse the mappingSpec string so that it is copied
                 * correctly into the builder. The mappingSpec cannot be treated
                 * as a string, or it will be quoted in its entirety in the
                 * resulting JSON string.
                 */
                String annotation = index.getAnnotationForField(field);
                annotation = (annotation == null ? "{}" : annotation);
                try (JsonParser parser = ESJsonUtil.createParser(annotation)) {

                    Map<String, Object> m = ESJsonUtil.parseAsMap(parser);

                    String mappingFieldName = getMappingFieldName(field);
                    if (null == m.get("type")) {
                        String type = getMappingFieldType(indexField);
                        if ("string".equals(type)) {
                            if (ES_VERSION.compareTo("5") > 0) {
                                m.putAll(getMappingTypeInfo("text"));
                            } else {
                                m.putAll(getMappingTypeInfo("string"));
                            }

                        }
                        m.putAll(getMappingTypeInfo(type));
                    }

                    jsonBuilder.field(mappingFieldName, m);
                }
            }

            jsonGen.writeEndObject(); // top leve properties end
            jsonGen.writeEndObject(); // mapping structure end

            return jsonGen;

        } catch (IOException e) {
            throw new IllegalStateException
                ("Unable to serialize ES mapping"+"" + " for text index " +
                 index.getName(), e);
        }
    }

    /**
     * Returns a map that contains the type information
     *
     * For non-TIMESTAMP type, its type information is as below: {
     * "type":<mapping-type> }
     *
     * For TIMESTAMP type, it is basically mapped to ES "date" field with an
     * additional parameter "format", but since ES "date" field supports up to
     * millisecond precision and TIMESTAMP in NoSQL supports up to nanosecond
     * precision, so based on its precision, it will be mapped to 2 kinds of
     * types:
     *
     * For TIMESTAMP with precision <= 3, it is mapped to a single "date" field:
     * { "type":"date", "format":"yyyy-MM-dd'T'HH:mm:ss[.SSS]G" }
     *
     * For TIMESTAMP with precision > 3, it is mapped it to a object of "date"
     * and "integer", the "date" field represents a Timestamp without fractional
     * second, the "integer" field represents the nanosecond: { "properties":{
     * "date": { "type":"date", "format":"yyyy-MM-dd'T'HH:mm:ssG" }, "nanos":{
     * "type":"integer" } } }
     * 
     * @throws IOException
     */
    private static Map<String, Object> getMappingTypeInfo(String type)
        throws IOException {

        if (type.startsWith(DATE)) {
            int precision = Integer.parseInt(type.substring(DATE.length()));
            return getTimestampTypeProps(precision);
        }
        Map<String, Object> map = new HashMap<String, Object>(1);
        map.put("type", type);
        return map;
    }

    /**
     * Returns a map that contains mapping type information for TIMESTAMP type.
     */
    private static Map<String, Object> getTimestampTypeProps(int precision)
        throws IOException {

        if (simpleDate(precision)) {
            try (JsonParser parser = ESJsonUtil.createParser(SIMPLE_TIMSTAMP_TYPE_JSON)) {
                return ESJsonUtil.parseAsMap(parser);
            }
        }
        Map<String, Object> map = new HashMap<String, Object>();
        try (JsonParser parser = ESJsonUtil.createParser(TIMESTAMP_TYPE_JSON)) {
            Map<String, Object> props = ESJsonUtil.parseAsMap(parser);
            map.put("properties", props);
        }
        return map;
    }

    /**
     * Checks if using simple "date" according to the given precision or
     * composite one.
     */
    private static boolean simpleDate(int precision) {
        return precision <= 3;
    }

    /*
     * Mangle a table field's name so that it works as an ES mapping field
     * name.  In particular, the '.' character is not allowed in mappings,
     * so we substitute '/' for '.'.  Otherwise, the name is used as is,
     * including the perverse coding [] that marks a map value.
     */
    private static String getMappingFieldName(String field) {
        return field.replace('.', '/');
    }

    /*
     * Return the default type for the field represented by the iField.
     */
    private static String getMappingFieldType(IndexField ifield) {

        /* The possibilities are as follows:
         *
         * 1. ifield represents a scalar type.
         *
         * 2. ifield represents an Array
         *    2a. The array contains a scalar type.
         *    2b. The array contains a record and ifield refers to a
         *        scalar type field in the record.
         *
         * 3. ifield represents a Record and refers to a scalar field
         *    in the Record.
         *
         * 4. ifield represents a Map
         *    4a. ifield refers to the Map's string key.
         *    4b. ifield refers to the Map's value.
         *    4c. ifield refers to a specific key name.
         */

        FieldDef fdef = ifield.getFirstDef();
        int stepIdx = 0;
        String fieldName = ifield.getStep(stepIdx++);

        switch (fdef.getType()) {
        case STRING:
        case INTEGER:
        case LONG:
        case BOOLEAN:
        case FLOAT:
        case DOUBLE:
        case TIMESTAMP:
            /* case 1 */
            return defaultESTypeFor(fdef);

        case ARRAY:
            final ArrayDef adef = fdef.asArray();
            fdef = adef.getElement();
            if (!fdef.isComplex()) {
                /* case 2a. */
                return defaultESTypeFor(fdef);
            }
            if (!fdef.isRecord()) {
                throw new IllegalStateException
                    ("Array type " + fdef + " not allowed as an index field.");
            }
            /* case 2b. */
            stepIdx++; /* Skip over the ifield placeholder "[]" */
            //$FALL-THROUGH$
         case RECORD:
            /* case 3. */
            fieldName = ifield.getStep(stepIdx++);
            fdef = fdef.asRecord().getFieldDef(fieldName);
            return defaultESTypeFor(fdef);
         case MAP:
            final MapDef mdef = fdef.asMap();
            fieldName = ifield.getStep(stepIdx++);
            if (TableImpl.KEYS.equalsIgnoreCase(fieldName)) {
                /* case 4a. Keys are always strings. */
                return defaultESTypeFor(FieldDefImpl.stringDef);
            }
            /* case 4b and 4c are the same from a schema standpoint. */
            fdef = mdef.getElement();
            return defaultESTypeFor(fdef);

        default:
            throw new IllegalStateException
                ("Fields of type " + fdef + " aren't allowed as index fields.");
        }
    }

    /*
     * Returns a put operation containing a JSON document suitable for
     * indexing at an ES index, based on the given RowImpl.
     * 
     * @param esIndexName  name of es index to which the put operation is
     *                     created
     * @param esIndexType  es index mapping to which the put operation is
     *                     created
     * @param row          row from which to create a put operation
     * @return  a put index operation to an es index; if null, it means
     *                     that no significant content was found.
     */
    static IndexOperation makePutOperation(IndexImpl index,
                                           String esIndexName,
                                           String esIndexType,
                                           RowImpl row) {

        final Table table = index.getTable();
        assert (table == row.getTable());

        /* The encoded string form of the row's primary key. */
        String pkPath = TableKey.createKey(table, row, false).getKey()
                                .toString();

        try {
            /* root object */
            ESJsonBuilder document = ESJsonBuilder.builder()
                                                  .startStructure()
                                                  .startStructure("_pkey")
                                                  . /* nested primary key object */
                                                  field("_table",
                                                        table.getFullName());

            for (String keyField : table.getPrimaryKey()) {
                new DocEmitter(keyField, document).putValue(row.get(keyField));
            }

            document.endStructure(); /* end of primary key object */

            List<IndexField> indexFields = index.getIndexFields();
            int indexFieldCounter = 0;
            boolean contentToIndex = false;
            for (String field : index.getFields()) {
                IndexField indexField = indexFields.get(indexFieldCounter++);
                if (addValue(indexField, row, getMappingFieldName(field),
                             document)) {
                    contentToIndex = true;
                }
            }

            if (!contentToIndex) {
                return null;
            }

            document.endStructure(); /* end of root object */

            return new IndexOperation(esIndexName, esIndexType, pkPath,
                                      document.byteArray(),
                                      IndexOperation.OperationType.PUT);
        } catch (IOException e) {
            throw new IllegalStateException
                ("Unable to serialize ES" + " document for text index " +
                 index.getName(), e);
        }
    }

    /*
     * Add a field to the JSON document using the value implied by indexField
     * and row.  A return value of false indicates that no indexable content
     * was found.
     */
    private static boolean addValue(IndexField indexField,
                                    RowImpl row,
                                    String mappingFieldName,
                                    ESJsonBuilder document) throws IOException {

        FieldDef fdef = indexField.getFirstDef();
        int stepIdx = 0;
        String fieldName = indexField.getStep(stepIdx++);

        /*
         * Emit the field name lazily; if there is nothing to index,
         * don't bother indexing the field at all.
         */
        final DocEmitter emitter = new DocEmitter(mappingFieldName, document);

        switch (fdef.getType()) {

                /* Scalar types are easy. */
        case STRING:
        case INTEGER:
        case LONG:
        case BOOLEAN:
        case FLOAT:
        case DOUBLE:
        case TIMESTAMP:
            emitter.putValue(row.get(fieldName));
            break;

            /* An array can contain either scalars or records.
             * If it's an array of records, one field of the record will
             * be indicated by IndexField.
             */
        case ARRAY:
            final ArrayValue aValue = row.get(fieldName).asArray();
            final ArrayDef adef = fdef.asArray();
            fdef = adef.getElement();
            if (fdef.isComplex()) {
                if (!fdef.isRecord()) {
                    throw new IllegalStateException
                        ("Array type " + fdef +
                         " not allowed as an index field.");
                }
                stepIdx++; /* Skip over the ifield placeholder "[]" */
                fieldName = indexField.getStep(stepIdx++);
            }
            for (FieldValue element : aValue.toList()) {
                if (element.isRecord()) {
                    emitter.putArrayValue(element.asRecord()
                                                 .get(fieldName));
                } else {
                    emitter.putArrayValue(element);
                }
            }
            break;

            /*
             * A record will have one field indicated for indexing.
             */
        case RECORD:
            RecordValue rValue = row.get(fieldName).asRecord();
            fieldName = indexField.getStep(stepIdx++);
            emitter.putValue(rValue.get(fieldName));
            break;

            /*
             * An index field can specify that all keys, all values, or one
             * value corresponding to a given key be included in the index.
             */
        case MAP:
            final MapValue mValue = row.get(fieldName).asMap();
            final Map<String, FieldValue> mFields = mValue.getFields();
            fieldName = indexField.getStep(stepIdx++);
            if (TableImpl.KEYS.equalsIgnoreCase(fieldName)) {
                /* add all the keys in the map */
                for (String key : mFields.keySet()) {
                    emitter.putArrayString(key);
                }
            } else if (TableImpl.VALUES.equalsIgnoreCase(fieldName)) {
                /* add all the values in the map */
                for (Entry<String, FieldValue> entry : mFields.entrySet()) {
                    emitter.putArrayValue(entry.getValue());
                }
            } else {
                emitter.putValue(mFields.get(fieldName));
            }
            break;
        default:
            throw new IllegalStateException
                ("Unexpected type in addValue" + fdef);
        }
        emitter.end();
        return emitter.emitted();
    }

    /*
     * DocEmitter is a helper class for writing fields into an XContentBuilder.
     * It delays writing the field name, so that if it is discovered that there
     * is no content of interest, it can avoid writing anything at all.
     */
    private static class DocEmitter {

        private final String fieldName;
        private final ESJsonBuilder document;
        private boolean emitted;
        private boolean emittingArray;

        DocEmitter(String fieldName, ESJsonBuilder document) {
            this.fieldName = fieldName;
            this.document = document;
            this.emitted = false;
            this.emittingArray = false;
        }

        private void startEmittingMaybe() throws IOException {
            if (!emitted) {
                document.field(fieldName);
                emitted = true;
            }
        }

        private void startEmittingArrayMaybe() throws IOException {
            startEmittingMaybe();
            if (!emittingArray) {
                document.startArray();
                emittingArray = true;
            }
        }

        void putString(String val) throws IOException {
            if (val == null) {
                return;
            }
            startEmittingMaybe();
            document.value(val);
        }

        void putValue(FieldValue val) throws IOException {
            if (val == null || val.isNull()) {
                return;
            }
            if (val.isTimestamp()) {
                putTimestamp(val.asTimestamp());
            } else {
                putString(val.toString());
            }
        }

        private void putTimestamp(TimestampValue tsv) throws IOException {
            final int precision = tsv.getDefinition().asTimestamp()
                                     .getPrecision();
            if (simpleDate(precision)) {
                putString(tsv.toString());
            } else {
                startEmittingMaybe();
                document.startStructure();
                putTimestampObject(tsv);
                document.endStructure();
            }
        }

        private void putTimestampObject(TimestampValue tsv) throws IOException {

            document.field(DATE);
            String str = tsv.toString();
            putString(str.substring(0, str.indexOf(".")));
            document.field(NANOS);
            putString(String.valueOf(tsv.get().getNanos()));
        }

        void putArrayString(String val) throws IOException {
            if (val == null || "".equals(val)) {
                return;
            }
            startEmittingArrayMaybe();
            putString(val);
        }

        void putArrayValue(FieldValue val) throws IOException {
            if (val == null || val.isNull()) {
                return;
            }
            startEmittingArrayMaybe();
            if (val.isTimestamp()) {
                putTimestamp(val.asTimestamp());
            } else {
                putString(val.toString());
            }
        }

        void end() throws IOException {
            if (emittingArray) {
                document.endArray();
                emittingArray = false;
            }
        }

        boolean emitted() {
            return emitted;
        }
    }

    /*
     * Returns a delete operation containing a JSON document suitable for
     * indexing at an ES index, based on the given RowImpl.
     * 
     * @param esIndexName  name of es index to which the delete operation is
     *                     created
     * @param esIndexType  es index mapping to which the delete operation is
     *                     created
     * @param row          row from which to create a delete operation
     * 
     * @return  a delete operation to an es index
     */
    static IndexOperation makeDeleteOperation(IndexImpl index,
                                              String esIndexName,
                                              String esIndexType,
                                              RowImpl row) {

        final Table table = index.getTable();
        assert table == row.getTable();

        /* The encoded string form of the row's primary key. */
        String pkPath =
            TableKey.createKey(table, row, false).getKey().toString();

        return new IndexOperation(esIndexName,
                                  esIndexType,
                                  pkPath,
                                  null,
                                  IndexOperation.OperationType.DEL);
    }

    /*
     * Provides a default translation between NoSQL types and ES types.
     * 
     * The TIMESTAMP type is translated to "date<precision>" e.g. "date3" for
     * TIMESTAMP(3)
     * 
     * @param fdef field definition in NoSQL DB
     * 
     * @return ES type translated from field type
     */
    static String defaultESTypeFor(FieldDef fdef) {
        FieldDef.Type t = fdef.getType();
        switch (t) {
            case STRING:
            case INTEGER:
            case LONG:
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
                return t.toString().toLowerCase();
            case TIMESTAMP:
                return "date" + fdef.asTimestamp().getPrecision();
            case ARRAY:
            case BINARY:
            case FIXED_BINARY:
            case MAP:
            case RECORD:
            case ENUM:
            default:
                throw new IllegalStateException
                    ("Unexpected default type mapping requested for " + t);
        }
    }

    /*
     * Returns true if f represents a retriable failure.  Some ES errors
     * indicate that a there is a problem with the document that was sent to
     * ES.  Such documents will never succeed in being indexed and so should
     * not be retried. The status code is intended for REST request statuses,
     * and not all of the possible values are relevant to the bulk request.  I
     * have chosen to list all possible values in the switch statement anyway;
     * any that seem irrelevant are simply relegated to the "not retriable"
     * category.
     * 
     * @param f  A Failure object from a BulkItemResponse.
     * 
     * @return   Boolean indication of whether the failure should be re-tried.
     */
    static boolean isRetriable(RestStatus status) {
        switch (status) {
        case BAD_GATEWAY:
        case CONFLICT:
        case GATEWAY_TIMEOUT:
        case INSUFFICIENT_STORAGE:
        case INTERNAL_SERVER_ERROR:
        case TOO_MANY_REQUESTS: /*
                                 * Returned if we try to use a client node that
                                 * has been shut down; which would be a bug
                                 */
        case SERVICE_UNAVAILABLE: /*
                                   * Returned if the shard has insufficient
                                   * replicas - this is the significant one
                                   */

            return true;

        case ACCEPTED:
        case BAD_REQUEST:
        case CONTINUE:
        case CREATED:
        case EXPECTATION_FAILED:
        case FAILED_DEPENDENCY:
        case FOUND:
        case FORBIDDEN:
        case GONE:
        case HTTP_VERSION_NOT_SUPPORTED:
        case LENGTH_REQUIRED:
        case LOCKED:
        case METHOD_NOT_ALLOWED:
        case MOVED_PERMANENTLY:
        case MULTIPLE_CHOICES:
        case MULTI_STATUS:
        case NON_AUTHORITATIVE_INFORMATION:
        case NOT_ACCEPTABLE:
        case NOT_FOUND:
        case NOT_IMPLEMENTED:
        case NOT_MODIFIED:
        case NO_CONTENT:
        case OK:
        case PARTIAL_CONTENT:
        case PAYMENT_REQUIRED:
        case PRECONDITION_FAILED:
        case PROXY_AUTHENTICATION:
        case REQUESTED_RANGE_NOT_SATISFIED:
        case REQUEST_ENTITY_TOO_LARGE:
        case REQUEST_TIMEOUT:
        case REQUEST_URI_TOO_LONG:
        case RESET_CONTENT:
        case SEE_OTHER:
        case SWITCHING_PROTOCOLS:
        case TEMPORARY_REDIRECT:
        case UNAUTHORIZED:
        case UNPROCESSABLE_ENTITY:
        case UNSUPPORTED_MEDIA_TYPE:
        case USE_PROXY:
        default:

            return false;
        }
    }

    /* Convenience Static Utility Methods */

    /*
     * For use in the context of KV AdminService
     */
    /**
     * For use in the context of KV AdminService
     * 
     * The parameter secure can be checked by the StorageNode Parameter
     * ES_CLUSTER_SECURE.
     *
     * @param clusterName - ES Cluster name.
     * @param esMembers - hostport pair of registered ES Node.
     * @param secure - true means ES Cluster is available on https.(TLS)
     * @param admin - The admin instance
     * @return - ESRestClient
     * @throws IOException - Exception thrown if Client could not be created.
     */
    public static ESRestClient createESRestClient(String clusterName,
                                                  String esMembers,
                                                  boolean secure,
                                                  Admin admin)
        throws IOException {
        ESHttpClient baseHttpClient = null;
        if (!secure) {
            baseHttpClient = createESHttpClient(clusterName, esMembers,
                                                admin.getLogger());
        } else {
            baseHttpClient = createESHttpClient(clusterName, esMembers,
                                                admin.getParams()
                                                     .getSecurityParams(),
                                                admin.getLogger());
        }
        return new ESRestClient(baseHttpClient, admin.getLogger());
    }

    /*
     * Non secure ES Client.
     */
    public static ESRestClient createESRestClient(String clusterName,
                                                  String esMembers,
                                                  Logger logger)
        throws IOException {
        ESHttpClient baseHttpClient = null;
        baseHttpClient = createESHttpClient(clusterName, esMembers, logger);
        return new ESRestClient(baseHttpClient, logger);
    }

    /*
     * The caller has to make sure that ES Cluster is set up in a secure
     * fashion.
     * 
     * This method does not check that because register-es plan makes sure that
     * if KVStore is running in secured mode, that ES Cluster has to be
     * registered for HTTPS.
     * 
     * Whether ESCluster is secured or not, can be checked by StorageNode
     * Parameter, SEARCH_CLUSTER_SECURE.
     */
    public static ESHttpClient createESHttpClient(String clusterName,
                                                  String esMembers,
                                                  SecurityParams secParams,
                                                  Logger logger)
        throws IOException {
        if (ESRestClientUtil.isEmpty(esMembers)) {
            throw new IllegalArgumentException();
        }
        final HostPort[] hps = HostPort.parse(esMembers.split(","));
        return createESHttpClient(clusterName, hps, secParams, logger);
    }

    /*
     * The caller has to make sure that ES Cluster is set up in a secure
     * fashion.
     * 
     * This method does not check that because register-es plan makes sure that
     * if KVStore is running in secured mode, that ES Cluster has to be
     * registered for HTTPS.
     * 
     * Whether ESCluster is secured or not, can be checked by StorageNode
     * Parameter, SEARCH_CLUSTER_SECURE.
     */
    public static ESHttpClient createESHttpClient(String clusterName,
                                                  HostPort hostPort,
                                                  SecurityParams secParams,
                                                  Logger logger)
        throws IOException {

        HostPort[] hostPorts = new HostPort[1];
        hostPorts[0] = hostPort;

        return createESHttpClient(clusterName, hostPorts, secParams, logger);

    }

    public static ESHttpClient createESHttpClient(String clusterName,
                                                  HostPort[] hostPorts,
                                                  SecurityParams secParams,
                                                  Logger logger)
        throws IOException {

        return createESHttpClient(clusterName, hostPorts, secParams, logger,
                                  maxRetryTimeoutMillis);
    }

    public static ESHttpClient createESHttpClient(String clusterName,
                                                  HostPort[] hostPorts,
                                                  SecurityParams secParams,
                                                  Logger logger,
                                                  int retryTimeout)
        throws IOException {

        if (logger == null) {
            logger = LoggerUtils.getLogger(ElasticsearchHandler.class,
                                           "[es]");
        }

        if (secParams == null || !secParams.isSecure()) {
            return createESHttpClient(clusterName, hostPorts, logger);
        }

        AtomicReference<char[]> ksPwdAR = new AtomicReference<char[]>();

        SSLContext sslContext = null;
        try {
            sslContext = TIFSSLContext.makeSSLContext(secParams, ksPwdAR, logger);

            /*
             * No need to check connections as that is done during
             * register-es-plan before setting the SN parameters.
             * 
             * For now, using same logger for low level httpclient and higher
             * level clients.
             */
            ESHttpClient client = createESHttpClient(hostPorts, true,
                                                     sslContext, retryTimeout,
                                                     logger);
            if (verifyClusterName(clusterName, client)) {
                return client;
            }

            throw new IOException("ClusterName does not match on ES Cluster");

        } catch (SSLContextException e) {
            throw new IOException(e);
        } finally {
            if (ksPwdAR.get() != null)
                Arrays.fill(ksPwdAR.get(), ' ');
        }
    }

    /*
     * Currently setting up ES Client does not check if the given ES Node
     * Members is hosting the cluster given by the clusterName parameter.
     */
    private static ESHttpClient createESHttpClient(String clusterName,
                                                   String esMembers,
                                                   Logger logger)
        throws IOException {
        if (ESRestClientUtil.isEmpty(esMembers)) {
            throw new IllegalArgumentException();
        }

        final HostPort[] hps = HostPort.parse(esMembers.split(","));

        return createESHttpClient(clusterName, hps, logger);

    }

    private static ESHttpClient createESHttpClient(String clusterName,
                                                   HostPort[] hps,
                                                   Logger logger)
        throws IOException {
        if (hps == null || hps.length == 0) {
            throw new IllegalArgumentException();
        }

        ESHttpClient client = createESHttpClient(hps, false, null,
                                                 maxRetryTimeoutMillis,
                                                 logger);
        if (verifyClusterName(clusterName, client)) {
            return client;
        }

        throw new IOException("ClusterName does not match on ES Cluster");

    }

    /*
     * FOR TEST PURPOSES ONLY - SSLContext Creation is private to this class. *
     */
    public static ESHttpClient createESHttpClient(String clusterName,
                                                  HostPort[] hostPorts,
                                                  boolean secure,
                                                  SSLContext sslContext,
                                                  int retryTimeout,
                                                  Logger logger)
        throws IOException {

        ESHttpClient client = createESHttpClient(hostPorts, secure,
                                                 sslContext, retryTimeout,
                                                 logger);

        if (verifyClusterName(clusterName, client)) {
            return client;
        }

        throw new IOException("ClusterName does not match on ES Cluster");

    }

    /**
     * 
     * @param hostPorts  ES Node HostPorts.
     * @param secure  A boolean variable coming from the user end.
     * @param sslContext  An SSLContext containing keystore info. Note that
     * the keystore password is filled with null after the method is done. if
     * secure is true, needs SSLContext. Caller method should create this in
     * case, user configures a secure connection.
     * @param retryTimeout  timeout before retries give up.
     * @return  ESHttpClient instance.
     */

    private static ESHttpClient createESHttpClient(
                                                   HostPort[] hostPorts,
                                                   boolean secure,
                                                   SSLContext sslContext,
                                                   int retryTimeout,
                                                   Logger logger) {
        if (hostPorts == null || hostPorts.length == 0) {
            throw new IllegalArgumentException("hostPorts is required");
        }

        ESHttpClient baseHttpClient = null;
        HttpHost[] httpHosts = new HttpHost[hostPorts.length];
        int i = 0;
        for (HostPort hostPort : hostPorts) {
            httpHosts[i++] = new HttpHost(hostPort.hostname(), hostPort.port(),
                                          secure ? "https" : "http");
        }
        ESHttpClientBuilder builder =
            new ESHttpClientBuilder(httpHosts).setMaxRetryTimeoutMillis
                                               (retryTimeout)
                                              .setLogger(logger);

        if (secure) {
            if (sslContext == null) {
                throw new IllegalArgumentException();
            }
            builder.setSecurityConfigCallback(new SecurityConfigCallback() {

                @Override
                public HttpAsyncClientBuilder addSecurityConfig
                                              (HttpAsyncClientBuilder
                                               httpClientBuilder) {
                    return httpClientBuilder.setSSLContext(sslContext);
                }

            });
        }
        baseHttpClient = builder.build();
        return baseHttpClient;
    }

    public static boolean verifyClusterName(String clusterName,
                                            ESHttpClient httpClient)
        throws IOException {
        if (ESRestClientUtil.isEmpty(clusterName)) {
            return false;
        }
        JsonParser parser = null;
        try {
            String esClusterName;
            RestResponse resp =
                httpClient.executeSync(HttpGet.METHOD_NAME, "");
            parser = ESRestClientUtil.initParser(resp);
            JsonToken token;
            String currentFieldName;
            while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {

                if (token.isScalarValue()) {
                    currentFieldName = parser.getCurrentName();
                    if ("cluster_name".equals(currentFieldName)) {
                        esClusterName = parser.getText();
                        if (clusterName.equals(esClusterName)) {
                            return true;
                        }
                    }

                }
            }

        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }

        return false;

    }

}
