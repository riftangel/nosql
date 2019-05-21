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

package oracle.kv.impl.tif.esclient.restClient;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.tif.ElasticsearchHandler;
import oracle.kv.impl.tif.esclient.esRequest.ESRequest;
import oracle.kv.impl.tif.esclient.esRequest.ESRequest.RequestType;
import oracle.kv.impl.tif.esclient.esRequest.ESRestRequestGenerator;
import oracle.kv.impl.tif.esclient.esResponse.ESException;
import oracle.kv.impl.tif.esclient.esResponse.ESResponse;
import oracle.kv.impl.tif.esclient.esResponse.InvalidResponseException;
import oracle.kv.impl.tif.esclient.httpClient.ESHttpClient;
import oracle.kv.impl.tif.esclient.httpClient.ESHttpClient.FailureListener;
import oracle.kv.impl.tif.esclient.restClient.utils.ESRestClientUtil;
import oracle.kv.impl.util.server.LoggerUtils;

import com.fasterxml.jackson.core.JsonParser;

import org.apache.http.HttpHost;

/**
 * A Rest Client for Elasticsearch which executes REST request and parses the
 * REST Response from ES.
 * 
 * Currently FTS works on a synchronous communication with ES. Most of the
 * requests are bulk requests which need to be synchronous to preserve event
 * ordering.
 * 
 * This client only provides a synchronous request execution. The monitoring
 * client which works independently provides asynchronous communication with
 * ES.
 * 
 */
public class ESRestClient {

    private final Logger logger;
    private final ESHttpClient httpClient;
    private ESAdminClient adminClient;
    private ESDMLClient dmlClient;
    private final ReentrantLock lock = new ReentrantLock();

    public ESRestClient(ESHttpClient httpClient, Logger logger) {
        this.httpClient = httpClient;
        if (logger == null) {
            logger = LoggerUtils.getLogger(ElasticsearchHandler.class, "[es]");
        }
        this.logger = logger;
    }

    /*
     * It is possible that first few admin APIs get executed in different
     * adminClient object. But that is fine.
     */
    public ESAdminClient admin() {
        if (adminClient != null) {
            return adminClient;
        }
        lock.lock();
        try {
            adminClient = new ESAdminClient(this, logger);
        } finally {
            lock.unlock();
        }
        return adminClient;
    }

    public ESDMLClient dml() {
        if (dmlClient != null) {
            return dmlClient;
        }
        lock.lock();
        try {
            dmlClient = new ESDMLClient(this, logger);
        } finally {
            lock.unlock();
        }
        return dmlClient;
    }

    public <R extends ESRequest<R>, S extends ESResponse> S
            executeSync(R req, S resp) throws IOException {

        @SuppressWarnings("unchecked")
        JsonResponseObjectMapper<S> respMapper =
            (JsonResponseObjectMapper<S>) resp;
        ESRestRequestGenerator reqGenerator = (ESRestRequestGenerator) req;
        return executeSync(req, reqGenerator, respMapper);

    }

    @SuppressWarnings("unchecked")
    public <R extends ESRequest<R>, S extends ESResponse> S executeSync
                                      (R req,
                                       ESRestRequestGenerator reqGenerator,
                                       JsonResponseObjectMapper<S> respMapper)
        throws IOException {

        RestRequest restReq = reqGenerator.generateRestRequest();
        RestResponse restResp = null;
        JsonParser parser = null;
        S esResponse = null;
        try {
            restResp =
                httpClient.executeSync(restReq.method(), restReq.endpoint(),
                                       restReq.params(), restReq.entity());
            // System.out.println(EntityUtils.toString(restResp.getEntity()));

            RequestType reqType = req.requestType();
            switch (reqType) {
                case GET_MAPPING:
                case EXIST_INDEX:
                case MAPPING_EXIST:
                case REFRESH:
                    esResponse = respMapper.buildFromRestResponse(restResp);
                    esResponse.statusCode(restResp.statusLine()
                                                  .getStatusCode());
                    break;
                default:
                    parser = ESRestClientUtil.initParser(restResp);
                    esResponse = respMapper.buildFromJson(parser);
                    esResponse.statusCode(restResp.statusLine()
                                                  .getStatusCode());
                    break;
            }

        } catch (ESException e) {
            /*
             * Logging the exception reads from the HttpEntity.ESException
             * constructor converts it into a repeatable entity,
             */
            logger.info("Exception thrown from the low level http client:" +
                    e);
            RequestType reqType = req.requestType();
            if (e.errorStatus() == RestStatus.NOT_FOUND) {
                switch (reqType) {
                    case EXIST_INDEX:
                    case MAPPING_EXIST:
                    case DELETE_INDEX:
                    case DELETE:
                    case CAT:
                    case REFRESH:
                        esResponse = respMapper.buildErrorReponse(e);
                        break;
                    default:
                        throw new IOException(e);

                }
            } else if (e.errorStatus() == RestStatus.BAD_REQUEST) {
                switch (reqType) {
                    case CREATE_INDEX:
                        esResponse = respMapper.buildErrorReponse(e);
                        break;
                    default:
                        throw new IOException(e);

                }
            }

        } catch (InvalidResponseException ire) {
            if (restResp.isSuccess() || restResp.isRetriable()) {
                /*
                 * TODO: log this exception, this might be a parsing exception,
                 * which may not result in any problems, in case response got
                 * sufficiently parsed.
                 * 
                 * In case response was not sufficiently parsed, that will be
                 * taken care separately.
                 */
                if (respMapper instanceof ESResponse) {
                    ((ESResponse) respMapper).statusCode(restResp.statusLine()
                                                                 .getStatusCode());
                }
                logger.info("Invalid Response Exception - response can" +
                            " not be parsed fully." +
                            ire);
            } else {
                throw new IOException(ire);
            }
        } catch (IOException ie) {
            logger.log(Level.SEVERE,
                       "http Client could not execute the request", ie);
            if (restResp != null &&
                    (restResp.isSuccess() || restResp.isRetriable())) {
                // TODO: log this exception, this must be a parsing exception.
                if (respMapper instanceof ESResponse) {
                    // set status code in the response.
                    ((ESResponse) respMapper).statusCode(restResp.statusLine()
                                                            .getStatusCode());

                }
                logger.info("IOException in a successful response:" + ie);

            } else {
                throw ie;
            }
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
        if (esResponse != null) {
            return esResponse;
        }
        if (restResp != null) {
            return respMapper.buildFromRestResponse(restResp);
        }
        return (S) respMapper;

    }

    public List<HttpHost> getAvailableNodes() {
        return httpClient.getAvailableNodes();
    }

    public void setAvailableNodes(List<HttpHost> availableNodes) {
        logger.fine("Setting availableNodes to: " + availableNodes);
        httpClient.setAvailableNodes(availableNodes);
    }

    public void close() {
        if (httpClient != null) {
            FailureListener failureListener = httpClient.getFailureListener();
            if (failureListener != null) {
                failureListener.close();
            }
            httpClient.close();
        }
    }

    List<HttpHost> getAllESHttpNodes() {
        return httpClient.getAllESHttpNodes();
    }

    public ESHttpClient getESHttpClient() {
        return httpClient;
    }

}
