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

package oracle.kv.impl.tif.esclient.httpClient;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLPeerUnverifiedException;

import oracle.kv.impl.tif.esclient.esResponse.ESException;
import oracle.kv.impl.tif.esclient.restClient.RestResponse;
import oracle.kv.impl.tif.esclient.restClient.RestStatus;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.AuthCache;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpTrace;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.nio.client.methods.HttpAsyncMethods;
import org.apache.http.nio.protocol.BasicAsyncResponseConsumer;
import org.apache.http.nio.protocol.HttpAsyncRequestProducer;
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;

/**
 * 
 * The base level HttpClient on which RestClient and MonitorClient are built.
 *
 */
public class ESHttpClient {

    private final Logger logger;
    private volatile CloseableHttpAsyncClient client;
    private volatile boolean closed = false;
    /*
     * The ESNodeMonitor would assign a new list to this variable, if there are
     * changes in available ES Nodes.
     * 
     * TODO: AuthCache needs to change for the new ES Node getting added to
     * availableESHttpNodes.
     * 
     * Currently this client does not use BasicAuthentication.
     */
    private volatile List<HttpHost> availableESHttpNodes;
    private Map<HttpHost, HttpHost> allESHttpNodes =
        new ConcurrentHashMap<HttpHost, HttpHost>();
    private volatile AuthCache authCache = new BasicAuthCache();
    private final long maxRetryTimeoutMillis;
    private final Header[] defaultHeaders;
    private final String pathPrefix;
    private FailureListener failureListener;
    private final long checkForESConnectionTimeoutMillis = 30 * 60 * 1000;

    public static final Comparator<HttpHost> httpHostComparator =
        new Comparator<HttpHost>() {

            @Override
            public int compare(HttpHost o1, HttpHost o2) {
                if (o1.getHostName()
                      .compareToIgnoreCase(o1.getHostName()) == 0) {
                    if (o1.getPort() > o2.getPort()) {
                        return 1;
                    } else if (o1.getPort() < o2.getPort()) {
                        return -1;
                    } else {
                        return 0;
                    }
                }
                return o1.getHostName().compareToIgnoreCase(o1.getHostName());
            }

        };

    public ESHttpClient(CloseableHttpAsyncClient httpClient,
            int maxRetryTimeout,
            Header[] defaultHeaders,
            List<HttpHost> esNodes,
            String pathPrefix,
            FailureListener failureListener,
            Logger logger) {
        this.client = httpClient;
        this.maxRetryTimeoutMillis = maxRetryTimeout;
        this.availableESHttpNodes = esNodes;
        populateAllHttpNodes(esNodes);
        this.pathPrefix = pathPrefix;
        this.failureListener = failureListener;
        this.defaultHeaders = defaultHeaders;
        this.logger = logger;

    }

    /*
     * AvailableNodes are changed in ESNodeMonitor. A new list should be
     * created and assigned here. Get method prepares a copy of this list. This
     * method should be called under a lock.
     */
    public synchronized void setAvailableNodes(List<HttpHost> availableNodes) {
        this.availableESHttpNodes = availableNodes;
        populateAllHttpNodes(availableNodes);
    }

    public synchronized List<HttpHost> getAvailableNodes() {
        List<HttpHost> copyofAvailableNodes = new ArrayList<HttpHost>();
        for (HttpHost host : availableESHttpNodes) {
            copyofAvailableNodes.add(host);
        }
        return copyofAvailableNodes;
    }

    /* Not thread safe. This method should be called under a lock. */
    private void populateAllHttpNodes(List<HttpHost> availableESNodes) {
        for (HttpHost host : availableESNodes) {
            allESHttpNodes.put(host, host);
        }
    }

    public List<HttpHost> getAllESHttpNodes() {

        List<HttpHost> allNodes = new ArrayList<HttpHost>();

        for (HttpHost host : allESHttpNodes.keySet()) {
            allNodes.add(host);
        }
        return allNodes;
    }

    public ESHttpClient.FailureListener getFailureListener() {
        return failureListener;
    }

    /*
     * ESNodeMonitor is a FailureListener, however ESNodeMonitor can be
     * instantiated only after ESHttpClient is constructed. So this
     * failureListener is set later in the ESHttpClientBuilder.
     */
    public void
            setFailureListener(ESHttpClient.FailureListener failureListener) {
        this.failureListener = failureListener;
    }

    public RestResponse executeSync(String method, String endpoint)
        throws IOException, ESException {
        return executeSync(method, endpoint,
                           Collections.<String, String> emptyMap());
    }

    public RestResponse
            executeSync(String method, String endpoint, Header... headers)
                throws IOException, ESException {
        return executeSync(method, endpoint,
                           Collections.<String, String> emptyMap(), null,
                           headers);
    }

    public RestResponse executeSync(
                                    String method,
                                    String endpoint,
                                    Map<String, String> params,
                                    Header... headers)
        throws IOException, ESException {
        return executeSync(method, endpoint, params, (HttpEntity) null,
                           headers);
    }

    public RestResponse executeSync(
                                    String method,
                                    String endpoint,
                                    Map<String, String> params,
                                    HttpEntity entity,
                                    Header... headers)
        throws IOException, ESException {
        ESSyncResponseListener listener =
            new ESSyncResponseListener(maxRetryTimeoutMillis);
        executeAsync(method, endpoint, params, entity, listener, headers);
        return listener.get();
    }

    public void executeAsync(
                             String method,
                             String endpoint,
                             Map<String, String> params,
                             ESResponseListener responseListener,
                             Header... headers) {
        executeAsync(method, endpoint, params, null, responseListener,
                     headers);
    }

    public void executeAsync(
                             String method,
                             String endpoint,
                             Map<String, String> params,
                             HttpEntity entity,
                             ESResponseListener responseListener,
                             Header... headers) {
        try {
            Objects.requireNonNull(params, "params must not be null");
            Map<String, String> requestParams = new HashMap<>(params);
            URI uri = buildUri(pathPrefix, endpoint, requestParams);
            HttpRequestBase request = createHttpRequest(method, uri, entity);
            setHeaders(request, headers);
            long startTime = System.nanoTime();
            executeAsync(startTime, request, responseListener,0);
        } catch (Exception e) {
            responseListener.onFailure(e);
        }
    }

    private void executeAsync(
                              final long startTime,
                              final HttpRequestBase request,
                              final ESResponseListener listener,
                              final int attempt) {
        rotateNodes(availableESHttpNodes);
        // Make sure that availableESHttpNodes should atleast have one node
        // before calling this method.
        final HttpHost host = availableESHttpNodes.get(0);
        if (!request.getRequestLine().getUri().toLowerCase()
                    .contains("_nodes")) {
            logger.fine("SENDING REQUEST:" + request + "  TO HOST: " + host);

        }
        if (!closed && !client.isRunning()) {
            try {
                synchronized (this) {
                    HttpAsyncClientBuilder httpClientBuilder =
                        HttpAsyncClientBuilder.create();
                    CloseableHttpAsyncClient httpclient =
                        httpClientBuilder.build();
                    client.close();
                    client = httpclient;
                }
                client.start();
            } catch (IOException e) {
                logger.severe("Closed Client -" +
                        " Could not create a new one and start it");
            }
            logger.warning("Found closed client." +
                    " New Client started: State - isRunning : " +
                    client.isRunning());
        }
        // we stream the request body if the entity allows for it
        final HttpAsyncRequestProducer requestProducer =
            HttpAsyncMethods.create(host, request);
        final HttpAsyncResponseConsumer<HttpResponse> asyncResponseConsumer =
            new BasicAsyncResponseConsumer();
        final HttpClientContext context = HttpClientContext.create();
        context.setAuthCache(authCache);
        client.execute(requestProducer, asyncResponseConsumer, context,
                       new FutureCallback<HttpResponse>() {
                           @Override
                           public void completed(HttpResponse httpResponse) {
                               try {
                                   RestResponse restResponse =
                                       new RestResponse(request
                                                        .getRequestLine(),
                                                        host, httpResponse);
                                   int status = httpResponse.getStatusLine()
                                                            .getStatusCode();
                                   if (!request.getRequestLine().getUri()
                                               .toLowerCase()
                                               .contains("_nodes")) {
                                       logger.fine("Got response:" + status +
                                               "  for request:" +
                                               request.getRequestLine());
                                   }
                                   /*
                                    * Leaving this commented..useful in dev
                                    * debugging
                                    */
                                   /*
                                    * logger.fine("Resp Entity is:" +
                                    * EntityUtils.toString(restResponse.
                                    * getEntity()));
                                    */

                                   if (status < 300) {
                                       restResponse.success(true);
                                       onSuccessResponse(host);
                                       listener.onSuccess(restResponse);
                                   } else {

                                       /*
                                        * Leaving this commented..useful in dev
                                        * debugging
                                        */
                                       /*
                                        * logger.fine("Resp Entity is:" +
                                        * EntityUtils.toString( httpResponse.
                                        * getEntity()));
                                        */
                                       ESException responseException =
                                           new ESException(restResponse);

                                       if (responseException.errorType() ==
                                               null &&
                                               responseException.errorStatus() ==
                                               null && status ==
                                               RestStatus.NOT_FOUND
                                               .getStatus()) {
                                           switch (request
                                                   .getMethod()
                                                   .toUpperCase(Locale.ROOT)) {
                                               case HttpDelete.METHOD_NAME:
                                               case HttpGet.METHOD_NAME:
                                                   /*
                                                    * Trying to access a
                                                    * document which does not
                                                    * exist.
                                                    */
                                                   listener.onSuccess(
                                                              restResponse);
                                                   return;
                                               default:
                                                   break;

                                           }
                                       }
                                       logger.info("Got failed response:" +
                                               status + "  for request:" +
                                               request.getRequestLine());
                                       if (isRetriable(status)) {
                                           logger.info(" Retriable response" +
                                                   " with status: " + status);
                                           restResponse.retriable(true);
                                           /*
                                            * mark host dead and retry against
                                            * next one
                                            */
                                           if (failureListener != null) {
                                               failureListener.onFailure(host);
                                           }
                                           retryIfPossible(responseException);
                                       } else {
                                           /*
                                            * mark host alive and don't retry,
                                            * as the error should be a request
                                            * problem
                                            */
                                           onSuccessResponse(host);
                                           listener
                                           .onFailure(responseException);
                                       }
                                   }
                               } catch (Exception e) {
                                   logger.severe(" ESHttpClient Could" +
                                           " not process the" +
                                           "Completed Async Event" +
                                           " Successfully");
                                   listener.onFailure(e);
                               }
                           }

                           /*
                            * The problem could be with the ES Node the request
                            * went to. Populate availableESNodes once more.
                            */
                           @Override
                           public void failed(Exception asynclientException) {
                               try {
                                   if (asynclientException instanceof
                                           SSLPeerUnverifiedException) {
                                       IOException e = new IOException(
                                                         "Please verify" +
                                                         " the Subject " +
                                                         "Alternative " +
                                                         "Name dns/ip" +
                                                         " in the " +
                                                         "" +
                                                         "certificates of ES" +
                                                         " ES Nodes." +
                                                         "SSL Exception:" +
                                                         asynclientException);
                                       logger.warning("Peer not verified." +
                                              " Exception:" + e.getMessage());
                                       listener.onFailure(e);
                                       return;
                                   } else if (asynclientException instanceof
                                           SSLException) {
                                       /*
                                        * Problems with SSL Layer. No point
                                        * retrying.
                                        */
                                       IOException e = new IOException(
                                                         "SSL Connection had" +
                                                         " issues.  " +
                                                         asynclientException);
                                       logger.warning("SSL Exception:" +
                                                     e.getMessage());

                                       listener.onFailure(e);
                                       return;
                                   } else if (asynclientException instanceof
                                           UnknownHostException) {
                                       IOException e = new IOException(
                                                         "Unknown Host." +
                                                         "One of The ES Host " +
                                                         "registered " +
                                                         "can not be resolved." +
                                                         asynclientException);
                                       logger.warning("Unknown Host:" +
                                               e.getMessage());
                                       listener.onFailure(e);
                                       return;
                                   }
                                   
                                   if (failureListener != null &&
                                           !failureListener.isClosed()) {
                                       logger.info("ES Http Client with nodes:" +
                                               availableESHttpNodes + "request:" +
                                               request.getRequestLine() +
                                               " failed due to " +
                                               asynclientException +
                                               " Failure Listener will handle" +
                                               " and will be retried.");
                                       failureListener.onFailure(host);
                                       retryIfPossible(asynclientException);
                                   } else {
                                       logger.info("ES Http Client with " +
                                               "nodes:" +
                                               availableESHttpNodes +
                                               "request:" +
                                               request.getRequestLine() +
                                               " failed due to " +
                                               asynclientException +
                                               " Failure Listener is not" +
                                               " available for this client" +
                                               " and this request will be" +
                                               " retried.");
                                       retryIfPossible(asynclientException);
                                   }
                                   
                               } catch (Exception e) {
                                   listener.onFailure(e);
                               }
                           }

                           @Override
                           public void cancelled() {
                               listener.onFailure(new ExecutionException(
                                       "request" + " was cancelled", null));
                           }

                           private void onSuccessResponse(
                                      @SuppressWarnings("unused") 
                                      HttpHost host2) {
                               /*
                                * Since onFailure adds all available nodes
                                * again, there is nothing onSuccess needs to
                                * do. TODO : this method may not be required at
                                * all.
                                */
                           }

                           @SuppressWarnings("static-access")
                           private void retryIfPossible(Exception exception) {
                               if(closed) {
                                   return;
                               }
                               if (availableESHttpNodes.size() >= 2) {
                                   rotateNodes(availableESHttpNodes);
                               }
                               if (exception instanceof ConnectException) {
                                   /*
                                    * Connection is refused. Try again with a
                                    * longer retry timeout.
                                    */
                                   logger.info("Connection Refused when" +
                                           " availableNodes:" +
                                           availableESHttpNodes + "and" +
                                           " allNodes:" + allESHttpNodes);
                                   try {
                                       Thread.currentThread()
                                       .sleep(1+500*attempt*attempt);
                                   } catch (InterruptedException e) {
                                       logger.info("Thread waiting due" +
                                               " to connection" +
                                               " refused, got interrupted");
                                   }
                                   retryRequestWithTimeout(
                                        checkForESConnectionTimeoutMillis);
                               } else {
                                   retryRequestWithTimeout(
                                                   maxRetryTimeoutMillis);
                               }
                           }

                           private void retryRequestWithTimeout(
                                             long timeoutSettingMillis) {
                               if(closed) {
                                   return;
                               }
                               long timeElapsedMillis = TimeUnit
                                                        .NANOSECONDS
                                                        .toMillis(
                                                           System.nanoTime() -
                                                           startTime);
                               long timeout = timeoutSettingMillis -
                                       timeElapsedMillis;
                               if (timeout <= 0) {
                                   IOException retryTimeoutException =
                                       new IOException(
                                         "request retries" +
                                         " exceeded max timeout for" +
                                         " ES to come up [" +
                                         checkForESConnectionTimeoutMillis +
                                         "]");
                                   listener.onFailure(retryTimeoutException);
                               } else {
                                   request.reset();
                                   if (!request.getRequestLine().getUri()
                                               .toLowerCase().contains(
                                                                       "_nodes")) {
                                       logger.fine(" Retrying request:" +
                                               request);
                                   }
                                   executeAsync(startTime, request, listener,attempt + 1);
                               }
                           }

                       });
        
    }

    private synchronized void rotateNodes(List<HttpHost> availableNodes) {
        Collections.rotate(availableNodes, 1);
    }

    private void setHeaders(HttpRequest httpRequest, Header[] requestHeaders) {
        Objects.requireNonNull(requestHeaders,
                               "request headers must not be null");
        /*
         * request headers override default headers, so we don't add default
         * headers if they exist as request headers
         */
        final Set<String> requestNames = new HashSet<>(requestHeaders.length);
        for (Header requestHeader : requestHeaders) {
            Objects.requireNonNull(requestHeader,
                                   "request header must not be null");
            httpRequest.addHeader(requestHeader);
            requestNames.add(requestHeader.getName());
        }
        for (Header defaultHeader : defaultHeaders) {
            if (requestNames.contains(defaultHeader.getName()) == false) {
                httpRequest.addHeader(defaultHeader);
            }
        }
    }

    public synchronized void close() {
        try {
            if (client.isRunning()) {
                logger.info("ES Http Client - Connection to ES Nodes:" +
                            availableESHttpNodes + " going down");
                logger.info("ES Http Client closing...");
                if(failureListener != null) {
                    failureListener.close();
                }
                client.close();
                closed = true;
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "ESHttpClient closing exception", e);
        }
    }

    public boolean isClosed() {
        return closed;
    }

    private static boolean isRetriable(int status) {
        switch (status) {
            case 408:
            case 502:
            case 503:
            case 504:
                return true;
        }
        return false;
    }

    private static URI buildUri(
                                String pathPrefix,
                                String path,
                                Map<String, String> params) {
        Objects.requireNonNull(path, "path must not be null");
        try {
            String fullPath;
            if (pathPrefix != null) {
                if (path.startsWith("/")) {
                    fullPath = pathPrefix + path;
                } else {
                    fullPath = pathPrefix + "/" + path;
                }
            } else {
                fullPath = path;
            }

            URIBuilder uriBuilder = new URIBuilder(fullPath);
            for (Map.Entry<String, String> param : params.entrySet()) {
                uriBuilder.addParameter(param.getKey(), param.getValue());
            }
            return uriBuilder.build();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    private static HttpRequestBase
            createHttpRequest(String method, URI uri, HttpEntity entity) {
        switch (method.toUpperCase(Locale.ROOT)) {
            case HttpDelete.METHOD_NAME:
                return addRequestBody(new ESHttpDeleteEntity(uri), entity);
            case HttpGet.METHOD_NAME:
                return addRequestBody(new ESHttpGetEntity(uri), entity);
            case HttpHead.METHOD_NAME:
                return addRequestBody(new HttpHead(uri), entity);
            case HttpOptions.METHOD_NAME:
                return addRequestBody(new HttpOptions(uri), entity);
            case HttpPatch.METHOD_NAME:
                return addRequestBody(new HttpPatch(uri), entity);
            case HttpPost.METHOD_NAME:
                HttpPost httpPost = new HttpPost(uri);
                addRequestBody(httpPost, entity);
                return httpPost;
            case HttpPut.METHOD_NAME:
                return addRequestBody(new HttpPut(uri), entity);
            case HttpTrace.METHOD_NAME:
                return addRequestBody(new HttpTrace(uri), entity);
            default:
                throw new UnsupportedOperationException("http method" +
                        " not supported: " +
                        method);
        }
    }

    private static HttpRequestBase
            addRequestBody(HttpRequestBase httpRequest, HttpEntity entity) {
        if (entity != null) {
            if (httpRequest instanceof HttpEntityEnclosingRequestBase) {
                ((HttpEntityEnclosingRequestBase) httpRequest)
                .setEntity(entity);
            } else {
                throw new UnsupportedOperationException(httpRequest
                                                        .getMethod() +
                        " with body is not supported");
            }
        }
        return httpRequest;
    }

    public static class FailureListener {

        /*
         * The ESNodeMonitor serves as FailureListener. That will override this
         * method. There is no default implementation currently.
         */
        public void onFailure(@SuppressWarnings("unused") HttpHost host) {

        }
        
        public void start() {
            
        }
        
        public void close() {
            
        }
        
        public boolean isClosed() {
            return false;
        }
    }

    static class ESSyncResponseListener implements ESResponseListener {
        private volatile Exception supressedExceptions;
        private final CountDownLatch latch = new CountDownLatch(1);
        private final AtomicReference<RestResponse> response =
            new AtomicReference<>();
        private final AtomicReference<Exception> failureException =
            new AtomicReference<>();

        private final long timeout;

        ESSyncResponseListener(long timeout) {
            assert timeout > 0;
            this.timeout = timeout;
        }

        @Override
        public void onSuccess(RestResponse response1) {
            Objects.requireNonNull(response1, "response must not be null");
            boolean wasResponseNull =
                this.response.compareAndSet(null, response1);
            if (wasResponseNull == false) {
                throw new IllegalStateException("response is already set");
            }

            latch.countDown();
        }

        @Override
        public void onFailure(Exception failureException1) {
            Objects.requireNonNull(failureException1,
                                   "exception must not be null");
            boolean wasExceptionNull =
                this.failureException.compareAndSet(null, failureException1);
            if (wasExceptionNull == false) {
                throw new IllegalStateException("exception is already set");
            }
            latch.countDown();
        }

        /**
         * Waits (up to a timeout) for some result of the request: either a
         * response, or an exception.
         * 
         * @throws ESException
         */
        RestResponse get() throws IOException, ESException {
            try {
                /*
                 * providing timeout is just a safety measure to prevent
                 * everlasting waits. the different client timeouts should
                 * already do their jobs
                 */
                if (latch.await(timeout, TimeUnit.MILLISECONDS) == false) {
                    throw new IOException("listener timeout after" +
                                          " waiting for [" +
                                          timeout + "] ms");
                }
            } catch (InterruptedException e) {
                throw new RuntimeException("thread waiting for the response" +
                                           " was interrupted",
                                           e);
            }

            Exception exception = failureException.get();

            RestResponse resp = this.response.get();

            if (exception != null) {
                if (resp != null) {
                    IllegalStateException e =
                        new IllegalStateException("response and exception" +
                                                  " are unexpectedly set " +
                                                  "at the same time");
                    e.addSuppressed(exception);
                    throw e;
                }
                /*
                 * Do not want to handle too many types of Exceptions. Two
                 * categories of Exception will be handled.
                 * 
                 * Rest will result in RunTimeException.
                 */
                if (exception instanceof IOException) {
                    throw (IOException) exception;
                } else if (exception instanceof ESException) {
                    throw (ESException) exception;
                }

                /*
                 * TODO: Handle retry timeout exception. ExecutionException
                 * from async client.
                 */

                throw new RuntimeException("error while performing request",
                                           exception);
            }

            if (resp == null) {
                throw new IllegalStateException("response not set and no " +
                                                "exception caught either");
            }
            return resp;
        }

        @Override
        public void onRetry(Exception retryException) {
            if (supressedExceptions == null) {
                this.supressedExceptions = retryException;
            } else {
                supressedExceptions.addSuppressed(retryException);
            }

        }
    }

    public static enum Scheme {
        HTTP, HTTPS;

        public static Scheme getScheme(String scheme) {
            for (Scheme s : values()) {
                if (valueOf(scheme.toUpperCase(Locale.ENGLISH)) == s) {
                    return s;
                }
            }
            return null;
        }

        public String getProtocol() {

            return name().toLowerCase(Locale.ENGLISH);
        }
    }

}
