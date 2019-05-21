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

import java.util.Objects;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.RequestLine;
import org.apache.http.StatusLine;

public class RestResponse {

    private final RequestLine requestLine;
    private final HttpHost host;
    private final HttpResponse response;
    /*
     * This is set to true if statusCode < 300.
     * For the status 200 OK, ESResponse statusCode is to be used.
     */
    private boolean success = false;
    private boolean retriable = false;

    public RestResponse(RequestLine requestLine,
            HttpHost host,
            HttpResponse response) {
        Objects.requireNonNull(requestLine, "requestLine cannot be null");
        Objects.requireNonNull(host, "node cannot be null");
        Objects.requireNonNull(response, "response cannot be null");
        this.requestLine = requestLine;
        this.host = host;
        this.response = response;
    }

    /**
     * Returns the request line that generated this response
     */
    public RequestLine requestLine() {
        return requestLine;
    }

    public boolean isSuccess() {
        return success;
    }

    public void success(boolean success1) {
        this.success = success1;
    }

    public boolean isRetriable() {
        return retriable;
    }

    public void retriable(boolean retriable1) {
        this.retriable = retriable1;
    }

    /**
     * Returns the node that returned this response
     */
    public HttpHost host() {
        return host;
    }

    /**
     * Returns the status line of the current response
     */
    public StatusLine statusLine() {
        return response.getStatusLine();
    }

    /**
     * Returns all the response headers
     */
    public Header[] headers() {
        return response.getAllHeaders();
    }

    /**
     * Returns the value of the first header with a specified name of
     * this message. If there is more than one matching header in the
     * message the first element is returned. If there is no matching
     * header in the message <code>null</code> is returned.
     */
    public String getHeader(String name) {
        Header header = response.getFirstHeader(name);
        if (header == null) {
            return null;
        }
        return header.getValue();
    }

    /**
     * Returns the response body available, null otherwise
     * @see HttpEntity
     */
    public HttpEntity getEntity() {
        return response.getEntity();
    }

    public HttpResponse httpResponse() {
        return response;
    }

    @Override
    public String toString() {
        return "Response{" + "requestLine=" + requestLine + ", host=" + host
                + ", response=" + response.getStatusLine() + '}';
    }

}
