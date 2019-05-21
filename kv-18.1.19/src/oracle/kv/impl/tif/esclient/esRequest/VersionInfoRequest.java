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

package oracle.kv.impl.tif.esclient.esRequest;

import oracle.kv.impl.tif.esclient.restClient.RestRequest;

import org.apache.http.client.methods.HttpGet;

public class VersionInfoRequest extends ESRequest<VersionInfoRequest>
        implements ESRestRequestGenerator {

    public VersionInfoRequest() {
        super();
    }

    @Override
    public RestRequest generateRestRequest() {
        String method = HttpGet.METHOD_NAME;
        String endpoint = endpoint();
        RequestParams parameters = new RequestParams();
        return new RestRequest(method, endpoint, null,
                               parameters.getParams());
    }

    @Override
    public RequestType requestType() {
        return RequestType.ES_VERSION;
    }
}