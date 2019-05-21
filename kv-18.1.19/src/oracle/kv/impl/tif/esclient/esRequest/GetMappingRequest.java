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

import org.apache.http.client.methods.HttpGet;

import oracle.kv.impl.tif.esclient.restClient.RestRequest;

public class GetMappingRequest extends ESRequest<GetMappingRequest>
        implements ESRestRequestGenerator {

    public GetMappingRequest() {

    }

    public GetMappingRequest(String index, String type) {
        super(index, type);
    }

    public InvalidRequestException validate() {
        InvalidRequestException exception = null;
        if (index == null || index.length() <= 0) {
            exception =
                new InvalidRequestException("index name is not provided");
        }
        if (type == null || type.length() <= 0) {
            exception =
                new InvalidRequestException("index type is not provided");
        }

        return exception;
    }

    @Override
    public RestRequest generateRestRequest() {
        if (validate() != null) {
            throw validate();
        }
        String method = HttpGet.METHOD_NAME;
        String endpoint = endpoint(
                                   index(), "_mapping", type());
        RequestParams parameters = new RequestParams();
        return new RestRequest(method, endpoint, null, parameters.getParams());
    }

    @Override
    public RequestType requestType() {
        return RequestType.GET_MAPPING;
    }

}
