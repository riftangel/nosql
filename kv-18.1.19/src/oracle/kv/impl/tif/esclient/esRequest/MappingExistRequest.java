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
import oracle.kv.impl.tif.esclient.restClient.utils.ESRestClientUtil;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;

public class MappingExistRequest extends ESRequest<MappingExistRequest>
        implements ESRestRequestGenerator {

    /*
     * ES Version is used just as a small optimization step here.
     * 
     * ES 5.0 onwards HEAD method is supported for ExistMapping Check. ES 2.X
     * needs a GET method. If no version is given 2.X GET will be used which
     * works for all versions.
     */
    private final String esVersion;

    public MappingExistRequest(String index, String type, String esVersion) {
        super(index, type);
        if (!ESRestClientUtil.isEmpty(
                                      esVersion)) {
            this.esVersion = esVersion;
        } else {
            this.esVersion = "2.0";
        }
    }

    public InvalidRequestException validate() {
        InvalidRequestException exception = null;
        if (index == null || index.length() <= 0) {
            exception =
                new InvalidRequestException("index name is not provided");
        }
        if (type == null || type.length() <= 0) {
            exception =
                new InvalidRequestException("type name is not provided");
        }

        return exception;
    }

    @Override
    public RestRequest generateRestRequest() {
        if (validate() != null) {
            throw validate();
        }
        String method = ((esVersion.compareTo
                                    ( "5") > 0)) ? HttpHead.METHOD_NAME
                                                 : HttpGet.METHOD_NAME;
        String endpoint = endpoint(
                                   index(), "_mapping", type());
        RequestParams parameters = new RequestParams();

        return new RestRequest(method, endpoint, null, parameters.getParams());
    }

    @Override
    public RequestType requestType() {
        return RequestType.MAPPING_EXIST;
    }

}
