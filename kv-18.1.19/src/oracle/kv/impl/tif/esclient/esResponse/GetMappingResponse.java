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

package oracle.kv.impl.tif.esclient.esResponse;

import java.io.IOException;
import java.nio.charset.Charset;

import oracle.kv.impl.tif.esclient.jsonContent.ESJsonUtil;
import oracle.kv.impl.tif.esclient.restClient.JsonResponseObjectMapper;
import oracle.kv.impl.tif.esclient.restClient.RestResponse;
import oracle.kv.impl.tif.esclient.restClient.RestStatus;

import com.fasterxml.jackson.core.JsonParser;

import org.apache.http.HttpEntity;
import org.apache.http.entity.BufferedHttpEntity;
import org.apache.http.util.EntityUtils;

public class GetMappingResponse extends ESResponse
        implements JsonResponseObjectMapper<GetMappingResponse> {

    private String mapping;
    private boolean found = false;

    public boolean isFound() {
        return found;
    }

    public GetMappingResponse found(boolean exists) {
        this.found = exists;
        return this;
    }

    public GetMappingResponse() {

    }

    public String mapping() {
        try {
            if (ESJsonUtil.isEmptyJsonStr(mapping)) {
                return null;
            }
        } catch (Exception e) {
            return null;
        }
        return mapping;
    }

    public GetMappingResponse mapping(String mappingSpec) {
        this.mapping = mappingSpec;
        return this;
    }

    @Override
    public GetMappingResponse buildFromJson(JsonParser parser)
        throws IOException {
        return null;
    }

    @Override
    public GetMappingResponse buildErrorReponse(ESException e) {
        return null;
    }

    /**
     * Get Mapping Response is built using the http response.
     */
    @Override
    public GetMappingResponse buildFromRestResponse(RestResponse restResp)
        throws IOException {
        statusCode(restResp.statusLine().getStatusCode());
        if (statusCode() == RestStatus.OK.getStatus()) {
            found = true;
        }
        HttpEntity entity = restResp.getEntity();

        if (entity != null) {
            if (entity.isRepeatable() == false) {
                entity = new BufferedHttpEntity(entity);
                restResp.httpResponse().setEntity(entity);
            }
            this.mapping =
                EntityUtils.toString(entity, Charset.forName("UTF-8"));
        }
        parsed(true);
        return this;
    }

    @Override
    public String toString() {
        return "GetIndexResponse:[" + "statusCode:" + statusCode() +
                "  Mapping:" + mapping() + " ]";
    }

}
