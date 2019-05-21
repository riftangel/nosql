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

import oracle.kv.impl.tif.esclient.jsonContent.ESJsonUtil;
import oracle.kv.impl.tif.esclient.restClient.JsonResponseObjectMapper;
import oracle.kv.impl.tif.esclient.restClient.RestResponse;
import oracle.kv.impl.tif.esclient.restClient.RestStatus;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class IndexDocumentResponse extends ESWriteResponse implements
        JsonResponseObjectMapper<IndexDocumentResponse> {

    private static final String CREATED = "created";

    private boolean created;

    public IndexDocumentResponse() {

    }

    public IndexDocumentResponse(String index,
            String type,
            String id,
            long seqNo,
            long version,
            boolean created,
            RestStatus restStatus) {
        super(index, type, id, seqNo, version, created ? Result.CREATED
                : Result.UPDATED, restStatus);
    }

    /**
     * Returns the current version of the doc indexed.
     */
    public IndexDocumentResponse created(boolean created1) {
        this.created = created1;
        result(Result.CREATED);
        return this;
    }

    /**
     * Returns true if the document was created, false if updated.
     */
    public boolean isCreated() {
        return created;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("IndexDocumentResponse[");
        builder.append("Successful=").append(isSuccessfulResponse());
        builder.append(",index=").append(index());
        builder.append(",type=").append(type());
        builder.append(",id=").append(id());
        builder.append(",version=").append(version());
        builder.append(",created=").append(isCreated());
        builder.append(",shards=").append(shardInfo());
        return builder.append("]").toString();
    }

    @Override
    public IndexDocumentResponse buildFromJson(JsonParser parser)
        throws IOException {
        JsonToken token = parser.nextToken();
        ESJsonUtil.validateToken(JsonToken.START_OBJECT, token, parser);

        while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
            String currentFieldName = parser.getCurrentName();

            if (CREATED.equals(currentFieldName)) {
                if (token.isScalarValue()) {
                    created(parser.getBooleanValue());
                }
            } else {
                ESWriteResponse.buildFromJson(parser, this);
            }
        }
        parsed(true);
        return this;
    }

    @Override
    public IndexDocumentResponse buildErrorReponse(ESException e) {
        return null;
    }

    @Override
    public IndexDocumentResponse buildFromRestResponse(RestResponse restResp) {
        switch (restResp.statusLine().getStatusCode()) {
            case 200:
            case 201:
            case 202:
                this.successfulResponse = true;
                break;
            default:
                this.successfulResponse = false;
        }

        return this;
    }

}
