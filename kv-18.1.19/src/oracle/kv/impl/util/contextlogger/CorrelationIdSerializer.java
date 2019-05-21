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

package oracle.kv.impl.util.contextlogger;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;

/**
 * The correlation Id is serialized into json as a simple string.
 */
public class CorrelationIdSerializer extends JsonSerializer<CorrelationId> {

    public CorrelationIdSerializer() {
        super();
    }

    @Override
    public void serialize
        (CorrelationId v, JsonGenerator g, SerializerProvider p)
        throws IOException, JsonProcessingException {

        g.writeString(v.toString());
    }
}
