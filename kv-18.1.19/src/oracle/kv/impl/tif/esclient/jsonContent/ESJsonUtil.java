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

package oracle.kv.impl.tif.esclient.jsonContent;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class ESJsonUtil {

    private static final JsonFactory jsonFactory;

    static {
        jsonFactory = new JsonFactory();
        jsonFactory.configure(JsonGenerator.Feature.QUOTE_FIELD_NAMES, true);
        jsonFactory.configure(JsonFactory.Feature.FAIL_ON_SYMBOL_HASH_OVERFLOW,
                              false);
        // Do not automatically close unclosed objects/arrays in
        // com.fasterxml.jackson.core.json.UTF8JsonGenerator#close() method
        jsonFactory.configure(JsonGenerator.Feature.AUTO_CLOSE_JSON_CONTENT,
                              false);
        jsonFactory.configure(JsonParser.Feature.STRICT_DUPLICATE_DETECTION,
                              true);
    }

    public static JsonFactory getJsonFactory() {
        return jsonFactory;
    }

    public static JsonGenerator createGenerator(OutputStream os)
        throws IOException {
        return jsonFactory.createGenerator(os, JsonEncoding.UTF8);
    }

    public static Map<String, Object> convertToMap(byte[] source)
        throws JsonParseException, IOException {
        return parseAsMap(createParser(source));

    }

    public static JsonGenerator map(Map<String, String> values)
        throws IOException {
        if (values == null) {
            return null;
        }

        JsonGenerator jsonGen = createGenerator(new ByteArrayOutputStream());

        jsonGen.writeStartObject();
        for (Map.Entry<String, String> value : values.entrySet()) {
            jsonGen.writeFieldName(value.getKey());
            jsonGen.writeString(value.getValue());
        }
        jsonGen.writeEndObject();
        jsonGen.flush();
        return jsonGen;
    }

    public static JsonParser createParser(InputStream in)
        throws JsonParseException, IOException {
        JsonParser parser = jsonFactory.createParser(in);
        return parser;
    }

    public static JsonParser createParser(byte[] b)
        throws JsonParseException, IOException {
        return jsonFactory.createParser(b);
    }

    public static JsonParser createParser(String jsonString)
        throws JsonParseException, IOException {
        JsonParser parser = jsonFactory.createParser(jsonString);
        return parser;
    }

    public static boolean isEmptyJsonStr(String jsonString)
        throws JsonParseException, IOException {
        if (jsonString == null || jsonString.length() == 0) {
            return true;
        }
        JsonParser parser = jsonFactory.createParser(jsonString);
        JsonToken token = parser.getCurrentToken();
        if (token == null) {
            token = parser.nextToken();
        }
        if (token == JsonToken.START_OBJECT &&
                parser.nextToken() == JsonToken.END_OBJECT) {
            return true;
        }
        return false;
    }

    public static void validateToken(
                                     JsonToken expectedToken,
                                     JsonToken token,
                                     JsonParser parser)
        throws JsonParseException {

        if (token != expectedToken) {
            String message =
                "Failed to parse object: expecting token of" +
                " type [%s] but found [%s]";
            throw new JsonParseException(parser,
                                         String.format(Locale.ROOT, message,
                                                       expectedToken, token));
        }

    }

    public static void validateFieldName(
                                         JsonParser parser,
                                         JsonToken token,
                                         String fieldName)
        throws IOException {
        validateToken(JsonToken.FIELD_NAME, token, parser);
        String currentName = parser.getCurrentName();
        if (currentName.equals(fieldName) == false) {
            String msg =
                "Parse error: expected field : [%s] actual field: [%s]";
            throw new JsonParseException(parser,
                                         String.format(Locale.ROOT, msg,
                                                       fieldName,
                                                       currentName));
        }
    }

    public static String toStringUTF8Bytes(byte[] source) {
        Charset utf8 = StandardCharsets.UTF_8;
        char[] cBuf = new char[source.length];
        ByteArrayInputStream bis = null;
        try {
            bis = new ByteArrayInputStream(source);
            final Reader reader = new InputStreamReader(bis, utf8);
            reader.read(cBuf, 0, source.length);
        } catch (IOException e) {
            return null;

        } finally {
            try {
                if (bis != null) {
                    bis.close();
                }
            } catch (IOException e) {
                // stream might be already closed.
            }
        }

        return new String(cBuf);
    }

    /**
     * Not for complex maps with array type values. Only parses
     * string,scalarValue as map OR string,Map<string,ScalarValue> OR any
     * nested maps of the same form. string,array<scalarValue> is not supported
     * by this method.
     */
    public static Map<String, Object> parseAsMap(JsonParser parser)
        throws IOException {
        Map<String, Object> map = new HashMap<String, Object>();
        JsonToken token = parser.currentToken();
        if (token == null) {
            token = parser.nextToken();
        }
        if (token == JsonToken.START_OBJECT) {
            token = parser.nextToken();
        }
        for (; token == JsonToken.FIELD_NAME; token = parser.nextToken()) {
            // Must point to field name
            String fieldName = parser.getCurrentName();
            Object value = null;
            // And then the value...
            token = parser.nextToken();
            if (token == JsonToken.START_OBJECT) {
                value = parseAsMap(parser);
            } else if (token == JsonToken.START_ARRAY) {
                List<Object> listVal = new ArrayList<Object>();
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    listVal.add(objectValue(parser));
                }
                value = listVal;
            } else {
                value = objectValue(parser);
            }
            map.put(fieldName, value);
        }

        return map;
    }

    public static Object objectValue(JsonParser parser) throws IOException {
        JsonToken currentToken = parser.getCurrentToken();
        if (currentToken == JsonToken.VALUE_STRING) {
            return parser.getText();
        } else if (currentToken == JsonToken.VALUE_NUMBER_INT ||
                currentToken == JsonToken.VALUE_NUMBER_FLOAT) {
            return parser.getNumberValue();
        } else if (currentToken == JsonToken.VALUE_TRUE) {
            return Boolean.TRUE;
        } else if (currentToken == JsonToken.VALUE_FALSE) {
            return Boolean.FALSE;
        } else if (currentToken == JsonToken.VALUE_NULL) {
            return null;
        } else {
            return parser.getText();
        }
    }

}
