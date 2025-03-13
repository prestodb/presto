/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import io.airlift.slice.Slice;

import java.io.IOException;
import java.io.InputStream;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class JsonPath
{
    private final JsonExtract.JsonExtractor<Slice> scalarExtractor;
    private final JsonExtract.JsonExtractor<Slice> objectExtractor;
    private final JsonExtract.JsonExtractor<Long> sizeExtractor;
    private static final ObjectMapper mapper = new ObjectMapper();

    private static JsonExtract.JsonExtractor<Slice> getScalarExtractorForJayway(com.jayway.jsonpath.JsonPath jsonPath, Configuration jaywayConfig)
    {
        return new JsonExtract.JsonExtractor<Slice>()
        {
            @Override
            public Slice extract(InputStream inputStream)
                    throws IOException
            {
                JsonNode node = jaywayExtract(jsonPath, jaywayConfig, inputStream);
                if (node == null || !node.isValueNode()) {
                    return null;
                }
                return utf8Slice(node.asText());
            }
        };
    }

    private static JsonExtract.JsonExtractor<Slice> getObjectExtractorForJayway(com.jayway.jsonpath.JsonPath jsonPath, Configuration jaywayConfig)
    {
        return new JsonExtract.JsonExtractor<Slice>()
        {
            @Override
            public Slice extract(InputStream inputStream)
                    throws IOException
            {
                JsonNode node = jaywayExtract(jsonPath, jaywayConfig, inputStream);
                if (node == null) {
                    return null;
                }
                return utf8Slice(node.toString());
            }
        };
    }

    private static JsonExtract.JsonExtractor<Long> getSizeExtractorForJayway(com.jayway.jsonpath.JsonPath jsonPath, Configuration jaywayConfig)
    {
        return new JsonExtract.JsonExtractor<Long>()
        {
            @Override
            public Long extract(InputStream inputStream)
                    throws IOException
            {
                JsonNode node = jaywayExtract(jsonPath, jaywayConfig, inputStream);
                if (node == null) {
                    return null;
                }
                return (long) node.size(); // Jackson correctly returns 0 for scalar nodes
            }
        };
    }

    private static JsonNode jaywayExtract(com.jayway.jsonpath.JsonPath jsonPath, Configuration jaywayConfig, InputStream inputStream)
            throws IOException
    {
        try {
            Object res = jsonPath.read(inputStream, jaywayConfig);
            if (res instanceof JsonNode) {
                return (JsonNode) res;
            }
            else {
                // Jayway will respect Jackson mappings as provided in the configuration and return a JsonNode for simple cases.
                // But for JsonPath functions ($.avg, ...), it will return a Java boxed type (Double, String etc.) instead
                // of a properly formed JsonNode. This is why we need to re-create a JsonNode in that case
                return mapper.valueToTree(res);
            }
        }
        catch (InvalidJsonException | PathNotFoundException ex) {
            // replicate Presto's JsonPath behaviour: if the input JSON is invalid or no result is found,
            // then return NULL instead of throwing an exception
            return null;
        }
    }

    private static JsonPath buildPresto(String pattern)
    {
        return new JsonPath(JsonExtract.generateExtractor(pattern, new JsonExtract.ScalarValueJsonExtractor()),
                JsonExtract.generateExtractor(pattern, new JsonExtract.JsonValueJsonExtractor()),
                JsonExtract.generateExtractor(pattern, new JsonExtract.JsonSizeExtractor()));
    }

    private static JsonPath buildJayway(String pattern)
    {
        try {
            Configuration jaywayConfig = Configuration.builder().jsonProvider(new JacksonJsonNodeJsonProvider()).build();
            if (pattern == null || pattern.isEmpty()) {
                // for some reason, jayway throws IllegalArgumentException for an empty path, but an InvalidPathException for other invalid paths
                throw new InvalidPathException();
            }
            com.jayway.jsonpath.JsonPath jsonPath = com.jayway.jsonpath.JsonPath.compile(pattern);
            return new JsonPath(getScalarExtractorForJayway(jsonPath, jaywayConfig), getObjectExtractorForJayway(jsonPath, jaywayConfig), getSizeExtractorForJayway(jsonPath, jaywayConfig));
        }
        catch (InvalidPathException ex) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Invalid JSON path: '%s'", pattern));
        }
    }

    public static JsonPath build(String pattern)
    {
        try {
            return buildPresto(pattern);
        }
        catch (PrestoException ex) {
            if (ex.getErrorCode() == INVALID_FUNCTION_ARGUMENT.toErrorCode()) {
                return buildJayway(pattern);
            }
            throw ex;
        }
    }

    public JsonPath(JsonExtract.JsonExtractor<Slice> scalar, JsonExtract.JsonExtractor<Slice> object, JsonExtract.JsonExtractor<Long> size)
    {
        requireNonNull(scalar, "scalar extractor is null");
        requireNonNull(object, "object extractor is null");
        requireNonNull(size, "size extractor is null");
        scalarExtractor = scalar;
        objectExtractor = object;
        sizeExtractor = size;
    }

    public JsonExtract.JsonExtractor<Slice> getScalarExtractor()
    {
        return scalarExtractor;
    }

    public JsonExtract.JsonExtractor<Slice> getObjectExtractor()
    {
        return objectExtractor;
    }

    public JsonExtract.JsonExtractor<Long> getSizeExtractor()
    {
        return sizeExtractor;
    }
}
