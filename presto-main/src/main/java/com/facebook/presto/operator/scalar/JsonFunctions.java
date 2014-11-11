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

import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.JsonPathType;
import com.facebook.presto.type.SqlType;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.type.ArrayType;

import com.google.common.base.Charsets;
import com.google.common.primitives.Doubles;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static com.fasterxml.jackson.core.JsonParser.NumberType;
import static com.fasterxml.jackson.core.JsonToken.END_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.VALUE_FALSE;
import static com.fasterxml.jackson.core.JsonToken.VALUE_NUMBER_FLOAT;
import static com.fasterxml.jackson.core.JsonToken.VALUE_NUMBER_INT;
import static com.fasterxml.jackson.core.JsonToken.VALUE_STRING;
import static com.fasterxml.jackson.core.JsonToken.VALUE_TRUE;
import static io.airlift.slice.Slices.utf8Slice;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class JsonFunctions
{
    private static final JsonFactory JSON_FACTORY = new JsonFactory()
            .disable(CANONICALIZE_FIELD_NAMES);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();

    private static final ArrayType STRING_ARRAY_TYPE = OBJECT_MAPPER.getTypeFactory().constructArrayType(String.class);

    private JsonFunctions() {}

    @ScalarOperator(OperatorType.CAST)
    @SqlType(JsonPathType.NAME)
    public static JsonPath castToJsonPath(@SqlType(StandardTypes.VARCHAR) Slice pattern)
    {
        return new JsonPath(pattern.toString(UTF_8));
    }

    @ScalarOperator(OperatorType.CAST)
    @SqlType(StandardTypes.JSON)
    public static Slice castToJson(@SqlType(StandardTypes.VARCHAR) Slice slice)
    {
        return slice;
    }

    @ScalarOperator(OperatorType.CAST)
    @SqlType(StandardTypes.VARCHAR)
    public static Slice castToVarchar(@SqlType(StandardTypes.JSON) Slice slice)
    {
        return slice;
    }

    @Nullable
    @ScalarFunction("json_array_length")
    @SqlType(StandardTypes.BIGINT)
    public static Long varcharJsonArrayLength(@SqlType(StandardTypes.VARCHAR) Slice json)
    {
        return jsonArrayLength(json);
    }

    @Nullable
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static Long jsonArrayLength(@SqlType(StandardTypes.JSON) Slice json)
    {
        try (JsonParser parser = JSON_FACTORY.createJsonParser(json.getInput())) {
            if (parser.nextToken() != START_ARRAY) {
                return null;
            }

            long length = 0;
            while (true) {
                JsonToken token = parser.nextToken();
                if (token == null) {
                    return null;
                }
                if (token == END_ARRAY) {
                    return length;
                }
                parser.skipChildren();

                length++;
            }
        }
        catch (IOException e) {
            return null;
        }
    }

    @Nullable
    @ScalarFunction("json_array_contains")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean varcharJsonArrayContains(@SqlType(StandardTypes.VARCHAR) Slice json, @SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        return jsonArrayContains(json, value);
    }

    @Nullable
    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean jsonArrayContains(@SqlType(StandardTypes.JSON) Slice json, @SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        try (JsonParser parser = JSON_FACTORY.createJsonParser(json.getInput())) {
            if (parser.nextToken() != START_ARRAY) {
                return null;
            }

            while (true) {
                JsonToken token = parser.nextToken();
                if (token == null) {
                    return null;
                }
                if (token == END_ARRAY) {
                    return false;
                }
                parser.skipChildren();

                if (((token == VALUE_TRUE) && value) ||
                        ((token == VALUE_FALSE) && (!value))) {
                    return true;
                }
            }
        }
        catch (IOException e) {
            return null;
        }
    }

    @Nullable
    @ScalarFunction("json_array_contains")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean varcharJsonArrayContains(@SqlType(StandardTypes.VARCHAR) Slice json, @SqlType(StandardTypes.BIGINT) long value)
    {
        return jsonArrayContains(json, value);
    }

    @Nullable
    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean jsonArrayContains(@SqlType(StandardTypes.JSON) Slice json, @SqlType(StandardTypes.BIGINT) long value)
    {
        try (JsonParser parser = JSON_FACTORY.createJsonParser(json.getInput())) {
            if (parser.nextToken() != START_ARRAY) {
                return null;
            }

            while (true) {
                JsonToken token = parser.nextToken();
                if (token == null) {
                    return null;
                }
                if (token == END_ARRAY) {
                    return false;
                }
                parser.skipChildren();

                if ((token == VALUE_NUMBER_INT) &&
                        ((parser.getNumberType() == NumberType.INT) || (parser.getNumberType() == NumberType.LONG)) &&
                        (parser.getLongValue() == value)) {
                    return true;
                }
            }
        }
        catch (IOException e) {
            return null;
        }
    }

    @Nullable
    @ScalarFunction("json_array_contains")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean varcharJsonArrayContains(@SqlType(StandardTypes.VARCHAR) Slice json, @SqlType(StandardTypes.DOUBLE) double value)
    {
        return jsonArrayContains(json, value);
    }

    @Nullable
    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean jsonArrayContains(@SqlType(StandardTypes.JSON) Slice json, @SqlType(StandardTypes.DOUBLE) double value)
    {
        if (!Doubles.isFinite(value)) {
            return false;
        }

        try (JsonParser parser = JSON_FACTORY.createJsonParser(json.getInput())) {
            if (parser.nextToken() != START_ARRAY) {
                return null;
            }

            while (true) {
                JsonToken token = parser.nextToken();
                if (token == null) {
                    return null;
                }
                if (token == END_ARRAY) {
                    return false;
                }
                parser.skipChildren();

                // noinspection FloatingPointEquality
                if ((token == VALUE_NUMBER_FLOAT) && (parser.getDoubleValue() == value) &&
                        (Doubles.isFinite(parser.getDoubleValue()))) {
                    return true;
                }
            }
        }
        catch (IOException e) {
            return null;
        }
    }

    @Nullable
    @ScalarFunction("json_array_contains")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean varcharJsonArrayContains(@SqlType(StandardTypes.VARCHAR) Slice json, @SqlType(StandardTypes.VARCHAR) Slice value)
    {
        return jsonArrayContains(json, value);
    }

    @Nullable
    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean jsonArrayContains(@SqlType(StandardTypes.JSON) Slice json, @SqlType(StandardTypes.VARCHAR) Slice value)
    {
        String valueString = value.toString(Charsets.UTF_8);

        try (JsonParser parser = JSON_FACTORY.createJsonParser(json.getInput())) {
            if (parser.nextToken() != START_ARRAY) {
                return null;
            }

            while (true) {
                JsonToken token = parser.nextToken();
                if (token == null) {
                    return null;
                }
                if (token == END_ARRAY) {
                    return false;
                }
                parser.skipChildren();

                if (token == VALUE_STRING && valueString.equals(parser.getValueAsString())) {
                    return true;
                }
            }
        }
        catch (IOException e) {
            return null;
        }
    }

    @Nullable
    @ScalarFunction("json_array_get")
    @SqlType(StandardTypes.JSON)
    public static Slice varcharJsonArrayGet(@SqlType(StandardTypes.VARCHAR) Slice json, @SqlType(StandardTypes.BIGINT) long index)
    {
        return jsonArrayGet(json, index);
    }

    @Nullable
    @ScalarFunction
    @SqlType(StandardTypes.JSON)
    public static Slice jsonArrayGet(@SqlType(StandardTypes.JSON) Slice json, @SqlType(StandardTypes.BIGINT) long index)
    {
        try (JsonParser parser = JSON_FACTORY.createJsonParser(json.getInput())) {
            if (parser.nextToken() != START_ARRAY) {
                return null;
            }

            List<String> tokens = null;
            if (index < 0) {
                tokens = new LinkedList<>();
            }

            long count = 0;
            while (true) {
                JsonToken token = parser.nextToken();
                if (token == null) {
                    return null;
                }
                if (token == END_ARRAY) {
                    if (tokens != null && count >= index * -1) {
                        return utf8Slice(tokens.get(0));
                    }

                    return null;
                }
                parser.skipChildren();

                if (count == index) {
                    if (parser.getValueAsString() == null) {
                        return null;
                    }
                    return utf8Slice(parser.getValueAsString());
                }

                if (tokens != null) {
                    tokens.add(parser.getValueAsString());

                    if (count >= index * -1) {
                        tokens.remove(0);
                    }
                }

                count++;
            }
        }
        catch (IOException e) {
            return null;
        }
    }

    @ScalarFunction("json_extract_scalar")
    @Nullable
    @SqlType(StandardTypes.VARCHAR)
    public static Slice varcharJsonExtractScalar(@SqlType(StandardTypes.VARCHAR) Slice json, @SqlType(JsonPathType.NAME) JsonPath jsonPath)
    {
        return JsonExtract.extract(json, jsonPath.getScalarExtractor());
    }

    @ScalarFunction
    @Nullable
    @SqlType(StandardTypes.VARCHAR)
    public static Slice jsonExtractScalar(@SqlType(StandardTypes.JSON) Slice json, @SqlType(JsonPathType.NAME) JsonPath jsonPath)
    {
        return JsonExtract.extract(json, jsonPath.getScalarExtractor());
    }

    @ScalarFunction("json_extract")
    @Nullable
    @SqlType(StandardTypes.JSON)
    public static Slice varcharJsonExtract(@SqlType(StandardTypes.VARCHAR) Slice json, @SqlType(JsonPathType.NAME) JsonPath jsonPath)
    {
        return JsonExtract.extract(json, jsonPath.getObjectExtractor());
    }

    @ScalarFunction
    @Nullable
    @SqlType(StandardTypes.JSON)
    public static Slice jsonExtract(@SqlType(StandardTypes.JSON) Slice json, @SqlType(JsonPathType.NAME) JsonPath jsonPath)
    {
        return JsonExtract.extract(json, jsonPath.getObjectExtractor());
    }

    @ScalarFunction("json_size")
    @Nullable
    @SqlType(StandardTypes.BIGINT)
    public static Long varcharJsonSize(@SqlType(StandardTypes.VARCHAR) Slice json, @SqlType(JsonPathType.NAME) JsonPath jsonPath)
    {
        return JsonExtract.extract(json, jsonPath.getSizeExtractor());
    }

    @ScalarFunction
    @Nullable
    @SqlType(StandardTypes.BIGINT)
    public static Long jsonSize(@SqlType(StandardTypes.JSON) Slice json, @SqlType(JsonPathType.NAME) JsonPath jsonPath)
    {
        return JsonExtract.extract(json, jsonPath.getSizeExtractor());
    }

    /**
     * Until Presto supports table generating functions, this is a workaround to support extracting multiple fields from a json string (similar to Hive's json_tuple)
     */
    @ScalarFunction
    @Nullable
    @SqlType(StandardTypes.VARCHAR)
    public static Slice jsonExtractMultiple(@SqlType(StandardTypes.VARCHAR) Slice json, @SqlType("array<varchar>") Slice keys)
    {
        if (json == null || keys == null) {
            return null;
        }

        try {
            String[] keys2Extract = OBJECT_MAPPER.readValue(keys.getInput(), STRING_ARRAY_TYPE);
            if (keys2Extract.length == 0) {
                return null;
            }

            ObjectNode newNode = OBJECT_MAPPER.createObjectNode();
            JsonNode rootNode = OBJECT_MAPPER.readTree(json.getInput());
            for (String key : keys2Extract) {
                    JsonNode node = rootNode.get(key);
                    if (node != null) {
                        newNode.set(key, node);
                    }
            }

            if (newNode.size() == 0) {
                return null;
            }
            else {
                return Slices.utf8Slice(newNode.toString());
            }
        }
        catch (IOException e) {
            return null;
        }
    }
}
