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

import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.SqlDecimal;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.LiteralParameter;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.type.JsonPathType;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Doubles;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;

import static com.facebook.presto.common.type.Chars.padSpaces;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.util.JsonUtil.createJsonParser;
import static com.facebook.presto.util.JsonUtil.truncateIfNecessaryForErrorMessage;
import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static com.fasterxml.jackson.core.JsonParser.NumberType;
import static com.fasterxml.jackson.core.JsonToken.END_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.START_OBJECT;
import static com.fasterxml.jackson.core.JsonToken.VALUE_FALSE;
import static com.fasterxml.jackson.core.JsonToken.VALUE_NUMBER_FLOAT;
import static com.fasterxml.jackson.core.JsonToken.VALUE_NUMBER_INT;
import static com.fasterxml.jackson.core.JsonToken.VALUE_STRING;
import static com.fasterxml.jackson.core.JsonToken.VALUE_TRUE;
import static com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;

public final class JsonFunctions
{
    private static final JsonFactory JSON_FACTORY = new JsonFactory()
            .disable(CANONICALIZE_FIELD_NAMES);

    private static final JsonFactory MAPPING_JSON_FACTORY = new MappingJsonFactory()
            .disable(CANONICALIZE_FIELD_NAMES);

    private static final ObjectMapper SORTED_MAPPER = new JsonObjectMapperProvider().get().configure(ORDER_MAP_ENTRIES_BY_KEYS, true);

    private JsonFunctions() {}

    @ScalarOperator(OperatorType.CAST)
    @SqlType(JsonPathType.NAME)
    @LiteralParameters("x")
    public static JsonPath castVarcharToJsonPath(@SqlType("varchar(x)") Slice pattern)
    {
        return JsonPath.build(pattern.toStringUtf8());
    }

    @ScalarOperator(OperatorType.CAST)
    @LiteralParameters("x")
    @SqlType(JsonPathType.NAME)
    public static JsonPath castCharToJsonPath(@LiteralParameter("x") Long charLength, @SqlType("char(x)") Slice pattern)
    {
        return JsonPath.build(padSpaces(pattern, charLength.intValue()).toStringUtf8());
    }

    @ScalarFunction("is_json_scalar")
    @LiteralParameters("x")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean varcharIsJsonScalar(@SqlType("varchar(x)") Slice json)
    {
        return isJsonScalar(json);
    }

    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isJsonScalar(@SqlType(StandardTypes.JSON) Slice json)
    {
        try (JsonParser parser = createJsonParser(JSON_FACTORY, json)) {
            JsonToken nextToken = parser.nextToken();
            if (nextToken == null) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid JSON value: " + truncateIfNecessaryForErrorMessage(json));
            }

            if (nextToken == START_ARRAY || nextToken == START_OBJECT) {
                parser.skipChildren();
                if (parser.nextToken() != null) {
                    // extra trailing token after json array/object
                    throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid JSON value: " + truncateIfNecessaryForErrorMessage(json));
                }
                return false;
            }

            if (parser.nextToken() != null) {
                // extra trailing token after json scalar
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid JSON value: " + truncateIfNecessaryForErrorMessage(json));
            }
            return true;
        }
        catch (IOException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid JSON value: " + truncateIfNecessaryForErrorMessage(json));
        }
    }

    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice jsonFormat(@SqlType(StandardTypes.JSON) Slice slice)
    {
        return slice;
    }

    @ScalarFunction
    @LiteralParameters("x")
    @SqlType(StandardTypes.JSON)
    public static Slice jsonParse(@SqlType("varchar(x)") Slice slice)
    {
        // cast(json_parse(x) AS t)` will be optimized into `$internal$json_string_to_array/map/row_cast` in ExpressionOptimizer
        // If you make changes to this function (e.g. use parse JSON string into some internal representation),
        // make sure `$internal$json_string_to_array/map/row_cast` is changed accordingly.
        try (JsonParser parser = createJsonParser(JSON_FACTORY, slice)) {
            SliceOutput dynamicSliceOutput = new DynamicSliceOutput(slice.length());
            SORTED_MAPPER.writeValue((OutputStream) dynamicSliceOutput, SORTED_MAPPER.readValue(parser, Object.class));
            // nextToken() returns null if the input is parsed correctly,
            // but will throw an exception if there are trailing characters.
            parser.nextToken();
            return dynamicSliceOutput.slice();
        }
        catch (Exception e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Cannot convert '%s' to JSON", slice.toStringUtf8()));
        }
    }

    @SqlNullable
    @ScalarFunction("json_array_length")
    @LiteralParameters("x")
    @SqlType(StandardTypes.BIGINT)
    public static Long varcharJsonArrayLength(@SqlType("varchar(x)") Slice json)
    {
        return jsonArrayLength(json);
    }

    @SqlNullable
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static Long jsonArrayLength(@SqlType(StandardTypes.JSON) Slice json)
    {
        try (JsonParser parser = createJsonParser(JSON_FACTORY, json)) {
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

    @SqlNullable
    @ScalarFunction("json_array_contains")
    @LiteralParameters("x")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean varcharJsonArrayContains(@SqlType("varchar(x)") Slice json, @SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        return jsonArrayContains(json, value);
    }

    @SqlNullable
    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean jsonArrayContains(@SqlType(StandardTypes.JSON) Slice json, @SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        try (JsonParser parser = createJsonParser(JSON_FACTORY, json)) {
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

    @SqlNullable
    @ScalarFunction("json_array_contains")
    @LiteralParameters("x")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean varcharJsonArrayContains(@SqlType("varchar(x)") Slice json, @SqlType(StandardTypes.BIGINT) long value)
    {
        return jsonArrayContains(json, value);
    }

    @SqlNullable
    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean jsonArrayContains(@SqlType(StandardTypes.JSON) Slice json, @SqlType(StandardTypes.BIGINT) long value)
    {
        try (JsonParser parser = createJsonParser(JSON_FACTORY, json)) {
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

    @SqlNullable
    @ScalarFunction("json_array_contains")
    @LiteralParameters("x")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean varcharJsonArrayContains(@SqlType("varchar(x)") Slice json, @SqlType(StandardTypes.DOUBLE) double value)
    {
        return jsonArrayContains(json, value);
    }

    @SqlNullable
    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean jsonArrayContains(@SqlType(StandardTypes.JSON) Slice json, @SqlType(StandardTypes.DOUBLE) double value)
    {
        if (!Doubles.isFinite(value)) {
            return false;
        }

        try (JsonParser parser = createJsonParser(JSON_FACTORY, json)) {
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

    @SqlNullable
    @ScalarFunction("json_array_contains")
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean varcharJsonArrayContains(@SqlType("varchar(x)") Slice json, @SqlType("varchar(y)") Slice value)
    {
        return jsonArrayContains(json, value);
    }

    @SqlNullable
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean jsonArrayContains(@SqlType(StandardTypes.JSON) Slice json, @SqlType("varchar(x)") Slice value)
    {
        String valueString = value.toStringUtf8();

        try (JsonParser parser = createJsonParser(JSON_FACTORY, json)) {
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

    @SqlNullable
    @ScalarFunction("json_array_get")
    @LiteralParameters("x")
    @SqlType(StandardTypes.JSON)
    public static Slice varcharJsonArrayGet(@SqlType("varchar(x)") Slice json, @SqlType(StandardTypes.BIGINT) long index)
    {
        return jsonArrayGet(json, index);
    }

    @SqlNullable
    @ScalarFunction
    @SqlType(StandardTypes.JSON)
    public static Slice jsonArrayGet(@SqlType(StandardTypes.JSON) Slice json, @SqlType(StandardTypes.BIGINT) long index)
    {
        // this value cannot be converted to positive number
        if (index == Long.MIN_VALUE) {
            return null;
        }

        try (JsonParser parser = createJsonParser(MAPPING_JSON_FACTORY, json)) {
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

                String arrayElement;
                if (token == START_OBJECT || token == START_ARRAY) {
                    arrayElement = parser.readValueAsTree().toString();
                }
                else {
                    arrayElement = parser.getValueAsString();
                }

                if (count == index) {
                    return arrayElement == null ? null : utf8Slice(arrayElement);
                }

                if (tokens != null) {
                    tokens.add(arrayElement);

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
    @SqlNullable
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice varcharJsonExtractScalar(SqlFunctionProperties properties, @SqlType("varchar(x)") Slice json, @SqlType(JsonPathType.NAME) JsonPath jsonPath)
    {
        return JsonExtract.extract(json, jsonPath.getScalarExtractor(), properties);
    }

    @ScalarFunction
    @SqlNullable
    @SqlType(StandardTypes.VARCHAR)
    public static Slice jsonExtractScalar(SqlFunctionProperties properties, @SqlType(StandardTypes.JSON) Slice json, @SqlType(JsonPathType.NAME) JsonPath jsonPath)
    {
        return JsonExtract.extract(json, jsonPath.getScalarExtractor(), properties);
    }

    @ScalarFunction("json_extract")
    @LiteralParameters("x")
    @SqlNullable
    @SqlType(StandardTypes.JSON)
    public static Slice varcharJsonExtract(SqlFunctionProperties properties, @SqlType("varchar(x)") Slice json, @SqlType(JsonPathType.NAME) JsonPath jsonPath)
    {
        return JsonExtract.extract(json, jsonPath.getObjectExtractor(), properties);
    }

    @ScalarFunction
    @SqlNullable
    @SqlType(StandardTypes.JSON)
    public static Slice jsonExtract(SqlFunctionProperties properties, @SqlType(StandardTypes.JSON) Slice json, @SqlType(JsonPathType.NAME) JsonPath jsonPath)
    {
        return JsonExtract.extract(json, jsonPath.getObjectExtractor(), properties);
    }

    @ScalarFunction("json_size")
    @LiteralParameters("x")
    @SqlNullable
    @SqlType(StandardTypes.BIGINT)
    public static Long varcharJsonSize(SqlFunctionProperties properties, @SqlType("varchar(x)") Slice json, @SqlType(JsonPathType.NAME) JsonPath jsonPath)
    {
        return JsonExtract.extract(json, jsonPath.getSizeExtractor(), properties);
    }

    @ScalarFunction
    @SqlNullable
    @SqlType(StandardTypes.BIGINT)
    public static Long jsonSize(SqlFunctionProperties properties, @SqlType(StandardTypes.JSON) Slice json, @SqlType(JsonPathType.NAME) JsonPath jsonPath)
    {
        return JsonExtract.extract(json, jsonPath.getSizeExtractor(), properties);
    }

    public static Object getJsonObjectValue(Type valueType, SqlFunctionProperties properties, Block block, int position)
    {
        Object objectValue = valueType.getObjectValue(properties, block, position);
        if (objectValue instanceof SqlDecimal) {
            objectValue = ((SqlDecimal) objectValue).toBigDecimal();
        }
        return objectValue;
    }
}
