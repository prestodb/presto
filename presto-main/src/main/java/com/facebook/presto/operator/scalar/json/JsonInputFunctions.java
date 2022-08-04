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
package com.facebook.presto.operator.scalar.json;

import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.slice.Slice;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;

import static com.facebook.presto.json.JsonInputErrorNode.JSON_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.HIDDEN;
import static java.nio.charset.StandardCharsets.UTF_16LE;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Read string input as JSON.
 * <p>
 * These functions are used by JSON_EXISTS, JSON_VALUE and JSON_QUERY functions
 * for parsing the JSON input arguments and applicable JSON path parameters.
 * <p>
 * If the error handling strategy of the enclosing JSON function is ERROR ON ERROR,
 * these input functions throw exception in case of parse error.
 * Otherwise, the parse error is suppressed, and a marker value JSON_ERROR
 * is returned, so that the enclosing function can handle the error accordingly
 * to its error handling strategy (e.g. return a default value).
 */
public final class JsonInputFunctions
{
    public static final String VARCHAR_TO_JSON = "$varchar_to_json";
    public static final String VARBINARY_TO_JSON = "$varbinary_to_json";
    public static final String VARBINARY_UTF8_TO_JSON = "$varbinary_utf8_to_json";
    public static final String VARBINARY_UTF16_TO_JSON = "$varbinary_utf16_to_json";
    public static final String VARBINARY_UTF32_TO_JSON = "$varbinary_utf32_to_json";

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Charset UTF_32LE = Charset.forName("UTF-32LE");

    private JsonInputFunctions() {}

    @ScalarFunction(value = VARCHAR_TO_JSON, visibility = HIDDEN)
    @SqlType(StandardTypes.JSON_2016)
    public static JsonNode varcharToJson(@SqlType(StandardTypes.VARCHAR) Slice inputExpression, @SqlType(StandardTypes.BOOLEAN) boolean failOnError)
    {
        Reader reader = new InputStreamReader(inputExpression.getInput(), UTF_8);
        return toJson(reader, failOnError);
    }

    @ScalarFunction(value = VARBINARY_TO_JSON, visibility = HIDDEN)
    @SqlType(StandardTypes.JSON_2016)
    public static JsonNode varbinaryToJson(@SqlType(StandardTypes.VARBINARY) Slice inputExpression, @SqlType(StandardTypes.BOOLEAN) boolean failOnError)
    {
        return varbinaryUtf8ToJson(inputExpression, failOnError);
    }

    @ScalarFunction(value = VARBINARY_UTF8_TO_JSON, visibility = HIDDEN)
    @SqlType(StandardTypes.JSON_2016)
    public static JsonNode varbinaryUtf8ToJson(@SqlType(StandardTypes.VARBINARY) Slice inputExpression, @SqlType(StandardTypes.BOOLEAN) boolean failOnError)
    {
        Reader reader = new InputStreamReader(inputExpression.getInput(), UTF_8);
        return toJson(reader, failOnError);
    }

    @ScalarFunction(value = VARBINARY_UTF16_TO_JSON, visibility = HIDDEN)
    @SqlType(StandardTypes.JSON_2016)
    public static JsonNode varbinaryUtf16ToJson(@SqlType(StandardTypes.VARBINARY) Slice inputExpression, @SqlType(StandardTypes.BOOLEAN) boolean failOnError)
    {
        Reader reader = new InputStreamReader(inputExpression.getInput(), UTF_16LE);
        return toJson(reader, failOnError);
    }

    @ScalarFunction(value = VARBINARY_UTF32_TO_JSON, visibility = HIDDEN)
    @SqlType(StandardTypes.JSON_2016)
    public static JsonNode varbinaryUtf32ToJson(@SqlType(StandardTypes.VARBINARY) Slice inputExpression, @SqlType(StandardTypes.BOOLEAN) boolean failOnError)
    {
        Reader reader = new InputStreamReader(inputExpression.getInput(), UTF_32LE);
        return toJson(reader, failOnError);
    }

    private static JsonNode toJson(Reader reader, boolean failOnError)
    {
        try {
            return MAPPER.readTree(reader);
        }
        catch (JsonProcessingException e) {
            if (failOnError) {
                throw new JsonInputConversionError(e);
            }
            return JSON_ERROR;
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
    }
}
