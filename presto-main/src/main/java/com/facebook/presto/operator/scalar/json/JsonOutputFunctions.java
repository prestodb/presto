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
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.util.ByteArrayBuilder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.HIDDEN;
import static com.facebook.presto.sql.tree.JsonQuery.EmptyOrErrorBehavior.EMPTY_ARRAY;
import static com.facebook.presto.sql.tree.JsonQuery.EmptyOrErrorBehavior.EMPTY_OBJECT;
import static com.facebook.presto.sql.tree.JsonQuery.EmptyOrErrorBehavior.ERROR;
import static com.facebook.presto.sql.tree.JsonQuery.EmptyOrErrorBehavior.NULL;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.util.Objects.requireNonNull;

/**
 * Format JSON as binary or character string, using given encoding.
 * <p>
 * These functions are used to format the output of JSON_QUERY function.
 * In case of error during JSON formatting, the error handling
 * strategy of the enclosing JSON_QUERY function is applied.
 * <p>
 * Additionally, the options KEEP / OMIT QUOTES [ON SCALAR STRING]
 * are respected when formatting the output.
 */
public final class JsonOutputFunctions
{
    public static final String JSON_TO_VARCHAR = "$json_to_varchar";
    public static final String JSON_TO_VARBINARY = "$json_to_varbinary";
    public static final String JSON_TO_VARBINARY_UTF8 = "$json_to_varbinary_utf8";
    public static final String JSON_TO_VARBINARY_UTF16 = "$json_to_varbinary_utf16";
    public static final String JSON_TO_VARBINARY_UTF32 = "$json_to_varbinary_utf32";

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final EncodingSpecificConstants UTF_8 = new EncodingSpecificConstants(
            JsonEncoding.UTF8,
            StandardCharsets.UTF_8,
            Slices.copiedBuffer(new ArrayNode(JsonNodeFactory.instance).asText(), StandardCharsets.UTF_8),
            Slices.copiedBuffer(new ObjectNode(JsonNodeFactory.instance).asText(), StandardCharsets.UTF_8));
    private static final EncodingSpecificConstants UTF_16 = new EncodingSpecificConstants(
            JsonEncoding.UTF16_LE,
            StandardCharsets.UTF_16LE,
            Slices.copiedBuffer(new ArrayNode(JsonNodeFactory.instance).asText(), StandardCharsets.UTF_16LE),
            Slices.copiedBuffer(new ObjectNode(JsonNodeFactory.instance).asText(), StandardCharsets.UTF_16LE));
    private static final EncodingSpecificConstants UTF_32 = new EncodingSpecificConstants(
            JsonEncoding.UTF32_LE,
            Charset.forName("UTF-32LE"),
            Slices.copiedBuffer(new ArrayNode(JsonNodeFactory.instance).asText(), Charset.forName("UTF-32LE")),
            Slices.copiedBuffer(new ObjectNode(JsonNodeFactory.instance).asText(), Charset.forName("UTF-32LE")));

    private JsonOutputFunctions() {}

    @SqlNullable
    @ScalarFunction(value = JSON_TO_VARCHAR, visibility = HIDDEN)
    @SqlType(StandardTypes.VARCHAR)
    public static Slice jsonToVarchar(@SqlType(StandardTypes.JSON_2016) JsonNode jsonExpression, @SqlType(StandardTypes.TINYINT) long errorBehavior, @SqlType(StandardTypes.BOOLEAN) boolean omitQuotes)
    {
        return serialize(jsonExpression, UTF_8, errorBehavior, omitQuotes);
    }

    @SqlNullable
    @ScalarFunction(value = JSON_TO_VARBINARY, visibility = HIDDEN)
    @SqlType(StandardTypes.VARBINARY)
    public static Slice jsonToVarbinary(@SqlType(StandardTypes.JSON_2016) JsonNode jsonExpression, @SqlType(StandardTypes.TINYINT) long errorBehavior, @SqlType(StandardTypes.BOOLEAN) boolean omitQuotes)
    {
        return jsonToVarbinaryUtf8(jsonExpression, errorBehavior, omitQuotes);
    }

    @SqlNullable
    @ScalarFunction(value = JSON_TO_VARBINARY_UTF8, visibility = HIDDEN)
    @SqlType(StandardTypes.VARBINARY)
    public static Slice jsonToVarbinaryUtf8(@SqlType(StandardTypes.JSON_2016) JsonNode jsonExpression, @SqlType(StandardTypes.TINYINT) long errorBehavior, @SqlType(StandardTypes.BOOLEAN) boolean omitQuotes)
    {
        return serialize(jsonExpression, UTF_8, errorBehavior, omitQuotes);
    }

    @SqlNullable
    @ScalarFunction(value = JSON_TO_VARBINARY_UTF16, visibility = HIDDEN)
    @SqlType(StandardTypes.VARBINARY)
    public static Slice jsonToVarbinaryUtf16(@SqlType(StandardTypes.JSON_2016) JsonNode jsonExpression, @SqlType(StandardTypes.TINYINT) long errorBehavior, @SqlType(StandardTypes.BOOLEAN) boolean omitQuotes)
    {
        return serialize(jsonExpression, UTF_16, errorBehavior, omitQuotes);
    }

    @SqlNullable
    @ScalarFunction(value = JSON_TO_VARBINARY_UTF32, visibility = HIDDEN)
    @SqlType(StandardTypes.VARBINARY)
    public static Slice jsonToVarbinaryUtf32(@SqlType(StandardTypes.JSON_2016) JsonNode jsonExpression, @SqlType(StandardTypes.TINYINT) long errorBehavior, @SqlType(StandardTypes.BOOLEAN) boolean omitQuotes)
    {
        return serialize(jsonExpression, UTF_32, errorBehavior, omitQuotes);
    }

    private static Slice serialize(JsonNode json, EncodingSpecificConstants constants, long errorBehavior, boolean omitQuotes)
    {
        if (omitQuotes && json.isTextual()) {
            return Slices.copiedBuffer(json.asText(), constants.charset);
        }

        ByteArrayBuilder builder = new ByteArrayBuilder();
        JsonFactory jsonFactory = new JsonFactory();

        try (JsonGenerator generator = jsonFactory.createGenerator(builder, constants.jsonEncoding)) {
            MAPPER.writeTree(generator, json);
        }
        catch (JsonProcessingException e) {
            if (errorBehavior == NULL.ordinal()) {
                return null;
            }
            if (errorBehavior == ERROR.ordinal()) {
                throw new JsonOutputConversionError(e);
            }
            if (errorBehavior == EMPTY_ARRAY.ordinal()) {
                return constants.emptyArray;
            }
            if (errorBehavior == EMPTY_OBJECT.ordinal()) {
                return constants.emptyObject;
            }
            throw new IllegalStateException("unexpected behavior");
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
        return wrappedBuffer(builder.toByteArray());
    }

    private static class EncodingSpecificConstants
    {
        private final JsonEncoding jsonEncoding;
        private final Charset charset;
        private final Slice emptyArray;
        private final Slice emptyObject;

        public EncodingSpecificConstants(JsonEncoding jsonEncoding, Charset charset, Slice emptyArray, Slice emptyObject)
        {
            this.jsonEncoding = requireNonNull(jsonEncoding, "jsonEncoding is null");
            this.charset = requireNonNull(charset, "charset is null");
            this.emptyArray = requireNonNull(emptyArray, "emptyArray is null");
            this.emptyObject = requireNonNull(emptyObject, "emptyObject is null");
        }
    }
}
