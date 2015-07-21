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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

import java.io.IOException;

import static com.facebook.presto.metadata.OperatorType.EQUAL;
import static com.facebook.presto.metadata.OperatorType.HASH_CODE;
import static com.facebook.presto.metadata.OperatorType.NOT_EQUAL;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.type.StandardTypes.BIGINT;
import static com.facebook.presto.spi.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.spi.type.StandardTypes.DOUBLE;
import static com.facebook.presto.spi.type.StandardTypes.JSON;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS;
import static java.lang.String.format;

public final class JsonOperators
{
    private static final ObjectMapper SORTED_MAPPER = new ObjectMapperProvider().get().configure(ORDER_MAP_ENTRIES_BY_KEYS, true);
    private static final JsonFactory JSON_FACTORY = new JsonFactory().disable(CANONICALIZE_FIELD_NAMES);

    private JsonOperators()
    {
    }

    @ScalarOperator(OperatorType.CAST)
    @SqlType(StandardTypes.VARCHAR)
    public static Slice castToVarchar(@SqlType(JSON) Slice slice)
    {
        return slice;
    }

    @ScalarOperator(OperatorType.CAST)
    @SqlType(BIGINT)
    public static long castToBigint(@SqlType(JSON) Slice json)
    {
        try {
            try (JsonParser parser = JSON_FACTORY.createJsonParser(json.getInput())) {
                parser.nextToken();
                long value = parser.getLongValue();
                checkCondition(parser.nextToken() == null, INVALID_CAST_ARGUMENT, "Cannot cast input json to %s", BIGINT);
                return value;
            }
        }
        catch (Exception e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", json.toStringUtf8(), BIGINT));
        }
    }

    @ScalarOperator(OperatorType.CAST)
    @SqlType(DOUBLE)
    public static double castToDouble(@SqlType(JSON) Slice json)
    {
        try {
            try (JsonParser parser = JSON_FACTORY.createJsonParser(json.getInput())) {
                parser.nextToken();
                double value = parser.getDoubleValue();
                checkCondition(parser.nextToken() == null, INVALID_CAST_ARGUMENT, "Cannot cast input json to %s", DOUBLE);
                return value;
            }
        }
        catch (Exception e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", json.toStringUtf8(), DOUBLE));
        }
    }

    @ScalarOperator(OperatorType.CAST)
    @SqlType(BOOLEAN)
    public static boolean castToBoolean(@SqlType(JSON) Slice json)
    {
        try {
            try (JsonParser parser = JSON_FACTORY.createJsonParser(json.getInput())) {
                parser.nextToken();
                boolean value = parser.getBooleanValue();
                checkCondition(parser.nextToken() == null, INVALID_CAST_ARGUMENT, "Cannot cast input json to %s", BOOLEAN);
                return value;
            }
        }
        catch (Exception e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", json.toStringUtf8(), BOOLEAN));
        }
    }

    @ScalarOperator(OperatorType.CAST)
    @SqlType(JSON)
    public static Slice castVarcharToJson(@SqlType(StandardTypes.VARCHAR) Slice slice) throws IOException
    {
        try {
            byte[] in = slice.getBytes();
            SliceOutput dynamicSliceOutput = new DynamicSliceOutput(in.length);
            SORTED_MAPPER.writeValue(dynamicSliceOutput, SORTED_MAPPER.readValue(in, Object.class));
            return dynamicSliceOutput.slice();
        }
        catch (Exception e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to JSON", slice.toStringUtf8()));
        }
    }

    @ScalarOperator(OperatorType.CAST)
    @SqlType(JSON)
    public static Slice castLongToJson(@SqlType(BIGINT) long value) throws IOException
    {
        try {
            SliceOutput dynamicSliceOutput = new DynamicSliceOutput(8);
            try (JsonGenerator jsonGenerator = JSON_FACTORY.createJsonGenerator(dynamicSliceOutput)) {
                jsonGenerator.writeNumber(value);
            }
            return dynamicSliceOutput.slice();
        }
        catch (Exception e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%d' to %s", value, JSON));
        }
    }

    @ScalarOperator(OperatorType.CAST)
    @SqlType(JSON)
    public static Slice castDoubleToJson(@SqlType(DOUBLE) double value) throws IOException
    {
        try {
            SliceOutput dynamicSliceOutput = new DynamicSliceOutput(8);
            try (JsonGenerator jsonGenerator = JSON_FACTORY.createJsonGenerator(dynamicSliceOutput)) {
                jsonGenerator.writeNumber(value);
            }
            return dynamicSliceOutput.slice();
        }
        catch (Exception e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%d' to %s", value, JSON));
        }
    }

    @ScalarOperator(OperatorType.CAST)
    @SqlType(JSON)
    public static Slice castBooleanToJson(@SqlType(BOOLEAN) boolean value) throws IOException
    {
        try {
            SliceOutput dynamicSliceOutput = new DynamicSliceOutput(8);
            try (JsonGenerator jsonGenerator = JSON_FACTORY.createJsonGenerator(dynamicSliceOutput)) {
                jsonGenerator.writeBoolean(value);
            }
            return dynamicSliceOutput.slice();
        }
        catch (Exception e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%d' to %s", value, JSON));
        }
    }

    @ScalarOperator(HASH_CODE)
    @SqlType(BIGINT)
    public static long hashCode(@SqlType(JSON) Slice value)
    {
        return value.hashCode();
    }

    @ScalarOperator(EQUAL)
    @SqlType(BOOLEAN)
    public static boolean equals(@SqlType(JSON) Slice leftJson, @SqlType(JSON) Slice rightJson)
    {
        return leftJson.equals(rightJson);
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(BOOLEAN)
    public static boolean notEqual(@SqlType(JSON) Slice leftJson, @SqlType(JSON) Slice rightJson)
    {
        return !leftJson.equals(rightJson);
    }
}
