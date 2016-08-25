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
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.type.BigintOperators;
import com.facebook.presto.type.BooleanOperators;
import com.facebook.presto.type.DoubleOperators;
import com.facebook.presto.type.VarcharOperators;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import javax.annotation.Nullable;

import java.io.IOException;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.function.OperatorType.CAST;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.function.OperatorType.HASH_CODE;
import static com.facebook.presto.spi.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.spi.type.StandardTypes.BIGINT;
import static com.facebook.presto.spi.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.spi.type.StandardTypes.DOUBLE;
import static com.facebook.presto.spi.type.StandardTypes.INTEGER;
import static com.facebook.presto.spi.type.StandardTypes.JSON;
import static com.facebook.presto.spi.type.StandardTypes.VARCHAR;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.facebook.presto.util.JsonUtil.createJsonGenerator;
import static com.facebook.presto.util.JsonUtil.createJsonParser;
import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static java.lang.String.format;

public final class JsonOperators
{
    public static final JsonFactory JSON_FACTORY = new JsonFactory().disable(CANONICALIZE_FIELD_NAMES);

    private JsonOperators()
    {
    }

    @ScalarOperator(CAST)
    @Nullable
    @SqlType(VARCHAR)
    public static Slice castToVarchar(@SqlType(JSON) Slice json)
    {
        try (JsonParser parser = createJsonParser(JSON_FACTORY, json)) {
            JsonToken nextToken = parser.nextToken();
            Slice result;
            switch (nextToken) {
                case VALUE_NULL:
                    result = null;
                    break;
                case VALUE_STRING:
                    result = Slices.utf8Slice(parser.getText());
                    break;
                case VALUE_NUMBER_FLOAT:
                    // Avoidance of loss of precision does not seem to be possible here because of Jackson implementation.
                    result = DoubleOperators.castToVarchar(parser.getDoubleValue());
                    break;
                case VALUE_NUMBER_INT:
                    // An alternative is calling getLongValue and then BigintOperators.castToVarchar.
                    // It doesn't work as well because it can result in overflow and underflow exceptions for large integral numbers.
                    result = Slices.utf8Slice(parser.getText());
                    break;
                case VALUE_TRUE:
                    result = BooleanOperators.castToVarchar(true);
                    break;
                case VALUE_FALSE:
                    result = BooleanOperators.castToVarchar(false);
                    break;
                default:
                    throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", json.toStringUtf8(), VARCHAR));
            }
            checkCondition(parser.nextToken() == null, INVALID_CAST_ARGUMENT, "Cannot cast input json to VARCHAR"); // check no trailing token
            return result;
        }
        catch (IOException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", json.toStringUtf8(), VARCHAR));
        }
    }

    @ScalarOperator(CAST)
    @Nullable
    @SqlType(BIGINT)
    public static Long castToBigint(@SqlType(JSON) Slice json)
    {
        try (JsonParser parser = createJsonParser(JSON_FACTORY, json)) {
            parser.nextToken();
            Long result;
            switch (parser.getCurrentToken()) {
                case VALUE_NULL:
                    result = null;
                    break;
                case VALUE_STRING:
                    result = VarcharOperators.castToBigint(Slices.utf8Slice(parser.getText()));
                    break;
                case VALUE_NUMBER_FLOAT:
                    result = DoubleOperators.castToLong(parser.getDoubleValue());
                    break;
                case VALUE_NUMBER_INT:
                    result = parser.getLongValue();
                    break;
                case VALUE_TRUE:
                    result = BooleanOperators.castToBigint(true);
                    break;
                case VALUE_FALSE:
                    result = BooleanOperators.castToBigint(false);
                    break;
                default:
                    throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", json.toStringUtf8(), BIGINT));
            }
            checkCondition(parser.nextToken() == null, INVALID_CAST_ARGUMENT, "Cannot cast input json to BIGINT"); // check no trailing token
            return result;
        }
        catch (IOException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", json.toStringUtf8(), BIGINT));
        }
    }

    @ScalarOperator(CAST)
    @Nullable
    @SqlType(INTEGER)
    public static Long castToInteger(@SqlType(JSON) Slice json)
    {
        try (JsonParser parser = createJsonParser(JSON_FACTORY, json)) {
            parser.nextToken();
            Long result;
            switch (parser.getCurrentToken()) {
                case VALUE_NULL:
                    result = null;
                    break;
                case VALUE_STRING:
                    result = VarcharOperators.castToInteger(Slices.utf8Slice(parser.getText()));
                    break;
                case VALUE_NUMBER_FLOAT:
                    result = DoubleOperators.castToInteger(parser.getDoubleValue());
                    break;
                case VALUE_NUMBER_INT:
                    result = (long) Math.toIntExact(parser.getLongValue());
                    break;
                case VALUE_TRUE:
                    result = BooleanOperators.castToInteger(true);
                    break;
                case VALUE_FALSE:
                    result = BooleanOperators.castToInteger(false);
                    break;
                default:
                    throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", json.toStringUtf8(), INTEGER));
            }
            checkCondition(parser.nextToken() == null, INVALID_CAST_ARGUMENT, "Cannot cast input json to INTEGER"); // check no trailing token
            return result;
        }
        catch (ArithmeticException | IOException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", json.toStringUtf8(), INTEGER));
        }
    }

    @ScalarOperator(CAST)
    @Nullable
    @SqlType(DOUBLE)
    public static Double castToDouble(@SqlType(JSON) Slice json)
    {
        try (JsonParser parser = createJsonParser(JSON_FACTORY, json)) {
            parser.nextToken();
            Double result;
            switch (parser.getCurrentToken()) {
                case VALUE_NULL:
                    result = null;
                    break;
                case VALUE_STRING:
                    result = VarcharOperators.castToDouble(Slices.utf8Slice(parser.getText()));
                    break;
                case VALUE_NUMBER_FLOAT:
                    result = parser.getDoubleValue();
                    break;
                case VALUE_NUMBER_INT:
                    // An alternative is calling getLongValue and then BigintOperators.castToDouble.
                    // It doesn't work as well because it can result in overflow and underflow exceptions for large integral numbers.
                    result = parser.getDoubleValue();
                    break;
                case VALUE_TRUE:
                    result = BooleanOperators.castToDouble(true);
                    break;
                case VALUE_FALSE:
                    result = BooleanOperators.castToDouble(false);
                    break;
                default:
                    throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", json.toStringUtf8(), DOUBLE));
            }
            checkCondition(parser.nextToken() == null, INVALID_CAST_ARGUMENT, "Cannot cast input json to DOUBLE"); // check no trailing token
            return result;
        }
        catch (IOException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", json.toStringUtf8(), DOUBLE));
        }
    }

    @ScalarOperator(CAST)
    @Nullable
    @SqlType(BOOLEAN)
    public static Boolean castToBoolean(@SqlType(JSON) Slice json)
    {
        try (JsonParser parser = createJsonParser(JSON_FACTORY, json)) {
            parser.nextToken();
            Boolean result;
            switch (parser.getCurrentToken()) {
                case VALUE_NULL:
                    result = null;
                    break;
                case VALUE_STRING:
                    result = VarcharOperators.castToBoolean(Slices.utf8Slice(parser.getText()));
                    break;
                case VALUE_NUMBER_FLOAT:
                    result = DoubleOperators.castToBoolean(parser.getDoubleValue());
                    break;
                case VALUE_NUMBER_INT:
                    result = BigintOperators.castToBoolean(parser.getLongValue());
                    break;
                case VALUE_TRUE:
                    result = true;
                    break;
                case VALUE_FALSE:
                    result = false;
                    break;
                default:
                    throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", json.toStringUtf8(), BOOLEAN));
            }
            checkCondition(parser.nextToken() == null, INVALID_CAST_ARGUMENT, "Cannot cast input json to BOOLEAN"); // check no trailing token
            return result;
        }
        catch (IOException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", json.toStringUtf8(), BOOLEAN));
        }
    }

    @ScalarOperator(CAST)
    @SqlType(JSON)
    public static Slice castFromVarchar(@SqlType(VARCHAR) Slice slice) throws IOException
    {
        try {
            SliceOutput output = new DynamicSliceOutput(slice.length() + 2);
            try (JsonGenerator jsonGenerator = createJsonGenerator(JSON_FACTORY, output)) {
                jsonGenerator.writeString(slice.toStringUtf8());
            }
            return output.slice();
        }
        catch (IOException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", slice.toStringUtf8(), JSON));
        }
    }

    @ScalarOperator(CAST)
    @SqlType(JSON)
    public static Slice castFromInteger(@SqlType(INTEGER) long value) throws IOException
    {
        try {
            SliceOutput output = new DynamicSliceOutput(20);
            try (JsonGenerator jsonGenerator = createJsonGenerator(JSON_FACTORY, output)) {
                jsonGenerator.writeNumber(value);
            }
            return output.slice();
        }
        catch (IOException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", value, JSON));
        }
    }

    @ScalarOperator(CAST)
    @SqlType(JSON)
    public static Slice castFromBigint(@SqlType(BIGINT) long value) throws IOException
    {
        try {
            SliceOutput output = new DynamicSliceOutput(20);
            try (JsonGenerator jsonGenerator = createJsonGenerator(JSON_FACTORY, output)) {
                jsonGenerator.writeNumber(value);
            }
            return output.slice();
        }
        catch (IOException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", value, JSON));
        }
    }

    @ScalarOperator(CAST)
    @SqlType(JSON)
    public static Slice castFromDouble(@SqlType(DOUBLE) double value) throws IOException
    {
        try {
            SliceOutput output = new DynamicSliceOutput(32);
            try (JsonGenerator jsonGenerator = createJsonGenerator(JSON_FACTORY, output)) {
                jsonGenerator.writeNumber(value);
            }
            return output.slice();
        }
        catch (IOException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", value, JSON));
        }
    }

    @ScalarOperator(CAST)
    @SqlType(JSON)
    public static Slice castFromBoolean(@SqlType(BOOLEAN) boolean value) throws IOException
    {
        try {
            SliceOutput output = new DynamicSliceOutput(5);
            try (JsonGenerator jsonGenerator = createJsonGenerator(JSON_FACTORY, output)) {
                jsonGenerator.writeBoolean(value);
            }
            return output.slice();
        }
        catch (IOException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", value, JSON));
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
