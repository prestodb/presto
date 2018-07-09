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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.IsNull;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.util.JsonCastException;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

import java.io.IOException;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static com.facebook.presto.spi.function.OperatorType.CAST;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.function.OperatorType.HASH_CODE;
import static com.facebook.presto.spi.function.OperatorType.INDETERMINATE;
import static com.facebook.presto.spi.function.OperatorType.IS_DISTINCT_FROM;
import static com.facebook.presto.spi.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.spi.type.StandardTypes.BIGINT;
import static com.facebook.presto.spi.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.spi.type.StandardTypes.DATE;
import static com.facebook.presto.spi.type.StandardTypes.DOUBLE;
import static com.facebook.presto.spi.type.StandardTypes.INTEGER;
import static com.facebook.presto.spi.type.StandardTypes.JSON;
import static com.facebook.presto.spi.type.StandardTypes.REAL;
import static com.facebook.presto.spi.type.StandardTypes.SMALLINT;
import static com.facebook.presto.spi.type.StandardTypes.TIMESTAMP;
import static com.facebook.presto.spi.type.StandardTypes.TINYINT;
import static com.facebook.presto.spi.type.StandardTypes.VARCHAR;
import static com.facebook.presto.util.DateTimeUtils.printDate;
import static com.facebook.presto.util.DateTimeUtils.printTimestampWithoutTimeZone;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.facebook.presto.util.JsonUtil.createJsonGenerator;
import static com.facebook.presto.util.JsonUtil.createJsonParser;
import static com.facebook.presto.util.JsonUtil.currentTokenAsBigint;
import static com.facebook.presto.util.JsonUtil.currentTokenAsBoolean;
import static com.facebook.presto.util.JsonUtil.currentTokenAsDouble;
import static com.facebook.presto.util.JsonUtil.currentTokenAsInteger;
import static com.facebook.presto.util.JsonUtil.currentTokenAsReal;
import static com.facebook.presto.util.JsonUtil.currentTokenAsSmallint;
import static com.facebook.presto.util.JsonUtil.currentTokenAsTinyint;
import static com.facebook.presto.util.JsonUtil.currentTokenAsVarchar;
import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static java.lang.Float.intBitsToFloat;
import static java.lang.String.format;

public final class JsonOperators
{
    public static final JsonFactory JSON_FACTORY = new JsonFactory().disable(CANONICALIZE_FIELD_NAMES);

    private JsonOperators()
    {
    }

    @ScalarOperator(CAST)
    @SqlNullable
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice castToVarchar(@SqlType(JSON) Slice json)
    {
        try (JsonParser parser = createJsonParser(JSON_FACTORY, json)) {
            parser.nextToken();
            Slice result = currentTokenAsVarchar(parser);
            checkCondition(parser.nextToken() == null, INVALID_CAST_ARGUMENT, "Cannot cast input json to VARCHAR"); // check no trailing token
            return result;
        }
        catch (IOException | JsonCastException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", json.toStringUtf8(), VARCHAR), e);
        }
    }

    @ScalarOperator(CAST)
    @SqlNullable
    @SqlType(BIGINT)
    public static Long castToBigint(@SqlType(JSON) Slice json)
    {
        try (JsonParser parser = createJsonParser(JSON_FACTORY, json)) {
            parser.nextToken();
            Long result = currentTokenAsBigint(parser);
            checkCondition(parser.nextToken() == null, INVALID_CAST_ARGUMENT, "Cannot cast input json to BIGINT"); // check no trailing token
            return result;
        }
        catch (IOException | JsonCastException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", json.toStringUtf8(), BIGINT), e);
        }
    }

    @ScalarOperator(CAST)
    @SqlNullable
    @SqlType(INTEGER)
    public static Long castToInteger(@SqlType(JSON) Slice json)
    {
        try (JsonParser parser = createJsonParser(JSON_FACTORY, json)) {
            parser.nextToken();
            Long result = currentTokenAsInteger(parser);
            checkCondition(parser.nextToken() == null, INVALID_CAST_ARGUMENT, "Cannot cast input json to INTEGER"); // check no trailing token
            return result;
        }
        catch (PrestoException e) {
            if (e.getErrorCode().equals(NUMERIC_VALUE_OUT_OF_RANGE.toErrorCode())) {
                throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", json.toStringUtf8(), INTEGER), e.getCause());
            }
            throw e;
        }
        catch (ArithmeticException | IOException | JsonCastException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", json.toStringUtf8(), INTEGER), e);
        }
    }

    @ScalarOperator(CAST)
    @SqlNullable
    @SqlType(SMALLINT)
    public static Long castToSmallint(@SqlType(JSON) Slice json)
    {
        try (JsonParser parser = createJsonParser(JSON_FACTORY, json)) {
            parser.nextToken();
            Long result = currentTokenAsSmallint(parser);
            checkCondition(parser.nextToken() == null, INVALID_CAST_ARGUMENT, "Cannot cast input json to SMALLINT"); // check no trailing token
            return result;
        }
        catch (PrestoException e) {
            if (e.getErrorCode().equals(NUMERIC_VALUE_OUT_OF_RANGE.toErrorCode())) {
                throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", json.toStringUtf8(), INTEGER), e.getCause());
            }
            throw e;
        }
        catch (IllegalArgumentException | IOException | JsonCastException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", json.toStringUtf8(), SMALLINT), e);
        }
    }

    @ScalarOperator(CAST)
    @SqlNullable
    @SqlType(TINYINT)
    public static Long castToTinyint(@SqlType(JSON) Slice json)
    {
        try (JsonParser parser = createJsonParser(JSON_FACTORY, json)) {
            parser.nextToken();
            Long result = currentTokenAsTinyint(parser);
            checkCondition(parser.nextToken() == null, INVALID_CAST_ARGUMENT, "Cannot cast input json to TINYINT"); // check no trailing token
            return result;
        }
        catch (PrestoException e) {
            if (e.getErrorCode().equals(NUMERIC_VALUE_OUT_OF_RANGE.toErrorCode())) {
                throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", json.toStringUtf8(), INTEGER), e.getCause());
            }
            throw e;
        }
        catch (IllegalArgumentException | IOException | JsonCastException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", json.toStringUtf8(), TINYINT), e);
        }
    }

    @ScalarOperator(CAST)
    @SqlNullable
    @SqlType(DOUBLE)
    public static Double castToDouble(@SqlType(JSON) Slice json)
    {
        try (JsonParser parser = createJsonParser(JSON_FACTORY, json)) {
            parser.nextToken();
            Double result = currentTokenAsDouble(parser);
            checkCondition(parser.nextToken() == null, INVALID_CAST_ARGUMENT, "Cannot cast input json to DOUBLE"); // check no trailing token
            return result;
        }
        catch (IOException | JsonCastException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", json.toStringUtf8(), DOUBLE), e);
        }
    }

    @ScalarOperator(CAST)
    @SqlNullable
    @SqlType(REAL)
    public static Long castToReal(@SqlType(JSON) Slice json)
    {
        try (JsonParser parser = createJsonParser(JSON_FACTORY, json)) {
            parser.nextToken();
            Long result = currentTokenAsReal(parser);
            checkCondition(parser.nextToken() == null, INVALID_CAST_ARGUMENT, "Cannot cast input json to REAL"); // check no trailing token
            return result;
        }
        catch (IOException | JsonCastException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", json.toStringUtf8(), REAL), e);
        }
    }

    @ScalarOperator(CAST)
    @SqlNullable
    @SqlType(BOOLEAN)
    public static Boolean castToBoolean(@SqlType(JSON) Slice json)
    {
        try (JsonParser parser = createJsonParser(JSON_FACTORY, json)) {
            parser.nextToken();
            Boolean result = currentTokenAsBoolean(parser);
            checkCondition(parser.nextToken() == null, INVALID_CAST_ARGUMENT, "Cannot cast input json to BOOLEAN"); // check no trailing token
            return result;
        }
        catch (IOException | JsonCastException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", json.toStringUtf8(), BOOLEAN), e);
        }
    }

    @ScalarOperator(CAST)
    @LiteralParameters("x")
    @SqlType(JSON)
    public static Slice castFromVarchar(@SqlType("varchar(x)") Slice value)
    {
        try {
            SliceOutput output = new DynamicSliceOutput(value.length() + 2);
            try (JsonGenerator jsonGenerator = createJsonGenerator(JSON_FACTORY, output)) {
                jsonGenerator.writeString(value.toStringUtf8());
            }
            return output.slice();
        }
        catch (IOException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", value.toStringUtf8(), JSON));
        }
    }

    @ScalarOperator(CAST)
    @SqlType(JSON)
    public static Slice castFromTinyInt(@SqlType(TINYINT) long value)
    {
        return internalCastFromLong(value, 4);
    }

    @ScalarOperator(CAST)
    @SqlType(JSON)
    public static Slice castFromSmallInt(@SqlType(SMALLINT) long value)
    {
        return internalCastFromLong(value, 8);
    }

    @ScalarOperator(CAST)
    @SqlType(JSON)
    public static Slice castFromInteger(@SqlType(INTEGER) long value)
    {
        return internalCastFromLong(value, 12);
    }

    @ScalarOperator(CAST)
    @SqlType(JSON)
    public static Slice castFromBigint(@SqlType(BIGINT) long value)
    {
        return internalCastFromLong(value, 20);
    }

    private static Slice internalCastFromLong(long value, int estimatedSize)
    {
        try {
            SliceOutput output = new DynamicSliceOutput(estimatedSize);
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
    public static Slice castFromDouble(@SqlType(DOUBLE) double value)
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
    public static Slice castFromReal(@SqlType(REAL) long value)
    {
        try {
            SliceOutput output = new DynamicSliceOutput(32);
            try (JsonGenerator jsonGenerator = createJsonGenerator(JSON_FACTORY, output)) {
                jsonGenerator.writeNumber(intBitsToFloat((int) value));
            }
            return output.slice();
        }
        catch (IOException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", value, JSON));
        }
    }

    @ScalarOperator(CAST)
    @SqlType(JSON)
    public static Slice castFromBoolean(@SqlType(BOOLEAN) boolean value)
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

    @ScalarOperator(CAST)
    @SqlType(JSON)
    public static Slice castFromTimestamp(ConnectorSession session, @SqlType(TIMESTAMP) long value)
    {
        try {
            SliceOutput output = new DynamicSliceOutput(25);
            try (JsonGenerator jsonGenerator = createJsonGenerator(JSON_FACTORY, output)) {
                jsonGenerator.writeString(printTimestampWithoutTimeZone(session.getTimeZoneKey(), value));
            }
            return output.slice();
        }
        catch (IOException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", value, JSON));
        }
    }

    @ScalarOperator(CAST)
    @SqlType(JSON)
    public static Slice castFromDate(ConnectorSession session, @SqlType(DATE) long value)
    {
        try {
            SliceOutput output = new DynamicSliceOutput(12);
            try (JsonGenerator jsonGenerator = createJsonGenerator(JSON_FACTORY, output)) {
                jsonGenerator.writeString(printDate((int) value));
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

    @ScalarOperator(INDETERMINATE)
    @SqlType(BOOLEAN)
    public static boolean indeterminate(@SqlType(JSON) Slice value, @IsNull boolean isNull)
    {
        return isNull;
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

    @ScalarOperator(IS_DISTINCT_FROM)
    @SqlType(BOOLEAN)
    public static boolean isDistinctFrom(@SqlType(JSON) Slice leftJson, @IsNull boolean leftNull, @SqlType(JSON) Slice rightJson, @IsNull boolean rightNull)
    {
        if (leftNull != rightNull) {
            return true;
        }
        if (leftNull) {
            return false;
        }
        return notEqual(leftJson, rightJson);
    }
}
