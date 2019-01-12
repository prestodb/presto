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
package io.prestosql.operator.scalar;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.BlockIndex;
import io.prestosql.spi.function.BlockPosition;
import io.prestosql.spi.function.IsNull;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.util.JsonCastException;

import java.io.IOException;

import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static io.prestosql.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.prestosql.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.spi.function.OperatorType.EQUAL;
import static io.prestosql.spi.function.OperatorType.HASH_CODE;
import static io.prestosql.spi.function.OperatorType.INDETERMINATE;
import static io.prestosql.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.prestosql.spi.function.OperatorType.NOT_EQUAL;
import static io.prestosql.spi.type.StandardTypes.BIGINT;
import static io.prestosql.spi.type.StandardTypes.BOOLEAN;
import static io.prestosql.spi.type.StandardTypes.DATE;
import static io.prestosql.spi.type.StandardTypes.DOUBLE;
import static io.prestosql.spi.type.StandardTypes.INTEGER;
import static io.prestosql.spi.type.StandardTypes.JSON;
import static io.prestosql.spi.type.StandardTypes.REAL;
import static io.prestosql.spi.type.StandardTypes.SMALLINT;
import static io.prestosql.spi.type.StandardTypes.TIMESTAMP;
import static io.prestosql.spi.type.StandardTypes.TINYINT;
import static io.prestosql.spi.type.StandardTypes.VARCHAR;
import static io.prestosql.util.DateTimeUtils.printDate;
import static io.prestosql.util.DateTimeUtils.printTimestampWithoutTimeZone;
import static io.prestosql.util.Failures.checkCondition;
import static io.prestosql.util.JsonUtil.createJsonGenerator;
import static io.prestosql.util.JsonUtil.createJsonParser;
import static io.prestosql.util.JsonUtil.currentTokenAsBigint;
import static io.prestosql.util.JsonUtil.currentTokenAsBoolean;
import static io.prestosql.util.JsonUtil.currentTokenAsDouble;
import static io.prestosql.util.JsonUtil.currentTokenAsInteger;
import static io.prestosql.util.JsonUtil.currentTokenAsReal;
import static io.prestosql.util.JsonUtil.currentTokenAsSmallint;
import static io.prestosql.util.JsonUtil.currentTokenAsTinyint;
import static io.prestosql.util.JsonUtil.currentTokenAsVarchar;
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
    @SqlNullable
    public static Boolean equals(@SqlType(JSON) Slice leftJson, @SqlType(JSON) Slice rightJson)
    {
        return leftJson.equals(rightJson);
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(BOOLEAN)
    @SqlNullable
    public static Boolean notEqual(@SqlType(JSON) Slice leftJson, @SqlType(JSON) Slice rightJson)
    {
        return !leftJson.equals(rightJson);
    }

    @ScalarOperator(IS_DISTINCT_FROM)
    public static class JsonDistinctFromOperator
    {
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

        @SqlType(BOOLEAN)
        public static boolean isDistinctFrom(
                @BlockPosition @SqlType(value = JSON, nativeContainerType = Slice.class) Block left,
                @BlockIndex int leftPosition,
                @BlockPosition @SqlType(value = JSON, nativeContainerType = Slice.class) Block right,
                @BlockIndex int rightPosition)
        {
            if (left.isNull(leftPosition) != right.isNull(rightPosition)) {
                return true;
            }
            if (left.isNull(leftPosition)) {
                return false;
            }
            int leftLength = left.getSliceLength(leftPosition);
            int rightLength = right.getSliceLength(rightPosition);
            if (leftLength != rightLength) {
                return true;
            }
            return !left.equals(leftPosition, 0, right, rightPosition, 0, leftLength);
        }
    }
}
