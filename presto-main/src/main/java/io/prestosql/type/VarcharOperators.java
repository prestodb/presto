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
package io.prestosql.type;

import io.airlift.slice.Slice;
import io.airlift.slice.XxHash64;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.function.BlockIndex;
import io.prestosql.spi.function.BlockPosition;
import io.prestosql.spi.function.IsNull;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import static io.prestosql.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.prestosql.spi.function.OperatorType.BETWEEN;
import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.spi.function.OperatorType.EQUAL;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static io.prestosql.spi.function.OperatorType.HASH_CODE;
import static io.prestosql.spi.function.OperatorType.INDETERMINATE;
import static io.prestosql.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.prestosql.spi.function.OperatorType.LESS_THAN;
import static io.prestosql.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.prestosql.spi.function.OperatorType.NOT_EQUAL;
import static io.prestosql.spi.function.OperatorType.XX_HASH_64;
import static java.lang.String.format;

public final class VarcharOperators
{
    private VarcharOperators()
    {
    }

    @LiteralParameters("x")
    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean equal(@SqlType("varchar(x)") Slice left, @SqlType("varchar(x)") Slice right)
    {
        return left.equals(right);
    }

    @LiteralParameters("x")
    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean notEqual(@SqlType("varchar(x)") Slice left, @SqlType("varchar(x)") Slice right)
    {
        return !left.equals(right);
    }

    @LiteralParameters("x")
    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThan(@SqlType("varchar(x)") Slice left, @SqlType("varchar(x)") Slice right)
    {
        return left.compareTo(right) < 0;
    }

    @LiteralParameters("x")
    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqual(@SqlType("varchar(x)") Slice left, @SqlType("varchar(x)") Slice right)
    {
        return left.compareTo(right) <= 0;
    }

    @LiteralParameters("x")
    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThan(@SqlType("varchar(x)") Slice left, @SqlType("varchar(x)") Slice right)
    {
        return left.compareTo(right) > 0;
    }

    @LiteralParameters("x")
    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanOrEqual(@SqlType("varchar(x)") Slice left, @SqlType("varchar(x)") Slice right)
    {
        return left.compareTo(right) >= 0;
    }

    @LiteralParameters("x")
    @ScalarOperator(BETWEEN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean between(@SqlType("varchar(x)") Slice value, @SqlType("varchar(x)") Slice min, @SqlType("varchar(x)") Slice max)
    {
        return min.compareTo(value) <= 0 && value.compareTo(max) <= 0;
    }

    @LiteralParameters("x")
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean castToBoolean(@SqlType("varchar(x)") Slice value)
    {
        if (value.length() == 1) {
            byte character = toUpperCase(value.getByte(0));
            if (character == 'T' || character == '1') {
                return true;
            }
            if (character == 'F' || character == '0') {
                return false;
            }
        }
        if ((value.length() == 4) &&
                (toUpperCase(value.getByte(0)) == 'T') &&
                (toUpperCase(value.getByte(1)) == 'R') &&
                (toUpperCase(value.getByte(2)) == 'U') &&
                (toUpperCase(value.getByte(3)) == 'E')) {
            return true;
        }
        if ((value.length() == 5) &&
                (toUpperCase(value.getByte(0)) == 'F') &&
                (toUpperCase(value.getByte(1)) == 'A') &&
                (toUpperCase(value.getByte(2)) == 'L') &&
                (toUpperCase(value.getByte(3)) == 'S') &&
                (toUpperCase(value.getByte(4)) == 'E')) {
            return false;
        }
        throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to BOOLEAN", value.toStringUtf8()));
    }

    private static byte toUpperCase(byte b)
    {
        return isLowerCase(b) ? ((byte) (b - 32)) : b;
    }

    private static boolean isLowerCase(byte b)
    {
        return (b >= 'a') && (b <= 'z');
    }

    @LiteralParameters("x")
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.DOUBLE)
    public static double castToDouble(@SqlType("varchar(x)") Slice slice)
    {
        try {
            return Double.parseDouble(slice.toStringUtf8());
        }
        catch (Exception e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to DOUBLE", slice.toStringUtf8()));
        }
    }

    @LiteralParameters("x")
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.REAL)
    public static long castToFloat(@SqlType("varchar(x)") Slice slice)
    {
        try {
            return Float.floatToIntBits(Float.parseFloat(slice.toStringUtf8()));
        }
        catch (Exception e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to REAL", slice.toStringUtf8()));
        }
    }

    @LiteralParameters("x")
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.BIGINT)
    public static long castToBigint(@SqlType("varchar(x)") Slice slice)
    {
        try {
            return Long.parseLong(slice.toStringUtf8());
        }
        catch (Exception e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to BIGINT", slice.toStringUtf8()));
        }
    }

    @LiteralParameters("x")
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.INTEGER)
    public static long castToInteger(@SqlType("varchar(x)") Slice slice)
    {
        try {
            return Integer.parseInt(slice.toStringUtf8());
        }
        catch (Exception e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to INT", slice.toStringUtf8()));
        }
    }

    @LiteralParameters("x")
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.SMALLINT)
    public static long castToSmallint(@SqlType("varchar(x)") Slice slice)
    {
        try {
            return Short.parseShort(slice.toStringUtf8());
        }
        catch (Exception e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to SMALLINT", slice.toStringUtf8()));
        }
    }

    @LiteralParameters("x")
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TINYINT)
    public static long castToTinyint(@SqlType("varchar(x)") Slice slice)
    {
        try {
            return Byte.parseByte(slice.toStringUtf8());
        }
        catch (Exception e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to TINYINT", slice.toStringUtf8()));
        }
    }

    @LiteralParameters("x")
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARBINARY)
    public static Slice castToBinary(@SqlType("varchar(x)") Slice slice)
    {
        return slice;
    }

    @LiteralParameters("x")
    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(@SqlType("varchar(x)") Slice value)
    {
        return xxHash64(value);
    }

    @ScalarOperator(IS_DISTINCT_FROM)
    public static class VarcharDistinctFromOperator
    {
        @LiteralParameters({"x", "y"})
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isDistinctFrom(
                @SqlType("varchar(x)") Slice left,
                @IsNull boolean leftNull,
                @SqlType("varchar(y)") Slice right,
                @IsNull boolean rightNull)
        {
            if (leftNull != rightNull) {
                return true;
            }
            if (leftNull) {
                return false;
            }
            return notEqual(left, right);
        }

        @LiteralParameters({"x", "y"})
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isDistinctFrom(
                @BlockPosition @SqlType(value = "varchar(x)", nativeContainerType = Slice.class) Block left,
                @BlockIndex int leftPosition,
                @BlockPosition @SqlType(value = "varchar(y)", nativeContainerType = Slice.class) Block right,
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

    @LiteralParameters("x")
    @ScalarOperator(XX_HASH_64)
    @SqlType(StandardTypes.BIGINT)
    public static long xxHash64(@SqlType("varchar(x)") Slice slice)
    {
        return XxHash64.hash(slice);
    }

    @LiteralParameters("x")
    @ScalarOperator(INDETERMINATE)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean indeterminate(@SqlType("varchar(x)") Slice value, @IsNull boolean isNull)
    {
        return isNull;
    }
}
