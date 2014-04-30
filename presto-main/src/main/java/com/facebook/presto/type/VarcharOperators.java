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
package com.facebook.presto.type;

import com.facebook.presto.operator.scalar.ScalarOperator;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.VarcharType;
import io.airlift.slice.Slice;

import static com.facebook.presto.metadata.OperatorInfo.OperatorType.BETWEEN;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.CAST;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.EQUAL;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.GREATER_THAN;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.HASH_CODE;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.LESS_THAN;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.NOT_EQUAL;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class VarcharOperators
{
    private VarcharOperators()
    {
    }

    @ScalarOperator(EQUAL)
    @SqlType(BooleanType.class)
    public static boolean equal(@SqlType(VarcharType.class) Slice left, @SqlType(VarcharType.class) Slice right)
    {
        return left.equals(right);
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(BooleanType.class)
    public static boolean notEqual(@SqlType(VarcharType.class) Slice left, @SqlType(VarcharType.class) Slice right)
    {
        return !left.equals(right);
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(BooleanType.class)
    public static boolean lessThan(@SqlType(VarcharType.class) Slice left, @SqlType(VarcharType.class) Slice right)
    {
        return left.compareTo(right) < 0;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(BooleanType.class)
    public static boolean lessThanOrEqual(@SqlType(VarcharType.class) Slice left, @SqlType(VarcharType.class) Slice right)
    {
        return left.compareTo(right) <= 0;
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(BooleanType.class)
    public static boolean greaterThan(@SqlType(VarcharType.class) Slice left, @SqlType(VarcharType.class) Slice right)
    {
        return left.compareTo(right) > 0;
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(BooleanType.class)
    public static boolean greaterThanOrEqual(@SqlType(VarcharType.class) Slice left, @SqlType(VarcharType.class) Slice right)
    {
        return left.compareTo(right) >= 0;
    }

    @ScalarOperator(BETWEEN)
    @SqlType(BooleanType.class)
    public static boolean between(@SqlType(VarcharType.class) Slice value, @SqlType(VarcharType.class) Slice min, @SqlType(VarcharType.class) Slice max)
    {
        return min.compareTo(value) <= 0 && value.compareTo(max) <= 0;
    }

    @ScalarOperator(CAST)
    @SqlType(BooleanType.class)
    public static boolean castToBoolean(@SqlType(VarcharType.class) Slice value)
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
        throw new IllegalArgumentException(String.format("Cannot cast '%s' to BOOLEAN", value.toString(UTF_8)));
    }

    private static byte toUpperCase(byte b)
    {
        return isLowerCase(b) ? ((byte) (b - 32)) : b;
    }

    private static boolean isLowerCase(byte b)
    {
        return (b >= 'a') && (b <= 'z');
    }

    @ScalarOperator(CAST)
    @SqlType(DoubleType.class)
    public static double castToDouble(@SqlType(VarcharType.class) Slice slice)
    {
        try {
            return Double.parseDouble(slice.toString(UTF_8));
        }
        catch (Exception e) {
            throw new IllegalArgumentException(String.format("Can not cast '%s' to DOUBLE", slice.toString(UTF_8)));
        }
    }

    @ScalarOperator(CAST)
    @SqlType(BigintType.class)
    public static long castToBigint(@SqlType(VarcharType.class) Slice slice)
    {
        try {
            return Long.parseLong(slice.toString(UTF_8));
        }
        catch (Exception e) {
            throw new IllegalArgumentException(String.format("Can not cast '%s' to BIGINT", slice.toString(UTF_8)));
        }
    }

    @ScalarOperator(HASH_CODE)
    public static int hashCode(@SqlType(VarcharType.class) Slice value)
    {
        return value.hashCode();
    }
}
