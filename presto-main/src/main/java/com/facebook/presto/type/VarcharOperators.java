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
    public static boolean equal(Slice left, Slice right)
    {
        return left.equals(right);
    }

    @ScalarOperator(NOT_EQUAL)
    public static boolean notEqual(Slice left, Slice right)
    {
        return !left.equals(right);
    }

    @ScalarOperator(LESS_THAN)
    public static boolean lessThan(Slice left, Slice right)
    {
        return left.compareTo(right) < 0;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    public static boolean lessThanOrEqual(Slice left, Slice right)
    {
        return left.compareTo(right) <= 0;
    }

    @ScalarOperator(GREATER_THAN)
    public static boolean greaterThan(Slice left, Slice right)
    {
        return left.compareTo(right) > 0;
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    public static boolean greaterThanOrEqual(Slice left, Slice right)
    {
        return left.compareTo(right) >= 0;
    }

    @ScalarOperator(BETWEEN)
    public static boolean between(Slice value, Slice min, Slice max)
    {
        return min.compareTo(value) <= 0 && value.compareTo(max) <= 0;
    }

    @ScalarOperator(CAST)
    public static boolean castToBoolean(Slice value)
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
    public static double castToDouble(Slice value)
    {
        if (value.length() >= 1) {
            try {
                char[] chars = new char[value.length()];
                for (int pos = 0; pos < value.length(); pos++) {
                    chars[pos] = (char) value.getByte(pos);
                }
                String string = new String(chars);
                return Double.parseDouble(string);
            }
            catch (RuntimeException ignored) {
            }
        }
        throw new IllegalArgumentException(String.format("Can not cast '%s' to DOUBLE", value.toString(UTF_8)));
    }

    @ScalarOperator(CAST)
    public static long castToLong(Slice slice)
    {
        if (slice.length() >= 1) {
            try {
                int start = 0;
                int sign = slice.getByte(start) == '-' ? -1 : 1;

                if (sign == -1 || slice.getByte(start) == '+') {
                    start++;
                }

                long value = getDecimalValue(slice, start++);
                while (start < slice.length()) {
                    value = value * 10 + (getDecimalValue(slice, start));
                    start++;
                }

                return value * sign;
            }
            catch (RuntimeException ignored) {
            }
        }
        throw new IllegalArgumentException(String.format("Can not cast '%s' to BIGINT", slice.toString(UTF_8)));
    }

    @SuppressWarnings("CharUsedInArithmeticContext")
    private static int getDecimalValue(Slice slice, int start)
    {
        int decimal = slice.getByte(start) - '0';
        if (decimal < 0 || decimal > 9) {
            throw new NumberFormatException();
        }
        return decimal;
    }

    @ScalarOperator(CAST)
    public static Slice castToSlice(Slice value)
    {
        return value;
    }

    @ScalarOperator(HASH_CODE)
    public static int hashCode(Slice value)
    {
        return value.hashCode();
    }
}
