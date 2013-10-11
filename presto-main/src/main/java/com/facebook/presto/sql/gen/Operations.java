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
package com.facebook.presto.sql.gen;

import com.facebook.presto.operator.scalar.MathFunctions;
import com.google.common.primitives.Booleans;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.google.common.base.Charsets.US_ASCII;
import static com.google.common.base.Charsets.UTF_8;

public final class Operations
{
    private static final Slice TRUE = Slices.copiedBuffer("true", US_ASCII);
    private static final Slice FALSE = Slices.copiedBuffer("false", US_ASCII);

    private Operations()
    {
    }

    public static boolean and(boolean left, boolean right)
    {
        return left && right;
    }

    public static boolean or(boolean left, boolean right)
    {
        return left || right;
    }

    public static boolean not(boolean value)
    {
        return !value;
    }

    public static boolean equal(boolean left, boolean right)
    {
        return left == right;
    }

    public static boolean isDistinctFrom(boolean left, boolean leftWasNull, boolean right, boolean rightWasNull)
    {
        if (leftWasNull) {
            return !rightWasNull;
        }
        return rightWasNull || left != right;
    }

    public static int hashCode(boolean value)
    {
        return Booleans.hashCode(value);
    }

    public static boolean notEqual(boolean left, boolean right)
    {
        return left != right;
    }

    public static boolean equal(long left, long right)
    {
        return left == right;
    }

    public static boolean isDistinctFrom(long left, boolean leftWasNull, long right, boolean rightWasNull)
    {
        if (leftWasNull) {
            return !rightWasNull;
        }
        return rightWasNull || left != right;
    }

    public static int hashCode(long value)
    {
        return Longs.hashCode(value);
    }

    public static long add(long left, long right)
    {
        return left + right;
    }

    public static long subtract(long left, long right)
    {
        return left - right;
    }

    public static long multiply(long left, long right)
    {
        return left * right;
    }

    public static long divide(long left, long right)
    {
        return left / right;
    }

    public static long modulus(long left, long right)
    {
        return left % right;
    }

    public static long negate(long value)
    {
        return -value;
    }

    public static double add(double left, double right)
    {
        return left + right;
    }

    public static double subtract(double left, double right)
    {
        return left - right;
    }

    public static double multiply(double left, double right)
    {
        return left * right;
    }

    public static double divide(double left, double right)
    {
        return left / right;
    }

    public static double modulus(double left, double right)
    {
        return left % right;
    }

    public static double negate(double value)
    {
        return -value;
    }

    public static boolean notEqual(long left, long right)
    {
        return left != right;
    }

    public static boolean lessThan(int left, int right)
    {
        return left < right;
    }

    public static boolean lessThan(long left, long right)
    {
        return left < right;
    }

    public static boolean lessThanOrEqual(long left, long right)
    {
        return left <= right;
    }

    public static boolean greaterThan(long left, long right)
    {
        return left > right;
    }

    public static boolean greaterThanOrEqual(long left, long right)
    {
        return left >= right;
    }

    public static boolean between(long value, long min, long max)
    {
        return min <= value && value <= max;
    }

    public static boolean equal(double left, double right)
    {
        return left == right;
    }

    public static boolean isDistinctFrom(double left, boolean leftWasNull, double right, boolean rightWasNull)
    {
        if (leftWasNull) {
            return !rightWasNull;
        }
        return rightWasNull || left != right;
    }

    public static int hashCode(double value)
    {
        return Doubles.hashCode(value);
    }

    public static boolean notEqual(double left, double right)
    {
        return left != right;
    }

    public static boolean lessThan(double left, double right)
    {
        return left < right;
    }

    public static boolean lessThanOrEqual(double left, double right)
    {
        return left <= right;
    }

    public static boolean greaterThan(double left, double right)
    {
        return left > right;
    }

    public static boolean greaterThanOrEqual(double left, double right)
    {
        return left >= right;
    }

    public static boolean between(double value, double min, double max)
    {
        return min <= value && value <= max;
    }

    public static boolean equal(Slice left, Slice right)
    {
        return left.equals(right);
    }

    public static boolean isDistinctFrom(Slice left, boolean leftWasNull, Slice right, boolean rightWasNull)
    {
        if (leftWasNull) {
            return !rightWasNull;
        }
        return rightWasNull || !left.equals(right);
    }

    public static int hashCode(Slice value)
    {
        return value.hashCode();
    }

    public static boolean notEqual(Slice left, Slice right)
    {
        return !left.equals(right);
    }

    public static boolean lessThan(Slice left, Slice right)
    {
        return left.compareTo(right) < 0;
    }

    public static boolean lessThanOrEqual(Slice left, Slice right)
    {
        return left.compareTo(right) <= 0;
    }

    public static boolean greaterThan(Slice left, Slice right)
    {
        return left.compareTo(right) > 0;
    }

    public static boolean greaterThanOrEqual(Slice left, Slice right)
    {
        return left.compareTo(right) >= 0;
    }

    public static boolean between(Slice value, Slice min, Slice max)
    {
        return min.compareTo(value) <= 0 && value.compareTo(max) <= 0;
    }

    public static boolean castToBoolean(boolean value)
    {
        return value;
    }

    public static boolean castToBoolean(long value)
    {
        return value != 0;
    }

    public static boolean castToBoolean(double value)
    {
        return value != 0;
    }

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

    public static long castToLong(boolean value)
    {
        return value ? 1 : 0;
    }

    public static long castToLong(long value)
    {
        return value;
    }

    public static long castToLong(double value)
    {
        return (long) MathFunctions.round(value);
    }

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

    private static int getDecimalValue(Slice slice, int start)
    {
        int decimal = slice.getByte(start) - '0';
        if (decimal < 0 || decimal > 9) {
            throw new NumberFormatException();
        }
        return decimal;
    }

    public static double castToDouble(boolean value)
    {
        return value ? 1 : 0;
    }

    public static double castToDouble(long value)
    {
        return value;
    }

    public static double castToDouble(double value)
    {
        return value;
    }

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

    public static Slice castToSlice(boolean value)
    {
        return value ? TRUE : FALSE;
    }

    public static Slice castToSlice(long value)
    {
        // todo optimize me
        return Slices.copiedBuffer(String.valueOf(value), UTF_8);
    }

    public static Slice castToSlice(double value)
    {
        return Slices.copiedBuffer(String.valueOf(value), UTF_8);
    }

    public static Slice castToSlice(Slice value)
    {
        return value;
    }
}
