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
package com.facebook.presto.spi.type;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.math.BigInteger;
import java.nio.ByteOrder;

import static com.facebook.presto.spi.type.Decimals.MAX_PRECISION;
import static com.facebook.presto.spi.type.Decimals.longTenToNth;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static java.lang.String.format;
import static java.lang.System.arraycopy;
import static java.util.Arrays.fill;

/**
 * 128 bit unscaled decimal arithmetic. The representation is:
 * <p>
 * [127_bit_unscaled_decimal_value sign_bit]
 * 0-bit...........64-bit............128-bit
 * <p>
 * 127_bit_unscaled_decimal_value is stored in little endian format to provide easy conversion to/from long and int types.
 * <p>
 * Based on: https://github.com/apache/hive/blob/master/common/src/java/org/apache/hadoop/hive/common/type/UnsignedInt128.java
 * and https://github.com/apache/hive/blob/master/common/src/java/org/apache/hadoop/hive/common/type/Decimal128.java
 */
public final class UnscaledDecimal128Arithmetic
{
    private static final int NUMBER_OF_LONGS = 2;
    private static final int NUMBER_OF_INTS = 2 * NUMBER_OF_LONGS;

    public static final int UNSCALED_DECIMAL_128_SLICE_LENGTH = NUMBER_OF_LONGS * SIZE_OF_LONG;

    private static final Slice[] POWERS_OF_TEN = new Slice[MAX_PRECISION];
    private static final Slice[] POWERS_OF_FIVE = new Slice[MAX_PRECISION];

    private static final int SIGN_LONG_INDEX = 1;
    private static final int SIGN_INT_INDEX = 3;
    private static final long SIGN_LONG_MASK = 1L << 63;
    private static final int SIGN_INT_MASK = 1 << 31;
    private static final int SIGN_BYTE_MASK = 1 << 7;
    private static final long ALL_BITS_SET_64 = 0xFFFFFFFFFFFFFFFFL;
    private static final long INT_BASE = 1L << 32;
    /**
     * Mask to convert signed integer to unsigned long.
     */
    private static final long LONG_MASK = 0xFFFFFFFFL;

    /**
     * 5^13 fits in 2^31.
     */
    private static final int MAX_POWER_OF_FIVE_INT = 13;
    /**
     * 5^x. All unsigned values.
     */
    private static final int[] POWERS_OF_FIVES_INT = new int[MAX_POWER_OF_FIVE_INT + 1];

    /**
     * 10^9 fits in 2^31.
     */
    private static final int MAX_POWER_OF_TEN_INT = 9;
    /**
     * 10^18 fits in 2^63.
     */
    private static final int MAX_POWER_OF_TEN_LONG = 18;
    /**
     * 10^x. All unsigned values.
     */
    private static final int[] POWERS_OF_TEN_INT = new int[MAX_POWER_OF_TEN_INT + 1];

    private static final Unsafe unsafe;

    static {
        try {
            // fetch theUnsafe object
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
            if (unsafe == null) {
                throw new RuntimeException("Unsafe access not available");
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        for (int i = 0; i < POWERS_OF_FIVE.length; ++i) {
            POWERS_OF_FIVE[i] = unscaledDecimal(BigInteger.valueOf(5).pow(i));
        }
        for (int i = 0; i < POWERS_OF_TEN.length; ++i) {
            POWERS_OF_TEN[i] = unscaledDecimal(BigInteger.TEN.pow(i));
        }

        POWERS_OF_FIVES_INT[0] = 1;
        for (int i = 1; i < POWERS_OF_FIVES_INT.length; ++i) {
            POWERS_OF_FIVES_INT[i] = POWERS_OF_FIVES_INT[i - 1] * 5;
        }

        POWERS_OF_TEN_INT[0] = 1;
        for (int i = 1; i < POWERS_OF_TEN_INT.length; ++i) {
            POWERS_OF_TEN_INT[i] = POWERS_OF_TEN_INT[i - 1] * 10;
        }

        if (!ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN)) {
            throw new IllegalStateException("UnsignedDecimal128Arithmetic is supported on little-endian machines only");
        }
    }

    public static Slice unscaledDecimal()
    {
        return Slices.allocate(UNSCALED_DECIMAL_128_SLICE_LENGTH);
    }

    public static Slice unscaledDecimal(Slice decimal)
    {
        return Slices.copyOf(decimal);
    }

    public static Slice unscaledDecimal(String unscaledValue)
    {
        return unscaledDecimal(new BigInteger(unscaledValue));
    }

    public static Slice unscaledDecimal(BigInteger unscaledValue)
    {
        Slice decimal = Slices.allocate(UNSCALED_DECIMAL_128_SLICE_LENGTH);
        return pack(unscaledValue, decimal);
    }

    public static Slice pack(BigInteger unscaledValue, Slice result)
    {
        pack(0, 0, false, result);
        byte[] bytes = unscaledValue.abs().toByteArray();

        if (bytes.length > UNSCALED_DECIMAL_128_SLICE_LENGTH
                || (bytes.length == UNSCALED_DECIMAL_128_SLICE_LENGTH && (bytes[0] & SIGN_BYTE_MASK) != 0)) {
            throwOverflowException();
        }

        // convert to little-endian order
        reverse(bytes);

        result.setBytes(0, bytes);
        if (unscaledValue.signum() < 0) {
            setNegative(result, true);
        }

        throwIfOverflows(result);

        return result;
    }

    public static Slice unscaledDecimal(long unscaledValue)
    {
        long[] longs = new long[NUMBER_OF_LONGS];
        if (unscaledValue < 0) {
            longs[0] = -unscaledValue;
            longs[1] = SIGN_LONG_MASK;
        }
        else {
            longs[0] = unscaledValue;
        }
        return Slices.wrappedLongArray(longs);
    }

    public static BigInteger unscaledDecimalToBigInteger(Slice decimal)
    {
        byte[] bytes = decimal.getBytes(0, UNSCALED_DECIMAL_128_SLICE_LENGTH);
        // convert to big-endian order
        reverse(bytes);
        bytes[0] &= ~SIGN_BYTE_MASK;
        return new BigInteger(isNegative(decimal) ? -1 : 1, bytes);
    }

    public static long unscaledDecimalToUnscaledLong(Slice decimal)
    {
        long low = getLong(decimal, 0);
        long high = getLong(decimal, 1);
        boolean negative = isNegative(decimal);

        if (high != 0 || ((low > Long.MIN_VALUE || !negative) && low < 0)) {
            throwOverflowException();
        }

        return negative ? -low : low;
    }

    public static long unscaledDecimalToUnscaledLongUnsafe(Slice decimal)
    {
        long low = getLong(decimal, 0);
        return isNegative(decimal) ? -low : low;
    }

    public static Slice rescale(Slice decimal, int rescaleFactor)
    {
        if (rescaleFactor == 0) {
            return decimal;
        }
        else {
            Slice result = unscaledDecimal();
            rescale(decimal, rescaleFactor, result);
            return result;
        }
    }

    public static void rescale(Slice decimal, int rescaleFactor, Slice result)
    {
        if (rescaleFactor == 0) {
            copyUnscaledDecimal(decimal, result);
        }
        else if (rescaleFactor > 0) {
            if (rescaleFactor >= POWERS_OF_TEN.length) {
                throwOverflowException();
            }
            multiply(decimal, POWERS_OF_TEN[rescaleFactor], result);
        }
        else {
            scaleDownRoundUp(decimal, -rescaleFactor, result);
        }
    }

    public static Slice rescaleTruncate(Slice decimal, int rescaleFactor)
    {
        if (rescaleFactor == 0) {
            return decimal;
        }
        else {
            Slice result = unscaledDecimal();
            rescaleTruncate(decimal, rescaleFactor, result);
            return result;
        }
    }

    public static void rescaleTruncate(Slice decimal, int rescaleFactor, Slice result)
    {
        if (rescaleFactor == 0) {
            copyUnscaledDecimal(decimal, result);
        }
        else if (rescaleFactor > 0) {
            if (rescaleFactor >= POWERS_OF_TEN.length) {
                throwOverflowException();
            }
            multiply(decimal, POWERS_OF_TEN[rescaleFactor], result);
        }
        else {
            scaleDownTruncate(decimal, -rescaleFactor, result);
        }
    }

    private static void scaleDownTruncate(Slice decimal, int scaleFactor, Slice result)
    {
        // optimized path for smaller values
        long low = getLong(decimal, 0);
        long high = getLong(decimal, 1);
        if (scaleFactor <= MAX_POWER_OF_TEN_LONG && high == 0 && low >= 0) {
            long divisor = longTenToNth(scaleFactor);
            long newLow = low / divisor;
            pack(result, newLow, 0, isNegative(decimal));
            return;
        }

        // Scales down for 10**rescaleFactor.
        // Because divide by int has limited divisor, we choose code path with the least amount of divisions
        if ((scaleFactor - 1) / MAX_POWER_OF_FIVE_INT < (scaleFactor - 1) / MAX_POWER_OF_TEN_INT) {
            // scale down for 10**rescale is equivalent to scaling down with 5**rescaleFactor first, then with 2**rescaleFactor
            scaleDownFive(decimal, scaleFactor, result);
            shiftRightTruncate(result, scaleFactor, result);
        }
        else {
            scaleDownTenTruncate(decimal, scaleFactor, result);
        }
    }

    public static Slice add(Slice left, Slice right)
    {
        Slice result = unscaledDecimal();
        add(left, right, result);
        return result;
    }

    public static void add(Slice left, Slice right, Slice result)
    {
        long overflow = addWithOverflow(left, right, result);
        if (overflow != 0) {
            throwOverflowException();
        }
    }

    /**
     * Instead of throwing overflow exception, this function returns:
     * 0 when there was no overflow
     * +1 when there was overflow
     * -1 when there was underflow
     */
    public static long addWithOverflow(Slice left, Slice right, Slice result)
    {
        boolean leftNegative = isNegative(left);
        boolean rightNegative = isNegative(right);
        long overflow = 0;
        if (leftNegative == rightNegative) {
            // either both negative or both positive
            overflow = addUnsignedReturnOverflow(left, right, result, leftNegative);
            if (leftNegative) {
                overflow = -overflow;
            }
        }
        else {
            int compare = compareAbsolute(left, right);
            if (compare > 0) {
                subtractUnsigned(left, right, result, leftNegative);
            }
            else if (compare < 0) {
                subtractUnsigned(right, left, result, !leftNegative);
            }
            else {
                setToZero(result);
            }
        }
        return overflow;
    }

    public static Slice subtract(Slice left, Slice right)
    {
        Slice result = unscaledDecimal();
        subtract(left, right, result);
        return result;
    }

    public static void subtract(Slice left, Slice right, Slice result)
    {
        if (isNegative(left) ^ isNegative(right)) {
            // only one is negative
            if (addUnsignedReturnOverflow(left, right, result, isNegative(left)) != 0) {
                throwOverflowException();
            }
        }
        else {
            int compare = compareAbsolute(left, right);
            if (compare > 0) {
                subtractUnsigned(left, right, result, isNegative(left) && isNegative(right));
            }
            else if (compare < 0) {
                subtractUnsigned(right, left, result, !(isNegative(left) && isNegative(right)));
            }
            else {
                setToZero(result);
            }
        }
    }

    /**
     * This method ignores signs of the left and right. Returns overflow value.
     */
    private static long addUnsignedReturnOverflow(Slice left, Slice right, Slice result, boolean resultNegative)
    {
        // TODO: consider two 7 bytes operations
        int l0 = getInt(left, 0);
        int l1 = getInt(left, 1);
        int l2 = getInt(left, 2);
        int l3 = getInt(left, 3);

        int r0 = getInt(right, 0);
        int r1 = getInt(right, 1);
        int r2 = getInt(right, 2);
        int r3 = getInt(right, 3);

        long intermediateResult;
        intermediateResult = (l0 & LONG_MASK) + (r0 & LONG_MASK);

        int z0 = (int) intermediateResult;

        intermediateResult = (l1 & LONG_MASK) + (r1 & LONG_MASK) + (intermediateResult >>> 32);

        int z1 = (int) intermediateResult;

        intermediateResult = (l2 & LONG_MASK) + (r2 & LONG_MASK) + (intermediateResult >>> 32);

        int z2 = (int) intermediateResult;

        intermediateResult = (l3 & LONG_MASK) + (r3 & LONG_MASK) + (intermediateResult >>> 32);

        int z3 = (int) intermediateResult & (~SIGN_INT_MASK);

        pack(result, z0, z1, z2, z3, resultNegative);

        return intermediateResult >> 31;
    }

    /**
     * This method ignores signs of the left and right and assumes that left is greater then right
     */
    private static void subtractUnsigned(Slice left, Slice right, Slice result, boolean resultNegative)
    {
        // TODO: consider two 7 bytes operations
        int l0 = getInt(left, 0);
        int l1 = getInt(left, 1);
        int l2 = getInt(left, 2);
        int l3 = getInt(left, 3);

        int r0 = getInt(right, 0);
        int r1 = getInt(right, 1);
        int r2 = getInt(right, 2);
        int r3 = getInt(right, 3);

        long intermediateResult;
        intermediateResult = (l0 & LONG_MASK) - (r0 & LONG_MASK);

        int z0 = (int) intermediateResult;

        intermediateResult = (l1 & LONG_MASK) - (r1 & LONG_MASK) + (intermediateResult >> 32);

        int z1 = (int) intermediateResult;

        intermediateResult = (l2 & LONG_MASK) - (r2 & LONG_MASK) + (intermediateResult >> 32);

        int z2 = (int) intermediateResult;

        intermediateResult = (l3 & LONG_MASK) - (r3 & LONG_MASK) + (intermediateResult >> 32);

        int z3 = (int) intermediateResult;

        pack(result, z0, z1, z2, z3, resultNegative);

        if ((intermediateResult >> 32) != 0) {
            throw new IllegalStateException(format("Non empty carry over after subtracting [%d]. right > left?", (intermediateResult >> 32)));
        }
    }

    public static Slice multiply(Slice left, Slice right)
    {
        Slice result = unscaledDecimal();
        multiply(left, right, result);
        return result;
    }

    public static void multiply(Slice left, Slice right, Slice result)
    {
        checkArgument(result.length() == NUMBER_OF_LONGS * Long.BYTES);

        long l0 = getInt(left, 0) & LONG_MASK;
        long l1 = getInt(left, 1) & LONG_MASK;
        long l2 = getInt(left, 2) & LONG_MASK;
        long l3 = getInt(left, 3) & LONG_MASK;

        long r0 = getInt(right, 0) & LONG_MASK;
        long r1 = getInt(right, 1) & LONG_MASK;
        long r2 = getInt(right, 2) & LONG_MASK;
        long r3 = getInt(right, 3) & LONG_MASK;

        // the combinations below definitely result in an overflow
        if (((r3 != 0 && (l3 | l2 | l1) != 0) || (r2 != 0 && (l3 | l2) != 0) || (r1 != 0 && l3 != 0))) {
            throwOverflowException();
        }

        long z0 = 0;
        long z1 = 0;
        long z2 = 0;
        long z3 = 0;

        if (l0 != 0) {
            long accumulator = r0 * l0;
            z0 = accumulator & LONG_MASK;
            accumulator = (accumulator >>> 32) + r1 * l0;

            z1 = accumulator & LONG_MASK;
            accumulator = (accumulator >>> 32) + r2 * l0;

            z2 = accumulator & LONG_MASK;
            accumulator = (accumulator >>> 32) + r3 * l0;

            z3 = accumulator & LONG_MASK;

            if ((accumulator >>> 32) != 0) {
                throwOverflowException();
            }
        }

        if (l1 != 0) {
            long accumulator = r0 * l1 + z1;
            z1 = accumulator & LONG_MASK;
            accumulator = (accumulator >>> 32) + r1 * l1 + z2;

            z2 = accumulator & LONG_MASK;
            accumulator = (accumulator >>> 32) + r2 * l1 + z3;

            z3 = accumulator & LONG_MASK;

            if ((accumulator >>> 32) != 0) {
                throwOverflowException();
            }
        }

        if (l2 != 0) {
            long accumulator = r0 * l2 + z2;
            z2 = accumulator & LONG_MASK;
            accumulator = (accumulator >>> 32) + r1 * l2 + z3;

            z3 = accumulator & LONG_MASK;

            if ((accumulator >>> 32) != 0) {
                throwOverflowException();
            }
        }

        if (l3 != 0) {
            long accumulator = r0 * l3 + z3;
            z3 = accumulator & LONG_MASK;

            if ((accumulator >>> 32) != 0) {
                throwOverflowException();
            }
        }

        pack(result, (int) z0, (int) z1, (int) z2, (int) z3, isNegative(left) ^ isNegative(right));
    }

    public static void multiply256(Slice left, Slice right, Slice result)
    {
        checkArgument(result.length() >= NUMBER_OF_LONGS * Long.BYTES * 2);

        long l0 = getInt(left, 0) & LONG_MASK;
        long l1 = getInt(left, 1) & LONG_MASK;
        long l2 = getInt(left, 2) & LONG_MASK;
        long l3 = getInt(left, 3) & LONG_MASK;

        long r0 = getInt(right, 0) & LONG_MASK;
        long r1 = getInt(right, 1) & LONG_MASK;
        long r2 = getInt(right, 2) & LONG_MASK;
        long r3 = getInt(right, 3) & LONG_MASK;

        long z0 = 0;
        long z1 = 0;
        long z2 = 0;
        long z3 = 0;
        long z4 = 0;
        long z5 = 0;
        long z6 = 0;
        long z7 = 0;

        if (l0 != 0) {
            long accumulator = r0 * l0;
            z0 = accumulator & LONG_MASK;
            accumulator = (accumulator >>> 32) + r1 * l0;

            z1 = accumulator & LONG_MASK;
            accumulator = (accumulator >>> 32) + r2 * l0;

            z2 = accumulator & LONG_MASK;
            accumulator = (accumulator >>> 32) + r3 * l0;

            z3 = accumulator & LONG_MASK;
            z4 = (accumulator >>> 32) & LONG_MASK;
        }

        if (l1 != 0) {
            long accumulator = r0 * l1 + z1;
            z1 = accumulator & LONG_MASK;
            accumulator = (accumulator >>> 32) + r1 * l1 + z2;

            z2 = accumulator & LONG_MASK;
            accumulator = (accumulator >>> 32) + r2 * l1 + z3;

            z3 = accumulator & LONG_MASK;
            accumulator = (accumulator >>> 32) + r3 * l1 + z4;

            z4 = accumulator & LONG_MASK;
            z5 = (accumulator >>> 32) & LONG_MASK;
        }

        if (l2 != 0) {
            long accumulator = r0 * l2 + z2;
            z2 = accumulator & LONG_MASK;
            accumulator = (accumulator >>> 32) + r1 * l2 + z3;

            z3 = accumulator & LONG_MASK;
            accumulator = (accumulator >>> 32) + r2 * l2 + z4;

            z4 = accumulator & LONG_MASK;
            accumulator = (accumulator >>> 32) + r3 * l2 + z5;

            z5 = accumulator & LONG_MASK;
            z6 = (accumulator >>> 32) & LONG_MASK;
        }

        if (l3 != 0) {
            long accumulator = r0 * l3 + z3;
            z3 = accumulator & LONG_MASK;
            accumulator = (accumulator >>> 32) + r1 * l3 + z4;

            z4 = accumulator & LONG_MASK;
            accumulator = (accumulator >>> 32) + r2 * l3 + z5;

            z5 = accumulator & LONG_MASK;
            accumulator = (accumulator >>> 32) + r3 * l3 + z6;

            z6 = accumulator & LONG_MASK;
            z7 = (accumulator >>> 32) & LONG_MASK;
        }

        setRawInt(result, 0, (int) z0);
        setRawInt(result, 1, (int) z1);
        setRawInt(result, 2, (int) z2);
        setRawInt(result, 3, (int) z3);
        setRawInt(result, 4, (int) z4);
        setRawInt(result, 5, (int) z5);
        setRawInt(result, 6, (int) z6);
        setRawInt(result, 7, (int) z7);
    }

    public static Slice multiply(Slice decimal, int multiplier)
    {
        Slice result = Slices.copyOf(decimal);
        multiplyDestructive(result, multiplier);
        return result;
    }

    private static void multiplyDestructive(Slice decimal, int multiplier)
    {
        long l0 = getInt(decimal, 0) & LONG_MASK;
        long l1 = getInt(decimal, 1) & LONG_MASK;
        long l2 = getInt(decimal, 2) & LONG_MASK;
        long l3 = getInt(decimal, 3) & LONG_MASK;

        long r0 = Math.abs(multiplier) & LONG_MASK;

        long product;

        product = r0 * l0;
        int z0 = (int) product;

        product = r0 * l1 + (product >>> 32);
        int z1 = (int) product;

        product = r0 * l2 + (product >>> 32);
        int z2 = (int) product;

        product = r0 * l3 + (product >>> 32);
        int z3 = (int) product;

        if ((product >>> 32) != 0) {
            throwOverflowException();
        }

        boolean negative = isNegative(decimal) ^ (multiplier < 0);
        pack(decimal, z0, z1, z2, z3, negative);
    }

    public static int compare(Slice left, Slice right)
    {
        boolean leftStrictlyNegative = isStrictlyNegative(left);
        boolean rightStrictlyNegative = isStrictlyNegative(right);

        if (leftStrictlyNegative != rightStrictlyNegative) {
            return leftStrictlyNegative ? -1 : 1;
        }
        else {
            return compareAbsolute(left, right) * (leftStrictlyNegative ? -1 : 1);
        }
    }

    public static int compare(long leftRawLow, long leftRawHigh, long rightRawLow, long rightRawHigh)
    {
        boolean leftStrictlyNegative = isStrictlyNegative(leftRawLow, leftRawHigh);
        boolean rightStrictlyNegative = isStrictlyNegative(rightRawLow, rightRawHigh);

        if (leftStrictlyNegative != rightStrictlyNegative) {
            return leftStrictlyNegative ? -1 : 1;
        }
        else {
            long leftHigh = unpackUnsignedLong(leftRawHigh);
            long rightHigh = unpackUnsignedLong(rightRawHigh);
            return compareUnsigned(leftRawLow, leftHigh, rightRawLow, rightHigh) * (leftStrictlyNegative ? -1 : 1);
        }
    }

    public static int compareAbsolute(Slice left, Slice right)
    {
        long leftHigh = getLong(left, 1);
        long rightHigh = getLong(right, 1);
        if (leftHigh != rightHigh) {
            return Long.compareUnsigned(leftHigh, rightHigh);
        }

        long leftLow = getLong(left, 0);
        long rightLow = getLong(right, 0);
        if (leftLow != rightLow) {
            return Long.compareUnsigned(leftLow, rightLow);
        }

        return 0;
    }

    public static int compareUnsigned(long leftRawLow, long leftRawHigh, long rightRawLow, long rightRawHigh)
    {
        if (leftRawHigh != rightRawHigh) {
            return Long.compareUnsigned(leftRawHigh, rightRawHigh);
        }
        if (leftRawLow != rightRawLow) {
            return Long.compareUnsigned(leftRawLow, rightRawLow);
        }
        return 0;
    }

    public static void incrementUnsafe(Slice decimal)
    {
        long low = getLong(decimal, 0);
        if (low != ALL_BITS_SET_64) {
            setRawLong(decimal, 0, low + 1);
            return;
        }

        long high = getLong(decimal, 1);
        setNegativeLong(decimal, high + 1, isNegative(decimal));
    }

    public static void negate(Slice decimal)
    {
        setNegative(decimal, !isNegative(decimal));
    }

    public static boolean isStrictlyNegative(Slice decimal)
    {
        return isNegative(decimal) && (getLong(decimal, 0) != 0 || getLong(decimal, 1) != 0);
    }

    public static boolean isStrictlyNegative(long rawLow, long rawHigh)
    {
        return isNegative(rawLow, rawHigh) && (rawLow != 0 || unpackUnsignedLong(rawHigh) != 0);
    }

    public static boolean isNegative(Slice decimal)
    {
        return (getRawInt(decimal, SIGN_INT_INDEX) & SIGN_INT_MASK) != 0;
    }

    public static boolean isNegative(long rawLow, long rawHigh)
    {
        return (rawHigh & SIGN_LONG_MASK) != 0;
    }

    public static boolean isZero(Slice decimal)
    {
        return getLong(decimal, 0) == 0 && getLong(decimal, 1) == 0;
    }

    public static long hash(Slice decimal)
    {
        return hash(getRawLong(decimal, 0), getRawLong(decimal, 1));
    }

    public static long hash(long rawLow, long rawHigh)
    {
        return XxHash64.hash(rawLow) ^ XxHash64.hash(unpackUnsignedLong(rawHigh));
    }

    public static String toUnscaledString(Slice decimal)
    {
        if (isZero(decimal)) {
            return "0";
        }

        char[] buffer = new char[MAX_PRECISION + 1];
        int index = buffer.length;
        boolean negative = isNegative(decimal);
        decimal = unscaledDecimal(decimal);
        do {
            int remainder = divide(decimal, 10, decimal);
            buffer[--index] = (char) ('0' + remainder);
        }
        while (!isZero(decimal));

        if (negative) {
            buffer[--index] = '-';
        }

        return new String(buffer, index, buffer.length - index);
    }

    public static boolean overflows(Slice value, int precision)
    {
        if (precision == MAX_PRECISION) {
            return exceedsOrEqualTenToThirtyEight(value);
        }
        return precision < MAX_PRECISION && compareAbsolute(value, POWERS_OF_TEN[precision]) >= 0;
    }

    public static void throwIfOverflows(Slice decimal)
    {
        if (exceedsOrEqualTenToThirtyEight(decimal)) {
            throwOverflowException();
        }
    }

    public static void throwIfOverflows(Slice value, int precision)
    {
        if (overflows(value, precision)) {
            throwOverflowException();
        }
    }

    private static void scaleDownRoundUp(Slice decimal, int scaleFactor, Slice result)
    {
        // optimized path for smaller values
        long low = getLong(decimal, 0);
        long high = getLong(decimal, 1);
        if (scaleFactor <= MAX_POWER_OF_TEN_LONG && high == 0 && low >= 0) {
            long divisor = longTenToNth(scaleFactor);
            long newLow = low / divisor;
            if (low % divisor >= (divisor >> 1)) {
                newLow++;
            }
            pack(result, newLow, 0, isNegative(decimal));
            return;
        }

        // Scales down for 10**rescaleFactor.
        // Because divide by int has limited divisor, we choose code path with the least amount of divisions
        if ((scaleFactor - 1) / MAX_POWER_OF_FIVE_INT < (scaleFactor - 1) / MAX_POWER_OF_TEN_INT) {
            // scale down for 10**rescale is equivalent to scaling down with 5**rescaleFactor first, then with 2**rescaleFactor
            scaleDownFive(decimal, scaleFactor, result);
            shiftRightRoundUp(result, scaleFactor, result);
        }
        else {
            scaleDownTenRoundUp(decimal, scaleFactor, result);
        }
    }

    /**
     * Scale down the value for 5**fiveScale (result := decimal / 5**fiveScale).
     */
    private static void scaleDownFive(Slice decimal, int fiveScale, Slice result)
    {
        while (true) {
            int powerFive = Math.min(fiveScale, MAX_POWER_OF_FIVE_INT);
            fiveScale -= powerFive;

            int divisor = POWERS_OF_FIVES_INT[powerFive];
            divide(decimal, divisor, result);
            decimal = result;

            if (fiveScale == 0) {
                return;
            }
        }
    }

    /**
     * Scale up the value for 5**fiveScale (decimal := decimal * 5**fiveScale).
     */
    private static void scaleUpFiveDestructive(Slice decimal, int fiveScale)
    {
        while (fiveScale > 0) {
            int powerFive = Math.min(fiveScale, MAX_POWER_OF_FIVE_INT);
            fiveScale -= powerFive;
            int multiplier = POWERS_OF_FIVES_INT[powerFive];
            multiplyDestructive(decimal, multiplier);
        }
    }

    /**
     * Scale down the value for 10**tenScale (this := this / 5**tenScale). This
     * method rounds-up, eg 44/10=4, 44/10=5.
     */
    private static void scaleDownTenRoundUp(Slice decimal, int tenScale, Slice result)
    {
        boolean round;
        do {
            int powerTen = Math.min(tenScale, MAX_POWER_OF_TEN_INT);
            tenScale -= powerTen;

            int divisor = POWERS_OF_TEN_INT[powerTen];
            round = divideCheckRound(decimal, divisor, result);
            decimal = result;
        }
        while (tenScale > 0);

        if (round) {
            incrementUnsafe(decimal);
        }
    }

    private static void scaleDownTenTruncate(Slice decimal, int tenScale, Slice result)
    {
        do {
            int powerTen = Math.min(tenScale, MAX_POWER_OF_TEN_INT);
            tenScale -= powerTen;

            int divisor = POWERS_OF_TEN_INT[powerTen];
            divide(decimal, divisor, result);
            decimal = result;
        }
        while (tenScale > 0);
    }

    private static void shiftRightRoundUp(Slice decimal, int rightShifts, Slice result)
    {
        shiftRight(decimal, rightShifts, true, result);
    }

    private static void shiftRightTruncate(Slice decimal, int rightShifts, Slice result)
    {
        shiftRight(decimal, rightShifts, false, result);
    }

    // visible for testing
    static void shiftRight(Slice decimal, int rightShifts, boolean roundUp, Slice result)
    {
        if (rightShifts == 0) {
            copyUnscaledDecimal(decimal, result);
            return;
        }

        int wordShifts = rightShifts / 64;
        int bitShiftsInWord = rightShifts % 64;
        int shiftRestore = 64 - bitShiftsInWord;

        // check round-ups before settings values to result.
        // be aware that result could be the same object as decimal.
        boolean roundCarry;
        if (bitShiftsInWord == 0) {
            roundCarry = roundUp && getLong(decimal, wordShifts - 1) < 0;
        }
        else {
            roundCarry = roundUp && (getLong(decimal, wordShifts) & (1L << (bitShiftsInWord - 1))) != 0;
        }

        // Store negative before settings values to result.
        boolean negative = isNegative(decimal);

        long low;
        long high;

        switch (wordShifts) {
            case 0:
                low = getLong(decimal, 0);
                high = getLong(decimal, 1);
                break;
            case 1:
                low = getLong(decimal, 1);
                high = 0;
                break;
            default:
                throw new IllegalArgumentException();
        }

        if (bitShiftsInWord > 0) {
            low = (low >>> bitShiftsInWord) | (high << shiftRestore);
            high = (high >>> bitShiftsInWord);
        }

        if (roundCarry) {
            if (low != ALL_BITS_SET_64) {
                low++;
            }
            else {
                low = 0;
                high++;
            }
        }

        pack(result, low, high, negative);
    }

    /**
     * shift right array of 8 ints (rounding up) and ensure that result fits in unscaledDecimal
     */
    public static void shiftRightArray8(int[] values, int rightShifts, Slice result)
    {
        if (values.length != NUMBER_OF_INTS * 2) {
            throw new IllegalArgumentException("Incorrect values length");
        }
        if (rightShifts == 0) {
            for (int i = NUMBER_OF_INTS; i < 2 * NUMBER_OF_INTS; i++) {
                if (values[i] != 0) {
                    throwOverflowException();
                }
            }
            for (int i = 0; i < NUMBER_OF_INTS; i++) {
                setRawInt(result, i, values[i]);
            }
            return;
        }

        int wordShifts = rightShifts / 32;
        int bitShiftsInWord = rightShifts % 32;
        int shiftRestore = 32 - bitShiftsInWord;

        // check round-ups before settings values to result.
        // be aware that result could be the same object as decimal.
        boolean roundCarry;
        if (bitShiftsInWord == 0) {
            roundCarry = values[wordShifts - 1] < 0;
        }
        else {
            roundCarry = (values[wordShifts] & (1 << (bitShiftsInWord - 1))) != 0;
        }

        int r0 = values[0 + wordShifts];
        int r1 = values[1 + wordShifts];
        int r2 = values[2 + wordShifts];
        int r3 = values[3 + wordShifts];
        int r4 = wordShifts >= 4 ? 0 : values[4 + wordShifts];
        int r5 = wordShifts >= 3 ? 0 : values[5 + wordShifts];
        int r6 = wordShifts >= 2 ? 0 : values[6 + wordShifts];
        int r7 = wordShifts >= 1 ? 0 : values[7 + wordShifts];

        if (bitShiftsInWord > 0) {
            r0 = (r0 >>> bitShiftsInWord) | (r1 << shiftRestore);
            r1 = (r1 >>> bitShiftsInWord) | (r2 << shiftRestore);
            r2 = (r2 >>> bitShiftsInWord) | (r3 << shiftRestore);
            r3 = (r3 >>> bitShiftsInWord) | (r4 << shiftRestore);
        }

        if ((r4 >>> bitShiftsInWord) != 0 || r5 != 0 || r6 != 0 || r7 != 0) {
            throwOverflowException();
        }

        if (r3 < 0) {
            throwOverflowException();
        }

        // increment
        if (roundCarry) {
            r0++;
            if (r0 == 0) {
                r1++;
                if (r1 == 0) {
                    r2++;
                    if (r2 == 0) {
                        r3++;
                        if (r3 < 0) {
                            throwOverflowException();
                        }
                    }
                }
            }
        }

        pack(result, r0, r1, r2, r3, false);
    }

    public static Slice divideRoundUp(long dividend, int dividendScaleFactor, long divisor)
    {
        return divideRoundUp(
                Math.abs(dividend), dividend < 0 ? SIGN_LONG_MASK : 0, dividendScaleFactor,
                Math.abs(divisor), divisor < 0 ? SIGN_LONG_MASK : 0);
    }

    public static Slice divideRoundUp(long dividend, int dividendScaleFactor, Slice divisor)
    {
        return divideRoundUp(
                Math.abs(dividend), dividend < 0 ? SIGN_LONG_MASK : 0, dividendScaleFactor,
                getRawLong(divisor, 0), getRawLong(divisor, 1));
    }

    public static Slice divideRoundUp(Slice dividend, int dividendScaleFactor, long divisor)
    {
        return divideRoundUp(
                getRawLong(dividend, 0), getRawLong(dividend, 1), dividendScaleFactor,
                Math.abs(divisor), divisor < 0 ? SIGN_LONG_MASK : 0);
    }

    public static Slice divideRoundUp(Slice dividend, int dividendScaleFactor, Slice divisor)
    {
        return divideRoundUp(
                getRawLong(dividend, 0), getRawLong(dividend, 1), dividendScaleFactor,
                getRawLong(divisor, 0), getRawLong(divisor, 1));
    }

    private static Slice divideRoundUp(long dividendLow, long dividendHigh, int dividendScaleFactor, long divisorLow, long divisorHigh)
    {
        Slice quotient = unscaledDecimal();
        Slice remainder = unscaledDecimal();
        divide(dividendLow, dividendHigh, dividendScaleFactor, divisorLow, divisorHigh, 0, quotient, remainder);

        // round
        boolean quotientIsNegative = isNegative(quotient);
        setNegative(quotient, false);
        setNegative(remainder, false);

        // if (2 * remainder >= divisor) - increment quotient by one
        shiftLeftDestructive(remainder, 1);
        long remainderLow = getRawLong(remainder, 0);
        long remainderHigh = getRawLong(remainder, 1);
        long divisorHighUnsigned = unpackUnsignedLong(divisorHigh);
        if (compareUnsigned(remainderLow, remainderHigh, divisorLow, divisorHighUnsigned) >= 0) {
            incrementUnsafe(quotient);
            throwIfOverflows(quotient);
        }

        setNegative(quotient, quotientIsNegative);
        return quotient;
    }

    // visible for testing
    static Slice shiftLeft(Slice decimal, int leftShifts)
    {
        Slice result = Slices.copyOf(decimal);
        shiftLeftDestructive(result, leftShifts);
        return result;
    }

    // visible for testing
    static void shiftLeftDestructive(Slice decimal, int leftShifts)
    {
        if (leftShifts == 0) {
            return;
        }

        int wordShifts = leftShifts / 64;
        int bitShiftsInWord = leftShifts % 64;
        int shiftRestore = 64 - bitShiftsInWord;

        // check overflow
        if (bitShiftsInWord != 0) {
            if ((getLong(decimal, 1 - wordShifts) & (-1L << shiftRestore)) != 0) {
                throwOverflowException();
            }
        }
        if (wordShifts == 1) {
            if (getLong(decimal, 1) != 0) {
                throwOverflowException();
            }
        }

        // Store negative before settings values to result.
        boolean negative = isNegative(decimal);

        long low;
        long high;

        switch (wordShifts) {
            case 0:
                low = getLong(decimal, 0);
                high = getLong(decimal, 1);
                break;
            case 1:
                low = 0;
                high = getLong(decimal, 0);
                break;
            default:
                throw new IllegalArgumentException();
        }

        if (bitShiftsInWord > 0) {
            high = (high << bitShiftsInWord) | (low >>> shiftRestore);
            low = (low << bitShiftsInWord);
        }

        pack(decimal, low, high, negative);
    }

    public static Slice remainder(long dividend, int dividendScaleFactor, long divisor, int divisorScaleFactor)
    {
        return remainder(
                Math.abs(dividend), dividend < 0 ? SIGN_LONG_MASK : 0, dividendScaleFactor,
                Math.abs(divisor), divisor < 0 ? SIGN_LONG_MASK : 0, divisorScaleFactor);
    }

    public static Slice remainder(long dividend, int dividendScaleFactor, Slice divisor, int divisorScaleFactor)
    {
        return remainder(
                Math.abs(dividend), dividend < 0 ? SIGN_LONG_MASK : 0, dividendScaleFactor,
                getRawLong(divisor, 0), getRawLong(divisor, 1), divisorScaleFactor);
    }

    public static Slice remainder(Slice dividend, int dividendScaleFactor, long divisor, int divisorScaleFactor)
    {
        return remainder(
                getRawLong(dividend, 0), getRawLong(dividend, 1), dividendScaleFactor,
                Math.abs(divisor), divisor < 0 ? SIGN_LONG_MASK : 0, divisorScaleFactor);
    }

    public static Slice remainder(Slice dividend, int dividendScaleFactor, Slice divisor, int divisorScaleFactor)
    {
        return remainder(
                getRawLong(dividend, 0), getRawLong(dividend, 1), dividendScaleFactor,
                getRawLong(divisor, 0), getRawLong(divisor, 1), divisorScaleFactor);
    }

    private static Slice remainder(long dividendLow, long dividendHigh, int dividendScaleFactor, long divisorLow, long divisorHigh, int divisorScaleFactor)
    {
        Slice quotient = unscaledDecimal();
        Slice remainder = unscaledDecimal();
        divide(dividendLow, dividendHigh, dividendScaleFactor, divisorLow, divisorHigh, divisorScaleFactor, quotient, remainder);
        return remainder;
    }

    // visible for testing
    static void divide(Slice dividend, int dividendScaleFactor, Slice divisor, int divisorScaleFactor, Slice quotient, Slice remainder)
    {
        divide(getRawLong(dividend, 0), getRawLong(dividend, 1), dividendScaleFactor, getRawLong(divisor, 0), getRawLong(divisor, 1), divisorScaleFactor, quotient, remainder);
    }

    private static void divide(long dividendLow, long dividendHigh, int dividendScaleFactor, long divisorLow, long divisorHigh, int divisorScaleFactor, Slice quotient, Slice remainder)
    {
        if (compare(divisorLow, divisorHigh, 0, 0) == 0) {
            throwDivisionByZeroException();
        }

        if (dividendScaleFactor >= MAX_PRECISION) {
            throwOverflowException();
        }

        if (divisorScaleFactor >= MAX_PRECISION) {
            throwOverflowException();
        }

        boolean dividendIsNegative = isNegative(dividendLow, dividendHigh);
        boolean divisorIsNegative = isNegative(divisorLow, divisorHigh);
        boolean quotientIsNegative = dividendIsNegative ^ divisorIsNegative;

        // to fit 128b * 128b * 32b unsigned multiplication
        int[] dividend = new int[NUMBER_OF_INTS * 2 + 1];
        dividend[0] = lowInt(dividendLow);
        dividend[1] = highInt(dividendLow);
        dividend[2] = lowInt(dividendHigh);
        dividend[3] = (highInt(dividendHigh) & ~SIGN_INT_MASK);

        if (dividendScaleFactor > 0) {
            Slice sliceDividend = Slices.wrappedIntArray(dividend);
            multiply256(POWERS_OF_FIVE[dividendScaleFactor], sliceDividend, sliceDividend);
            shiftLeftMultiPrecision(dividend, NUMBER_OF_INTS * 2, dividendScaleFactor);
        }

        int[] divisor = new int[NUMBER_OF_INTS * 2];
        divisor[0] = lowInt(divisorLow);
        divisor[1] = highInt(divisorLow);
        divisor[2] = lowInt(divisorHigh);
        divisor[3] = (highInt(divisorHigh) & ~SIGN_INT_MASK);

        if (divisorScaleFactor > 0) {
            Slice sliceDivisor = Slices.wrappedIntArray(divisor);
            multiply256(POWERS_OF_FIVE[divisorScaleFactor], sliceDivisor, sliceDivisor);
            shiftLeftMultiPrecision(divisor, NUMBER_OF_INTS * 2, divisorScaleFactor);
        }

        int[] multiPrecisionQuotient = new int[NUMBER_OF_INTS * 2];
        divideUnsignedMultiPrecision(dividend, divisor, multiPrecisionQuotient);

        packUnsigned(multiPrecisionQuotient, quotient);
        packUnsigned(dividend, remainder);
        setNegative(quotient, quotientIsNegative);
        setNegative(remainder, dividendIsNegative);
        throwIfOverflows(quotient);
        throwIfOverflows(remainder);
    }

    /**
     * Divides mutableDividend / mutable divisor
     * Places quotient in first argument and reminder in first argument
     */
    private static void divideUnsignedMultiPrecision(int[] dividend, int[] divisor, int[] quotient)
    {
        checkArgument(dividend.length == NUMBER_OF_INTS * 2 + 1);
        checkArgument(divisor.length == NUMBER_OF_INTS * 2);
        checkArgument(quotient.length == NUMBER_OF_INTS * 2);

        int divisorLength = digitsInIntegerBase(divisor);
        int dividendLength = digitsInIntegerBase(dividend);

        if (dividendLength < divisorLength) {
            return;
        }

        if (divisorLength == 1) {
            int remainder = divideUnsignedMultiPrecision(dividend, dividendLength, divisor[0]);
            checkState(dividend[dividend.length - 1] == 0);
            arraycopy(dividend, 0, quotient, 0, quotient.length);
            fill(dividend, 0);
            dividend[0] = remainder;
            return;
        }

        // normalize divisor. Most significant divisor word must be > BASE/2
        // effectively it can be achieved by shifting divisor left until the leftmost bit is 1
        int nlz = Integer.numberOfLeadingZeros(divisor[divisorLength - 1]);
        shiftLeftMultiPrecision(divisor, divisorLength, nlz);
        int normalizedDividendLength = Math.min(dividend.length, dividendLength + 1);
        shiftLeftMultiPrecision(dividend, normalizedDividendLength, nlz);

        divideKnuthNormalized(dividend, normalizedDividendLength, divisor, divisorLength, quotient);

        // un-normalize remainder which is stored in dividend
        shiftRightMultiPrecision(dividend, normalizedDividendLength, nlz);
    }

    private static void divideKnuthNormalized(int[] remainder, int dividendLength, int[] divisor, int divisorLength, int[] quotient)
    {
        int v1 = divisor[divisorLength - 1];
        int v0 = divisor[divisorLength - 2];
        for (int reminderIndex = dividendLength - 1; reminderIndex >= divisorLength; reminderIndex--) {
            int qHat = estimateQuotient(remainder[reminderIndex], remainder[reminderIndex - 1], remainder[reminderIndex - 2], v1, v0);
            if (qHat != 0) {
                boolean overflow = multiplyAndSubtractUnsignedMultiPrecision(remainder, reminderIndex, divisor, divisorLength, qHat);
                // Add back - probability is 2**(-31). R += D. Q[digit] -= 1
                if (overflow) {
                    qHat--;
                    addUnsignedMultiPrecision(remainder, reminderIndex, divisor, divisorLength);
                }
            }
            quotient[reminderIndex - divisorLength] = qHat;
        }
    }

    /**
     * Use the Knuth notation
     * <p>
     * u{x} - dividend
     * v{v} - divisor
     */
    private static int estimateQuotient(int u2, int u1, int u0, int v1, int v0)
    {
        // estimate qhat based on the first 2 digits of divisor divided by the first digit of a dividend
        long u21 = combineInts(u2, u1);
        long qhat;
        if (u2 == v1) {
            qhat = INT_BASE - 1;
        }
        else if (u21 >= 0) {
            qhat = u21 / (v1 & LONG_MASK);
        }
        else {
            qhat = divideUnsignedLong(u21, v1);
        }

        if (qhat == 0) {
            return 0;
        }

        // Check if qhat is greater than expected considering only first 3 digits of a dividend
        // This step help to eliminate all the cases when the estimation is greater than q by 2
        // and eliminates most of the cases when qhat is greater than q by 1
        //
        // u2 * b * b + u1 * b + u0 >= (v1 * b + v0) * qhat
        // u2 * b * b + u1 * b + u0 >= v1 * b * qhat + v0 * qhat
        // u2 * b * b + u1 * b - v1 * b * qhat >=  v0 * qhat - u0
        // (u21 - v1 * qhat) * b >=  v0 * qhat - u0
        // (u21 - v1 * qhat) * b + u0 >=  v0 * qhat
        // When ((u21 - v1 * qhat) * b + u0) is less than (v0 * qhat) decrease qhat by one

        int iterations = 0;
        long rhat = u21 - (v1 & LONG_MASK) * qhat;
        while (Long.compareUnsigned(rhat, INT_BASE) < 0 && Long.compareUnsigned((v0 & LONG_MASK) * qhat, combineInts(lowInt(rhat), u0)) > 0) {
            iterations++;
            qhat--;
            rhat += (v1 & LONG_MASK);
        }

        if (iterations > 2) {
            throw new IllegalStateException("qhat is greater than q by more than 2: " + iterations);
        }

        return (int) qhat;
    }

    private static long divideUnsignedLong(long dividend, int divisor)
    {
        if (divisor == 1) {
            return dividend;
        }
        long unsignedDivisor = divisor & LONG_MASK;
        long quotient = (dividend >>> 1) / (unsignedDivisor >>> 1);
        long remainder = dividend - quotient * unsignedDivisor;
        while (remainder < 0) {
            remainder += unsignedDivisor;
            quotient--;
        }
        while (remainder >= unsignedDivisor) {
            remainder -= unsignedDivisor;
            quotient++;
        }
        return quotient;
    }

    /**
     * Calculate multi-precision [left - right * multiplier] with given left offset and length.
     * Return true when overflow occurred
     */
    private static boolean multiplyAndSubtractUnsignedMultiPrecision(int[] left, int leftOffset, int[] right, int length, int multiplier)
    {
        long unsignedMultiplier = multiplier & LONG_MASK;
        int leftIndex = leftOffset - length;
        long multiplyAccumulator = 0;
        long subtractAccumulator = INT_BASE;
        for (int rightIndex = 0; rightIndex < length; rightIndex++, leftIndex++) {
            multiplyAccumulator = (right[rightIndex] & LONG_MASK) * unsignedMultiplier + multiplyAccumulator;
            subtractAccumulator = (subtractAccumulator + (left[leftIndex] & LONG_MASK)) - (lowInt(multiplyAccumulator) & LONG_MASK);
            multiplyAccumulator = (multiplyAccumulator >>> 32);
            left[leftIndex] = lowInt(subtractAccumulator);
            subtractAccumulator = (subtractAccumulator >>> 32) + INT_BASE - 1;
        }
        subtractAccumulator += (left[leftIndex] & LONG_MASK) - multiplyAccumulator;
        left[leftIndex] = lowInt(subtractAccumulator);
        return highInt(subtractAccumulator) == 0;
    }

    private static void addUnsignedMultiPrecision(int[] left, int leftOffset, int[] right, int length)
    {
        int leftIndex = leftOffset - length;
        int carry = 0;
        for (int rightIndex = 0; rightIndex < length; rightIndex++, leftIndex++) {
            long accumulator = (left[leftIndex] & LONG_MASK) + (right[rightIndex] & LONG_MASK) + (carry & LONG_MASK);
            left[leftIndex] = lowInt(accumulator);
            carry = highInt(accumulator);
        }
        left[leftIndex] += carry;
    }

    // visible for testing
    static int[] shiftLeftMultiPrecision(int[] number, int length, int shifts)
    {
        if (shifts == 0) {
            return number;
        }
        // wordShifts = shifts / 32
        int wordShifts = shifts >>> 5;
        // we don't wan't to loose any leading bits
        for (int i = 0; i < wordShifts; i++) {
            checkState(number[length - i - 1] == 0);
        }
        if (wordShifts > 0) {
            arraycopy(number, 0, number, wordShifts, length - wordShifts);
            fill(number, 0, wordShifts, 0);
        }
        // bitShifts = shifts % 32
        int bitShifts = shifts & 0b11111;
        if (bitShifts > 0) {
            // we don't wan't to loose any leading bits
            checkState(number[length - 1] >>> (Integer.SIZE - bitShifts) == 0);
            for (int position = length - 1; position > 0; position--) {
                number[position] = (number[position] << bitShifts) | (number[position - 1] >>> (Integer.SIZE - bitShifts));
            }
            number[0] = number[0] << bitShifts;
        }
        return number;
    }

    // visible for testing
    static int[] shiftRightMultiPrecision(int[] number, int length, int shifts)
    {
        if (shifts == 0) {
            return number;
        }
        // wordShifts = shifts / 32
        int wordShifts = shifts >>> 5;
        // we don't wan't to loose any trailing bits
        for (int i = 0; i < wordShifts; i++) {
            checkState(number[i] == 0);
        }
        if (wordShifts > 0) {
            arraycopy(number, wordShifts, number, 0, length - wordShifts);
            fill(number, length - wordShifts, length, 0);
        }
        // bitShifts = shifts % 32
        int bitShifts = shifts & 0b11111;
        if (bitShifts > 0) {
            // we don't wan't to loose any trailing bits
            checkState(number[0] << (Integer.SIZE - bitShifts) == 0);
            for (int position = 0; position < length - 1; position++) {
                number[position] = (number[position] >>> bitShifts) | (number[position + 1] << (Integer.SIZE - bitShifts));
            }
            number[length - 1] = number[length - 1] >>> bitShifts;
        }
        return number;
    }

    private static int divideUnsignedMultiPrecision(int[] dividend, int dividendLength, int divisor)
    {
        if (divisor == 0) {
            throwDivisionByZeroException();
        }

        if (dividendLength == 1) {
            long dividendUnsigned = dividend[0] & LONG_MASK;
            long divisorUnsigned = divisor & LONG_MASK;
            long quotient = dividendUnsigned / divisorUnsigned;
            long remainder = dividendUnsigned - (divisorUnsigned * quotient);
            dividend[0] = (int) quotient;
            return (int) remainder;
        }

        long divisorUnsigned = divisor & LONG_MASK;
        long remainder = 0;
        for (int dividendIndex = dividendLength - 1; dividendIndex >= 0; dividendIndex--) {
            remainder = (remainder << 32) + (dividend[dividendIndex] & LONG_MASK);
            long quotient = remainder / divisorUnsigned;
            dividend[dividendIndex] = (int) quotient;
            remainder = remainder - (quotient * divisorUnsigned);
        }
        return (int) remainder;
    }

    private static int digitsInIntegerBase(int[] digits)
    {
        int length = digits.length;
        while (length > 0 && digits[length - 1] == 0) {
            length--;
        }
        return length;
    }

    private static long combineInts(int high, int low)
    {
        return ((high & LONG_MASK) << 32L) | (low & LONG_MASK);
    }

    private static int highInt(long val)
    {
        return (int) (val >>> 32);
    }

    private static int lowInt(long val)
    {
        return (int) val;
    }

    private static void packUnsigned(int[] digits, Slice decimal)
    {
        if (digitsInIntegerBase(digits) > NUMBER_OF_INTS) {
            throwOverflowException();
        }
        if ((digits[3] & SIGN_INT_MASK) != 0) {
            throwOverflowException();
        }
        pack(decimal, digits[0], digits[1], digits[2], digits[3], false);
    }

    private static boolean divideCheckRound(Slice decimal, int divisor, Slice result)
    {
        int remainder = divide(decimal, divisor, result);
        return (remainder >= (divisor >> 1));
    }

    // visible for testing
    static int divide(Slice decimal, int divisor, Slice result)
    {
        if (divisor == 0) {
            throwDivisionByZeroException();
        }
        checkArgument(divisor > 0);

        long remainder = getLong(decimal, 1);
        long high = remainder / divisor;
        remainder %= divisor;

        remainder = (getInt(decimal, 1) & LONG_MASK) + (remainder << 32);
        int z1 = (int) (remainder / divisor);
        remainder %= divisor;

        remainder = (getInt(decimal, 0) & LONG_MASK) + (remainder << 32);
        int z0 = (int) (remainder / divisor);

        pack(result, z0, z1, high, isNegative(decimal));
        return (int) (remainder % divisor);
    }

    private static void throwDivisionByZeroException()
    {
        throw new ArithmeticException("Division by zero");
    }

    private static void multiplyShiftDestructive(Slice decimal, Slice multiplier, int rightShifts)
    {
        int[] product = new int[NUMBER_OF_INTS * 2];
        Slice multiplicationResult = Slices.wrappedIntArray(product);
        multiply256(decimal, multiplier, multiplicationResult);
        shiftRightArray8(product, rightShifts, decimal);
    }

    private static void setNegative(Slice decimal, boolean negative)
    {
        setRawInt(decimal, SIGN_INT_INDEX, getInt(decimal, SIGN_INT_INDEX) | (negative ? SIGN_INT_MASK : 0));
    }

    private static void setNegativeInt(Slice decimal, int v3, boolean negative)
    {
        if (v3 < 0) {
            throwOverflowException();
        }
        setRawInt(decimal, SIGN_INT_INDEX, v3 | (negative ? SIGN_INT_MASK : 0));
    }

    private static void setNegativeLong(Slice decimal, long high, boolean negative)
    {
        setRawLong(decimal, SIGN_LONG_INDEX, high | (negative ? SIGN_LONG_MASK : 0));
    }

    private static void copyUnscaledDecimal(Slice from, Slice to)
    {
        setRawLong(to, 0, getRawLong(from, 0));
        setRawLong(to, 1, getRawLong(from, 1));
    }

    private static void pack(Slice decimal, int v0, int v1, int v2, int v3, boolean negative)
    {
        setRawInt(decimal, 0, v0);
        setRawInt(decimal, 1, v1);
        setRawInt(decimal, 2, v2);
        setNegativeInt(decimal, v3, negative);
    }

    private static void pack(Slice decimal, int v0, int v1, long high, boolean negative)
    {
        setRawInt(decimal, 0, v0);
        setRawInt(decimal, 1, v1);
        setNegativeLong(decimal, high, negative);
    }

    private static void pack(Slice decimal, long low, long high, boolean negative)
    {
        setRawLong(decimal, 0, low);
        setNegativeLong(decimal, high, negative);
    }

    public static Slice pack(long low, long high, boolean negative)
    {
        Slice decimal = unscaledDecimal();
        pack(low, high, negative, decimal);
        return decimal;
    }

    public static void pack(long low, long high, boolean negative, Slice result)
    {
        setRawLong(result, 0, low);
        setRawLong(result, 1, high | (negative ? SIGN_LONG_MASK : 0));
    }

    public static void throwOverflowException()
    {
        throw new ArithmeticException("Decimal overflow");
    }

    public static int getInt(Slice decimal, int index)
    {
        int value = getRawInt(decimal, index);
        if (index == SIGN_INT_INDEX) {
            value &= ~SIGN_INT_MASK;
        }
        return value;
    }

    public static long getLong(Slice decimal, int index)
    {
        long value = getRawLong(decimal, index);
        if (index == SIGN_LONG_INDEX) {
            return unpackUnsignedLong(value);
        }
        return value;
    }

    private static boolean exceedsOrEqualTenToThirtyEight(Slice decimal)
    {
        // 10**38=
        // i0 = 0(0), i1 = 160047680(98a22400), i2 = 1518781562(5a86c47a), i3 = 1262177448(4b3b4ca8)
        // low = 0x98a2240000000000l, high = 0x4b3b4ca85a86c47al
        long high = getLong(decimal, 1);
        if (high >= 0 && high < 0x4b3b4ca85a86c47aL) {
            return false;
        }
        else if (high != 0x4b3b4ca85a86c47aL) {
            return true;
        }

        long low = getLong(decimal, 0);
        return low < 0 || low >= 0x098a224000000000L;
    }

    private static byte[] reverse(final byte[] a)
    {
        final int length = a.length;
        for (int i = length / 2; i-- != 0; ) {
            final byte t = a[length - i - 1];
            a[length - i - 1] = a[i];
            a[i] = t;
        }
        return a;
    }

    private static void setToZero(Slice decimal)
    {
        for (int i = 0; i < NUMBER_OF_LONGS; i++) {
            setRawLong(decimal, i, 0);
        }
    }

    private static long unpackUnsignedLong(long value)
    {
        return value & ~SIGN_LONG_MASK;
    }

    private static int getRawInt(Slice decimal, int index)
    {
        return unsafe.getInt(decimal.getBase(), decimal.getAddress() + SIZE_OF_INT * index);
    }

    private static void setRawInt(Slice decimal, int index, int value)
    {
        unsafe.putInt(decimal.getBase(), decimal.getAddress() + SIZE_OF_INT * index, value);
    }

    private static long getRawLong(Slice decimal, int index)
    {
        return unsafe.getLong(decimal.getBase(), decimal.getAddress() + SIZE_OF_LONG * index);
    }

    private static void setRawLong(Slice decimal, int index, long value)
    {
        unsafe.putLong(decimal.getBase(), decimal.getAddress() + SIZE_OF_LONG * index, value);
    }

    private static void checkArgument(boolean condition)
    {
        if (!condition) {
            throw new IllegalArgumentException();
        }
    }

    private static void checkState(boolean condition)
    {
        if (!condition) {
            throw new IllegalStateException();
        }
    }

    private UnscaledDecimal128Arithmetic() {}

    public static Slice doubleToLongDecimal(double value, long precision, int scale)
    {
        if (Double.isInfinite(value) || Double.isNaN(value)) {
            throwOverflowException();
        }

        // Translate the double into sign, exponent and significand, according
        // to the formulae in JLS, Section 20.10.22.
        long valBits = Double.doubleToLongBits(value);
        int sign = ((valBits >> 63) == 0 ? 1 : -1);
        int exponent = (int) ((valBits >> 52) & 0x7ffL);
        long significand = (exponent == 0
                ? (valBits & ((1L << 52) - 1)) << 1
                : (valBits & ((1L << 52) - 1)) | (1L << 52));
        exponent -= 1075;
        // At this point, value == sign * significand * 2**exponent.

        // zero check
        if (significand == 0) {
            return unscaledDecimal();
        }

        // Normalize
        while ((significand & 1) == 0) { // i.e., significand is even
            significand >>= 1;
            exponent++;
        }

        // so far same as java.math.BigDecimal, but the scaling below is
        // specific to ANSI SQL Numeric.

        // first, underflow is NOT an error in ANSI SQL Numeric.
        // CAST(0.000000000....0001 AS DECIMAL(38,1)) is "0.0" without an error.

        // second, overflow IS an error in ANSI SQL Numeric.
        // CAST(10000 AS DECIMAL(38,38)) throws overflow error.

        // value == sign * significand * 2**exponent.
        // decimal == sign * unscaledValue / 10**scale.
        // so, to make value == decimal, we need to scale it up/down such that:
        // unscaledValue = significand * 2**exponent * 10**scale
        // Notice that we must do the scaling carefully to check overflow and
        // preserve precision.

        Slice unscaledValue = UnscaledDecimal128Arithmetic.unscaledDecimal(significand);

        if (exponent >= 0) {
            // both parts are scaling up. easy. Just check overflow.
            shiftLeftDestructive(unscaledValue, exponent);
            rescale(unscaledValue, scale, unscaledValue);
        }
        else {
            // 2**exponent part is scaling down while 10**scale is scaling up.
            // Now it's tricky.
            // unscaledValue = significand * 10**scale / 2**twoScaleDown
            short twoScaleDown = (short) -exponent;

            if (scale >= twoScaleDown) {
                // make both scaling up as follows
                // unscaledValue = significand * 5**(scale) *
                // 2**(scale-twoScaleDown)
                shiftLeftDestructive(unscaledValue, scale - twoScaleDown);
                scaleUpFiveDestructive(unscaledValue, scale);
            }
            else {
                // Gosh, really both scaling up and down.
                // unscaledValue = significand * 5**(scale) /
                // 2**(twoScaleDown-scale)
                // To check overflow while preserving precision, we need to do a
                // real multiplication

                multiplyShiftDestructive(unscaledValue, POWERS_OF_FIVE[scale], (twoScaleDown - scale));
            }
        }

        setNegative(unscaledValue, sign < 0);

        return unscaledValue;
    }
}
