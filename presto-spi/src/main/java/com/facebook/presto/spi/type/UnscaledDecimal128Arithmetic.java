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

import static com.facebook.presto.spi.type.Decimals.MAX_PRECISION;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;

/**
 * 128 bit unscaled decimal arithmetic.
 * <p>
 * Based on: https://github.com/apache/hive/blob/master/common/src/java/org/apache/hadoop/hive/common/type/UnsignedInt128.java
 * and https://github.com/apache/hive/blob/master/common/src/java/org/apache/hadoop/hive/common/type/Decimal128.java
 */
public final class UnscaledDecimal128Arithmetic
{
    public static final int UNSCALED_DECIMAL_128_SLICE_LENGTH = 2 * SIZE_OF_LONG;

    private static final Slice[] POWERS_OF_TEN = new Slice[MAX_PRECISION - 1];

    private static final int SIGN_LONG_INDEX = 1;
    private static final int SIGN_INT_INDEX = 3;
    private static final long SIGN_LONG_MASK = 1L << 63;
    private static final int SIGN_INT_MASK = 1 << 31;
    private static final int SIGN_BYTE_MASK = 1 << 7;
    private static final int NUMBER_OF_INTS = 4;
    private static final int NUMBER_OF_LONGS = 2;
    private static final int INT_MASK = 0xFFFFFFFF;
    private static final long LONG_MASK = 0xFFFFFFFFL;

    /**
     * 5^13 fits in 2^31.
     */
    private static final int MAX_POWER_FIVE_INT = 13;
    /**
     * 5^x. All unsigned values.
     */
    private static final int[] POWER_FIVES_INT = new int[MAX_POWER_FIVE_INT + 1];

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

        for (int i = 0; i < POWERS_OF_TEN.length; ++i) {
            POWERS_OF_TEN[i] = unscaledDecimal(BigInteger.TEN.pow(i));
        }

        for (int i = 0; i < POWER_FIVES_INT.length; ++i) {
            POWER_FIVES_INT[i] = BigInteger.valueOf(5).pow(i).intValue();
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
        byte[] bytes = unscaledValue.abs().toByteArray();

        if (bytes.length > UNSCALED_DECIMAL_128_SLICE_LENGTH
                || (bytes.length == UNSCALED_DECIMAL_128_SLICE_LENGTH && (bytes[0] & SIGN_BYTE_MASK) != 0)) {
            throwOverflowException();
        }

        // convert to little-endian order
        reverse(bytes);

        Slice decimal = Slices.allocate(UNSCALED_DECIMAL_128_SLICE_LENGTH);
        decimal.setBytes(0, bytes);
        if (unscaledValue.signum() < 0) {
            setNegative(decimal, true);
        }

        throwIfOverflows(decimal);

        return decimal;
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
        long lo = getLong(decimal, 0);
        long hi = getLong(decimal, 1);
        boolean negative = isNegative(decimal);

        if (hi != 0 || ((lo > Long.MIN_VALUE || !negative) && lo < 0)) {
            throwOverflowException();
        }

        return negative ? -lo : lo;
    }

    public static void copyUnscaledDecimal(Slice from, Slice to)
    {
        setRawLong(to, 0, getRawLong(from, 0));
        setRawLong(to, 1, getRawLong(from, 1));
    }

    public static Slice rescale(Slice decimal, int rescaleFactor)
    {
        if (rescaleFactor == 0) {
            return decimal;
        }
        else if (rescaleFactor > 0) {
            return multiply(decimal, POWERS_OF_TEN[rescaleFactor]);
        }
        else {
            Slice result = unscaledDecimal();
            scaleDownFive(decimal, -rescaleFactor, result);
            shiftRightRoundUp(result, -rescaleFactor, result);
            return result;
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
        int l0 = getInt(left, 0);
        int l1 = getInt(left, 1);
        int l2 = getInt(left, 2);
        int l3 = getInt(left, 3);

        int r0 = getInt(right, 0);
        int r1 = getInt(right, 1);
        int r2 = getInt(right, 2);
        int r3 = getInt(right, 3);

        // the combinations below definitely result in overflow
        if ((r3 != 0 && (l3 | l2 | l1) != 0)
                || (r2 != 0 && (l3 | l2) != 0)
                || (r1 != 0 && l3 != 0)) {
            throwOverflowException();
        }

        long product;

        product = (r0 & LONG_MASK) * (l0 & LONG_MASK);
        int z0 = (int) product;

        product = (r0 & LONG_MASK) * (l1 & LONG_MASK)
                + (r1 & LONG_MASK) * (l0 & LONG_MASK)
                + (product >> 32);
        int z1 = (int) product;

        product = (r0 & LONG_MASK) * (l2 & LONG_MASK)
                + (r1 & LONG_MASK) * (l1 & LONG_MASK)
                + (r2 & LONG_MASK) * (l0 & LONG_MASK)
                + (product >> 32);
        int z2 = (int) product;

        product = (r0 & LONG_MASK) * (l3 & LONG_MASK)
                + (r1 & LONG_MASK) * (l2 & LONG_MASK)
                + (r2 & LONG_MASK) * (l1 & LONG_MASK)
                + (r3 & LONG_MASK) * (l0 & LONG_MASK)
                + (product >> 32);
        int z3 = (int) product;
        if ((product >>> 32) != 0) {
            throwOverflowException();
        }

        boolean negative = isNegative(left) ^ isNegative(right);
        pack(result, z0, z1, z2, z3, negative);

        throwIfOverflows(z0, z1, z2, z3);
    }

    public static boolean overflows(Slice value, int precision)
    {
        return precision < MAX_PRECISION && compareUnsigned(value, POWERS_OF_TEN[precision]) >= 0;
    }

    public static int compare(Slice left, Slice right)
    {
        boolean leftStrictlyNegative = isStrictlyNegative(left);
        boolean rightNegative = isNegative(right);

        if (leftStrictlyNegative != rightNegative) {
            return leftStrictlyNegative ? -1 : 1;
        }
        else {
            return compareUnsigned(left, right) * (leftStrictlyNegative ? -1 : 1);
        }
    }

    public static int compare(long leftRawLo, long leftRawHi, long rightRawLo, long rightRawHi)
    {
        boolean leftStrictlyNegative = isStrictlyNegative(leftRawLo, leftRawHi);
        boolean rightNegative = isNegative(rightRawLo, rightRawHi);

        if (leftStrictlyNegative != rightNegative) {
            return leftStrictlyNegative ? -1 : 1;
        }
        else {
            return compareUnsigned(leftRawLo, leftRawHi, rightRawLo, rightRawHi) * (leftStrictlyNegative ? -1 : 1);
        }
    }

    public static int compareUnsigned(Slice left, Slice right)
    {
        long leftHi = getLong(left, 1);
        long rightHi = getLong(right, 1);
        if (leftHi != rightHi) {
            return Long.compareUnsigned(leftHi, rightHi);
        }

        long leftLo = getLong(left, 0);
        long rightLo = getLong(right, 0);
        if (leftLo != rightLo) {
            return Long.compareUnsigned(leftLo, rightLo);
        }

        return 0;
    }

    public static int compareUnsigned(long leftRawLo, long leftRawHi, long rightRawLo, long rightRawHi)
    {
        long leftHi = unpackUnsignedLong(leftRawHi);
        long rightHi = unpackUnsignedLong(rightRawHi);
        if (leftHi != rightHi) {
            return Long.compareUnsigned(leftHi, rightHi);
        }

        if (leftRawLo != rightRawLo) {
            return Long.compareUnsigned(leftRawLo, rightRawLo);
        }

        return 0;
    }

    public static void negate(Slice decimal)
    {
        setNegative(decimal, !isNegative(decimal));
    }

    public static boolean isStrictlyNegative(Slice decimal)
    {
        return isNegative(decimal) && (getLong(decimal, 0) != 0 || getLong(decimal, 1) != 0);
    }

    public static boolean isStrictlyNegative(long rawLo, long rawHi)
    {
        return isNegative(rawLo, rawHi) && (rawLo != 0 || unpackUnsignedLong(rawHi) != 0);
    }

    public static boolean isNegative(Slice decimal)
    {
        return (getRawInt(decimal, SIGN_INT_INDEX) & SIGN_INT_MASK) != 0;
    }

    public static boolean isNegative(long rawLo, long rawHi)
    {
        return (rawHi & SIGN_LONG_MASK) != 0;
    }

    public static long hash(Slice decimal)
    {
        return hash(getRawLong(decimal, 0), getRawLong(decimal, 1));
    }

    public static long hash(long rawLo, long rawHi)
    {
        return XxHash64.hash(rawLo) ^ XxHash64.hash(unpackUnsignedLong(rawHi));
    }

    private static void scaleDownFive(Slice decimal, int fiveScale, Slice result)
    {
        while (true) {
            int powerFive = Math.min(fiveScale, MAX_POWER_FIVE_INT);
            fiveScale -= powerFive;

            int divisor = POWER_FIVES_INT[powerFive];
            divide(decimal, divisor, result);

            if (fiveScale == 0) {
                return;
            }
        }
    }

    private static void shiftRightRoundUp(Slice decimal, int rightShifts, Slice result)
    {
        shiftRight(decimal, rightShifts, true, result);
    }

    // visible for testing
    static void shiftRight(Slice decimal, int rightShifts, boolean roundUp, Slice result)
    {
        if (rightShifts == 0) {
            copyUnscaledDecimal(decimal, result);
        }
        else {
            final int wordShifts = rightShifts / 32;
            final int bitShiftsInWord = rightShifts % 32;
            final int shiftRestore = 32 - bitShiftsInWord;

            // check this because "123 << 32" will be 123.
            final boolean noRestore = bitShiftsInWord == 0;

            // check round-ups before settings values to result.
            // be aware that result could be the same object as z.
            boolean roundCarry;
            if (bitShiftsInWord == 0) {
                roundCarry = roundUp && getInt(decimal, wordShifts - 1) < 0;
            }
            else {
                roundCarry = roundUp && (getInt(decimal, wordShifts) & (1 << (bitShiftsInWord - 1))) != 0;
            }

            // Store negative before settings values to result.
            boolean negative = isNegative(decimal);

            for (int i = 0; i < NUMBER_OF_INTS; ++i) {
                int val = 0;
                if (!noRestore && i + wordShifts + 1 < NUMBER_OF_INTS) {
                    val = getInt(decimal, i + wordShifts + 1) << shiftRestore;
                }
                if (i + wordShifts < NUMBER_OF_INTS) {
                    val |= (getInt(decimal, i + wordShifts) >>> bitShiftsInWord);
                }

                if (roundCarry) {
                    if (val != INT_MASK) {
                        setRawInt(result, i, val + 1);
                        roundCarry = false;
                    }
                    else {
                        setRawInt(result, i, 0);
                    }
                }
                else {
                    setRawInt(result, i, val);
                }
            }

            setNegative(result, negative);
        }
    }

    // visible for testing
    static void divide(Slice decimal, int divisor, Slice result)
    {
        long remainder = (getInt(decimal, 3) & LONG_MASK);
        int z3 = (int) (remainder / divisor);
        remainder %= divisor;

        remainder = (getInt(decimal, 2) & LONG_MASK) + (remainder << 32);
        int z2 = (int) (remainder / divisor);
        remainder %= divisor;

        remainder = (getInt(decimal, 1) & LONG_MASK) + (remainder << 32);
        int z1 = (int) (remainder / divisor);
        remainder %= divisor;

        remainder = (getInt(decimal, 0) & LONG_MASK) + (remainder << 32);
        int z0 = (int) (remainder / divisor);

        pack(result, z0, z1, z2, z3, isNegative(decimal));
    }

    private static void setNegative(Slice decimal, boolean negative)
    {
        setNegative(decimal, getInt(decimal, SIGN_INT_INDEX), negative);
    }

    private static void setNegative(Slice decimal, int v3, boolean negative)
    {
        setRawInt(decimal, SIGN_INT_INDEX, v3 | (negative ? SIGN_INT_MASK : 0));
    }

    private static void pack(Slice decimal, int v0, int v1, int v2, int v3, boolean negative)
    {
        setRawInt(decimal, 0, v0);
        setRawInt(decimal, 1, v1);
        setRawInt(decimal, 2, v2);
        setNegative(decimal, v3, negative);
    }

    private static void throwIfOverflows(int v0, int v1, int v2, int v3)
    {
        if (exceedsOrEqualTenToThirtyEight(v0, v1, v2, v3)) {
            throwOverflowException();
        }
    }

    private static void throwIfOverflows(Slice decimal)
    {
        if (exceedsOrEqualTenToThirtyEight(decimal)) {
            throwOverflowException();
        }
    }

    private static boolean exceedsOrEqualTenToThirtyEight(int v0, int v1, int v2, int v3)
    {
        // 10**38=
        // v0=0(0),v1=160047680(98a22400),v2=1518781562(5a86c47a),v3=1262177448(4b3b4ca8)

        if (v3 >= 0 && v3 < 0x4b3b4ca8) {
            return false;
        }
        else if (v3 > 0x4b3b4ca8) {
            return true;
        }

        if (v2 >= 0 && v2 < 0x5a86c47a) {
            return false;
        }
        else if (v2 > 0x5a86c47a) {
            return true;
        }

        return v1 < 0 || v1 >= 0x098a2240;
    }

    private static boolean exceedsOrEqualTenToThirtyEight(Slice decimal)
    {
        // 10**38=
        // v0=0(0),v1=160047680(98a22400),v2=1518781562(5a86c47a),v3=1262177448(4b3b4ca8)

        int v3 = getInt(decimal, 3);
        if (v3 >= 0 && v3 < 0x4b3b4ca8) {
            return false;
        }
        else if (v3 > 0x4b3b4ca8) {
            return true;
        }

        int v2 = getInt(decimal, 2);
        if (v2 >= 0 && v2 < 0x5a86c47a) {
            return false;
        }
        else if (v2 > 0x5a86c47a) {
            return true;
        }

        int v1 = getInt(decimal, 1);
        return v1 < 0 || v1 >= 0x098a2240;
    }

    private static void throwOverflowException()
    {
        throw new ArithmeticException("Decimal overflow");
    }

    /* Based on: it.unimi.dsi.fastutil.bytes.ByteArrays.reverse; */
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

    private static int getInt(Slice decimal, int index)
    {
        int value = getRawInt(decimal, index);
        if (index == SIGN_INT_INDEX) {
            value &= ~SIGN_INT_MASK;
        }
        return value;
    }

    private static long getLong(Slice decimal, int index)
    {
        long value = getRawLong(decimal, index);
        if (index == SIGN_LONG_INDEX) {
            return unpackUnsignedLong(value);
        }
        return value;
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

    private UnscaledDecimal128Arithmetic() {}
}
