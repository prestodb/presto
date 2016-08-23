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

/**
 * 128 bit unscaled decimal arithmetic.
 * <p>
 * Based on: https://github.com/apache/hive/blob/master/common/src/java/org/apache/hadoop/hive/common/type/UnsignedInt128.java
 * and https://github.com/apache/hive/blob/master/common/src/java/org/apache/hadoop/hive/common/type/Decimal128.java
 */
public final class UnscaledDecimal128Arithmetic
{
    public static final int UNSCALED_DECIMAL_128_SLICE_LENGTH = 2 * SIZE_OF_LONG;

    private static final Slice[] POWERS_OF_TEN = new Slice[MAX_PRECISION];

    private static final int SIGN_LONG_INDEX = 1;
    private static final int SIGN_INT_INDEX = 3;
    private static final long SIGN_LONG_MASK = 1L << 63;
    private static final int SIGN_INT_MASK = 1 << 31;
    private static final int SIGN_BYTE_MASK = 1 << 7;
    private static final int NUMBER_OF_LONGS = 2;
    private static final long ALL_BITS_SET_64 = 0xFFFFFFFFFFFFFFFFL;
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
    private static final int MAX_POWER_OF_TEN_INT_VALUE = (int) Math.pow(10, 9);
    /**
     * 10^18 fits in 2^63.
     */
    private static final int MAX_POWER_OF_TEN_LONG = 18;
    /**
     * 10^x. All unsigned values.
     */
    private static final int[] POWERS_OF_TEN_INT = new int[MAX_POWER_OF_TEN_INT + 1];

    /**
     * ZERO_STRINGS[i] is a string of i consecutive zeros.
     */
    private static final String[] ZERO_STRINGS = new String[64];

    static {
        ZERO_STRINGS[63] =
                "000000000000000000000000000000000000000000000000000000000000000";
        for (int i = 0; i < 63; i++) {
            ZERO_STRINGS[i] = ZERO_STRINGS[63].substring(0, i);
        }
    }

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

        POWERS_OF_FIVES_INT[0] = 1;
        for (int i = 1; i < POWERS_OF_FIVES_INT.length; ++i) {
            POWERS_OF_FIVES_INT[i] = POWERS_OF_FIVES_INT[i - 1] * 5;
        }

        POWERS_OF_TEN_INT[0] = 1;
        for (int i = 1; i < POWERS_OF_TEN_INT.length; ++i) {
            POWERS_OF_TEN_INT[i] = POWERS_OF_TEN_INT[i - 1] * 10;
        }

        if (ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN)) {
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

    public static long unscaledDecimalToUnscaledLongUnsafe(Slice decimal)
    {
        long lo = getLong(decimal, 0);
        boolean negative = isNegative(decimal);
        return negative ? -lo : lo;
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

    public static Slice add(Slice left, Slice right)
    {
        Slice result = unscaledDecimal();
        add(left, right, result);
        return result;
    }

    public static void add(Slice left, Slice right, Slice result)
    {
        long overflow = addReturnOverflow(left, right, result);
        if (overflow != 0) {
            throwOverflowException();
        }
    }

    /**
     * Instead of throwing overflow exception, this function returns:
     *     0 when there was no overflow
     *     +1 when there was overflow
     *     -1 when there was underflow
     */
    public static long addReturnOverflow(Slice left, Slice right, Slice result)
    {
        long overflow = 0;
        if (!(isNegative(left) ^ isNegative(right))) {
            // either both negative or both positive
            overflow = addUnsignedReturnOverflow(left, right, result, isNegative(left));
            if (isNegative(left)) {
                overflow = -overflow;
            }
        }
        else {
            int compare = compareUnsigned(left, right);
            if (compare > 0) {
                subtractUnsigned(left, right, result, isNegative(left));
            }
            else if (compare < 0) {
                subtractUnsigned(right, left, result, !(isNegative(left)));
            }
            else {
                clearDecimal(result);
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
            int compare = compareUnsigned(left, right);
            if (compare > 0) {
                subtractUnsigned(left, right, result, isNegative(left) && isNegative(right));
            }
            else if (compare < 0) {
                subtractUnsigned(right, left, result, !(isNegative(left) && isNegative(right)));
            }
            else {
                clearDecimal(result);
            }
        }
    }

    /**
     * This method ignores signs of the left and right. Returns overflow value.
     */
    private static long addUnsignedReturnOverflow(Slice left, Slice right, Slice result, boolean resultNegative)
    {
        int l0 = getInt(left, 0);
        int l1 = getInt(left, 1);
        int l2 = getInt(left, 2);
        int l3 = getInt(left, 3);

        int r0 = getInt(right, 0);
        int r1 = getInt(right, 1);
        int r2 = getInt(right, 2);
        int r3 = getInt(right, 3);

        long product;
        product = (l0 & LONG_MASK) + (r0 & LONG_MASK);

        int z0 = (int) product;

        product = (l1 & LONG_MASK) + (r1 & LONG_MASK) + (product >> 32);

        int z1 = (int) product;

        product = (l2 & LONG_MASK) + (r2 & LONG_MASK) + (product >> 32);

        int z2 = (int) product;

        product = (l3 & LONG_MASK) + (r3 & LONG_MASK) + (product >> 32);

        int z3 = (int) product & (~SIGN_INT_MASK);

        pack(result, z0, z1, z2, z3, resultNegative);

        return product >> 31;
    }

    /**
     * This method ignores signs of the left and right and assumes that left is greater then right
     */
    private static void subtractUnsigned(Slice left, Slice right, Slice result, boolean resultNegative)
    {
        int l0 = getInt(left, 0);
        int l1 = getInt(left, 1);
        int l2 = getInt(left, 2);
        int l3 = getInt(left, 3);

        int r0 = getInt(right, 0);
        int r1 = getInt(right, 1);
        int r2 = getInt(right, 2);
        int r3 = getInt(right, 3);

        long product;
        product = (l0 & LONG_MASK) - (r0 & LONG_MASK);

        int z0 = (int) product;

        product = (l1 & LONG_MASK) - (r1 & LONG_MASK) + (product >> 32);

        int z1 = (int) product;

        product = (l2 & LONG_MASK) - (r2 & LONG_MASK) + (product >> 32);

        int z2 = (int) product;

        product = (l3 & LONG_MASK) - (r3 & LONG_MASK) + (product >> 32);

        int z3 = (int) product;

        pack(result, z0, z1, z2, z3, resultNegative);

        if ((product >> 32) != 0) {
            throw new IllegalStateException(format("Non empty carry over after subtracting [%d]. right > left?", (product >> 32)));
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

        // the combinations below definitely result in an overflow
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

    public static boolean isZero(Slice decimal)
    {
        return getLong(decimal, 0) == 0 && getLong(decimal, 1) == 0;
    }

    public static long hash(Slice decimal)
    {
        return hash(getRawLong(decimal, 0), getRawLong(decimal, 1));
    }

    public static long hash(long rawLo, long rawHi)
    {
        return XxHash64.hash(rawLo) ^ XxHash64.hash(unpackUnsignedLong(rawHi));
    }

    public static String toStringDestructive(Slice decimal)
    {
        if (isZero(decimal)) {
            return "0";
        }

        String[] digitGroup = new String[MAX_PRECISION / MAX_POWER_OF_TEN_INT + 1];
        int numGroups = 0;
        boolean negative = isNegative(decimal);

        // convert decimal to strings one digit group at a time
        do {
            int remainder = divide(decimal, MAX_POWER_OF_TEN_INT_VALUE, decimal);
            digitGroup[numGroups++] = Integer.toString(remainder, 10);
        }
        while (!isZero(decimal));

        StringBuilder buf = new StringBuilder(MAX_PRECISION + 1);
        if (negative) {
            buf.append('-');
        }
        buf.append(digitGroup[numGroups - 1]);

        // append leading zeros for middle digit groups
        for (int i = numGroups - 2; i >= 0; i--) {
            int numLeadingZeros = MAX_POWER_OF_TEN_INT - digitGroup[i].length();
            if (numLeadingZeros != 0) {
                buf.append(ZERO_STRINGS[numLeadingZeros]);
            }
            buf.append(digitGroup[i]);
        }
        return buf.toString();
    }

    public static boolean overflows(Slice value, int precision)
    {
        if (precision == MAX_PRECISION) {
            return exceedsOrEqualTenToThirtyEight(value);
        }
        return precision < MAX_PRECISION && compareUnsigned(value, POWERS_OF_TEN[precision]) >= 0;
    }

    public static void throwIfOverflows(Slice decimal)
    {
        if (exceedsOrEqualTenToThirtyEight(decimal)) {
            throwOverflowException();
        }
    }

    private static void throwIfOverflows(int v0, int v1, int v2, int v3)
    {
        if (exceedsOrEqualTenToThirtyEight(v0, v1, v2, v3)) {
            throwOverflowException();
        }
    }

    private static void scaleDownRoundUp(Slice decimal, int scaleFactor, Slice result)
    {
        // optimized path for smaller values
        long lo = getLong(decimal, 0);
        long hi = getLong(decimal, 1);
        if (scaleFactor <= MAX_POWER_OF_TEN_LONG && hi == 0 && lo >= 0) {
            long divisor = longTenToNth(scaleFactor);
            long newLo = lo / divisor;
            if (lo % divisor >= (divisor >> 1)) {
                newLo++;
            }
            pack(result, newLo, 0, isNegative(decimal));
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
     * Scale down the value for 5**tenScale (this := this / 5**tenScale).
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

    private static void shiftRightRoundUp(Slice decimal, int rightShifts, Slice result)
    {
        shiftRight(decimal, rightShifts, true, result);
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

        long lo;
        long hi;

        switch (wordShifts) {
            case 0:
                lo = getLong(decimal, 0);
                hi = getLong(decimal, 1);
                break;
            case 1:
                lo = getLong(decimal, 1);
                hi = 0;
                break;
            default:
                throw new IllegalArgumentException();
        }

        if (bitShiftsInWord > 0) {
            lo = (lo >>> bitShiftsInWord) | (hi << shiftRestore);
            hi = (hi >>> bitShiftsInWord);
        }

        if (roundCarry) {
            if (lo != ALL_BITS_SET_64) {
                lo++;
            }
            else {
                lo = 0;
                hi++;
            }
        }

        pack(result, lo, hi, negative);
    }

    private static boolean divideCheckRound(Slice decimal, int divisor, Slice result)
    {
        int remainder = divide(decimal, divisor, result);
        return (remainder >= (divisor >> 1));
    }

    // visible for testing
    static int divide(Slice decimal, int divisor, Slice result)
    {
        long remainder = getLong(decimal, 1);
        long hi = remainder / divisor;
        remainder %= divisor;

        remainder = (getInt(decimal, 1) & LONG_MASK) + (remainder << 32);
        int z1 = (int) (remainder / divisor);
        remainder %= divisor;

        remainder = (getInt(decimal, 0) & LONG_MASK) + (remainder << 32);
        int z0 = (int) (remainder / divisor);

        pack(result, z0, z1, hi, isNegative(decimal));
        return (int) (remainder % divisor);
    }

    private static void incrementUnsafe(Slice decimal)
    {
        long lo = getLong(decimal, 0);
        if (lo != ALL_BITS_SET_64) {
            setRawLong(decimal, 0, lo + 1);
            return;
        }

        long hi = getLong(decimal, 1);
        setNegativeLong(decimal, hi + 1, isNegative(decimal));
    }

    private static void setNegative(Slice decimal, boolean negative)
    {
        setNegativeInt(decimal, getInt(decimal, SIGN_INT_INDEX), negative);
    }

    private static void setNegativeInt(Slice decimal, int v3, boolean negative)
    {
        if (v3 < 0) {
            throwOverflowException();
        }
        setRawInt(decimal, SIGN_INT_INDEX, v3 | (negative ? SIGN_INT_MASK : 0));
    }

    private static void setNegativeLong(Slice decimal, long hi, boolean negative)
    {
        setRawLong(decimal, SIGN_LONG_INDEX, hi | (negative ? SIGN_LONG_MASK : 0));
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

    private static void pack(Slice decimal, int v0, int v1, long hi, boolean negative)
    {
        setRawInt(decimal, 0, v0);
        setRawInt(decimal, 1, v1);
        setNegativeLong(decimal, hi, negative);
    }

    private static void pack(Slice decimal, long lo, long hi, boolean negative)
    {
        setRawLong(decimal, 0, lo);
        setNegativeLong(decimal, hi, negative);
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

    private static boolean exceedsOrEqualTenToThirtyEight(int v0, int v1, int v2, int v3)
    {
        // 10**38=
        // v0=0(0),v1=160047680(98a22400),v2=1518781562(5a86c47a),v3=1262177448(4b3b4ca8)

        if (v3 >= 0 && v3 < 0x4b3b4ca8) {
            return false;
        }
        else if (v3 != 0x4b3b4ca8) {
            return true;
        }

        if (v2 >= 0 && v2 < 0x5a86c47a) {
            return false;
        }
        else if (v2 != 0x5a86c47a) {
            return true;
        }

        return v1 < 0 || v1 >= 0x098a2240;
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

    private static void clearDecimal(Slice decimal)
    {
        for (int i = 0; i < NUMBER_OF_LONGS; i++) {
            setRawLong(decimal, i, 0);
        }
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
