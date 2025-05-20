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
package com.facebook.presto.common.type;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import io.airlift.slice.Slice;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.UnscaledDecimal128Arithmetic.toUnscaledString;
import static com.facebook.presto.common.type.UnscaledDecimal128Arithmetic.unscaledDecimal;
import static com.facebook.presto.common.type.UnscaledDecimal128Arithmetic.unscaledDecimalToBigInteger;
import static java.lang.Math.abs;
import static java.lang.Math.pow;
import static java.lang.Math.round;
import static java.math.BigInteger.TEN;
import static java.math.RoundingMode.UNNECESSARY;

public final class Decimals
{
    private Decimals() {}

    public static final int MAX_PRECISION = 38;
    public static final int MAX_SHORT_PRECISION = 18;

    public static final BigInteger MAX_DECIMAL_UNSCALED_VALUE = new BigInteger(
            // repeat digit '9' MAX_PRECISION times
            new String(new char[MAX_PRECISION]).replace("\0", "9"));
    public static final BigInteger MIN_DECIMAL_UNSCALED_VALUE = MAX_DECIMAL_UNSCALED_VALUE.negate();

    private static final Pattern DECIMAL_PATTERN = Pattern.compile("(\\+?|-?)((0*)(\\d*))(\\.(\\d*))?");

    private static final int LONG_POWERS_OF_TEN_TABLE_LENGTH = 19;
    private static final int BIG_INTEGER_POWERS_OF_TEN_TABLE_LENGTH = 100;
    private static final long[] LONG_POWERS_OF_TEN = new long[LONG_POWERS_OF_TEN_TABLE_LENGTH];
    private static final BigInteger[] BIG_INTEGER_POWERS_OF_TEN = new BigInteger[BIG_INTEGER_POWERS_OF_TEN_TABLE_LENGTH];

    static {
        for (int i = 0; i < LONG_POWERS_OF_TEN.length; ++i) {
            // Although this computes using doubles, incidentally, this is exact for all powers of 10 that fit in a long.
            LONG_POWERS_OF_TEN[i] = round(pow(10, i));
        }

        for (int i = 0; i < BIG_INTEGER_POWERS_OF_TEN.length; ++i) {
            BIG_INTEGER_POWERS_OF_TEN[i] = TEN.pow(i);
        }
    }

    public static long longTenToNth(int n)
    {
        return LONG_POWERS_OF_TEN[n];
    }

    public static BigInteger bigIntegerTenToNth(int n)
    {
        return BIG_INTEGER_POWERS_OF_TEN[n];
    }

    public static DecimalParseResult parse(String stringValue)
    {
        return parse(stringValue, false);
    }

    // visible for testing
    public static DecimalParseResult parseIncludeLeadingZerosInPrecision(String stringValue)
    {
        return parse(stringValue, true);
    }

    private static DecimalParseResult parse(String stringValue, boolean includeLeadingZerosInPrecision)
    {
        Matcher matcher = DECIMAL_PATTERN.matcher(stringValue);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid decimal value '" + stringValue + "'");
        }

        String sign = getMatcherGroup(matcher, 1);
        if (sign.isEmpty()) {
            sign = "+";
        }
        String leadingZeros = getMatcherGroup(matcher, 3);
        String integralPart = getMatcherGroup(matcher, 4);
        String fractionalPart = getMatcherGroup(matcher, 6);

        if (leadingZeros.isEmpty() && integralPart.isEmpty() && fractionalPart.isEmpty()) {
            throw new IllegalArgumentException("Invalid decimal value '" + stringValue + "'");
        }

        int scale = fractionalPart.length();
        int precision;
        if (includeLeadingZerosInPrecision) {
            precision = leadingZeros.length() + integralPart.length() + scale;
        }
        else {
            precision = integralPart.length() + scale;
            if (precision == 0) {
                precision = 1;
            }
        }

        String unscaledValue = sign + leadingZeros + integralPart + fractionalPart;
        Object value;
        if (precision <= MAX_SHORT_PRECISION) {
            value = Long.parseLong(unscaledValue);
        }
        else {
            value = encodeUnscaledValue(new BigInteger(unscaledValue));
        }
        return new DecimalParseResult(value, createDecimalType(precision, scale));
    }

    private static String getMatcherGroup(Matcher matcher, int group)
    {
        String groupValue = matcher.group(group);
        if (groupValue == null) {
            groupValue = "";
        }
        return groupValue;
    }

    @SuppressWarnings("NumericCastThatLosesPrecision")
    public static Slice encodeUnscaledValue(BigInteger unscaledValue)
    {
        return unscaledDecimal(unscaledValue);
    }

    public static Slice encodeUnscaledValue(long unscaledValue)
    {
        return unscaledDecimal(unscaledValue);
    }

    public static long encodeShortScaledValue(BigDecimal value, int scale)
    {
        checkArgument(scale >= 0);
        return value.setScale(scale, UNNECESSARY).unscaledValue().longValueExact();
    }

    public static Slice encodeScaledValue(BigDecimal value, int scale)
    {
        checkArgument(scale >= 0);
        return encodeScaledValue(value.setScale(scale, UNNECESSARY));
    }

    /**
     * Converts {@link BigDecimal} to {@link Slice} representing it for long {@link DecimalType}.
     * It is caller responsibility to ensure that {@code value.scale()} equals to {@link DecimalType#getScale()}.
     */
    public static Slice encodeScaledValue(BigDecimal value)
    {
        return encodeUnscaledValue(value.unscaledValue());
    }
    public static BigInteger bigIntegerValue(BigDecimal value)
    {
        return value.toBigInteger();
    }

    public static BigInteger decodeUnscaledValue(Slice valueSlice)
    {
        return unscaledDecimalToBigInteger(valueSlice);
    }

    public static String toString(long unscaledValue, int scale)
    {
        return toString(Long.toString(unscaledValue), scale);
    }

    public static String toString(Slice unscaledValue, int scale)
    {
        return toString(toUnscaledString(unscaledValue), scale);
    }

    public static String toString(BigInteger unscaledValue, int scale)
    {
        return toString(unscaledValue.toString(), scale);
    }

    private static String toString(String unscaledValueString, int scale)
    {
        StringBuilder resultBuilder = new StringBuilder();
        // add sign
        if (unscaledValueString.startsWith("-")) {
            resultBuilder.append("-");
            unscaledValueString = unscaledValueString.substring(1);
        }

        // integral part
        if (unscaledValueString.length() <= scale) {
            resultBuilder.append("0");
        }
        else {
            resultBuilder.append(unscaledValueString.substring(0, unscaledValueString.length() - scale));
        }

        // fractional part
        if (scale > 0) {
            resultBuilder.append(".");
            if (unscaledValueString.length() < scale) {
                // prepend zeros to fractional part if unscaled value length is shorter than scale
                for (int i = 0; i < scale - unscaledValueString.length(); ++i) {
                    resultBuilder.append("0");
                }
                resultBuilder.append(unscaledValueString);
            }
            else {
                // otherwise just use scale last digits of unscaled value
                resultBuilder.append(unscaledValueString.substring(unscaledValueString.length() - scale));
            }
        }
        return resultBuilder.toString();
    }

    public static boolean overflows(long value, int precision)
    {
        if (precision > MAX_SHORT_PRECISION) {
            throw new IllegalArgumentException("expected precision to be less than " + MAX_SHORT_PRECISION);
        }
        return abs(value) >= longTenToNth(precision);
    }

    public static boolean overflows(BigInteger value, int precision)
    {
        return value.abs().compareTo(bigIntegerTenToNth(precision)) >= 0;
    }

    public static boolean overflows(BigInteger value)
    {
        return value.compareTo(MAX_DECIMAL_UNSCALED_VALUE) > 0 || value.compareTo(MIN_DECIMAL_UNSCALED_VALUE) < 0;
    }

    public static boolean overflows(BigDecimal value, long precision)
    {
        return value.precision() > precision;
    }

    public static BigDecimal readBigDecimal(DecimalType type, Block block, int position)
    {
        BigInteger unscaledValue = type.isShort()
                ? BigInteger.valueOf(type.getLong(block, position))
                : decodeUnscaledValue(type.getSlice(block, position));
        return new BigDecimal(unscaledValue, type.getScale(), new MathContext(type.getPrecision()));
    }

    public static void writeBigDecimal(DecimalType decimalType, BlockBuilder blockBuilder, BigDecimal value)
    {
        decimalType.writeSlice(blockBuilder, encodeScaledValue(value));
    }

    public static BigDecimal rescale(BigDecimal value, DecimalType type)
    {
        value = value.setScale(type.getScale(), UNNECESSARY);

        if (value.precision() > type.getPrecision()) {
            throw new IllegalArgumentException("decimal precision larger than column precision");
        }
        return value;
    }

    public static void writeShortDecimal(BlockBuilder blockBuilder, long value)
    {
        blockBuilder.writeLong(value).closeEntry();
    }

    public static long rescale(long value, int fromScale, int toScale)
    {
        if (toScale < fromScale) {
            throw new IllegalArgumentException("target scale must be larger than source scale");
        }
        return value * longTenToNth(toScale - fromScale);
    }

    public static BigInteger rescale(BigInteger value, int fromScale, int toScale)
    {
        if (toScale < fromScale) {
            throw new IllegalArgumentException("target scale must be larger than source scale");
        }
        return value.multiply(bigIntegerTenToNth(toScale - fromScale));
    }

    public static boolean isShortDecimal(Type type)
    {
        return type instanceof ShortDecimalType;
    }

    public static boolean isLongDecimal(Type type)
    {
        return type instanceof LongDecimalType;
    }

    private static void checkArgument(boolean condition)
    {
        if (!condition) {
            throw new IllegalArgumentException();
        }
    }
}
