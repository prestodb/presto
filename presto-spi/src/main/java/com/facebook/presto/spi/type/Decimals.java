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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.BlockBuilder;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.facebook.presto.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static java.lang.Math.abs;
import static java.lang.Math.pow;
import static java.lang.Math.round;
import static java.lang.String.format;
import static java.math.BigInteger.TEN;

public class Decimals
{
    private Decimals() {}

    public static final int SIZE_OF_LONG_DECIMAL = 2 * SIZE_OF_LONG;

    public static final int MAX_PRECISION = 38;
    public static final int MAX_SHORT_PRECISION = 17;

    public static final BigInteger MAX_DECIMAL_UNSCALED_VALUE = new BigInteger(
            // repeat digit '9' MAX_PRECISION times
            new String(new char[MAX_PRECISION]).replace("\0", "9"));
    public static final BigInteger MIN_DECIMAL_UNSCALED_VALUE = MAX_DECIMAL_UNSCALED_VALUE.negate();

    private static final Pattern DECIMAL_PATTERN = Pattern.compile("(\\+?|-?)((0*)(\\d*))(\\.(\\d+))?");

    private static final int POWERS_OF_TEN_TABLE_LENGTH = 100;
    private static final long[] LONG_POWERS_OF_TEN = new long[POWERS_OF_TEN_TABLE_LENGTH];
    private static final BigInteger[] BIG_INTEGER_POWERS_OF_TEN = new BigInteger[POWERS_OF_TEN_TABLE_LENGTH];

    static {
        for (int i = 0; i < LONG_POWERS_OF_TEN.length; ++i) {
            LONG_POWERS_OF_TEN[i] = round(pow(10, i));
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
            throw new IllegalArgumentException("invalid decimal value '" + stringValue + "'");
        }

        String sign = getMatcherGroup(matcher, 1);
        if (sign.isEmpty()) {
            sign = "+";
        }
        String leadingZeros = getMatcherGroup(matcher, 3);
        String integralPart = getMatcherGroup(matcher, 4);
        String fractionalPart = getMatcherGroup(matcher, 6);

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

    public static Slice encodeUnscaledValue(BigInteger unscaledValue)
    {
        Slice result = Slices.allocate(SIZE_OF_LONG_DECIMAL);
        byte[] bytes = unscaledValue.toByteArray();
        if (unscaledValue.signum() < 0) {
            // need to fill with 0xff for negative values as we
            // represent value in two's-complement representation.
            result.fill((byte) 0xff);
        }
        result.setBytes(SIZE_OF_LONG_DECIMAL - bytes.length, bytes);
        return result;
    }

    public static Slice encodeUnscaledValue(long unscaledValue)
    {
        // we just fill top 8 bytes with unscaled value from long
        // the bottom 8 bytes are filled with 0 for positive values and 0xff for negative ones
        // conforming two's-complement representation.
        Slice result = Slices.allocate(SIZE_OF_LONG_DECIMAL);
        if (unscaledValue < 0) {
            result.setLong(0, -1L); // fill bottom 8 bytes with 0xff
        }
        result.setLong(SIZE_OF_LONG, Long.reverseBytes(unscaledValue));
        return result;
    }

    public static Slice encodeScaledValue(BigDecimal value)
    {
        return encodeUnscaledValue(value.unscaledValue());
    }

    public static BigInteger decodeUnscaledValue(Slice valueSlice)
    {
        return new BigInteger(valueSlice.getBytes());
    }

    public static String toString(long unscaledValue, int scale)
    {
        return toString(Long.toString(unscaledValue), scale);
    }

    public static String toString(Slice unscaledValue, int scale)
    {
        return toString(decodeUnscaledValue(unscaledValue), scale);
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

    public static void checkOverflow(BigInteger value)
    {
        if (overflows(value)) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, format("Value is out of range: %s", value.toString()));
        }
    }

    public static void writeBigDecimal(DecimalType decimalType, BlockBuilder blockBuilder, BigDecimal value)
    {
        decimalType.writeSlice(blockBuilder, encodeScaledValue(value));
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
}
