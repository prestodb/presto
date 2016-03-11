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
import org.testng.annotations.Test;

import java.math.BigInteger;

import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static org.testng.Assert.assertEquals;

public class TestDecimals
{
    @Test
    public void testParse()
            throws Exception
    {
        assertParseResult("0", 0L, 1, 0);
        assertParseResult("+0", 0L, 1, 0);
        assertParseResult("-0", 0L, 1, 0);
        assertParseResult("000", 0L, 1, 0);
        assertParseResult("+000", 0L, 1, 0);
        assertParseResult("-000", 0L, 1, 0);
        assertParseResult("0000000000000000000000000000", 0L, 1, 0);
        assertParseResult("+0000000000000000000000000000", 0L, 1, 0);
        assertParseResult("-0000000000000000000000000000", 0L, 1, 0);
        assertParseResult("1.1", 11L, 2, 1);
        assertParseResult("+1.1", 11L, 2, 1);
        assertParseResult("-1.1", -11L, 2, 1);
        assertParseResult("0001.1", 11L, 2, 1);
        assertParseResult("+0001.1", 11L, 2, 1);
        assertParseResult("-0001.1", -11L, 2, 1);
        assertParseResult("0.1", 1L, 1, 1);
        assertParseResult("+0.1", 1L, 1, 1);
        assertParseResult("-0.1", -1L, 1, 1);
        assertParseResult(".1", 1L, 1, 1);
        assertParseResult("+.1", 1L, 1, 1);
        assertParseResult("-.1", -1L, 1, 1);
        assertParseResult("000.1", 1L, 1, 1);
        assertParseResult("+000.1", 1L, 1, 1);
        assertParseResult("-000.1", -1L, 1, 1);
        assertParseResult("12345678901234567", 12345678901234567L, 17, 0);
        assertParseResult("+12345678901234567", 12345678901234567L, 17, 0);
        assertParseResult("-12345678901234567", -12345678901234567L, 17, 0);
        assertParseResult("00012345678901234567", 12345678901234567L, 17, 0);
        assertParseResult("+00012345678901234567", 12345678901234567L, 17, 0);
        assertParseResult("-00012345678901234567", -12345678901234567L, 17, 0);
        assertParseResult("0.12345678901234567", 12345678901234567L, 17, 17);
        assertParseResult("+0.12345678901234567", 12345678901234567L, 17, 17);
        assertParseResult("-0.12345678901234567", -12345678901234567L, 17, 17);
        assertParseResult("000.12345678901234567", 12345678901234567L, 17, 17);
        assertParseResult("+000.12345678901234567", 12345678901234567L, 17, 17);
        assertParseResult("-000.12345678901234567", -12345678901234567L, 17, 17);
        assertParseResult("12345678901234567890.123456789012345678", encodeUnscaledValue("12345678901234567890123456789012345678"), 38, 18);
        assertParseResult("+12345678901234567890.123456789012345678", encodeUnscaledValue("12345678901234567890123456789012345678"), 38, 18);
        assertParseResult("-12345678901234567890.123456789012345678", encodeUnscaledValue("-12345678901234567890123456789012345678"), 38, 18);
        assertParseResult("00012345678901234567890.123456789012345678", encodeUnscaledValue("12345678901234567890123456789012345678"), 38, 18);
        assertParseResult("+00012345678901234567890.123456789012345678", encodeUnscaledValue("12345678901234567890123456789012345678"), 38, 18);
        assertParseResult("-00012345678901234567890.123456789012345678", encodeUnscaledValue("-12345678901234567890123456789012345678"), 38, 18);
        assertParseResult("0.12345678901234567890123456789012345678", encodeUnscaledValue("12345678901234567890123456789012345678"), 38, 38);
        assertParseResult("+0.12345678901234567890123456789012345678", encodeUnscaledValue("12345678901234567890123456789012345678"), 38, 38);
        assertParseResult("-0.12345678901234567890123456789012345678", encodeUnscaledValue("-12345678901234567890123456789012345678"), 38, 38);
        assertParseResult(".12345678901234567890123456789012345678", encodeUnscaledValue("12345678901234567890123456789012345678"), 38, 38);
        assertParseResult("+.12345678901234567890123456789012345678", encodeUnscaledValue("12345678901234567890123456789012345678"), 38, 38);
        assertParseResult("-.12345678901234567890123456789012345678", encodeUnscaledValue("-12345678901234567890123456789012345678"), 38, 38);
        assertParseResult("0000.12345678901234567890123456789012345678", encodeUnscaledValue("12345678901234567890123456789012345678"), 38, 38);
        assertParseResult("+0000.12345678901234567890123456789012345678", encodeUnscaledValue("12345678901234567890123456789012345678"), 38, 38);
        assertParseResult("-0000.12345678901234567890123456789012345678", encodeUnscaledValue("-12345678901234567890123456789012345678"), 38, 38);
    }

    @Test
    public void testParseIncludeLeadingZerosInPrecision()
            throws Exception
    {
        assertParseResultIncludeLeadingZerosInPrecision("0", 0L, 1, 0);
        assertParseResultIncludeLeadingZerosInPrecision("+0", 0L, 1, 0);
        assertParseResultIncludeLeadingZerosInPrecision("-0", 0L, 1, 0);
        assertParseResultIncludeLeadingZerosInPrecision("00000000000000000", 0L, 17, 0);
        assertParseResultIncludeLeadingZerosInPrecision("+00000000000000000", 0L, 17, 0);
        assertParseResultIncludeLeadingZerosInPrecision("-00000000000000000", 0L, 17, 0);
        assertParseResultIncludeLeadingZerosInPrecision("1.1", 11L, 2, 1);
        assertParseResultIncludeLeadingZerosInPrecision("+1.1", 11L, 2, 1);
        assertParseResultIncludeLeadingZerosInPrecision("-1.1", -11L, 2, 1);
        assertParseResultIncludeLeadingZerosInPrecision("0001.1", 11L, 5, 1);
        assertParseResultIncludeLeadingZerosInPrecision("+0001.1", 11L, 5, 1);
        assertParseResultIncludeLeadingZerosInPrecision("-0001.1", -11L, 5, 1);
        assertParseResultIncludeLeadingZerosInPrecision("000", 0L, 3, 0);
        assertParseResultIncludeLeadingZerosInPrecision("+000", 0L, 3, 0);
        assertParseResultIncludeLeadingZerosInPrecision("-000", -0L, 3, 0);
        assertParseResultIncludeLeadingZerosInPrecision("000.1", 1L, 4, 1);
        assertParseResultIncludeLeadingZerosInPrecision("+000.1", 1L, 4, 1);
        assertParseResultIncludeLeadingZerosInPrecision("-000.1", -1L, 4, 1);
        assertParseResultIncludeLeadingZerosInPrecision("000000000000000000", encodeUnscaledValue("0"), 18, 0);
        assertParseResultIncludeLeadingZerosInPrecision("+000000000000000000", encodeUnscaledValue("0"), 18, 0);
        assertParseResultIncludeLeadingZerosInPrecision("-000000000000000000", encodeUnscaledValue("0"), 18, 0);
        assertParseResultIncludeLeadingZerosInPrecision("000000000000000000.123", encodeUnscaledValue("123"), 21, 3);
        assertParseResultIncludeLeadingZerosInPrecision("+000000000000000000.123", encodeUnscaledValue("123"), 21, 3);
        assertParseResultIncludeLeadingZerosInPrecision("-000000000000000000.123", encodeUnscaledValue("-123"), 21, 3);
    }

    private void assertParseResult(String value, Object expectedObject, int expectedPrecision, int expectedScale)
    {
        assertEquals(Decimals.parse(value),
                new DecimalParseResult(
                        expectedObject,
                        createDecimalType(expectedPrecision, expectedScale)));
    }

    private void assertParseResultIncludeLeadingZerosInPrecision(String value, Object expectedObject, int expectedPrecision, int expectedScale)
    {
        assertEquals(Decimals.parseIncludeLeadingZerosInPrecision(value),
                new DecimalParseResult(
                        expectedObject,
                        createDecimalType(expectedPrecision, expectedScale)));
    }

    private static Slice encodeUnscaledValue(String unscaledValue)
    {
        return Decimals.encodeUnscaledValue(new BigInteger(unscaledValue));
    }
}
