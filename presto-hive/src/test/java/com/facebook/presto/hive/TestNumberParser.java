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
package com.facebook.presto.hive;

import org.testng.annotations.Test;

import static com.facebook.presto.hive.NumberParser.parseDouble;
import static com.facebook.presto.hive.NumberParser.parseLong;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.testng.Assert.assertEquals;

public class TestNumberParser
{
    @Test
    public void testLong()
            throws Exception
    {
        assertParseLong("1");
        assertParseLong("12");
        assertParseLong("123");
        assertParseLong("-1");
        assertParseLong("-12");
        assertParseLong("-123");
        assertParseLong("+1");
        assertParseLong("+12");
        assertParseLong("+123");
        assertParseLong("0");
        assertParseLong("-0");
        assertParseLong("+0");
        assertParseLong(Long.toString(Long.MAX_VALUE));
        assertParseLong(Long.toString(Long.MIN_VALUE));
    }

    @Test
    public void testDouble()
            throws Exception
    {
        assertParseDouble("123");
        assertParseDouble("123.0");
        assertParseDouble("123.456");
        assertParseDouble("123.456e5");
        assertParseDouble("123.456e-5");
        assertParseDouble("123e5");
        assertParseDouble("123e-5");
        assertParseDouble("0");
        assertParseDouble("0.0");
        assertParseDouble("0.456");
        assertParseDouble("-0");
        assertParseDouble("-0.0");
        assertParseDouble("-0.456");
        assertParseDouble("-123");
        assertParseDouble("-123.0");
        assertParseDouble("-123.456");
        assertParseDouble("-123.456e-5");
        assertParseDouble("-123e5");
        assertParseDouble("-123e-5");
        assertParseDouble("+123");
        assertParseDouble("+123.0");
        assertParseDouble("+123.456");
        assertParseDouble("+123.456e5");
        assertParseDouble("+123.456e-5");
        assertParseDouble("+123e5");
        assertParseDouble("+123e-5");
        assertParseDouble("+0");
        assertParseDouble("+0.0");
        assertParseDouble("+0.456");

        assertParseDouble("NaN");
        assertParseDouble("-Infinity");
        assertParseDouble("Infinity");
        assertParseDouble("+Infinity");

        assertParseDouble(Double.toString(Double.MAX_VALUE));
        assertParseDouble(Double.toString(-Double.MAX_VALUE));
        assertParseDouble(Double.toString(Double.MIN_VALUE));
        assertParseDouble(Double.toString(-Double.MIN_VALUE));
    }

    private static void assertParseLong(String string)
    {
        assertEquals(parseLong(string.getBytes(US_ASCII), 0, string.length()), Long.parseLong(string));

        // verify we can parse using a non-zero offset
        String padding = "9999";
        String padded = padding + string + padding;
        assertEquals(parseLong(padded.getBytes(US_ASCII), padding.length(), string.length()), Long.parseLong(string));
    }

    private static void assertParseDouble(String string)
    {
        assertEquals(parseDouble(string.getBytes(US_ASCII), 0, string.length()), Double.parseDouble(string));

        // verify we can parse using a non-zero offset
        String padding = "9999";
        String padded = padding + string + padding;
        assertEquals(parseDouble(padded.getBytes(US_ASCII), padding.length(), string.length()), Double.parseDouble(string));
    }
}
