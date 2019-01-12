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
package io.prestosql.plugin.hive;

import io.prestosql.spi.type.DecimalType;
import org.testng.annotations.Test;

import java.math.BigDecimal;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.testng.Assert.assertEquals;

public class TestHiveDecimalParser
{
    @Test
    public void testParseDecimal()
    {
        checkParseDecimal("3", 2, 1, new BigDecimal("3.0"));
        checkParseDecimal("3.1", 2, 1, new BigDecimal("3.1"));

        // rounding
        checkParseDecimal("3.11", 2, 1, new BigDecimal("3.1"));
        checkParseDecimal("3.16", 2, 1, new BigDecimal("3.2"));

        // rouding of half (odd and even)
        checkParseDecimal("3.15", 2, 1, new BigDecimal("3.2"));
        checkParseDecimal("3.25", 2, 1, new BigDecimal("3.3"));

        // negative
        checkParseDecimal("-3", 2, 1, new BigDecimal("-3.0"));
        checkParseDecimal("-3.1", 2, 1, new BigDecimal("-3.1"));

        // negative rounding
        checkParseDecimal("-3.11", 2, 1, new BigDecimal("-3.1"));
        checkParseDecimal("-3.16", 2, 1, new BigDecimal("-3.2"));

        // negative rounding of half (odd and even)
        checkParseDecimal("-3.15", 2, 1, new BigDecimal("-3.2"));
        checkParseDecimal("-3.25", 2, 1, new BigDecimal("-3.3"));
    }

    private void checkParseDecimal(String input, int precision, int scale, BigDecimal expected)
    {
        byte[] bytes = input.getBytes(US_ASCII);
        BigDecimal parsed = HiveDecimalParser.parseHiveDecimal(bytes, 0, bytes.length, DecimalType.createDecimalType(precision, scale));
        assertEquals(parsed, expected);
    }
}
