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

import org.testng.annotations.Test;

import java.math.BigInteger;

import static org.testng.Assert.assertEquals;

public class TestSqlDecimal
{
    @Test
    public void testToString()
    {
        assertEquals(new SqlDecimal(new BigInteger("0"), 2, 1).toString(), "0.0");
        assertEquals(new SqlDecimal(new BigInteger("0"), 3, 2).toString(), "0.00");
        assertEquals(new SqlDecimal(new BigInteger("0"), 6, 5).toString(), "0.00000");
        assertEquals(new SqlDecimal(new BigInteger("0"), 10, 5).toString(), "0.00000");
        assertEquals(new SqlDecimal(new BigInteger("1"), 2, 1).toString(), "0.1");
        assertEquals(new SqlDecimal(new BigInteger("0"), 3, 3).toString(), "0.000");
        assertEquals(new SqlDecimal(new BigInteger("1"), 1, 0).toString(), "1");
        assertEquals(new SqlDecimal(new BigInteger("1000"), 4, 3).toString(), "1.000");
        assertEquals(new SqlDecimal(new BigInteger("12345678901234567890123456789012345678"), 38, 20)
                .toString(), "123456789012345678.90123456789012345678");

        assertEquals(new SqlDecimal(new BigInteger("-10"), 2, 1).toString(), "-1.0");
        assertEquals(new SqlDecimal(new BigInteger("-100"), 3, 2).toString(), "-1.00");
        assertEquals(new SqlDecimal(new BigInteger("-100000"), 6, 5).toString(), "-1.00000");
        assertEquals(new SqlDecimal(new BigInteger("-100000"), 10, 5).toString(), "-1.00000");
        assertEquals(new SqlDecimal(new BigInteger("-1"), 2, 1).toString(), "-0.1");
        assertEquals(new SqlDecimal(new BigInteger("-1"), 3, 3).toString(), "-0.001");
        assertEquals(new SqlDecimal(new BigInteger("-1"), 1, 0).toString(), "-1");
        assertEquals(new SqlDecimal(new BigInteger("-1000"), 4, 3).toString(), "-1.000");
        assertEquals(new SqlDecimal(new BigInteger("-12345678901234567890123456789012345678"), 38, 20)
                .toString(), "-123456789012345678.90123456789012345678");
    }
}
