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
package com.facebook.presto.type;

import com.facebook.presto.spi.type.SqlDecimal;
import org.testng.annotations.Test;

import java.math.BigInteger;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class TestDecimalCasts
        extends AbstractTestDecimalFunctions
{
    @Test
    public void testDecimalToBooleanCasts()
    {
        assertFunction("CAST(DECIMAL 1.1 AS BOOLEAN)", BOOLEAN, true);
        assertFunction("CAST(DECIMAL 0.0 AS BOOLEAN)", BOOLEAN, false);
    }

    @Test
    public void testDecimalToBigintCasts()
    {
        assertFunction("CAST(DECIMAL 2.34 AS BIGINT)", BIGINT, 2L);
        assertFunction("CAST(DECIMAL 2.5 AS BIGINT)", BIGINT, 3L);
        assertFunction("CAST(DECIMAL 2.49 AS BIGINT)", BIGINT, 2L);
        assertFunction("CAST(DECIMAL 20 AS BIGINT)", BIGINT, 20L);
        assertFunction("CAST(DECIMAL 1 AS BIGINT)", BIGINT, 1L);
        assertFunction("CAST(DECIMAL 0 AS BIGINT)", BIGINT, 0L);
        assertFunction("CAST(DECIMAL -20 AS BIGINT)", BIGINT, -20L);
        assertFunction("CAST(DECIMAL -1 AS BIGINT)", BIGINT, -1L);
        assertFunction("CAST(DECIMAL -2.49 AS BIGINT)", BIGINT, -2L);
        assertFunction("CAST(DECIMAL -2.5 AS BIGINT)", BIGINT, -3L);
        assertFunction("CAST(DECIMAL 0.12345678901234567 AS BIGINT)", BIGINT, 0L);
        assertFunction("CAST(DECIMAL 0.99999999999999999 AS BIGINT)", BIGINT, 1L);
    }

    @Test
    public void testBigintToDecimalCasts()
    {
        assertDecimalFunction("CAST(234 AS DECIMAL(4,1))", decimal("234.0"));
        assertDecimalFunction("CAST(234 AS DECIMAL(5,2))", decimal("234.00"));
        assertDecimalFunction("CAST(234 AS DECIMAL(4,0))", decimal("0234"));
        assertDecimalFunction("CAST(-234 AS DECIMAL(4,1))", decimal("-234.0"));
        assertDecimalFunction("CAST(0 AS DECIMAL(4,2))", decimal("00.00"));
        assertDecimalFunction("CAST(0 AS DECIMAL(0,0))", new SqlDecimal(new BigInteger("0"), 0, 0));
        assertDecimalFunction("CAST(123456789012345678 AS DECIMAL(18, 0))", decimal("123456789012345678"));

        assertInvalidFunction("CAST(1234567890 AS DECIMAL(18, 10))", "Cannot cast BIGINT 1234567890 to decimal(18, 10)");
        assertInvalidFunction("CAST(123 AS DECIMAL(2, 1))", "Cannot cast BIGINT 123 to decimal(2, 1)");
        assertInvalidFunction("CAST(-123 AS DECIMAL(2, 1))", "Cannot cast BIGINT -123 to decimal(2, 1)");
        assertInvalidFunction("CAST(123456789012345678 AS DECIMAL(18, 1))", "Cannot cast BIGINT 123456789012345678 to decimal(18, 1)");
    }

    @Test
    public void testDecimalToDoubleCasts()
    {
        assertFunction("CAST(DECIMAL 2.34 AS DOUBLE)", DOUBLE, 2.34);
        assertFunction("CAST(DECIMAL 0 AS DOUBLE)", DOUBLE, 0.0);
        assertFunction("CAST(DECIMAL 1 AS DOUBLE)", DOUBLE, 1.0);
        assertFunction("CAST(DECIMAL -2.49 AS DOUBLE)", DOUBLE, -2.49);
    }

    @Test
    public void testDecimalToVarcharCasts()
    {
        assertFunction("CAST(DECIMAL 2.34 AS VARCHAR)", VARCHAR, "2.34");
        assertFunction("CAST(DECIMAL 23400 AS VARCHAR)", VARCHAR, "23400");
        assertFunction("CAST(DECIMAL 0.0034 AS VARCHAR)", VARCHAR, "0.0034");
        assertFunction("CAST(DECIMAL 0 AS VARCHAR)", VARCHAR, "0");
        assertFunction("CAST(DECIMAL 0.12345678901234567 AS VARCHAR)", VARCHAR, "0.12345678901234567");

        assertFunction("CAST(DECIMAL -10 AS VARCHAR)", VARCHAR, "-10");
        assertFunction("CAST(DECIMAL -1.0 AS VARCHAR)", VARCHAR, "-1.0");
        assertFunction("CAST(DECIMAL -1.00 AS VARCHAR)", VARCHAR, "-1.00");
        assertFunction("CAST(DECIMAL -1.00000 AS VARCHAR)", VARCHAR, "-1.00000");
        assertFunction("CAST(DECIMAL -0.1 AS VARCHAR)", VARCHAR, "-0.1");
        assertFunction("CAST(DECIMAL -.001 AS VARCHAR)", VARCHAR, "-.001");
        assertFunction("CAST(DECIMAL -1234567890.12345678 AS VARCHAR)", VARCHAR, "-1234567890.12345678");
    }
}
