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

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.spi.type.SqlDecimal;
import org.testng.annotations.Test;

import java.math.BigInteger;

import static com.facebook.presto.spi.type.ShortDecimalType.createDecimalType;

public class TestDecimalOperators
        extends AbstractTestFunctions
{
    @Test
    public void testAdd()
            throws Exception
    {
        // short short -> short
        assertDecimalFunction("DECIMAL 37.7 + DECIMAL 17.1", decimal("054.8"));
        assertDecimalFunction("DECIMAL -1 + DECIMAL -2", decimal("-03"));
        assertDecimalFunction("DECIMAL 1 + DECIMAL 2", decimal("03"));
        assertDecimalFunction("DECIMAL .12345678901234567 + DECIMAL .12345678901234567", decimal("0.24691357802469134"));
        assertDecimalFunction("DECIMAL -.12345678901234567 + DECIMAL -.12345678901234567", decimal("-0.24691357802469134"));
        assertDecimalFunction("DECIMAL 12345678901234567 + DECIMAL 12345678901234567", decimal("024691357802469134"));

        // long long -> long
        assertDecimalFunction("DECIMAL 1234567890123456789 + DECIMAL 1234567890123456789", decimal("02469135780246913578"));
        assertDecimalFunction("DECIMAL .1234567890123456789 + DECIMAL .1234567890123456789", decimal("0.2469135780246913578"));
        assertDecimalFunction("DECIMAL 12345678901234567890 + DECIMAL 12345678901234567890", decimal("024691357802469135780"));
        assertDecimalFunction("DECIMAL 12345678901234567890123456789012345678 + DECIMAL 12345678901234567890123456789012345678", decimal("24691357802469135780246913578024691356"));
        assertDecimalFunction("DECIMAL -12345678901234567890 + DECIMAL 12345678901234567890", decimal("000000000000000000000"));
        assertDecimalFunction("DECIMAL -99999999999999999999999999999999999999 + DECIMAL 99999999999999999999999999999999999999", decimal("00000000000000000000000000000000000000"));

        // short short -> long
        assertDecimalFunction("DECIMAL 999999999999999999 + DECIMAL 999999999999999999", decimal("1999999999999999998"));
        assertDecimalFunction("DECIMAL 999999999999999999 + DECIMAL .999999999999999999", decimal("0999999999999999999.999999999999999999"));

        // long short -> long
        assertDecimalFunction("DECIMAL 123456789012345678901234567890 + DECIMAL .12345678", decimal("123456789012345678901234567890.12345678"));
        assertDecimalFunction("DECIMAL .123456789012345678901234567890 + DECIMAL 12345678", decimal("12345678.123456789012345678901234567890"));

        // short long -> long
        assertDecimalFunction("DECIMAL .12345678 + DECIMAL 123456789012345678901234567890", decimal("123456789012345678901234567890.12345678"));
        assertDecimalFunction("DECIMAL 12345678 + DECIMAL .123456789012345678901234567890", decimal("12345678.123456789012345678901234567890"));

        // overflow tests
        assertInvalidFunction("DECIMAL 99999999999999999999999999999999999999 + DECIMAL 1", "DECIMAL result exceeds 38 digits");
        assertInvalidFunction("DECIMAL .1 + DECIMAL 99999999999999999999999999999999999999", "DECIMAL result exceeds 38 digits");
        assertInvalidFunction("DECIMAL 1 + DECIMAL 99999999999999999999999999999999999999", "DECIMAL result exceeds 38 digits");
        assertInvalidFunction("DECIMAL 99999999999999999999999999999999999999 + DECIMAL .1", "DECIMAL result exceeds 38 digits");
        assertInvalidFunction("DECIMAL 99999999999999999999999999999999999999 + DECIMAL 99999999999999999999999999999999999999", "DECIMAL result exceeds 38 digits");
        assertInvalidFunction("DECIMAL -99999999999999999999999999999999999999 + DECIMAL -99999999999999999999999999999999999999", "DECIMAL result exceeds 38 digits");
    }

    @Test
    public void testSubtract()
            throws Exception
    {
        // short short -> short
        assertDecimalFunction("DECIMAL 37.7 - DECIMAL 17.1", decimal("020.6"));
        assertDecimalFunction("DECIMAL -1 - DECIMAL -2", decimal("01"));
        assertDecimalFunction("DECIMAL 1 - DECIMAL 2", decimal("-01"));
        assertDecimalFunction("DECIMAL .12345678901234567 - DECIMAL .12345678901234567", decimal("0.00000000000000000"));
        assertDecimalFunction("DECIMAL -.12345678901234567 - DECIMAL -.12345678901234567", decimal("0.00000000000000000"));
        assertDecimalFunction("DECIMAL 12345678901234567 - DECIMAL 12345678901234567", decimal("000000000000000000"));

        // long long -> long
        assertDecimalFunction("DECIMAL 1234567890123456789 - DECIMAL 1234567890123456789", decimal("00000000000000000000"));
        assertDecimalFunction("DECIMAL .1234567890123456789 - DECIMAL .1234567890123456789", decimal("0.0000000000000000000"));
        assertDecimalFunction("DECIMAL 12345678901234567890 - DECIMAL 12345678901234567890", decimal("000000000000000000000"));
        assertDecimalFunction("DECIMAL 12345678901234567890123456789012345678 - DECIMAL 12345678901234567890123456789012345678", decimal("00000000000000000000000000000000000000"));
        assertDecimalFunction("DECIMAL -12345678901234567890 - DECIMAL 12345678901234567890", decimal("-024691357802469135780"));

        // short short -> long
        assertDecimalFunction("DECIMAL 999999999999999999 - DECIMAL 999999999999999999", decimal("0000000000000000000"));
        assertDecimalFunction("DECIMAL 999999999999999999 - DECIMAL .999999999999999999", decimal("0999999999999999998.000000000000000001"));

        // long short -> long
        assertDecimalFunction("DECIMAL 123456789012345678901234567890 - DECIMAL .00000001", decimal("123456789012345678901234567889.99999999"));
        assertDecimalFunction("DECIMAL .000000000000000000000000000001 - DECIMAL 87654321", decimal("-87654320.999999999999999999999999999999"));

        // short long -> long
        assertDecimalFunction("DECIMAL .00000001 - DECIMAL 123456789012345678901234567890", decimal("-123456789012345678901234567889.99999999"));
        assertDecimalFunction("DECIMAL 12345678 - DECIMAL .000000000000000000000000000001", decimal("12345677.999999999999999999999999999999"));

        // overflow tests
        assertInvalidFunction("DECIMAL -99999999999999999999999999999999999999 - DECIMAL 1", "DECIMAL result exceeds 38 digits");
        assertInvalidFunction("DECIMAL .1 - DECIMAL 99999999999999999999999999999999999999", "DECIMAL result exceeds 38 digits");
        assertInvalidFunction("DECIMAL -1 - DECIMAL 99999999999999999999999999999999999999", "DECIMAL result exceeds 38 digits");
        assertInvalidFunction("DECIMAL 99999999999999999999999999999999999999 - DECIMAL .1", "DECIMAL result exceeds 38 digits");
        assertInvalidFunction("DECIMAL -99999999999999999999999999999999999999 - DECIMAL 99999999999999999999999999999999999999", "DECIMAL result exceeds 38 digits");
    }

    private void assertDecimalFunction(String statement, SqlDecimal expectedResult)
    {
        assertFunction(statement,
                createDecimalType(expectedResult.getPrecision(), expectedResult.getScale()),
                expectedResult);
    }

    private SqlDecimal decimal(String decimalString)
    {
        int dotPos = decimalString.indexOf('.');
        String decimalStringNoDot = decimalString.replace(".", "");
        int precision = decimalStringNoDot.length();
        if (decimalStringNoDot.startsWith("-")) {
            precision--;
        }
        int scale = 0;
        if (dotPos != -1) {
            scale = decimalString.length() - dotPos - 1;
        }
        return new SqlDecimal(new BigInteger(decimalStringNoDot), precision, scale);
    }
}
