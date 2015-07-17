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
        assertDecimalFunction("DECIMAL 37.7 + DECIMAL 17.1", decimal("054.8"));
        assertDecimalFunction("DECIMAL 1 + DECIMAL 2", decimal("03"));

        assertDecimalFunction("DECIMAL -1 + DECIMAL -2", decimal("-03"));
        assertDecimalFunction("DECIMAL 1234567890123456789 + DECIMAL 1234567890123456789", decimal("2469135780246913578"));
        assertDecimalFunction("DECIMAL .1234567890123456789 + DECIMAL .1234567890123456789", decimal(".2469135780246913578"));
        assertDecimalFunction("DECIMAL 12345678901234567 + DECIMAL 12345678901234567", decimal("024691357802469134"));
        assertDecimalFunction("DECIMAL .12345678901234567 + DECIMAL .12345678901234567", decimal("0.24691357802469134"));

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
