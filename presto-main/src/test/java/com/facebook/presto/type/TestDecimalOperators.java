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

import com.facebook.presto.common.type.UnknownType;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import org.testng.annotations.Test;

import static com.facebook.presto.common.function.OperatorType.INDETERMINATE;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static com.facebook.presto.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;

public class TestDecimalOperators
        extends AbstractTestFunctions
{
    @Test
    public void testAdd()
    {
        // short short -> short
        assertDecimalFunction("DECIMAL '137.7' + DECIMAL '17.1'", decimal("0154.8"));
        assertDecimalFunction("DECIMAL '-1' + DECIMAL '-2'", decimal("-03"));
        assertDecimalFunction("DECIMAL '1' + DECIMAL '2'", decimal("03"));
        assertDecimalFunction("DECIMAL '.1234567890123456' + DECIMAL '.1234567890123456'", decimal("0.2469135780246912"));
        assertDecimalFunction("DECIMAL '-.1234567890123456' + DECIMAL '-.1234567890123456'", decimal("-0.2469135780246912"));
        assertDecimalFunction("DECIMAL '1234567890123456' + DECIMAL '1234567890123456'", decimal("02469135780246912"));

        // long long -> long
        assertDecimalFunction("DECIMAL '123456789012345678' + DECIMAL '123456789012345678'", decimal("0246913578024691356"));
        assertDecimalFunction("DECIMAL '.123456789012345678' + DECIMAL '.123456789012345678'", decimal("0.246913578024691356"));
        assertDecimalFunction("DECIMAL '1234567890123456789' + DECIMAL '1234567890123456789'", decimal("02469135780246913578"));
        assertDecimalFunction("DECIMAL '12345678901234567890123456789012345678' + DECIMAL '12345678901234567890123456789012345678'", decimal("24691357802469135780246913578024691356"));
        assertDecimalFunction("DECIMAL '-99999999999999999999999999999999999999' + DECIMAL '99999999999999999999999999999999999999'", decimal("00000000000000000000000000000000000000"));

        // negative numbers
        assertDecimalFunction("DECIMAL '12345678901234567890' + DECIMAL '-12345678901234567890'", decimal("000000000000000000000"));
        assertDecimalFunction("DECIMAL '-12345678901234567890' + DECIMAL '12345678901234567890'", decimal("000000000000000000000"));
        assertDecimalFunction("DECIMAL '-12345678901234567890' + DECIMAL '-12345678901234567890'", decimal("-024691357802469135780"));
        assertDecimalFunction("DECIMAL '12345678901234567890' + DECIMAL '-12345678901234567891'", decimal("-000000000000000000001"));
        assertDecimalFunction("DECIMAL '-12345678901234567891' + DECIMAL '12345678901234567890'", decimal("-000000000000000000001"));
        assertDecimalFunction("DECIMAL '-12345678901234567890' + DECIMAL '12345678901234567891'", decimal("000000000000000000001"));
        assertDecimalFunction("DECIMAL '12345678901234567891' + DECIMAL '-12345678901234567890'", decimal("000000000000000000001"));

        // short short -> long
        assertDecimalFunction("DECIMAL '999999999999999999' + DECIMAL '999999999999999999'", decimal("1999999999999999998"));
        assertDecimalFunction("DECIMAL '999999999999999999' + DECIMAL '.999999999999999999'", decimal("0999999999999999999.999999999999999999"));

        // long short -> long
        assertDecimalFunction("DECIMAL '123456789012345678901234567890' + DECIMAL '.12345678'", decimal("123456789012345678901234567890.12345678"));
        assertDecimalFunction("DECIMAL '.123456789012345678901234567890' + DECIMAL '12345678'", decimal("12345678.123456789012345678901234567890"));

        // short long -> long
        assertDecimalFunction("DECIMAL '.12345678' + DECIMAL '123456789012345678901234567890'", decimal("123456789012345678901234567890.12345678"));
        assertDecimalFunction("DECIMAL '12345678' + DECIMAL '.123456789012345678901234567890'", decimal("12345678.123456789012345678901234567890"));

        // overflow tests
        assertInvalidFunction("DECIMAL '99999999999999999999999999999999999999' + DECIMAL '1'", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("DECIMAL '.1' + DECIMAL '99999999999999999999999999999999999999'", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("DECIMAL '1' + DECIMAL '99999999999999999999999999999999999999'", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("DECIMAL '99999999999999999999999999999999999999' + DECIMAL '.1'", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("DECIMAL '99999999999999999999999999999999999999' + DECIMAL '99999999999999999999999999999999999999'", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("DECIMAL '-99999999999999999999999999999999999999' + DECIMAL '-99999999999999999999999999999999999999'", NUMERIC_VALUE_OUT_OF_RANGE);

        // max supported value for rescaling
        // this works because rescaling allows overflowed values that exceed 10^38 but still fit in 127 bits.
        // 17014000000000000000000000000000000000 * 10 is an example of such number. Both arguments and result can be stored using DECIMAL(38,0) or DECIMAL(38,1)
        assertDecimalFunction("DECIMAL '17014000000000000000000000000000000000' + DECIMAL '-7014000000000000000000000000000000000.1'", decimal("9999999999999999999999999999999999999.9"));
        // 17015000000000000000000000000000000000 on the other hand is too large and rescaled to DECIMAL(38,1) it does not fit in in 127 bits
        assertInvalidFunction("DECIMAL '17015000000000000000000000000000000000' + DECIMAL '-7015000000000000000000000000000000000.1'", NUMERIC_VALUE_OUT_OF_RANGE);
    }

    @Test
    public void testSubtract()
    {
        // short short -> short
        assertDecimalFunction("DECIMAL '107.7' - DECIMAL '17.1'", decimal("0090.6"));
        assertDecimalFunction("DECIMAL '-1' - DECIMAL '-2'", decimal("01"));
        assertDecimalFunction("DECIMAL '1' - DECIMAL '2'", decimal("-01"));
        assertDecimalFunction("DECIMAL '.1234567890123456' - DECIMAL '.1234567890123456'", decimal("0.0000000000000000"));
        assertDecimalFunction("DECIMAL '-.1234567890123456' - DECIMAL '-.1234567890123456'", decimal("0.0000000000000000"));
        assertDecimalFunction("DECIMAL '1234567890123456' - DECIMAL '1234567890123456'", decimal("00000000000000000"));

        // long long -> long
        assertDecimalFunction("DECIMAL '1234567890123456789' - DECIMAL '1234567890123456789'", decimal("00000000000000000000"));
        assertDecimalFunction("DECIMAL '.1234567890123456789' - DECIMAL '.1234567890123456789'", decimal("0.0000000000000000000"));
        assertDecimalFunction("DECIMAL '12345678901234567890' - DECIMAL '12345678901234567890'", decimal("000000000000000000000"));
        assertDecimalFunction("DECIMAL '12345678901234567890123456789012345678' - DECIMAL '12345678901234567890123456789012345678'", decimal("00000000000000000000000000000000000000"));
        assertDecimalFunction("DECIMAL '-12345678901234567890' - DECIMAL '12345678901234567890'", decimal("-024691357802469135780"));

        // negative numbers
        assertDecimalFunction("DECIMAL '12345678901234567890' - DECIMAL '12345678901234567890'", decimal("000000000000000000000"));
        assertDecimalFunction("DECIMAL '-12345678901234567890' - DECIMAL '-12345678901234567890'", decimal("000000000000000000000"));
        assertDecimalFunction("DECIMAL '-12345678901234567890' - DECIMAL '12345678901234567890'", decimal("-024691357802469135780"));
        assertDecimalFunction("DECIMAL '12345678901234567890' - DECIMAL '12345678901234567891'", decimal("-000000000000000000001"));
        assertDecimalFunction("DECIMAL '-12345678901234567891' - DECIMAL '-12345678901234567890'", decimal("-000000000000000000001"));
        assertDecimalFunction("DECIMAL '-12345678901234567890' - DECIMAL '-12345678901234567891'", decimal("000000000000000000001"));
        assertDecimalFunction("DECIMAL '12345678901234567891' - DECIMAL '12345678901234567890'", decimal("000000000000000000001"));

        // short short -> long
        assertDecimalFunction("DECIMAL '999999999999999999' - DECIMAL '999999999999999999'", decimal("0000000000000000000"));
        assertDecimalFunction("DECIMAL '999999999999999999' - DECIMAL '.999999999999999999'", decimal("0999999999999999998.000000000000000001"));

        // long short -> long
        assertDecimalFunction("DECIMAL '123456789012345678901234567890' - DECIMAL '.00000001'", decimal("123456789012345678901234567889.99999999"));
        assertDecimalFunction("DECIMAL '.000000000000000000000000000001' - DECIMAL '87654321'", decimal("-87654320.999999999999999999999999999999"));

        // short long -> long
        assertDecimalFunction("DECIMAL '.00000001' - DECIMAL '123456789012345678901234567890'", decimal("-123456789012345678901234567889.99999999"));
        assertDecimalFunction("DECIMAL '12345678' - DECIMAL '.000000000000000000000000000001'", decimal("12345677.999999999999999999999999999999"));

        // overflow tests
        assertInvalidFunction("DECIMAL '-99999999999999999999999999999999999999' - DECIMAL '1'", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("DECIMAL '.1' - DECIMAL '99999999999999999999999999999999999999'", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("DECIMAL '-1' - DECIMAL '99999999999999999999999999999999999999'", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("DECIMAL '99999999999999999999999999999999999999' - DECIMAL '.1'", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("DECIMAL '-99999999999999999999999999999999999999' - DECIMAL '99999999999999999999999999999999999999'", NUMERIC_VALUE_OUT_OF_RANGE);

        // max supported value for rescaling
        // this works because rescaling allows overflowed values that exceed 10^38 but still fit in 127 bits.
        // 17014000000000000000000000000000000000 * 10 is an example of such number. Both arguments and result can be stored using DECIMAL(38,0) or DECIMAL(38,1)
        assertDecimalFunction("DECIMAL '17014000000000000000000000000000000000' - DECIMAL '7014000000000000000000000000000000000.1'", decimal("9999999999999999999999999999999999999.9"));
        // 17015000000000000000000000000000000000 on the other hand is too large and rescaled to DECIMAL(38,1) it does not fit in in 127 bits
        assertInvalidFunction("DECIMAL '17015000000000000000000000000000000000' - DECIMAL '7015000000000000000000000000000000000.1'", NUMERIC_VALUE_OUT_OF_RANGE);
    }

    @Test
    public void testMultiply()
    {
        // short short -> short
        assertDecimalFunction("DECIMAL '12' * DECIMAL '3'", decimal("036"));
        assertDecimalFunction("DECIMAL '12' * DECIMAL '-3'", decimal("-036"));
        assertDecimalFunction("DECIMAL '-12' * DECIMAL '3'", decimal("-036"));
        assertDecimalFunction("DECIMAL '1234567890123456' * DECIMAL '3'", decimal("03703703670370368"));
        assertDecimalFunction("DECIMAL '.1234567890123456' * DECIMAL '3'", decimal("0.3703703670370368"));
        assertDecimalFunction("DECIMAL '.1234567890123456' * DECIMAL '.3'", decimal(".03703703670370368"));

        // short short -> long
        assertDecimalFunction("DECIMAL '12345678901234567' * DECIMAL '123456789012345670'", decimal("01524157875323883455265967556774890"));
        assertDecimalFunction("DECIMAL '-12345678901234567' * DECIMAL '123456789012345670'", decimal("-01524157875323883455265967556774890"));
        assertDecimalFunction("DECIMAL '-12345678901234567' * DECIMAL '-123456789012345670'", decimal("01524157875323883455265967556774890"));
        assertDecimalFunction("DECIMAL '.12345678901234567' * DECIMAL '.123456789012345670'", decimal(".01524157875323883455265967556774890"));

        // long short -> long
        assertDecimalFunction("DECIMAL '12345678901234567890123456789012345678' * DECIMAL '3'", decimal("37037036703703703670370370367037037034"));
        assertDecimalFunction("DECIMAL '1234567890123456789.0123456789012345678' * DECIMAL '3'", decimal("3703703670370370367.0370370367037037034"));
        assertDecimalFunction("DECIMAL '.12345678901234567890123456789012345678' * DECIMAL '3'", decimal(".37037036703703703670370370367037037034"));

        // short long -> long
        assertDecimalFunction("DECIMAL '3' * DECIMAL '12345678901234567890123456789012345678'", decimal("37037036703703703670370370367037037034"));
        assertDecimalFunction("DECIMAL '3' * DECIMAL '1234567890123456789.0123456789012345678'", decimal("3703703670370370367.0370370367037037034"));
        assertDecimalFunction("DECIMAL '3' * DECIMAL '.12345678901234567890123456789012345678'", decimal(".37037036703703703670370370367037037034"));

        // long long -> long
        assertDecimalFunction("CAST(3 AS DECIMAL(38,0)) * CAST(2 AS DECIMAL(38,0))", decimal("00000000000000000000000000000000000006"));
        assertDecimalFunction("CAST(3 AS DECIMAL(38,0)) * CAST(DECIMAL '0.2' AS DECIMAL(38,1))", decimal("0000000000000000000000000000000000000.6"));
        assertDecimalFunction("DECIMAL '.1234567890123456789' * DECIMAL '.1234567890123456789'", decimal(".01524157875323883675019051998750190521"));

        // scale exceeds max precision
        assertInvalidFunction("DECIMAL '.1234567890123456789' * DECIMAL '.12345678901234567890'", "DECIMAL scale must be in range [0, precision]");
        assertInvalidFunction("DECIMAL '.1' * DECIMAL '.12345678901234567890123456789012345678'", "DECIMAL scale must be in range [0, precision]");

        // runtime overflow tests
        assertInvalidFunction("DECIMAL '12345678901234567890123456789012345678' * DECIMAL '9'", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("DECIMAL '.12345678901234567890123456789012345678' * DECIMAL '9'", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("DECIMAL '12345678901234567890123456789012345678' * DECIMAL '-9'", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("DECIMAL '.12345678901234567890123456789012345678' * DECIMAL '-9'", NUMERIC_VALUE_OUT_OF_RANGE);
    }

    @Test
    public void testDivide()
    {
        // short short -> short
        assertDecimalFunction("DECIMAL '1' / DECIMAL '3'", decimal("0"));
        assertDecimalFunction("DECIMAL '1' / DECIMAL '3'", decimal("0"));
        assertDecimalFunction("DECIMAL '1.0' / DECIMAL '3'", decimal("0.3"));
        assertDecimalFunction("DECIMAL '1.0' / DECIMAL '0.1'", decimal("10.0"));
        assertDecimalFunction("DECIMAL '1.0' / DECIMAL '9.0'", decimal("00.1"));
        assertDecimalFunction("DECIMAL '500.00' / DECIMAL '0.1'", decimal("5000.00"));
        assertDecimalFunction("DECIMAL '100.00' / DECIMAL '0.3'", decimal("0333.33"));
        assertDecimalFunction("DECIMAL '100.00' / DECIMAL '0.30'", decimal("00333.33"));
        assertDecimalFunction("DECIMAL '100.00' / DECIMAL '-0.30'", decimal("-00333.33"));
        assertDecimalFunction("DECIMAL '200.00' / DECIMAL '0.3'", decimal("0666.67"));
        assertDecimalFunction("DECIMAL '200.00000' / DECIMAL '0.3'", decimal("0666.66667"));
        assertDecimalFunction("DECIMAL '200.00000' / DECIMAL '-0.3'", decimal("-0666.66667"));
        assertDecimalFunction("DECIMAL '999999999999999999' / DECIMAL '1'", decimal("999999999999999999"));
        assertDecimalFunction("DECIMAL '9' / DECIMAL '000000000000000003'", decimal("3"));
        assertDecimalFunction("DECIMAL '9.0' / DECIMAL '3.0'", decimal("03.0"));
        assertDecimalFunction("DECIMAL '999999999999999999' / DECIMAL '500000000000000000'", decimal("000000000000000002"));
        assertDecimalFunction("DECIMAL '1' / DECIMAL '999999999999999999'", decimal("0"));
        // round
        assertDecimalFunction("DECIMAL '9' / DECIMAL '5'", decimal("2"));
        assertDecimalFunction("DECIMAL '7' / DECIMAL '5'", decimal("1"));
        assertDecimalFunction("DECIMAL '-9' / DECIMAL '5'", decimal("-2"));
        assertDecimalFunction("DECIMAL '-7' / DECIMAL '5'", decimal("-1"));
        assertDecimalFunction("DECIMAL '-9' / DECIMAL '-5'", decimal("2"));
        assertDecimalFunction("DECIMAL '-7' / DECIMAL '-5'", decimal("1"));
        assertDecimalFunction("DECIMAL '9' / DECIMAL '-5'", decimal("-2"));
        assertDecimalFunction("DECIMAL '7' / DECIMAL '-5'", decimal("-1"));

        // short short -> long
        assertDecimalFunction("DECIMAL '10' / DECIMAL '.000000001'", decimal("10000000000.000000000"));

        // long short -> long
        assertDecimalFunction("DECIMAL '200000000000000000000000000000000000' / DECIMAL '0.30'", decimal("666666666666666666666666666666666666.67"));
        assertDecimalFunction("DECIMAL '200000000000000000000000000000000000' / DECIMAL '-0.30'", decimal("-666666666666666666666666666666666666.67"));
        assertDecimalFunction("DECIMAL '-.20000000000000000000000000000000000000' / DECIMAL '0.30'", decimal("-.66666666666666666666666666666666666667"));
        assertDecimalFunction("DECIMAL '-.20000000000000000000000000000000000000' / DECIMAL '-0.30'", decimal(".66666666666666666666666666666666666667"));
        assertDecimalFunction("DECIMAL '.20000000000000000000000000000000000000' / DECIMAL '0.30'", decimal(".66666666666666666666666666666666666667"));
        // round
        assertDecimalFunction("DECIMAL '500000000000000000000000000000000075' / DECIMAL '50'", decimal("010000000000000000000000000000000002"));
        assertDecimalFunction("DECIMAL '500000000000000000000000000000000070' / DECIMAL '50'", decimal("010000000000000000000000000000000001"));
        assertDecimalFunction("DECIMAL '-500000000000000000000000000000000075' / DECIMAL '50'", decimal("-010000000000000000000000000000000002"));
        assertDecimalFunction("DECIMAL '-500000000000000000000000000000000070' / DECIMAL '50'", decimal("-010000000000000000000000000000000001"));
        assertDecimalFunction("DECIMAL '500000000000000000000000000000000075' / DECIMAL '-50'", decimal("-010000000000000000000000000000000002"));
        assertDecimalFunction("DECIMAL '500000000000000000000000000000000070' / DECIMAL '-50'", decimal("-010000000000000000000000000000000001"));
        assertDecimalFunction("DECIMAL '-500000000000000000000000000000000075' / DECIMAL '-50'", decimal("010000000000000000000000000000000002"));
        assertDecimalFunction("DECIMAL '-500000000000000000000000000000000070' / DECIMAL '-50'", decimal("010000000000000000000000000000000001"));

        // short long -> long
        assertDecimalFunction("DECIMAL '0.1' / DECIMAL '.0000000000000000001'", decimal("1000000000000000000.0000000000000000000"));
        assertDecimalFunction("DECIMAL '-0.1' / DECIMAL '.0000000000000000001'", decimal("-1000000000000000000.0000000000000000000"));

        // short long -> short
        assertDecimalFunction("DECIMAL '9' / DECIMAL '000000000000000003.0'", decimal("03.0"));
        assertDecimalFunction("DECIMAL '1' / DECIMAL '99999999999999999999999999999999999999'", decimal("0"));

        // long long -> long
        assertDecimalFunction("DECIMAL '99999999999999999999999999999999999999' / DECIMAL '11111111111111111111111111111111111111'", decimal("00000000000000000000000000000000000009"));
        assertDecimalFunction("DECIMAL '99999999999999999999999999999999999999' / DECIMAL '-11111111111111111111111111111111111111'", decimal("-00000000000000000000000000000000000009"));
        assertDecimalFunction("DECIMAL '99999999999999999999999999999999999998' / DECIMAL '99999999999999999999999999999999999999'", decimal("00000000000000000000000000000000000001"));
        assertDecimalFunction("DECIMAL '9999999999999999999999999999999999999.8' / DECIMAL '9999999999999999999999999999999999999.9'", decimal("0000000000000000000000000000000000001.0"));
        assertDecimalFunction("DECIMAL '9999999999999999999999.9' / DECIMAL '1111111111111111111111.100'", decimal("0000000000000000000000009.000"));
        assertDecimalFunction("CAST('1635619.3155' AS DECIMAL(38,4)) / CAST('47497517.7405' AS DECIMAL(38,4))", decimal("0000000000000000000000000000000000.0344"));
        assertDecimalFunction("CAST('1000' AS DECIMAL(38,8)) / CAST('25' AS DECIMAL(38,8))", decimal("000000000000000000000000000040.00000000"));

        // runtime overflow
        assertInvalidFunction("DECIMAL '12345678901234567890123456789012345678' / DECIMAL '.1'", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("DECIMAL '.12345678901234567890123456789012345678' / DECIMAL '.1'", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("DECIMAL '12345678901234567890123456789012345678' / DECIMAL '.12345678901234567890123456789012345678'", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("DECIMAL '1' / DECIMAL '.12345678901234567890123456789012345678'", NUMERIC_VALUE_OUT_OF_RANGE);

        // division by zero tests
        assertInvalidFunction("DECIMAL '1' / DECIMAL '0'", DIVISION_BY_ZERO);
        assertInvalidFunction("DECIMAL '1.000000000000000000000000000000000000' / DECIMAL '0'", DIVISION_BY_ZERO);
        assertInvalidFunction("DECIMAL '1.000000000000000000000000000000000000' / DECIMAL '0.0000000000000000000000000000000000000'", DIVISION_BY_ZERO);
        assertInvalidFunction("DECIMAL '1' / DECIMAL '0.0000000000000000000000000000000000000'", DIVISION_BY_ZERO);
    }

    @Test
    public void testModulus()
    {
        // short short -> short
        assertDecimalFunction("DECIMAL '1' % DECIMAL '3'", decimal("1"));
        assertDecimalFunction("DECIMAL '10' % DECIMAL '3'", decimal("1"));
        assertDecimalFunction("DECIMAL '0' % DECIMAL '3'", decimal("0"));
        assertDecimalFunction("DECIMAL '10.0' % DECIMAL '3'", decimal("1.0"));
        assertDecimalFunction("DECIMAL '10.0' % DECIMAL '3.000'", decimal("1.000"));
        assertDecimalFunction("DECIMAL '7' % DECIMAL '3.0000000000000000'", decimal("1.0000000000000000"));
        assertDecimalFunction("DECIMAL '7.00000000000000000' % DECIMAL '3.00000000000000000'", decimal("1.00000000000000000"));
        assertDecimalFunction("DECIMAL '7.00000000000000000' % DECIMAL '3'", decimal("1.00000000000000000"));
        assertDecimalFunction("DECIMAL '7' % CAST(3 AS DECIMAL(17,0))", decimal("1"));
        assertDecimalFunction("DECIMAL '.1' % DECIMAL '.03'", decimal(".01"));
        assertDecimalFunction("DECIMAL '.0001' % DECIMAL '.03'", decimal(".0001"));
        assertDecimalFunction("DECIMAL '-10' % DECIMAL '3'", decimal("-1"));
        assertDecimalFunction("DECIMAL '10' % DECIMAL '-3'", decimal("1"));
        assertDecimalFunction("DECIMAL '-10' % DECIMAL '-3'", decimal("-1"));

        // short long -> short
        assertDecimalFunction("DECIMAL '7' % CAST(3 AS DECIMAL(38,0))", decimal("1"));
        assertDecimalFunction("DECIMAL '7' % CAST(3 AS DECIMAL(38,16))", decimal("1.0000000000000000"));
        assertDecimalFunction("DECIMAL '7.00000000000000000' % CAST(3 AS DECIMAL(38,17))", decimal("1.00000000000000000"));
        assertDecimalFunction("DECIMAL '-7.00000000000000000' % CAST(3 AS DECIMAL(38,17))", decimal("-1.00000000000000000"));
        assertDecimalFunction("DECIMAL '7.0000000000000000' % CAST(-3 AS DECIMAL(38,16))", decimal("1.0000000000000000"));
        assertDecimalFunction("DECIMAL '-7.0000000000000000' % CAST(-3 AS DECIMAL(38,16))", decimal("-1.0000000000000000"));

        // short long -> long
        assertDecimalFunction("DECIMAL '7' % DECIMAL '3.0000000000000000000000000000000000000'", decimal("1.0000000000000000000000000000000000000"));
        assertDecimalFunction("DECIMAL '7.00000000000000000' % DECIMAL '3.0000000000000000000000000000000000000'", decimal("1.0000000000000000000000000000000000000"));
        assertDecimalFunction("DECIMAL '.01' % DECIMAL '3.0000000000000000000000000000000000000'", decimal(".0100000000000000000000000000000000000"));
        assertDecimalFunction("DECIMAL '-7' % DECIMAL '3.0000000000000000000000000000000000000'", decimal("-1.0000000000000000000000000000000000000"));
        assertDecimalFunction("DECIMAL '7' % DECIMAL '-3.0000000000000000000000000000000000000'", decimal("1.0000000000000000000000000000000000000"));
        assertDecimalFunction("DECIMAL '-7' % DECIMAL '-3.0000000000000000000000000000000000000'", decimal("-1.0000000000000000000000000000000000000"));

        // long short -> short
        assertDecimalFunction("DECIMAL '99999999999999999999999999999999999997' % DECIMAL '3'", decimal("1"));
        assertDecimalFunction("DECIMAL '99999999999999999999999999999999999997' % DECIMAL '3.0000000000000000'", decimal("1.0000000000000000"));
        assertDecimalFunction("DECIMAL '-99999999999999999999999999999999999997' % DECIMAL '3'", decimal("-1"));
        assertDecimalFunction("DECIMAL '99999999999999999999999999999999999997' % DECIMAL '-3'", decimal("1"));
        assertDecimalFunction("DECIMAL '-99999999999999999999999999999999999997' % DECIMAL '-3'", decimal("-1"));

        // long short -> long
        assertDecimalFunction("DECIMAL '7.000000000000000000000000000000000000' % DECIMAL '3'", decimal("1.000000000000000000000000000000000000"));
        assertDecimalFunction("DECIMAL '-7.000000000000000000000000000000000000' % DECIMAL '3'", decimal("-1.000000000000000000000000000000000000"));
        assertDecimalFunction("DECIMAL '7.000000000000000000000000000000000000' % DECIMAL '-3'", decimal("1.000000000000000000000000000000000000"));
        assertDecimalFunction("DECIMAL '-7.000000000000000000000000000000000000' % DECIMAL '-3'", decimal("-1.000000000000000000000000000000000000"));

        // long long -> long
        assertDecimalFunction("CAST(7 AS DECIMAL(38,0)) % CAST(3 AS DECIMAL(38,0))", decimal("00000000000000000000000000000000000001"));
        assertDecimalFunction("CAST(7 AS DECIMAL(34,0)) % CAST(3 AS DECIMAL(38,0))", decimal("0000000000000000000000000000000001"));
        assertDecimalFunction("CAST(7 AS DECIMAL(38,0)) % CAST(3 AS DECIMAL(34,0))", decimal("0000000000000000000000000000000001"));
        assertDecimalFunction("CAST(-7 AS DECIMAL(38,0)) % CAST(3 AS DECIMAL(38,0))", decimal("-00000000000000000000000000000000000001"));
        assertDecimalFunction("CAST(7 AS DECIMAL(38,0)) % CAST(-3 AS DECIMAL(38,0))", decimal("00000000000000000000000000000000000001"));
        assertDecimalFunction("CAST(-7 AS DECIMAL(38,0)) % CAST(-3 AS DECIMAL(38,0))", decimal("-00000000000000000000000000000000000001"));

        // division by zero tests
        assertInvalidFunction("DECIMAL '1' % DECIMAL '0'", DIVISION_BY_ZERO);
        assertInvalidFunction("DECIMAL '1.000000000000000000000000000000000000' % DECIMAL '0'", DIVISION_BY_ZERO);
        assertInvalidFunction("DECIMAL '1.000000000000000000000000000000000000' % DECIMAL '0.0000000000000000000000000000000000000'", DIVISION_BY_ZERO);
        assertInvalidFunction("DECIMAL '1' % DECIMAL '0.0000000000000000000000000000000000000'", DIVISION_BY_ZERO);
        assertInvalidFunction("DECIMAL '1' % CAST(0 AS DECIMAL(38,0))", DIVISION_BY_ZERO);
    }

    @Test
    public void testNegation()
    {
        // short
        assertDecimalFunction("-DECIMAL '1' ", decimal("-1"));
        assertDecimalFunction("-DECIMAL '-1' ", decimal("1"));
        assertDecimalFunction("-DECIMAL '123456.00000010' ", decimal("-123456.00000010"));
        assertDecimalFunction("-DECIMAL '1234567.00500010734' ", decimal("-1234567.00500010734"));
        assertDecimalFunction("-DECIMAL '-1234567.00500010734' ", decimal("1234567.00500010734"));
        assertDecimalFunction("-DECIMAL '0' ", decimal("0"));

        // long
        assertDecimalFunction("-DECIMAL '12345678901234567890123456789012345678'", decimal("-12345678901234567890123456789012345678"));
        assertDecimalFunction("-DECIMAL '-12345678901234567890123456789012345678'", decimal("12345678901234567890123456789012345678"));
        assertDecimalFunction("-DECIMAL '123456789012345678.90123456789012345678'", decimal("-123456789012345678.90123456789012345678"));
    }

    @Test
    public void testEqual()
    {
        // short short
        assertFunction("DECIMAL '37' = DECIMAL '37'", BOOLEAN, true);
        assertFunction("DECIMAL '37' = DECIMAL '0037'", BOOLEAN, true);
        assertFunction("DECIMAL '37' = DECIMAL '37.0'", BOOLEAN, true);
        assertFunction("DECIMAL '37.0' = DECIMAL '37'", BOOLEAN, true);
        assertFunction("DECIMAL '-0.000' = DECIMAL '0000.00000'", BOOLEAN, true);
        assertFunction("DECIMAL '37' = DECIMAL '38'", BOOLEAN, false);
        assertFunction("DECIMAL '37' = DECIMAL '38.0'", BOOLEAN, false);
        assertFunction("DECIMAL '37.0' = DECIMAL '38'", BOOLEAN, false);
        assertFunction("DECIMAL '-37.000' = DECIMAL '37.000'", BOOLEAN, false);
        assertFunction("DECIMAL '-123456789123456789' = DECIMAL '-123456789123456789'", BOOLEAN, true);
        assertFunction("DECIMAL '-123456789123456789' = DECIMAL '123456789123456789'", BOOLEAN, false);

        // short long
        assertFunction("DECIMAL '37' = DECIMAL '37.0000000000000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '37' = DECIMAL '00000000037.0000000000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '037.0' = DECIMAL '00000000037.0000000000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '-0.000' = DECIMAL '00000000000000000.00000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '37' = DECIMAL '00000000038.0000000000000000000000'", BOOLEAN, false);
        assertFunction("DECIMAL '-37' = DECIMAL '00000000037.0000000000000000000000'", BOOLEAN, false);
        assertFunction("DECIMAL '37.0000000000000000' = DECIMAL '00000000037.0000000000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '37.0000000000000000' = DECIMAL '00000000036.0000000000000000000000'", BOOLEAN, false);

        // long short
        assertFunction("DECIMAL '37.0000000000000000000000000' = DECIMAL '37'", BOOLEAN, true);
        assertFunction("DECIMAL '00000000037.0000000000000000000000' = DECIMAL '37'", BOOLEAN, true);
        assertFunction("DECIMAL '00000000037.0000000000000000000000' = DECIMAL '037.0'", BOOLEAN, true);
        assertFunction("DECIMAL '-00000000000000.0000000000000000' = DECIMAL '000.00'", BOOLEAN, true);
        assertFunction("DECIMAL '00000000038.0000000000000000000000' = DECIMAL '37'", BOOLEAN, false);
        assertFunction("DECIMAL '-00000000037.0000000000000000000000' = DECIMAL '37'", BOOLEAN, false);
        assertFunction("DECIMAL '00000000037.0000000000000000000000' = DECIMAL '37.0000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '00000000036.0000000000000000000000' = DECIMAL '37.0000000000000000'", BOOLEAN, false);

        // long long
        assertFunction("DECIMAL '37.0000000000000000000000000' = DECIMAL '37.0000000000000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '00000000037.0000000000000000000000' = DECIMAL '0037.0000000000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '000000000000000.00000000000000000' = DECIMAL '-000000000000000000000000.0000000'", BOOLEAN, true);
        assertFunction("DECIMAL '00000000038.0000000000000000000000' = DECIMAL '000000000037.00000000000000000000000'", BOOLEAN, false);
        assertFunction("DECIMAL '-00000000038.0000000000000000000000' = DECIMAL '00000000038.0000000000000000000000'", BOOLEAN, false);
    }

    @Test
    public void testNotEqual()
    {
        // short short
        assertFunction("DECIMAL '37' != DECIMAL '37'", BOOLEAN, false);
        assertFunction("DECIMAL '37' != DECIMAL '0037'", BOOLEAN, false);
        assertFunction("DECIMAL '37' != DECIMAL '37.0'", BOOLEAN, false);
        assertFunction("DECIMAL '37.0' != DECIMAL '37'", BOOLEAN, false);
        assertFunction("DECIMAL '0' != DECIMAL '-0.00'", BOOLEAN, false);
        assertFunction("DECIMAL '37' != DECIMAL '-37'", BOOLEAN, true);
        assertFunction("DECIMAL '37' != DECIMAL '38'", BOOLEAN, true);
        assertFunction("DECIMAL '37' != DECIMAL '38.0'", BOOLEAN, true);
        assertFunction("DECIMAL '37.0' != DECIMAL '38'", BOOLEAN, true);
        assertFunction("DECIMAL '37' != DECIMAL '-37'", BOOLEAN, true);
        assertFunction("DECIMAL '-999999999999999999' != DECIMAL '-999999999999999999'", BOOLEAN, false);
        assertFunction("DECIMAL '-999999999999999999' != DECIMAL '999999999999999998'", BOOLEAN, true);

        // short long
        assertFunction("DECIMAL '37' != DECIMAL '37.0000000000000000000000000'", BOOLEAN, false);
        assertFunction("DECIMAL '37' != DECIMAL '00000000037.0000000000000000000000'", BOOLEAN, false);
        assertFunction("DECIMAL '037.0' != DECIMAL '00000000037.0000000000000000000000'", BOOLEAN, false);
        assertFunction("DECIMAL '0' != DECIMAL '-00000000.000000000000000000000'", BOOLEAN, false);
        assertFunction("DECIMAL '37' != DECIMAL '00000000038.0000000000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '37' != DECIMAL '-000000000037.00000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '38.0000000000000000' != DECIMAL '00000000038.0000000000000000000001'", BOOLEAN, true);
        assertFunction("DECIMAL '-987654321987654321' != DECIMAL '-987654321987654321.00000000000000000'", BOOLEAN, false);

        // long short
        assertFunction("DECIMAL '37.0000000000000000000000000' != DECIMAL '37'", BOOLEAN, false);
        assertFunction("DECIMAL '00000000037.0000000000000000000000' != DECIMAL '37'", BOOLEAN, false);
        assertFunction("DECIMAL '00000000037.0000000000000000000000' != DECIMAL '037.0'", BOOLEAN, false);
        assertFunction("DECIMAL '0000000.000000000000000' != DECIMAL '-0'", BOOLEAN, false);
        assertFunction("DECIMAL '00000000038.0000000000000000000000' != DECIMAL '37'", BOOLEAN, true);
        assertFunction("DECIMAL '00000000000037.00000000000000000000' != DECIMAL '-37'", BOOLEAN, true);
        assertFunction("DECIMAL '00000000037.0000000000000000000000' != DECIMAL '37.0000000000000001'", BOOLEAN, true);
        assertFunction("DECIMAL '-00000000000037.00000000000000000000' != DECIMAL '-37.0000000000000001'", BOOLEAN, true);

        // long long
        assertFunction("DECIMAL '37.0000000000000000000000000' != DECIMAL '37.0000000000000000000000000'", BOOLEAN, false);
        assertFunction("DECIMAL '00000000037.0000000000000000000000' != DECIMAL '0037.0000000000000000000000'", BOOLEAN, false);
        assertFunction("DECIMAL '000000000000000000000.000000000000000' != DECIMAL '-000000000.0000000000000000000000000'", BOOLEAN, false);
        assertFunction("DECIMAL '00000000038.0000000000000000000000' != DECIMAL '000000000037.00000000000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '00000000000037.00000000000000000000' != DECIMAL '-00000000000037.00000000000000000000'", BOOLEAN, true);
    }

    @Test
    public void testLessThan()
    {
        // short short
        assertFunction("DECIMAL '37' < DECIMAL '37'", BOOLEAN, false);
        assertFunction("DECIMAL '37' < DECIMAL '0037'", BOOLEAN, false);
        assertFunction("DECIMAL '37' < DECIMAL '37.0'", BOOLEAN, false);
        assertFunction("DECIMAL '37.0' < DECIMAL '37'", BOOLEAN, false);
        assertFunction("DECIMAL '0037.0' < DECIMAL '00036.0'", BOOLEAN, false);
        assertFunction("DECIMAL '37' < DECIMAL '38'", BOOLEAN, true);
        assertFunction("DECIMAL '37' < DECIMAL '0038.0'", BOOLEAN, true);
        assertFunction("DECIMAL '0037.0' < DECIMAL '38'", BOOLEAN, true);
        assertFunction("DECIMAL '-100' < DECIMAL '20'", BOOLEAN, true);
        // test possible overflow on rescale
        assertFunction("DECIMAL '1' < DECIMAL '10000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '1.0000000000000000' < DECIMAL '10000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '-123456789123456789' < DECIMAL '-123456789123456789'", BOOLEAN, false);
        assertFunction("DECIMAL '-123456789123456789' < DECIMAL '123456789123456789'", BOOLEAN, true);

        // short long
        assertFunction("DECIMAL '37' < DECIMAL '37.0000000000000000000000000'", BOOLEAN, false);
        assertFunction("DECIMAL '37' < DECIMAL '00000000037.0000000000000000000000'", BOOLEAN, false);
        assertFunction("DECIMAL '037.0' < DECIMAL '00000000037.0000000000000000000000'", BOOLEAN, false);
        assertFunction("DECIMAL '037.0' < DECIMAL '00000000036.0000000000000000000000'", BOOLEAN, false);
        assertFunction("DECIMAL '37' < DECIMAL '37.00000000000000000000000001'", BOOLEAN, true);
        assertFunction("DECIMAL '37' < DECIMAL '00000000038.0000000000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '037.0' < DECIMAL '00000000038.0000000000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '-100' < DECIMAL '20.00000000000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '38.0000000000000000' < DECIMAL '00000000038.0000000000000000000001'", BOOLEAN, true);
        assertFunction("DECIMAL '-987654321987654321' < DECIMAL '-987654321987654321.00000000000000000'", BOOLEAN, false);

        // long short
        assertFunction("DECIMAL '37.0000000000000000000000000' < DECIMAL '37'", BOOLEAN, false);
        assertFunction("DECIMAL '00000000037.0000000000000000000000' < DECIMAL '37'", BOOLEAN, false);
        assertFunction("DECIMAL '00000000037.0000000000000000000000' < DECIMAL '037.0'", BOOLEAN, false);
        assertFunction("DECIMAL '00000000037.0000000000000000000000' < DECIMAL '036.0'", BOOLEAN, false);
        assertFunction("DECIMAL '37.0000000000000000000000000' < DECIMAL '38'", BOOLEAN, true);
        assertFunction("DECIMAL '00000000037.0000000000000000000001' < DECIMAL '38'", BOOLEAN, true);
        assertFunction("DECIMAL '00000000037.0000000000000000000000' < DECIMAL '038.0'", BOOLEAN, true);
        assertFunction("DECIMAL '-00000000000100.000000000000' < DECIMAL '20'", BOOLEAN, true);
        assertFunction("DECIMAL '00000000037.0000000000000000000000' < DECIMAL '37.0000000000000001'", BOOLEAN, true);
        assertFunction("DECIMAL '-00000000000037.00000000000000000000' < DECIMAL '-37.0000000000000001'", BOOLEAN, false);

        // long long
        assertFunction("DECIMAL '37.0000000000000000000000000' < DECIMAL '37.0000000000000000000000000'", BOOLEAN, false);
        assertFunction("DECIMAL '37.0000000000000000000000000' < DECIMAL '00000037.0000000000000000000000000'", BOOLEAN, false);
        assertFunction("DECIMAL '37.0000000000000000000000000' < DECIMAL '00000036.0000000000000000000000000'", BOOLEAN, false);
        assertFunction("DECIMAL '00000000037.0000000000000000000000' < DECIMAL '38.0000000000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '00000000037.0000000000000000000000' < DECIMAL '000000000037.00000000000000000000001'", BOOLEAN, true);
        assertFunction("DECIMAL '-00000000000100.000000000000' < DECIMAL '0000000020.0000000000000'", BOOLEAN, true);
    }

    @Test
    public void testGreaterThan()
    {
        // short short
        assertFunction("DECIMAL '37' > DECIMAL '37'", BOOLEAN, false);
        assertFunction("DECIMAL '37' > DECIMAL '0037'", BOOLEAN, false);
        assertFunction("DECIMAL '37' > DECIMAL '37.0'", BOOLEAN, false);
        assertFunction("DECIMAL '37.0' > DECIMAL '37'", BOOLEAN, false);
        assertFunction("DECIMAL '0037.0' > DECIMAL '00038.0'", BOOLEAN, false);
        assertFunction("DECIMAL '37' > DECIMAL '36'", BOOLEAN, true);
        assertFunction("DECIMAL '37' > DECIMAL '0036.0'", BOOLEAN, true);
        assertFunction("DECIMAL '0037.0' > DECIMAL '36'", BOOLEAN, true);
        assertFunction("DECIMAL '100' > DECIMAL '-20'", BOOLEAN, true);
        // test possible overflow on rescale
        assertFunction("DECIMAL '10000000000000000' > DECIMAL '1'", BOOLEAN, true);
        assertFunction("DECIMAL '10000000000000000' > DECIMAL '1.0000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '-123456789123456788' > DECIMAL '-123456789123456789'", BOOLEAN, true);
        assertFunction("DECIMAL '-123456789123456789' > DECIMAL '123456789123456789'", BOOLEAN, false);

        // short long
        assertFunction("DECIMAL '37' > DECIMAL '37.0000000000000000000000000'", BOOLEAN, false);
        assertFunction("DECIMAL '37' > DECIMAL '00000000037.0000000000000000000000'", BOOLEAN, false);
        assertFunction("DECIMAL '037.0' > DECIMAL '00000000037.0000000000000000000000'", BOOLEAN, false);
        assertFunction("DECIMAL '037.0' > DECIMAL '00000000038.0000000000000000000000'", BOOLEAN, false);
        assertFunction("DECIMAL '37' > DECIMAL '36.00000000000000000000000001'", BOOLEAN, true);
        assertFunction("DECIMAL '37' > DECIMAL '00000000036.0000000000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '037.0' > DECIMAL '00000000036.0000000000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '100' > DECIMAL '-0000000020.00000000000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '38.0000000000000000' > DECIMAL '00000000038.0000000000000000000001'", BOOLEAN, false);
        assertFunction("DECIMAL '-987654321987654320' > DECIMAL '-987654321987654321.00000000000000000'", BOOLEAN, true);

        // long short
        assertFunction("DECIMAL '37.0000000000000000000000000' > DECIMAL '37'", BOOLEAN, false);
        assertFunction("DECIMAL '00000000037.0000000000000000000000' > DECIMAL '37'", BOOLEAN, false);
        assertFunction("DECIMAL '00000000037.0000000000000000000000' > DECIMAL '037.0'", BOOLEAN, false);
        assertFunction("DECIMAL '00000000037.0000000000000000000000' > DECIMAL '038.0'", BOOLEAN, false);
        assertFunction("DECIMAL '37.0000000000000000000000000' > DECIMAL '36'", BOOLEAN, true);
        assertFunction("DECIMAL '00000000037.0000000000000000000001' > DECIMAL '36'", BOOLEAN, true);
        assertFunction("DECIMAL '00000000037.0000000000000000000000' > DECIMAL '036.0'", BOOLEAN, true);
        assertFunction("DECIMAL '0000000000100.000000000000' > DECIMAL '20'", BOOLEAN, true);
        assertFunction("DECIMAL '00000000037.0000000000000000000002' > DECIMAL '37.0000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '-00000000000037.00000000000000000000' > DECIMAL '-37.0000000000000001'", BOOLEAN, true);

        // long long
        assertFunction("DECIMAL '37.0000000000000000000000000' > DECIMAL '37.0000000000000000000000000'", BOOLEAN, false);
        assertFunction("DECIMAL '37.0000000000000000000000000' > DECIMAL '00000037.0000000000000000000000000'", BOOLEAN, false);
        assertFunction("DECIMAL '37.0000000000000000000000000' > DECIMAL '00000038.0000000000000000000000000'", BOOLEAN, false);
        assertFunction("DECIMAL '00000000037.0000000000000000000000' > DECIMAL '36.0000000000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '00000000037.0000000000000000000000' > DECIMAL '000000000036.9999999999999999999999'", BOOLEAN, true);
        assertFunction("DECIMAL '000000000000100.0000000000000000000000' > DECIMAL '-0000000020.00000000000000000000000'", BOOLEAN, true);
    }

    @Test
    public void testLessThanOrEqual()
    {
        // short short
        assertFunction("DECIMAL '37' <= DECIMAL '36'", BOOLEAN, false);
        assertFunction("DECIMAL '37' <= DECIMAL '000036.99999'", BOOLEAN, false);
        assertFunction("DECIMAL '37' <= DECIMAL '37'", BOOLEAN, true);
        assertFunction("DECIMAL '37' <= DECIMAL '0037'", BOOLEAN, true);
        assertFunction("DECIMAL '37' <= DECIMAL '37.0'", BOOLEAN, true);
        assertFunction("DECIMAL '37.0' <= DECIMAL '37'", BOOLEAN, true);
        assertFunction("DECIMAL '37' <= DECIMAL '38'", BOOLEAN, true);
        assertFunction("DECIMAL '37' <= DECIMAL '0038.0'", BOOLEAN, true);
        assertFunction("DECIMAL '0037.0' <= DECIMAL '38'", BOOLEAN, true);
        assertFunction("DECIMAL '-100' <= DECIMAL '20'", BOOLEAN, true);
        assertFunction("DECIMAL '-123456789123456789' <= DECIMAL '-123456789123456789'", BOOLEAN, true);
        assertFunction("DECIMAL '-123456789123456789' <= DECIMAL '123456789123456789'", BOOLEAN, true);

        // short long
        assertFunction("DECIMAL '037.0' <= DECIMAL '00000000036.0000000000000000000000'", BOOLEAN, false);
        assertFunction("DECIMAL '037.0' <= DECIMAL '00000000036.9999999999999999999'", BOOLEAN, false);
        assertFunction("DECIMAL '37' <= DECIMAL '37.0000000000000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '37' <= DECIMAL '00000000037.0000000000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '037.0' <= DECIMAL '00000000037.0000000000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '37' <= DECIMAL '37.00000000000000000000000001'", BOOLEAN, true);
        assertFunction("DECIMAL '37' <= DECIMAL '00000000038.0000000000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '037.0' <= DECIMAL '00000000038.0000000000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '-100' <= DECIMAL '20.00000000000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '38.0000000000000000' <= DECIMAL '00000000038.0000000000000000000001'", BOOLEAN, true);
        assertFunction("DECIMAL '-987654321987654321' <= DECIMAL '-987654321987654321.00000000000000000'", BOOLEAN, true);

        // long short
        assertFunction("DECIMAL '00000000037.0000000000000000000000' <= DECIMAL '036.0'", BOOLEAN, false);
        assertFunction("DECIMAL '00000000037.0000000000000000000000' <= DECIMAL '000036.99999999'", BOOLEAN, false);
        assertFunction("DECIMAL '37.0000000000000000000000000' <= DECIMAL '37'", BOOLEAN, true);
        assertFunction("DECIMAL '00000000037.0000000000000000000000' <= DECIMAL '37'", BOOLEAN, true);
        assertFunction("DECIMAL '00000000037.0000000000000000000000' <= DECIMAL '037.0'", BOOLEAN, true);
        assertFunction("DECIMAL '37.0000000000000000000000000' <= DECIMAL '38'", BOOLEAN, true);
        assertFunction("DECIMAL '00000000037.0000000000000000000001' <= DECIMAL '38'", BOOLEAN, true);
        assertFunction("DECIMAL '00000000037.0000000000000000000000' <= DECIMAL '038.0'", BOOLEAN, true);
        assertFunction("DECIMAL '-00000000000100.000000000000' <= DECIMAL '20'", BOOLEAN, true);
        assertFunction("DECIMAL '00000000037.0000000000000000000000' <= DECIMAL '37.0000000000000001'", BOOLEAN, true);
        assertFunction("DECIMAL '-00000000000037.00000000000000000000' <= DECIMAL '-37.0000000000000001'", BOOLEAN, false);

        // long long
        assertFunction("DECIMAL '37.0000000000000000000000000' <= DECIMAL '00000036.0000000000000000000000000'", BOOLEAN, false);
        assertFunction("DECIMAL '37.0000000000000000000000000' <= DECIMAL '37.0000000000000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '37.0000000000000000000000000' <= DECIMAL '00000037.0000000000000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '00000000037.0000000000000000000000' <= DECIMAL '38.0000000000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '00000000037.0000000000000000000000' <= DECIMAL '000000000037.00000000000000000000001'", BOOLEAN, true);
        assertFunction("DECIMAL '-00000000000100.000000000000' <= DECIMAL '0000000020.0000000000000'", BOOLEAN, true);
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        // short short
        assertFunction("DECIMAL '37' >= DECIMAL '38'", BOOLEAN, false);
        assertFunction("DECIMAL '37' >= DECIMAL '000038.00001'", BOOLEAN, false);
        assertFunction("DECIMAL '37' >= DECIMAL '37'", BOOLEAN, true);
        assertFunction("DECIMAL '37' >= DECIMAL '0037'", BOOLEAN, true);
        assertFunction("DECIMAL '37' >= DECIMAL '37.0'", BOOLEAN, true);
        assertFunction("DECIMAL '37.0' >= DECIMAL '37'", BOOLEAN, true);
        assertFunction("DECIMAL '37' >= DECIMAL '36'", BOOLEAN, true);
        assertFunction("DECIMAL '37' >= DECIMAL '0036.0'", BOOLEAN, true);
        assertFunction("DECIMAL '0037.0' >= DECIMAL '36'", BOOLEAN, true);
        assertFunction("DECIMAL '100' >= DECIMAL '-20'", BOOLEAN, true);
        assertFunction("DECIMAL '-123456789123456789' >= DECIMAL '-123456789123456789'", BOOLEAN, true);
        assertFunction("DECIMAL '-123456789123456789' >= DECIMAL '123456789123456789'", BOOLEAN, false);

        // short long
        assertFunction("DECIMAL '037.0' >= DECIMAL '00000000038.0000000000000000000000'", BOOLEAN, false);
        assertFunction("DECIMAL '037.0' >= DECIMAL '00000000037.0000000000000000001'", BOOLEAN, false);
        assertFunction("DECIMAL '37' >= DECIMAL '37.0000000000000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '37' >= DECIMAL '00000000037.0000000000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '037.0' >= DECIMAL '00000000037.0000000000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '37' >= DECIMAL '36.9999999999999999999'", BOOLEAN, true);
        assertFunction("DECIMAL '37' >= DECIMAL '00000000036.0000000000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '037.0' >= DECIMAL '00000000036.0000000000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '100' >= DECIMAL '-0000000020.00000000000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '38.0000000000000000' >= DECIMAL '00000000038.0000000000000000000001'", BOOLEAN, false);
        assertFunction("DECIMAL '-987654321987654321' >= DECIMAL '-987654321987654321.00000000000000000'", BOOLEAN, true);

        // long short
        assertFunction("DECIMAL '00000000037.0000000000000000000000' >= DECIMAL '038.0'", BOOLEAN, false);
        assertFunction("DECIMAL '00000000037.0000000000000000000000' >= DECIMAL '000037.00000001'", BOOLEAN, false);
        assertFunction("DECIMAL '37.0000000000000000000000000' >= DECIMAL '37'", BOOLEAN, true);
        assertFunction("DECIMAL '00000000037.0000000000000000000000' >= DECIMAL '37'", BOOLEAN, true);
        assertFunction("DECIMAL '00000000037.0000000000000000000000' >= DECIMAL '037.0'", BOOLEAN, true);
        assertFunction("DECIMAL '37.0000000000000000000000000' >= DECIMAL '36'", BOOLEAN, true);
        assertFunction("DECIMAL '00000000037.0000000000000000000001' >= DECIMAL '36'", BOOLEAN, true);
        assertFunction("DECIMAL '00000000037.0000000000000000000000' >= DECIMAL '036.0'", BOOLEAN, true);
        assertFunction("DECIMAL '0000000000100.000000000000' >= DECIMAL '20'", BOOLEAN, true);
        assertFunction("DECIMAL '00000000037.0000000000000000000000' >= DECIMAL '37.0000000000000001'", BOOLEAN, false);
        assertFunction("DECIMAL '-00000000000037.00000000000000000000' >= DECIMAL '-37.0000000000000001'", BOOLEAN, true);

        // long long
        assertFunction("DECIMAL '37.0000000000000000000000000' >= DECIMAL '00000038.0000000000000000000000000'", BOOLEAN, false);
        assertFunction("DECIMAL '37.0000000000000000000000000' >= DECIMAL '37.0000000000000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '37.0000000000000000000000000' >= DECIMAL '00000037.0000000000000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '00000000037.0000000000000000000000' >= DECIMAL '36.0000000000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '00000000037.0000000000000000000000' >= DECIMAL '000000000036.9999999999999999'", BOOLEAN, true);
        assertFunction("DECIMAL '000000000000100.0000000000000000000000' >= DECIMAL '-0000000020.00000000000000000000000'", BOOLEAN, true);
    }

    @Test
    public void testBetween()
    {
        // short short short
        assertFunction("DECIMAL '1' BETWEEN DECIMAL '-5' AND DECIMAL '5'", BOOLEAN, true);
        assertFunction("DECIMAL '-6' BETWEEN DECIMAL '-5' AND DECIMAL '5'", BOOLEAN, false);
        assertFunction("DECIMAL '6' BETWEEN DECIMAL '-5' AND DECIMAL '5'", BOOLEAN, false);
        assertFunction("DECIMAL '333333333333333333' BETWEEN DECIMAL '-111111111111111111' AND DECIMAL '999999999999999999'", BOOLEAN, true);

        // short short long
        assertFunction("DECIMAL '1' BETWEEN DECIMAL '-5' AND DECIMAL '5.00000000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '-6' BETWEEN DECIMAL '-5' AND DECIMAL '5.00000000000000000000'", BOOLEAN, false);
        assertFunction("DECIMAL '6' BETWEEN DECIMAL '-5' AND DECIMAL '5.00000000000000000000'", BOOLEAN, false);
        assertFunction("DECIMAL '333333333333333333' BETWEEN DECIMAL '-111111111111111111' AND DECIMAL '9999999999999999999'", BOOLEAN, true);

        // short long long
        assertFunction("DECIMAL '1' BETWEEN DECIMAL '-5.00000000000000000000' AND DECIMAL '5.00000000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '-6' BETWEEN DECIMAL '-5.00000000000000000000' AND DECIMAL '5.00000000000000000000'", BOOLEAN, false);
        assertFunction("DECIMAL '6' BETWEEN DECIMAL '-5.00000000000000000000' AND DECIMAL '5.00000000000000000000'", BOOLEAN, false);
        assertFunction("DECIMAL '333333333333333333' BETWEEN DECIMAL '-1111111111111111111' AND DECIMAL '9999999999999999999'", BOOLEAN, true);

        // long short short
        assertFunction("DECIMAL '1.00000000000000000000' BETWEEN DECIMAL '-5' AND DECIMAL '5'", BOOLEAN, true);
        assertFunction("DECIMAL '-6.00000000000000000000' BETWEEN DECIMAL '-5' AND DECIMAL '5'", BOOLEAN, false);
        assertFunction("DECIMAL '6.00000000000000000000' BETWEEN DECIMAL '-5' AND DECIMAL '5'", BOOLEAN, false);
        assertFunction("DECIMAL '333333333333333333.3' BETWEEN DECIMAL '-111111111111111111' AND DECIMAL '999999999999999999'", BOOLEAN, true);

        // long short long
        assertFunction("DECIMAL '1.00000000000000000000' BETWEEN DECIMAL '-5' AND DECIMAL '5.00000000000000000000' ", BOOLEAN, true);
        assertFunction("DECIMAL '-6.00000000000000000000' BETWEEN DECIMAL '-5' AND DECIMAL '5.00000000000000000000' ", BOOLEAN, false);
        assertFunction("DECIMAL '6.00000000000000000000' BETWEEN DECIMAL '-5' AND DECIMAL '5.00000000000000000000' ", BOOLEAN, false);
        assertFunction("DECIMAL '333333333333333333.3' BETWEEN DECIMAL '-111111111111111111' AND DECIMAL '9999999999999999999'", BOOLEAN, true);

        // long long short
        assertFunction("DECIMAL '1.00000000000000000000' BETWEEN DECIMAL '-5.00000000000000000000'  AND DECIMAL '5'", BOOLEAN, true);
        assertFunction("DECIMAL '-6.00000000000000000000' BETWEEN DECIMAL '-5.00000000000000000000'  AND DECIMAL '5'", BOOLEAN, false);
        assertFunction("DECIMAL '6.00000000000000000000' BETWEEN DECIMAL '-5.00000000000000000000'  AND DECIMAL '5'", BOOLEAN, false);
        assertFunction("DECIMAL '333333333333333333.3' BETWEEN DECIMAL '-111111111111111111.1' AND DECIMAL '999999999999999999'", BOOLEAN, true);

        // long long long
        assertFunction("DECIMAL '1.00000000000000000000' BETWEEN DECIMAL '-5.00000000000000000000'  AND DECIMAL '5.00000000000000000000'", BOOLEAN, true);
        assertFunction("DECIMAL '-6.00000000000000000000' BETWEEN DECIMAL '-5.00000000000000000000'  AND DECIMAL '5.00000000000000000000'", BOOLEAN, false);
        assertFunction("DECIMAL '6.00000000000000000000' BETWEEN DECIMAL '-5.00000000000000000000'  AND DECIMAL '5.00000000000000000000'", BOOLEAN, false);
    }

    @Test
    public void testAddDecimalBigint()
    {
        // decimal + bigint
        assertDecimalFunction("DECIMAL '123456789012345678' + 123456789012345678", decimal("00246913578024691356"));
        assertDecimalFunction("DECIMAL '.123456789012345678' + 123456789012345678", decimal("00123456789012345678.123456789012345678"));
        assertDecimalFunction("DECIMAL '-1234567890123456789' + 1234567890123456789", decimal("00000000000000000000"));

        // bigint + decimal
        assertDecimalFunction("123456789012345678 + DECIMAL '123456789012345678'", decimal("00246913578024691356"));
        assertDecimalFunction("123456789012345678 + DECIMAL '.123456789012345678'", decimal("00123456789012345678.123456789012345678"));
        assertDecimalFunction("1234567890123456789 + DECIMAL '-1234567890123456789'", decimal("00000000000000000000"));
    }

    @Test
    public void testSubtractDecimalBigint()
    {
        // decimal - bigint
        assertDecimalFunction("DECIMAL '1234567890123456789' - 1234567890123456789", decimal("00000000000000000000"));
        assertDecimalFunction("DECIMAL '.1234567890123456789' - 1234567890123456789", decimal("-1234567890123456788.8765432109876543211"));
        assertDecimalFunction("DECIMAL '-1234567890123456789' - 1234567890123456789", decimal("-02469135780246913578"));

        // bigint - decimal
        assertDecimalFunction("1234567890123456789 - DECIMAL '1234567890123456789'", decimal("00000000000000000000"));
        assertDecimalFunction("1234567890123456789 - DECIMAL '.1234567890123456789'", decimal("1234567890123456788.8765432109876543211"));
        assertDecimalFunction("-1234567890123456789 - DECIMAL '1234567890123456789'", decimal("-02469135780246913578"));
    }

    @Test
    public void testMultiplyDecimalBigint()
    {
        // decimal bigint
        assertDecimalFunction("DECIMAL '12345678901234567' * 12345678901234567", decimal("000152415787532388345526596755677489"));
        assertDecimalFunction("DECIMAL '-12345678901234567' * 12345678901234567", decimal("-000152415787532388345526596755677489"));
        assertDecimalFunction("DECIMAL '-12345678901234567' * -12345678901234567", decimal("000152415787532388345526596755677489"));
        assertDecimalFunction("DECIMAL '.1234567890' * BIGINT '3'", decimal("0000000000000000000.3703703670"));

        // bigint decimal
        assertDecimalFunction("12345678901234567 * DECIMAL '12345678901234567'", decimal("000152415787532388345526596755677489"));
        assertDecimalFunction("12345678901234567 * DECIMAL '-12345678901234567'", decimal("-000152415787532388345526596755677489"));
        assertDecimalFunction("-12345678901234567 * DECIMAL '-12345678901234567'", decimal("000152415787532388345526596755677489"));
        assertDecimalFunction("BIGINT '3' * DECIMAL '.1234567890'", decimal("0000000000000000000.3703703670"));
    }

    @Test
    public void testDivideDecimalBigint()
    {
        // bigint / decimal
        assertDecimalFunction("BIGINT '9' / DECIMAL '3.0'", decimal("00000000000000000003.0"));
        assertDecimalFunction("BIGINT '9' / DECIMAL '000000000000000003.0'", decimal("00000000000000000003.0"));
        assertDecimalFunction("BIGINT '18' / DECIMAL '0.01'", decimal("000000000000000001800.00"));
        assertDecimalFunction("BIGINT '9' / DECIMAL '00000000000000000.1'", decimal("00000000000000000090.0"));

        // decimal / bigint
        assertDecimalFunction("DECIMAL '9.0' / BIGINT '3'", decimal("3.0"));
        assertDecimalFunction("DECIMAL '0.018' / BIGINT '9'", decimal(".002"));
        assertDecimalFunction("DECIMAL '.999' / BIGINT '9'", decimal(".111"));
    }

    @Test
    public void testModulusDecimalBigint()
    {
        // bigint % decimal
        assertDecimalFunction("BIGINT '13' % DECIMAL '9.0'", decimal("4.0"));
        assertDecimalFunction("BIGINT '18' % DECIMAL '0.01'", decimal(".00"));
        assertDecimalFunction("BIGINT '9' % DECIMAL '.1'", decimal(".0"));

        // decimal % bigint
        assertDecimalFunction("DECIMAL '9.0' / BIGINT '3'", decimal("3.0"));
        assertDecimalFunction("DECIMAL '0.018' / BIGINT '9'", decimal(".002"));
        assertDecimalFunction("DECIMAL '.999' / BIGINT '9'", decimal(".111"));
    }

    @Test
    public void testIsDistinctFrom()
    {
        assertFunction("CAST(NULL AS DECIMAL) IS DISTINCT FROM CAST(NULL AS DECIMAL)", BOOLEAN, false);
        assertFunction("DECIMAL '37' IS DISTINCT FROM DECIMAL '37'", BOOLEAN, false);
        assertFunction("37 IS DISTINCT FROM 38", BOOLEAN, true);
        assertFunction("NULL IS DISTINCT FROM 37", BOOLEAN, true);
        assertFunction("37 IS DISTINCT FROM NULL", BOOLEAN, true);

        // short short
        assertFunction("DECIMAL '-2' IS DISTINCT FROM DECIMAL '-3'", BOOLEAN, true);
        assertFunction("DECIMAL '-2' IS DISTINCT FROM CAST(NULL AS DECIMAL(1,0))", BOOLEAN, true);
        assertFunction("CAST(NULL AS DECIMAL(2,0)) IS DISTINCT FROM CAST(NULL AS DECIMAL(1,0))", BOOLEAN, false);
        assertFunction("DECIMAL '-2' IS DISTINCT FROM DECIMAL '-2'", BOOLEAN, false);
        assertFunction("CAST(NULL AS DECIMAL(1,0)) IS DISTINCT FROM DECIMAL '-2'", BOOLEAN, true);

        // long long
        assertFunction("DECIMAL '12345678901234567.89012345678901234567' IS DISTINCT FROM DECIMAL '12345678901234567.8902345678901234567'", BOOLEAN, true);
        assertFunction("DECIMAL '12345678901234567.89012345678901234567' IS DISTINCT FROM CAST(NULL AS DECIMAL(36,1))", BOOLEAN, true);
        assertFunction("CAST(NULL AS DECIMAL(36,1)) IS DISTINCT FROM CAST(NULL AS DECIMAL(27,3))", BOOLEAN, false);
        assertFunction("DECIMAL '-12345678901234567.89012345678901234567' IS DISTINCT FROM DECIMAL '-12345678901234567.89012345678901234567'", BOOLEAN, false);
        assertFunction("CAST(NULL AS DECIMAL(36,1)) IS DISTINCT FROM DECIMAL '12345678901234567.89012345678901234567'", BOOLEAN, true);

        // short long
        assertFunction("DECIMAL '12345678901234567.89012345678901234567' IS DISTINCT FROM DECIMAL '-3'", BOOLEAN, true);
        assertFunction("DECIMAL '12345678901234567.89012345678901234567' IS DISTINCT FROM CAST(NULL AS DECIMAL(1,0))", BOOLEAN, true);
        assertFunction("CAST(NULL AS DECIMAL(36,1)) IS DISTINCT FROM CAST(NULL AS DECIMAL(1,0))", BOOLEAN, false);
        assertFunction("DECIMAL '00000000000000007.80000000000000000000' IS DISTINCT FROM DECIMAL '7.8'", BOOLEAN, false);
        assertFunction("CAST(NULL AS DECIMAL(36,1)) IS DISTINCT FROM DECIMAL '7.8'", BOOLEAN, true);

        // with unknown
        assertFunction("NULL IS DISTINCT FROM DECIMAL '-2'", BOOLEAN, true);
        assertFunction("DECIMAL '-2' IS DISTINCT FROM NULL", BOOLEAN, true);
        assertFunction("NULL IS DISTINCT FROM DECIMAL '12345678901234567.89012345678901234567'", BOOLEAN, true);
        assertFunction("DECIMAL '12345678901234567.89012345678901234567' IS DISTINCT FROM NULL", BOOLEAN, true);

        // delegation from other operator (exercises block-position convention implementation)
        assertFunction("ARRAY [1.23, 4.56] IS DISTINCT FROM ARRAY [1.23, 4.56]", BOOLEAN, false);
        assertFunction("ARRAY [1.23, NULL] IS DISTINCT FROM ARRAY [1.23, 4.56]", BOOLEAN, true);
        assertFunction("ARRAY [1.23, NULL] IS DISTINCT FROM ARRAY [NULL, 4.56]", BOOLEAN, true);
        assertFunction("ARRAY [1234567890.123456789, 9876543210.987654321] IS DISTINCT FROM ARRAY [1234567890.123456789, 9876543210.987654321]", BOOLEAN, false);
        assertFunction("ARRAY [1234567890.123456789, NULL] IS DISTINCT FROM ARRAY [1234567890.123456789, 9876543210.987654321]", BOOLEAN, true);
        assertFunction("ARRAY [1234567890.123456789, NULL] IS DISTINCT FROM ARRAY [NULL, 9876543210.987654321]", BOOLEAN, true);
    }

    @Test
    public void testNullIf()
    {
        // short short
        assertDecimalFunction("nullif(DECIMAL '-2', DECIMAL '-3')", decimal("-2"));
        assertDecimalFunction("nullif(DECIMAL '-2', CAST(NULL AS DECIMAL(1,0)))", decimal("-2"));
        assertFunction("nullif(DECIMAL '-2', DECIMAL '-2')", createDecimalType(1, 0), null);
        assertFunction("nullif(CAST(NULL AS DECIMAL(1,0)), DECIMAL '-2')", createDecimalType(1, 0), null);
        assertFunction("nullif(CAST(NULL AS DECIMAL(1,0)), CAST(NULL AS DECIMAL(1,0)))", createDecimalType(1, 0), null);

        // long long
        assertDecimalFunction("nullif(DECIMAL '12345678901234567.89012345678901234567', DECIMAL '12345678901234567.8902345678901234567')", decimal("12345678901234567.89012345678901234567"));
        assertDecimalFunction("nullif(DECIMAL '12345678901234567.89012345678901234567', CAST(NULL AS DECIMAL(36,1)))", decimal("12345678901234567.89012345678901234567"));
        assertFunction("nullif(DECIMAL '12345678901234567.89012345678901234567', DECIMAL '12345678901234567.89012345678901234567')", createDecimalType(37, 20), null);
        assertFunction("nullif(CAST(NULL AS DECIMAL(38,0)), DECIMAL '12345678901234567.89012345678901234567')", createDecimalType(38, 0), null);
        assertFunction("nullif(CAST(NULL AS DECIMAL(38,0)), CAST(NULL AS DECIMAL(38,0)))", createDecimalType(38, 0), null);

        // short long
        assertDecimalFunction("nullif(DECIMAL '12345678901234567.89012345678901234567', DECIMAL '-3')", decimal("12345678901234567.89012345678901234567"));
        assertDecimalFunction("nullif(DECIMAL '12345678901234567.89012345678901234567', CAST(NULL AS DECIMAL(1,0)))", decimal("12345678901234567.89012345678901234567"));
        assertFunction("nullif(DECIMAL '12345678901234567.89012345678901234567', DECIMAL '12345678901234567.89012345678901234567')", createDecimalType(37, 20), null);
        assertFunction("nullif(CAST(NULL AS DECIMAL(1,0)), DECIMAL '12345678901234567.89012345678901234567')", createDecimalType(1, 0), null);

        // with unknown
        assertFunction("nullif(NULL, NULL)", UnknownType.UNKNOWN, null);
        assertFunction("nullif(NULL, DECIMAL '-2')", UnknownType.UNKNOWN, null);
        assertDecimalFunction("nullif(DECIMAL '-2', NULL)", decimal("-2"));
        assertFunction("nullif(NULL, DECIMAL '12345678901234567.89012345678901234567')", UnknownType.UNKNOWN, null);
        assertDecimalFunction("nullif(DECIMAL '12345678901234567.89012345678901234567', NULL)", decimal("12345678901234567.89012345678901234567"));
    }

    @Test
    public void testCoalesce()
    {
        assertDecimalFunction("coalesce(2.1, null, cast(null as decimal(5,3)))", decimal("02.100"));
        assertFunction("coalesce(cast(null as decimal(17,3)), null, cast(null as decimal(12,3)))", createDecimalType(17, 3), null);
        assertDecimalFunction("coalesce(3, 2.1, null, cast(null as decimal(6,3)))", decimal("0000000003.000"));
        assertFunction("coalesce(cast(null as decimal(17,3)), null, cast(null as decimal(12,3)))", createDecimalType(17, 3), null);
    }

    @Test
    public void testIndeterminate()
    {
        assertOperator(INDETERMINATE, "cast(null as DECIMAL)", BOOLEAN, true);
        assertOperator(INDETERMINATE, "cast(null as DECIMAL(37,3))", BOOLEAN, true);
        assertOperator(INDETERMINATE, "DECIMAL '.999'", BOOLEAN, false);
        assertOperator(INDETERMINATE, "DECIMAL '18'", BOOLEAN, false);
        assertOperator(INDETERMINATE, "DECIMAL '9.0'", BOOLEAN, false);
        assertOperator(INDETERMINATE, "DECIMAL '12345678901234567.89012345678901234567'", BOOLEAN, false);
    }
}
