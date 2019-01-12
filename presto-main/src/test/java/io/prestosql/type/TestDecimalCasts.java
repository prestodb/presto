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
package io.prestosql.type;

import io.prestosql.operator.scalar.AbstractTestFunctions;
import org.testng.annotations.Test;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;

public class TestDecimalCasts
        extends AbstractTestFunctions
{
    @Test
    public void testBooleanToDecimalCasts()
    {
        assertDecimalFunction("CAST(true AS DECIMAL(2, 0))", decimal("01"));
        assertDecimalFunction("CAST(true AS DECIMAL(3, 1))", decimal("01.0"));
        assertDecimalFunction("CAST(true AS DECIMAL)", maxPrecisionDecimal(1));
        assertDecimalFunction("CAST(true AS DECIMAL(2))", decimal("01"));
        assertDecimalFunction("CAST(false AS DECIMAL(2, 0))", decimal("00"));
        assertDecimalFunction("CAST(false AS DECIMAL(2))", decimal("00"));
        assertDecimalFunction("CAST(false AS DECIMAL)", maxPrecisionDecimal(0));
        assertDecimalFunction("CAST(true AS DECIMAL(18, 0))", decimal("000000000000000001"));
        assertDecimalFunction("CAST(false AS DECIMAL(18, 2))", decimal("0000000000000000.00"));

        assertDecimalFunction("CAST(true AS DECIMAL(20, 10))", decimal("0000000001.0000000000"));
        assertDecimalFunction("CAST(false AS DECIMAL(20, 10))", decimal("0000000000.0000000000"));
        assertDecimalFunction("CAST(true as DECIMAL(30, 20))", decimal("0000000001.00000000000000000000"));
        assertDecimalFunction("CAST(false as DECIMAL(30, 20))", decimal("0000000000.00000000000000000000"));
    }

    @Test
    public void testDecimalToBooleanCasts()
    {
        assertFunction("CAST(DECIMAL '1.1' AS BOOLEAN)", BOOLEAN, true);
        assertFunction("CAST(DECIMAL '-1.1' AS BOOLEAN)", BOOLEAN, true);
        assertFunction("CAST(DECIMAL '0.0' AS BOOLEAN)", BOOLEAN, false);

        assertFunction("CAST(DECIMAL '1234567890.1234567890' AS BOOLEAN)", BOOLEAN, true);
        assertFunction("CAST(DECIMAL '-1234567890.1234567890' AS BOOLEAN)", BOOLEAN, true);
        assertFunction("CAST(DECIMAL '0.0000000000000000000' AS BOOLEAN)", BOOLEAN, false);
        assertFunction("CAST(DECIMAL '00000000000000001.0' AS BOOLEAN)", BOOLEAN, true);
        assertFunction("CAST(DECIMAL '000000000000000.000' AS BOOLEAN)", BOOLEAN, false);

        assertFunction("CAST(DECIMAL '0000000001.00000000000000000000' as BOOLEAN)", BOOLEAN, true);
        assertFunction("CAST(DECIMAL '0000000000.00000000000000000000' as BOOLEAN)", BOOLEAN, false);
    }

    @Test
    public void testBigintToDecimalCasts()
    {
        assertDecimalFunction("CAST(BIGINT '234' AS DECIMAL(4,1))", decimal("234.0"));
        assertDecimalFunction("CAST(BIGINT '234' AS DECIMAL(5,2))", decimal("234.00"));
        assertDecimalFunction("CAST(BIGINT '234' AS DECIMAL(4,0))", decimal("0234"));
        assertDecimalFunction("CAST(BIGINT '-234' AS DECIMAL(4,1))", decimal("-234.0"));
        assertDecimalFunction("CAST(BIGINT '0' AS DECIMAL(4,2))", decimal("00.00"));
        assertDecimalFunction("CAST(BIGINT '12345678901234567' AS DECIMAL(17, 0))", decimal("12345678901234567"));
        assertDecimalFunction("CAST(BIGINT '123456789012345679' AS DECIMAL(18, 0))", decimal("123456789012345679"));
        assertDecimalFunction("CAST(BIGINT '1234567890' AS DECIMAL(20, 10))", decimal("1234567890.0000000000"));
        assertDecimalFunction("CAST(BIGINT '-1234567890' AS DECIMAL(20, 10))", decimal("-1234567890.0000000000"));
        assertDecimalFunction("CAST(BIGINT '1234567890' AS DECIMAL(30, 20))", decimal("1234567890.00000000000000000000"));
        assertDecimalFunction("CAST(BIGINT '-1234567890' AS DECIMAL(30, 20))", decimal("-1234567890.00000000000000000000"));
        assertDecimalFunction("CAST(BIGINT '-1234567' AS DECIMAL(17, 10))", decimal("-1234567.0000000000"));

        assertInvalidCast("CAST(BIGINT '10' AS DECIMAL(38,37))", "Cannot cast BIGINT '10' to DECIMAL(38, 37)");
        assertInvalidCast("CAST(BIGINT '1234567890' AS DECIMAL(17,10))", "Cannot cast BIGINT '1234567890' to DECIMAL(17, 10)");
        assertInvalidCast("CAST(BIGINT '123' AS DECIMAL(2,1))", "Cannot cast BIGINT '123' to DECIMAL(2, 1)");
        assertInvalidCast("CAST(BIGINT '-123' AS DECIMAL(2,1))", "Cannot cast BIGINT '-123' to DECIMAL(2, 1)");
        assertInvalidCast("CAST(BIGINT '123456789012345678' AS DECIMAL(17,1))", "Cannot cast BIGINT '123456789012345678' to DECIMAL(17, 1)");
        assertInvalidCast("CAST(BIGINT '12345678901' AS DECIMAL(20, 10))", "Cannot cast BIGINT '12345678901' to DECIMAL(20, 10)");
    }

    @Test
    public void testIntegerToDecimalCasts()
    {
        assertDecimalFunction("CAST(INTEGER '234' AS DECIMAL(4,1))", decimal("234.0"));
        assertDecimalFunction("CAST(INTEGER '234' AS DECIMAL(5,2))", decimal("234.00"));
        assertDecimalFunction("CAST(INTEGER '234' AS DECIMAL(4,0))", decimal("0234"));
        assertDecimalFunction("CAST(INTEGER '-234' AS DECIMAL(4,1))", decimal("-234.0"));
        assertDecimalFunction("CAST(INTEGER '0' AS DECIMAL(4,2))", decimal("00.00"));
        assertDecimalFunction("CAST(INTEGER '1345678901' AS DECIMAL(18,8))", decimal("1345678901.00000000"));
        assertDecimalFunction("CAST(INTEGER '1234567890' AS DECIMAL(20, 10))", decimal("1234567890.0000000000"));
        assertDecimalFunction("CAST(INTEGER '-1234567890' AS DECIMAL(20, 10))", decimal("-1234567890.0000000000"));
        assertDecimalFunction("CAST(INTEGER '1234567890' AS DECIMAL(30, 20))", decimal("1234567890.00000000000000000000"));
        assertDecimalFunction("CAST(INTEGER '-1234567890' AS DECIMAL(30, 20))", decimal("-1234567890.00000000000000000000"));

        assertInvalidCast("CAST(INTEGER '10' AS DECIMAL(38,37))", "Cannot cast INTEGER '10' to DECIMAL(38, 37)");
        assertInvalidCast("CAST(INTEGER '1234567890' AS DECIMAL(17,10))", "Cannot cast INTEGER '1234567890' to DECIMAL(17, 10)");
        assertInvalidCast("CAST(INTEGER '123' AS DECIMAL(2,1))", "Cannot cast INTEGER '123' to DECIMAL(2, 1)");
        assertInvalidCast("CAST(INTEGER '-123' AS DECIMAL(2,1))", "Cannot cast INTEGER '-123' to DECIMAL(2, 1)");
    }

    @Test
    public void testSmallintToDecimalCasts()
    {
        assertDecimalFunction("CAST(SMALLINT '234' AS DECIMAL(4,1))", decimal("234.0"));
        assertDecimalFunction("CAST(SMALLINT '234' AS DECIMAL(5,2))", decimal("234.00"));
        assertDecimalFunction("CAST(SMALLINT '234' AS DECIMAL(4,0))", decimal("0234"));
        assertDecimalFunction("CAST(SMALLINT '-234' AS DECIMAL(4,1))", decimal("-234.0"));
        assertDecimalFunction("CAST(SMALLINT '0' AS DECIMAL(4,2))", decimal("00.00"));
        assertDecimalFunction("CAST(SMALLINT '1234' AS DECIMAL(20, 10))", decimal("0000001234.0000000000"));
        assertDecimalFunction("CAST(SMALLINT '-1234' AS DECIMAL(20, 10))", decimal("-0000001234.0000000000"));
        assertDecimalFunction("CAST(SMALLINT '1234' AS DECIMAL(30, 20))", decimal("0000001234.00000000000000000000"));
        assertDecimalFunction("CAST(SMALLINT '-1234' AS DECIMAL(30, 20))", decimal("-0000001234.00000000000000000000"));
        assertDecimalFunction("CAST(SMALLINT '12345' AS DECIMAL(18,13))", decimal("12345.0000000000000"));

        assertInvalidCast("CAST(SMALLINT '10' AS DECIMAL(38,37))", "Cannot cast SMALLINT '10' to DECIMAL(38, 37)");
        assertInvalidCast("CAST(SMALLINT '1234' AS DECIMAL(17,14))", "Cannot cast SMALLINT '1234' to DECIMAL(17, 14)");
        assertInvalidCast("CAST(SMALLINT '123' AS DECIMAL(2,1))", "Cannot cast SMALLINT '123' to DECIMAL(2, 1)");
        assertInvalidCast("CAST(SMALLINT '-123' AS DECIMAL(2,1))", "Cannot cast SMALLINT '-123' to DECIMAL(2, 1)");
    }

    @Test
    public void testTinyintToDecimalCasts()
    {
        assertDecimalFunction("CAST(TINYINT '23' AS DECIMAL(4,1))", decimal("023.0"));
        assertDecimalFunction("CAST(TINYINT '23' AS DECIMAL(5,2))", decimal("023.00"));
        assertDecimalFunction("CAST(TINYINT '23' AS DECIMAL(4,0))", decimal("0023"));
        assertDecimalFunction("CAST(TINYINT '-23' AS DECIMAL(4,1))", decimal("-023.0"));
        assertDecimalFunction("CAST(TINYINT '0' AS DECIMAL(4,2))", decimal("00.00"));
        assertDecimalFunction("CAST(TINYINT '123' AS DECIMAL(20, 10))", decimal("0000000123.0000000000"));
        assertDecimalFunction("CAST(TINYINT '-123' AS DECIMAL(20, 10))", decimal("-0000000123.0000000000"));
        assertDecimalFunction("CAST(TINYINT '123' AS DECIMAL(30, 20))", decimal("0000000123.00000000000000000000"));
        assertDecimalFunction("CAST(TINYINT '-123' AS DECIMAL(30, 20))", decimal("-0000000123.00000000000000000000"));
        assertDecimalFunction("CAST(TINYINT '123' AS DECIMAL(18,15))", decimal("123.000000000000000"));

        assertInvalidCast("CAST(TINYINT '10' AS DECIMAL(38,37))", "Cannot cast TINYINT '10' to DECIMAL(38, 37)");
        assertInvalidCast("CAST(TINYINT '123' AS DECIMAL(17,15))", "Cannot cast TINYINT '123' to DECIMAL(17, 15)");
        assertInvalidCast("CAST(TINYINT '123' AS DECIMAL(2,1))", "Cannot cast TINYINT '123' to DECIMAL(2, 1)");
        assertInvalidCast("CAST(TINYINT '-123' AS DECIMAL(2,1))", "Cannot cast TINYINT '-123' to DECIMAL(2, 1)");
    }

    @Test
    public void testDecimalToBigintCasts()
    {
        assertFunction("CAST(DECIMAL '2.34' AS BIGINT)", BIGINT, 2L);
        assertFunction("CAST(DECIMAL '2.5' AS BIGINT)", BIGINT, 3L);
        assertFunction("CAST(DECIMAL '2.49' AS BIGINT)", BIGINT, 2L);
        assertFunction("CAST(DECIMAL '20' AS BIGINT)", BIGINT, 20L);
        assertFunction("CAST(DECIMAL '1' AS BIGINT)", BIGINT, 1L);
        assertFunction("CAST(DECIMAL '0' AS BIGINT)", BIGINT, 0L);
        assertFunction("CAST(DECIMAL '-20' AS BIGINT)", BIGINT, -20L);
        assertFunction("CAST(DECIMAL '-1' AS BIGINT)", BIGINT, -1L);
        assertFunction("CAST(DECIMAL '-2.49' AS BIGINT)", BIGINT, -2L);
        assertFunction("CAST(DECIMAL '-2.5' AS BIGINT)", BIGINT, -3L);
        assertFunction("CAST(DECIMAL '0.1234567890123456' AS BIGINT)", BIGINT, 0L);
        assertFunction("CAST(DECIMAL '0.9999999999999999' AS BIGINT)", BIGINT, 1L);
        assertFunction("CAST(DECIMAL '0.00000000000000000000' AS BIGINT)", BIGINT, 0L);
        assertFunction("CAST(DECIMAL '0.99999999999999999999' AS BIGINT)", BIGINT, 1L);
        assertFunction("CAST(DECIMAL '123.999999999999999' AS BIGINT)", BIGINT, 124L);
        assertFunction("CAST(DECIMAL '999999999999999999' AS BIGINT)", BIGINT, 999999999999999999L);

        assertFunction("CAST(DECIMAL '1234567890.1234567890' AS BIGINT)", BIGINT, 1234567890L);
        assertFunction("CAST(DECIMAL '-1234567890.1234567890' AS BIGINT)", BIGINT, -1234567890L);
        assertInvalidCast("CAST(DECIMAL '12345678901234567890' AS BIGINT)", "Cannot cast '12345678901234567890' to BIGINT");
    }

    @Test
    public void testDecimalToIntegerCasts()
    {
        assertFunction("CAST(DECIMAL '2.34' AS INTEGER)", INTEGER, 2);
        assertFunction("CAST(DECIMAL '2.5' AS INTEGER)", INTEGER, 3);
        assertFunction("CAST(DECIMAL '2.49' AS INTEGER)", INTEGER, 2);
        assertFunction("CAST(DECIMAL '20' AS INTEGER)", INTEGER, 20);
        assertFunction("CAST(DECIMAL '1' AS INTEGER)", INTEGER, 1);
        assertFunction("CAST(DECIMAL '0' AS INTEGER)", INTEGER, 0);
        assertFunction("CAST(DECIMAL '-20' AS INTEGER)", INTEGER, -20);
        assertFunction("CAST(DECIMAL '-1' AS INTEGER)", INTEGER, -1);
        assertFunction("CAST(DECIMAL '-2.49' AS INTEGER)", INTEGER, -2);
        assertFunction("CAST(DECIMAL '-2.5' AS INTEGER)", INTEGER, -3);
        assertFunction("CAST(DECIMAL '0.1234567890123456' AS INTEGER)", INTEGER, 0);
        assertFunction("CAST(DECIMAL '0.9999999999999999' AS INTEGER)", INTEGER, 1);
        assertFunction("CAST(DECIMAL '0.00000000000000000000' AS INTEGER)", INTEGER, 0);
        assertFunction("CAST(DECIMAL '0.99999999999999999999' AS INTEGER)", INTEGER, 1);
        assertFunction("CAST(DECIMAL '123.999999999999999' AS INTEGER)", INTEGER, 124);

        assertFunction("CAST(DECIMAL '1234567890.1234567890' AS INTEGER)", INTEGER, 1234567890);
        assertFunction("CAST(DECIMAL '-1234567890.1234567890' AS INTEGER)", INTEGER, -1234567890);
        assertInvalidCast("CAST(DECIMAL '12345678901234567890' AS INTEGER)", "Cannot cast '12345678901234567890' to INTEGER");
    }

    @Test
    public void testDecimalToSmallintCasts()
    {
        assertFunction("CAST(DECIMAL '2.34' AS SMALLINT)", SMALLINT, (short) 2);
        assertFunction("CAST(DECIMAL '2.5' AS SMALLINT)", SMALLINT, (short) 3);
        assertFunction("CAST(DECIMAL '2.49' AS SMALLINT)", SMALLINT, (short) 2);
        assertFunction("CAST(DECIMAL '20' AS SMALLINT)", SMALLINT, (short) 20);
        assertFunction("CAST(DECIMAL '1' AS SMALLINT)", SMALLINT, (short) 1);
        assertFunction("CAST(DECIMAL '0' AS SMALLINT)", SMALLINT, (short) 0);
        assertFunction("CAST(DECIMAL '-20' AS SMALLINT)", SMALLINT, (short) -20);
        assertFunction("CAST(DECIMAL '-1' AS SMALLINT)", SMALLINT, (short) -1);
        assertFunction("CAST(DECIMAL '-2.49' AS SMALLINT)", SMALLINT, (short) -2);
        assertFunction("CAST(DECIMAL '-2.5' AS SMALLINT)", SMALLINT, (short) -3);
        assertFunction("CAST(DECIMAL '0.1234567890123456' AS SMALLINT)", SMALLINT, (short) 0);
        assertFunction("CAST(DECIMAL '0.9999999999999999' AS SMALLINT)", SMALLINT, (short) 1);
        assertFunction("CAST(DECIMAL '0.00000000000000000000' AS SMALLINT)", SMALLINT, (short) 0);
        assertFunction("CAST(DECIMAL '0.99999999999999999999' AS SMALLINT)", SMALLINT, (short) 1);
        assertFunction("CAST(DECIMAL '123.999999999999999' AS SMALLINT)", SMALLINT, (short) 124);

        assertFunction("CAST(DECIMAL '1234.1234567890' AS SMALLINT)", SMALLINT, (short) 1234);
        assertFunction("CAST(DECIMAL '-1234.1234567890' AS SMALLINT)", SMALLINT, (short) -1234);
        assertInvalidCast("CAST(DECIMAL '12345678901234567890' AS SMALLINT)", "Cannot cast '12345678901234567890' to SMALLINT");
    }

    @Test
    public void testDecimalToTinyintCasts()
    {
        assertFunction("CAST(DECIMAL '2.34' AS TINYINT)", TINYINT, (byte) 2);
        assertFunction("CAST(DECIMAL '2.5' AS TINYINT)", TINYINT, (byte) 3);
        assertFunction("CAST(DECIMAL '2.49' AS TINYINT)", TINYINT, (byte) 2);
        assertFunction("CAST(DECIMAL '20' AS TINYINT)", TINYINT, (byte) 20);
        assertFunction("CAST(DECIMAL '1' AS TINYINT)", TINYINT, (byte) 1);
        assertFunction("CAST(DECIMAL '0' AS TINYINT)", TINYINT, (byte) 0);
        assertFunction("CAST(DECIMAL '-20' AS TINYINT)", TINYINT, (byte) -20);
        assertFunction("CAST(DECIMAL '-1' AS TINYINT)", TINYINT, (byte) -1);
        assertFunction("CAST(DECIMAL '-2.49' AS TINYINT)", TINYINT, (byte) -2);
        assertFunction("CAST(DECIMAL '-2.5' AS TINYINT)", TINYINT, (byte) -3);
        assertFunction("CAST(DECIMAL '0.1234567890123456' AS TINYINT)", TINYINT, (byte) 0);
        assertFunction("CAST(DECIMAL '0.9999999999999999' AS TINYINT)", TINYINT, (byte) 1);
        assertFunction("CAST(DECIMAL '0.00000000000000000000' AS TINYINT)", TINYINT, (byte) 0);
        assertFunction("CAST(DECIMAL '0.99999999999999999999' AS TINYINT)", TINYINT, (byte) 1);
        assertFunction("CAST(DECIMAL '123.999999999999999' AS TINYINT)", TINYINT, (byte) 124);

        assertFunction("CAST(DECIMAL '12.1234567890' AS TINYINT)", TINYINT, (byte) 12);
        assertFunction("CAST(DECIMAL '-12.1234567890' AS TINYINT)", TINYINT, (byte) -12);
        assertInvalidCast("CAST(DECIMAL '12345678901234567890' AS TINYINT)", "Cannot cast '12345678901234567890' to TINYINT");
    }

    @Test
    public void testDoubleToShortDecimalCasts()
    {
        assertDecimalFunction("CAST(DOUBLE '234.0' AS DECIMAL(4,1))", decimal("234.0"));
        assertDecimalFunction("CAST(DOUBLE '.01' AS DECIMAL(3,3))", decimal(".010"));
        assertDecimalFunction("CAST(DOUBLE '.0' AS DECIMAL(3,3))", decimal(".000"));
        assertDecimalFunction("CAST(DOUBLE '0.0' AS DECIMAL(1,0))", decimal("0"));
        assertDecimalFunction("CAST(DOUBLE '0.0' AS DECIMAL(4,0))", decimal("0000"));
        assertDecimalFunction("CAST(DOUBLE '1000.0' AS DECIMAL(4,0))", decimal("1000"));
        assertDecimalFunction("CAST(DOUBLE '1000.01' AS DECIMAL(7,2))", decimal("01000.01"));
        assertDecimalFunction("CAST(DOUBLE '-234.0' AS DECIMAL(3,0))", decimal("-234"));
        assertDecimalFunction("CAST(DOUBLE '1234567890123456.0' AS DECIMAL(16,0))", decimal("1234567890123456"));
        assertDecimalFunction("CAST(DOUBLE '-1234567890123456.0' AS DECIMAL(16,0))", decimal("-1234567890123456"));
        assertDecimalFunction("CAST(DOUBLE '1234567890123456.0' AS DECIMAL(17,0))", decimal("01234567890123456"));
        assertDecimalFunction("CAST(DOUBLE '-1234567890123456.0' AS DECIMAL(17,0))", decimal("-01234567890123456"));
        assertDecimalFunction("CAST(DOUBLE '1234567890.0' AS DECIMAL(20,10))", decimal("1234567890.0000000000"));
        assertDecimalFunction("CAST(DOUBLE '-1234567890.0' AS DECIMAL(20,10))", decimal("-1234567890.0000000000"));
        assertDecimalFunction("CAST(DOUBLE '1234567890.0' AS DECIMAL(30,20))", decimal("1234567890.00000000000000000000"));
        assertDecimalFunction("CAST(DOUBLE '-1234567890.0' AS DECIMAL(30,20))", decimal("-1234567890.00000000000000000000"));
        assertDecimalFunction("CAST(DOUBLE '123456789123456784' AS DECIMAL(18,0))", decimal("123456789123456784"));
        assertDecimalFunction("CAST(DOUBLE '123456789.123456790' AS DECIMAL(18,9))", decimal("123456789.123456790"));

        // test rounding
        assertDecimalFunction("CAST(DOUBLE '1234567890.49' AS DECIMAL(16,0))", decimal("0000001234567890"));
        assertDecimalFunction("CAST(DOUBLE '1234567890.51' AS DECIMAL(16,0))", decimal("0000001234567891"));
        assertDecimalFunction("CAST(DOUBLE '-1234567890.49' AS DECIMAL(16,0))", decimal("-0000001234567890"));
        assertDecimalFunction("CAST(DOUBLE '-1234567890.51' AS DECIMAL(16,0))", decimal("-0000001234567891"));

        assertInvalidCast("CAST(DOUBLE '100.02' AS DECIMAL(17,16))", "Cannot cast DOUBLE '100.02' to DECIMAL(17, 16)");
        assertInvalidCast("CAST(DOUBLE '234.0' AS DECIMAL(2,0))", "Cannot cast DOUBLE '234.0' to DECIMAL(2, 0)");
        assertInvalidCast("CAST(DOUBLE '1000.01' AS DECIMAL(5,2))", "Cannot cast DOUBLE '1000.01' to DECIMAL(5, 2)");
        assertInvalidCast("CAST(DOUBLE '-234.0' AS DECIMAL(2,0))", "Cannot cast DOUBLE '-234.0' to DECIMAL(2, 0)");
        assertInvalidCast("CAST(infinity() AS DECIMAL(17,16))", "Cannot cast DOUBLE 'Infinity' to DECIMAL(17, 16)");
        assertInvalidCast("CAST(nan() AS DECIMAL(10,5))", "Cannot cast DOUBLE 'NaN' to DECIMAL(10, 5)");
        assertInvalidCast("CAST(infinity() AS DECIMAL(10,1))", "Cannot cast DOUBLE 'Infinity' to DECIMAL(10, 1)");
        assertInvalidCast("CAST(-infinity() AS DECIMAL(1,1))", "Cannot cast DOUBLE '-Infinity' to DECIMAL(1, 1)");
    }

    @Test
    public void testDoubleToLongDecimalCasts()
    {
        assertDecimalFunction("CAST(DOUBLE '234.0' AS DECIMAL(20,1))", decimal("0000000000000000234.0"));
        assertDecimalFunction("CAST(DOUBLE '.25' AS DECIMAL(20,5))", decimal("000000000000000.25000"));
        assertDecimalFunction("CAST(DOUBLE '.01' AS DECIMAL(20,3))", decimal("00000000000000000.010"));
        assertDecimalFunction("CAST(DOUBLE '.0' AS DECIMAL(20,3))", decimal("00000000000000000.000"));
        assertDecimalFunction("CAST(DOUBLE '0.0' AS DECIMAL(20,0))", decimal("00000000000000000000"));
        assertDecimalFunction("CAST(DOUBLE '1000.01' AS DECIMAL(20,2))", decimal("000000000000001000.01"));
        assertDecimalFunction("CAST(DOUBLE '-234.0' AS DECIMAL(20,0))", decimal("-00000000000000000234"));
        assertDecimalFunction("CAST(DOUBLE '12345678901234567.0' AS DECIMAL(20,0))", decimal("00012345678901234568"));
        assertDecimalFunction("CAST(DOUBLE '-12345678901234567.0' AS DECIMAL(20,0))", decimal("-00012345678901234568"));
        assertDecimalFunction("CAST(DOUBLE '1234567890.0' AS DECIMAL(20,10))", decimal("1234567890.0000000000"));
        assertDecimalFunction("CAST(DOUBLE '-1234567890.0' AS DECIMAL(20,10))", decimal("-1234567890.0000000000"));
        assertDecimalFunction("CAST(DOUBLE '1234567890123456.9' AS DECIMAL(16,0))", decimal("1234567890123457"));
        assertDecimalFunction("CAST(DOUBLE '-1234567890123456.9' AS DECIMAL(16,0))", decimal("-1234567890123457"));
        assertDecimalFunction("CAST(DOUBLE '1234567890123456789012345' AS DECIMAL(30,5))", decimal("1234567890123456800000000.00000"));
        assertDecimalFunction("CAST(DOUBLE '-1234567890123456789012345' AS DECIMAL(30,5))", decimal("-1234567890123456800000000.00000"));
        assertDecimalFunction("CAST(DOUBLE '1.2345678901234568E24' AS DECIMAL(30,5))", decimal("1234567890123456800000000.00000"));
        assertDecimalFunction("CAST(DOUBLE '-1.2345678901234568E24' AS DECIMAL(30,5))", decimal("-1234567890123456800000000.00000"));
        assertDecimalFunction("CAST(DOUBLE '.1234567890123456789012345' AS DECIMAL(30,30))", decimal(".123456789012345680000000000000"));
        assertDecimalFunction("CAST(DOUBLE '-.1234567890123456789012345' AS DECIMAL(30,30))", decimal("-.123456789012345680000000000000"));

        // test roundtrip
        assertFunction("CAST(CAST(DOUBLE '1234567890123456789012345' AS DECIMAL(30,5)) as DOUBLE) = DOUBLE '1234567890123456789012345'", BOOLEAN, true);
        assertFunction("CAST(CAST(DOUBLE '1.2345678901234568E24' AS DECIMAL(30,5)) as DOUBLE) = DOUBLE '1.2345678901234568E24'", BOOLEAN, true);

        // test rounding
        assertDecimalFunction("CAST(DOUBLE '1234567890.49' AS DECIMAL(20,0))", decimal("00000000001234567890"));
        assertDecimalFunction("CAST(DOUBLE '1234567890.51' AS DECIMAL(20,0))", decimal("00000000001234567891"));
        assertDecimalFunction("CAST(DOUBLE '-1234567890.49' AS DECIMAL(20,0))", decimal("-00000000001234567890"));
        assertDecimalFunction("CAST(DOUBLE '-1234567890.51' AS DECIMAL(20,0))", decimal("-00000000001234567891"));
        assertDecimalFunction("CAST(DOUBLE '1234567890.49' AS DECIMAL(10,0))", decimal("1234567890"));
        assertDecimalFunction("CAST(DOUBLE '1234567890.51' AS DECIMAL(10,0))", decimal("1234567891"));

        assertInvalidCast("CAST(DOUBLE '100.02' AS DECIMAL(38,37))", "Cannot cast DOUBLE '100.02' to DECIMAL(38, 37)");
        assertInvalidCast("CAST(DOUBLE '234000000000000000000.0' AS DECIMAL(20,0))", "Cannot cast DOUBLE '2.34E20' to DECIMAL(20, 0)");
        assertInvalidCast("CAST(DOUBLE '1000000000000000000.01' AS DECIMAL(20,2))", "Cannot cast DOUBLE '1.0E18' to DECIMAL(20, 2)");
        assertInvalidCast("CAST(DOUBLE '-234000000000000000000.0' AS DECIMAL(20,0))", "Cannot cast DOUBLE '-2.34E20' to DECIMAL(20, 0)");
        assertInvalidCast("CAST(DOUBLE '12345678901.1' AS DECIMAL(20, 10))", "Cannot cast DOUBLE '1.23456789011E10' to DECIMAL(20, 10)");

        assertInvalidCast("CAST(infinity() AS DECIMAL(38,37))", "Cannot cast DOUBLE 'Infinity' to DECIMAL(38, 37)");
        assertInvalidCast("CAST(nan() AS DECIMAL(38,10))", "Cannot cast DOUBLE 'NaN' to DECIMAL(38, 10)");
        assertInvalidCast("CAST(infinity() AS DECIMAL(38,2))", "Cannot cast DOUBLE 'Infinity' to DECIMAL(38, 2)");
        assertInvalidCast("CAST(-infinity() AS DECIMAL(38,1))", "Cannot cast DOUBLE '-Infinity' to DECIMAL(38, 1)");
        assertInvalidCast("CAST(nan() AS DECIMAL(10,5))", "Cannot cast DOUBLE 'NaN' to DECIMAL(10, 5)");
        assertInvalidCast("CAST(infinity() AS DECIMAL(10,1))", "Cannot cast DOUBLE 'Infinity' to DECIMAL(10, 1)");
        assertInvalidCast("CAST(-infinity() AS DECIMAL(1,1))", "Cannot cast DOUBLE '-Infinity' to DECIMAL(1, 1)");
    }

    @Test
    public void testDecimalToDoubleCasts()
    {
        assertFunction("CAST(DECIMAL '2.34' AS DOUBLE)", DOUBLE, 2.34);
        assertFunction("CAST(DECIMAL '0' AS DOUBLE)", DOUBLE, 0.0);
        assertFunction("CAST(DECIMAL '1' AS DOUBLE)", DOUBLE, 1.0);
        assertFunction("CAST(DECIMAL '-2.49' AS DOUBLE)", DOUBLE, -2.49);
        assertFunction("CAST(DECIMAL '123456789123456784' AS DOUBLE)", DOUBLE, 123456789123456784d);
        assertFunction("CAST(DECIMAL '123456789.123456791' AS DOUBLE)", DOUBLE, 123456789.123456791d);

        assertFunction("CAST(CAST(DECIMAL '0' AS DECIMAL(20, 2)) AS DOUBLE)", DOUBLE, 0.0);
        assertFunction("CAST(CAST(DECIMAL '12.12' AS DECIMAL(20, 2)) AS DOUBLE)", DOUBLE, 12.12);
        assertFunction("CAST(DECIMAL '1234567890.1234567890' AS DOUBLE)", DOUBLE, 1234567890.1234567890);
        assertFunction("CAST(DECIMAL '-1234567890.1234567890' AS DOUBLE)", DOUBLE, -1234567890.1234567890);
        assertFunction("CAST(DECIMAL '1234567890.12345678900000000000' AS DOUBLE)", DOUBLE, 1234567890.1234567890);
        assertFunction("CAST(DECIMAL '-1234567890.12345678900000000000' AS DOUBLE)", DOUBLE, -1234567890.1234567890);
        assertFunction("CAST(DECIMAL '-1234567890123456789012345678' AS DOUBLE)", DOUBLE, -1.2345678901234569E27);
        assertFunction("CAST(DECIMAL '99999999999999999999999999999999999999' AS DOUBLE)", DOUBLE, 1.0E38);
    }

    @Test
    public void testFloatToDecimalCasts()
    {
        assertDecimalFunction("CAST(REAL '234.0' AS DECIMAL(4,1))", decimal("234.0"));
        assertDecimalFunction("CAST(REAL '.01' AS DECIMAL(3,3))", decimal(".010"));
        assertDecimalFunction("CAST(REAL '.0' AS DECIMAL(3,3))", decimal(".000"));
        assertDecimalFunction("CAST(REAL '0' AS DECIMAL(1,0))", decimal("0"));
        assertDecimalFunction("CAST(REAL '0' AS DECIMAL(4,0))", decimal("0000"));
        assertDecimalFunction("CAST(REAL '1000' AS DECIMAL(4,0))", decimal("1000"));
        assertDecimalFunction("CAST(REAL '1000.01' AS DECIMAL(7,2))", decimal("01000.01"));
        assertDecimalFunction("CAST(REAL '-234.0' AS DECIMAL(3,0))", decimal("-234"));
        assertDecimalFunction("CAST(REAL '12345678400000000' AS DECIMAL(17,0))", decimal("12345678400000000"));
        assertDecimalFunction("CAST(REAL '-12345678400000000' AS DECIMAL(17,0))", decimal("-12345678400000000"));
        assertDecimalFunction("CAST(REAL '1234567940' AS DECIMAL(20,10))", decimal("1234567940.0000000000"));
        assertDecimalFunction("CAST(REAL '-1234567940' AS DECIMAL(20,10))", decimal("-1234567940.0000000000"));
        assertDecimalFunction("CAST(REAL '1234567940' AS DECIMAL(30,20))", decimal("1234567940.00000000000000000000"));
        assertDecimalFunction("CAST(REAL '-1234567940' AS DECIMAL(30,20))", decimal("-1234567940.00000000000000000000"));
        assertDecimalFunction("CAST(REAL '123456790519087104' AS DECIMAL(18,0))", decimal("123456791000000000"));
        assertDecimalFunction("CAST(REAL '-123456790519087104' AS DECIMAL(18,0))", decimal("-123456791000000000"));
        assertDecimalFunction("CAST(REAL '123456790519087104' AS DECIMAL(20,2))", decimal("123456791000000000.00"));
        assertDecimalFunction("CAST(REAL '-123456790519087104' AS DECIMAL(20,2))", decimal("-123456791000000000.00"));
        assertDecimalFunction("CAST(REAL '1234567905190871' AS DECIMAL(18,2))", decimal("1234567950000000.00"));
        assertDecimalFunction("CAST(REAL '-1234567905190871' AS DECIMAL(18,2))", decimal("-1234567950000000.00"));
        assertDecimalFunction("CAST(REAL '1456213.432632456' AS DECIMAL(18,9))", decimal("001456213.400000000"));

        // test roundtrip
        assertFunction("CAST(CAST(DOUBLE '123456790519087104' AS DECIMAL(18,0)) as DOUBLE) = DOUBLE '123456790519087104'", BOOLEAN, true);
        assertFunction("CAST(CAST(DOUBLE '123456790519087104' AS DECIMAL(30,0)) as DOUBLE) = DOUBLE '123456790519087104'", BOOLEAN, true);

        assertInvalidCast("CAST(REAL '100.02' AS DECIMAL(38,37))", "Cannot cast REAL '100.02' to DECIMAL(38, 37)");
        assertInvalidCast("CAST(REAL '100.02' AS DECIMAL(17,16))", "Cannot cast REAL '100.02' to DECIMAL(17, 16)");
        assertInvalidCast("CAST(REAL '234.0' AS DECIMAL(2,0))", "Cannot cast REAL '234.0' to DECIMAL(2, 0)");
        assertInvalidCast("CAST(REAL '1000.01' AS DECIMAL(5,2))", "Cannot cast REAL '1000.01' to DECIMAL(5, 2)");
        assertInvalidCast("CAST(REAL '-234.0' AS DECIMAL(2,0))", "Cannot cast REAL '-234.0' to DECIMAL(2, 0)");
        assertInvalidCast("CAST(REAL '98765430784.0' AS DECIMAL(20, 10))", "Cannot cast REAL '9.8765431E10' to DECIMAL(20, 10)");

        assertInvalidCast("CAST(CAST(nan() as REAL) AS DECIMAL(10,5))", "Cannot cast REAL 'NaN' to DECIMAL(10, 5)");
        assertInvalidCast("CAST(CAST(infinity() as REAL) AS DECIMAL(10,1))", "Cannot cast REAL 'Infinity' to DECIMAL(10, 1)");
        assertInvalidCast("CAST(CAST(-infinity() as REAL) AS DECIMAL(1,1))", "Cannot cast REAL '-Infinity' to DECIMAL(1, 1)");
        assertInvalidCast("CAST(CAST(nan() as REAL) AS DECIMAL(38,10))", "Cannot cast REAL 'NaN' to DECIMAL(38, 10)");
        assertInvalidCast("CAST(CAST(infinity() as REAL) AS DECIMAL(38,2))", "Cannot cast REAL 'Infinity' to DECIMAL(38, 2)");
        assertInvalidCast("CAST(CAST(-infinity() as REAL) AS DECIMAL(38,1))", "Cannot cast REAL '-Infinity' to DECIMAL(38, 1)");
    }

    @Test
    public void testDecimalToFloatCasts()
    {
        assertFunction("CAST(DECIMAL '2.34' AS REAL)", REAL, 2.34f);
        assertFunction("CAST(DECIMAL '0' AS REAL)", REAL, 0.0f);
        assertFunction("CAST(DECIMAL '-0' AS REAL)", REAL, 0.0f);
        assertFunction("CAST(DECIMAL '1' AS REAL)", REAL, 1.0f);
        assertFunction("CAST(DECIMAL '-2.49' AS REAL)", REAL, -2.49f);
        assertFunction("CAST(DECIMAL '123456790519087104' AS REAL)", REAL, 123456790519087104f);
        assertFunction("CAST(DECIMAL '121456.213432632456' AS REAL)", REAL, 121456.21f);

        assertFunction("CAST(CAST(DECIMAL '0' AS DECIMAL(20, 2)) AS REAL)", REAL, 0.0f);
        assertFunction("CAST(CAST(DECIMAL '12.12' AS DECIMAL(20, 2)) AS REAL)", REAL, 12.12f);
        assertFunction("CAST(DECIMAL '1234567890.1234567890' AS REAL)", REAL, 1234567890.1234567890f);
        assertFunction("CAST(DECIMAL '-1234567890.1234567890' AS REAL)", REAL, -1234567890.1234567890f);
        assertFunction("CAST(DECIMAL '1234567890.12345678900000000000' AS REAL)", REAL, 1234567890.1234567890f);
        assertFunction("CAST(DECIMAL '-1234567890.12345678900000000000' AS REAL)", REAL, -1234567890.1234567890f);
        assertFunction("CAST(DECIMAL '-1234567890123456789012345678' AS REAL)", REAL, -1.2345678901234569E27f);
        assertFunction("CAST(DECIMAL '99999999999999999999999999999999999999' AS REAL)", REAL, 1.0E38f);
    }

    @Test
    public void testVarcharToDecimalCasts()
    {
        assertDecimalFunction("CAST('234.0' AS DECIMAL(4,1))", decimal("234.0"));
        assertDecimalFunction("CAST('.01' AS DECIMAL(3,3))", decimal(".010"));
        assertDecimalFunction("CAST('.0' AS DECIMAL(3,3))", decimal(".000"));
        assertDecimalFunction("CAST('0' AS DECIMAL(1,0))", decimal("0"));
        assertDecimalFunction("CAST('0' AS DECIMAL(4,0))", decimal("0000"));
        assertDecimalFunction("CAST('1000' AS DECIMAL(4,0))", decimal("1000"));
        assertDecimalFunction("CAST('1000.01' AS DECIMAL(7,2))", decimal("01000.01"));
        assertDecimalFunction("CAST('-234.0' AS DECIMAL(3,0))", decimal("-234"));
        assertDecimalFunction("CAST('12345678901234567' AS DECIMAL(17,0))", decimal("12345678901234567"));
        assertDecimalFunction("CAST('123456789012345679' AS DECIMAL(18,0))", decimal("123456789012345679"));
        assertDecimalFunction("CAST('1234567890.12345679' AS DECIMAL(18,8))", decimal("1234567890.12345679"));
        assertDecimalFunction("CAST('-12345678901234567' AS DECIMAL(17,0))", decimal("-12345678901234567"));
        assertDecimalFunction("CAST('1234567890' AS DECIMAL(20,10))", decimal("1234567890.0000000000"));
        assertDecimalFunction("CAST('-1234567890' AS DECIMAL(20,10))", decimal("-1234567890.0000000000"));
        assertDecimalFunction("CAST('1234567890' AS DECIMAL(30,20))", decimal("1234567890.00000000000000000000"));
        assertDecimalFunction("CAST('-1234567890' AS DECIMAL(30,20))", decimal("-1234567890.00000000000000000000"));

        assertInvalidCast("CAST('234.0' AS DECIMAL(2,0))", "Cannot cast VARCHAR '234.0' to DECIMAL(2, 0). Value too large.");
        assertInvalidCast("CAST('1000.01' AS DECIMAL(5,2))", "Cannot cast VARCHAR '1000.01' to DECIMAL(5, 2). Value too large.");
        assertInvalidCast("CAST('-234.0' AS DECIMAL(2,0))", "Cannot cast VARCHAR '-234.0' to DECIMAL(2, 0). Value too large.");
        assertInvalidCast("CAST('12345678901' AS DECIMAL(20, 10))", "Cannot cast VARCHAR '12345678901' to DECIMAL(20, 10). Value too large.");
        assertInvalidCast("CAST('foo' AS DECIMAL(2, 0))", "Cannot cast VARCHAR 'foo' to DECIMAL(2, 0). Value is not a number.");
        assertInvalidCast("CAST('bar' AS DECIMAL)", "Cannot cast VARCHAR 'bar' to DECIMAL(38, 0). Value is not a number.");
    }

    @Test
    public void testDecimalToVarcharCasts()
    {
        assertFunction("CAST(DECIMAL '2.34' AS VARCHAR)", VARCHAR, "2.34");
        assertFunction("CAST(DECIMAL '23400' AS VARCHAR)", VARCHAR, "23400");
        assertFunction("CAST(DECIMAL '0.0034' AS VARCHAR)", VARCHAR, "0.0034");
        assertFunction("CAST(DECIMAL '0' AS VARCHAR)", VARCHAR, "0");
        assertFunction("CAST(DECIMAL '0.1234567890123456' AS VARCHAR)", VARCHAR, "0.1234567890123456");
        assertFunction("CAST(DECIMAL '0.12345678901234567' AS VARCHAR)", VARCHAR, "0.12345678901234567");

        assertFunction("CAST(DECIMAL '-10' AS VARCHAR)", VARCHAR, "-10");
        assertFunction("CAST(DECIMAL '-1.0' AS VARCHAR)", VARCHAR, "-1.0");
        assertFunction("CAST(DECIMAL '-1.00' AS VARCHAR)", VARCHAR, "-1.00");
        assertFunction("CAST(DECIMAL '-1.00000' AS VARCHAR)", VARCHAR, "-1.00000");
        assertFunction("CAST(DECIMAL '-0.1' AS VARCHAR)", VARCHAR, "-0.1");
        assertFunction("CAST(DECIMAL '-.001' AS VARCHAR)", VARCHAR, "-0.001");
        assertFunction("CAST(DECIMAL '-1234567890.1234567' AS VARCHAR)", VARCHAR, "-1234567890.1234567");

        assertFunction("CAST(DECIMAL '1234567890.1234567890' AS VARCHAR)", VARCHAR, "1234567890.1234567890");
        assertFunction("CAST(DECIMAL '-1234567890.1234567890' AS VARCHAR)", VARCHAR, "-1234567890.1234567890");
        assertFunction("CAST(DECIMAL '1234567890.12345678900000000000' AS VARCHAR)", VARCHAR, "1234567890.12345678900000000000");
        assertFunction("CAST(DECIMAL '-1234567890.12345678900000000000' AS VARCHAR)", VARCHAR, "-1234567890.12345678900000000000");
    }
}
