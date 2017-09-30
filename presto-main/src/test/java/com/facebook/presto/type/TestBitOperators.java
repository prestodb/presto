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
import org.testng.annotations.Test;

import static com.facebook.presto.operator.scalar.TestVarbinaryFunctions.varbinary;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.type.BitType.bitType;

public class TestBitOperators
        extends AbstractTestFunctions
{
    @Test
    public void testVarcharToBit()
    {
        assertFunction("CAST('0' AS bit(1))", bitType(1), "0");
        assertFunction("CAST('1' AS bit(1))", bitType(1), "1");
        assertFunction("CAST('1101' AS bit(4))", bitType(4), "1101");
        assertFunction("CAST('1011101' AS bit(7))", bitType(7), "1011101");
        assertFunction("CAST('1011101' AS bit(8))", bitType(8), "01011101");
        assertFunction("CAST('101' AS bit(9))", bitType(9), "000000101");
        assertFunction("CAST('101' AS bit(18))", bitType(18), "000000000000000101");
        assertFunction("CAST('1010011001011' AS bit(15))", bitType(15), "001010011001011");
    }

    @Test
    public void testBitToVarchar()
    {
        assertFunction("CAST(CAST('0' AS bit(1)) AS varchar(1))", createVarcharType(1), "0");
        assertFunction("CAST(CAST('1' AS bit(1)) AS varchar(1))", createVarcharType(1), "1");
        assertFunction("CAST(CAST('1101' AS bit(4)) AS varchar(4))", createVarcharType(4), "1101");
        assertFunction("CAST(CAST('1011101' AS bit(7)) AS varchar(7))", createVarcharType(7), "1011101");
        assertFunction("CAST(CAST('1011101' AS bit(8)) AS varchar(8))", createVarcharType(8), "01011101");
        assertFunction("CAST(CAST('101' AS bit(9)) AS varchar(9))", createVarcharType(9), "000000101");
        assertFunction("CAST(CAST('101' AS bit(18)) AS varchar(18))", createVarcharType(18), "000000000000000101");
        assertFunction("CAST(CAST('1010011001011' AS bit(15)) AS varchar(15))", createVarcharType(15), "001010011001011");
    }

    @Test
    public void testBitToBinary()
    {
        assertFunction("CAST(CAST('1101' AS bit(4)) AS varbinary)", VARBINARY, varbinary(0b1101));
        assertFunction("CAST(CAST('1011101' AS bit(7)) AS varbinary)", VARBINARY, varbinary(0b1011101));
        assertFunction("CAST(CAST('1011101' AS bit(8)) AS varbinary)", VARBINARY, varbinary(0b1011101));
        assertFunction("CAST(CAST('101' AS bit(9)) AS varbinary)", VARBINARY, varbinary(0, 0b101));
        assertFunction("CAST(CAST('101' AS bit(18)) AS varbinary)", VARBINARY, varbinary(0, 0, 0b101));
        assertFunction("CAST(CAST('1010011001011' AS bit(15)) AS varbinary)", VARBINARY, varbinary(0b10100, 0b11001011));
    }

    @Test
    public void testBinaryToBit()
    {
        assertFunction("CAST(from_hex('0D') AS bit(8))", bitType(8), "00001101");
        assertFunction("CAST(from_hex('0D') AS bit(8))", bitType(8), "00001101");
        assertFunction("CAST(from_hex('DD') AS bit(8))", bitType(8), "11011101");
        assertFunction("CAST(from_hex('DD') AS bit(16))", bitType(16), "0000000011011101");
        assertFunction("CAST(from_hex('50DD') AS bit(16))", bitType(16), "0101000011011101");
    }

    @Test
    public void testEqual()
            throws Exception
    {
        assertFunction("CAST('1' AS bit(1)) = CAST('1' AS bit(1))", BOOLEAN, true);
        assertFunction("CAST('0' AS bit(1)) = CAST('0' AS bit(1))", BOOLEAN, true);
        assertFunction("CAST('1' AS bit(1)) = CAST('0' AS bit(1))", BOOLEAN, false);
        assertFunction("CAST('1101' AS bit(4)) = CAST('1101' AS bit(4))", BOOLEAN, true);
        assertFunction("CAST('1101' AS bit(4)) = CAST('1011' AS bit(4))", BOOLEAN, false);
        assertFunction("CAST('1101' AS bit(4)) = CAST('1101' AS bit(5))", BOOLEAN, true);
        assertFunction("CAST('1100001111' AS bit(10)) = CAST('1100001111' AS bit(10))", BOOLEAN, true);
        assertFunction("CAST('1111' AS bit(4)) = CAST('1111' AS bit(10))", BOOLEAN, true);
    }

    @Test
    public void testNotEqual()
            throws Exception
    {
        assertFunction("CAST('1' AS bit(1)) <> CAST('1' AS bit(1))", BOOLEAN, false);
        assertFunction("CAST('0' AS bit(1)) <> CAST('0' AS bit(1))", BOOLEAN, false);
        assertFunction("CAST('1' AS bit(1)) <> CAST('0' AS bit(1))", BOOLEAN, true);
        assertFunction("CAST('1101' AS bit(4)) <> CAST('1101' AS bit(4))", BOOLEAN, false);
        assertFunction("CAST('1101' AS bit(4)) <> CAST('1011' AS bit(4))", BOOLEAN, true);
        assertFunction("CAST('1101' AS bit(4)) <> CAST('1101' AS bit(5))", BOOLEAN, false);
        assertFunction("CAST('1100001111' AS bit(10)) <> CAST('1100001111' AS bit(10))", BOOLEAN, false);
        assertFunction("CAST('1111' AS bit(4)) <> CAST('1111' AS bit(10))", BOOLEAN, false);
    }

    @Test
    public void testLessThan()
            throws Exception
    {
        assertFunction("CAST('1' AS bit(1)) < CAST('1' AS bit(1))", BOOLEAN, false);
        assertFunction("CAST('0' AS bit(1)) < CAST('0' AS bit(1))", BOOLEAN, false);
        assertFunction("CAST('1' AS bit(1)) < CAST('0' AS bit(1))", BOOLEAN, false);
        assertFunction("CAST('0' AS bit(1)) < CAST('1' AS bit(1))", BOOLEAN, true);
        assertFunction("CAST('1101' AS bit(4)) < CAST('1101' AS bit(4))", BOOLEAN, false);
        assertFunction("CAST('1101' AS bit(4)) < CAST('1011' AS bit(4))", BOOLEAN, false);
        assertFunction("CAST('1001' AS bit(4)) < CAST('1101' AS bit(4))", BOOLEAN, true);
        assertFunction("CAST('1100001011' AS bit(10)) < CAST('1100001111' AS bit(10))", BOOLEAN, true);
    }

    @Test
    public void testLessThanOrEqual()
            throws Exception
    {
        assertFunction("CAST('1' AS bit(1)) <= CAST('1' AS bit(1))", BOOLEAN, true);
        assertFunction("CAST('0' AS bit(1)) <= CAST('0' AS bit(1))", BOOLEAN, true);
        assertFunction("CAST('1' AS bit(1)) <= CAST('0' AS bit(1))", BOOLEAN, false);
        assertFunction("CAST('0' AS bit(1)) <= CAST('1' AS bit(1))", BOOLEAN, true);
        assertFunction("CAST('1101' AS bit(4)) <= CAST('1101' AS bit(4))", BOOLEAN, true);
        assertFunction("CAST('1101' AS bit(4)) <= CAST('1011' AS bit(4))", BOOLEAN, false);
        assertFunction("CAST('1001' AS bit(4)) <= CAST('1101' AS bit(4))", BOOLEAN, true);
        assertFunction("CAST('1100001011' AS bit(10)) <= CAST('1100001111' AS bit(10))", BOOLEAN, true);
    }

    @Test
    public void testGreaterThan()
            throws Exception
    {
        assertFunction("CAST('1' AS bit(1)) > CAST('1' AS bit(1))", BOOLEAN, false);
        assertFunction("CAST('0' AS bit(1)) > CAST('0' AS bit(1))", BOOLEAN, false);
        assertFunction("CAST('1' AS bit(1)) > CAST('0' AS bit(1))", BOOLEAN, true);
        assertFunction("CAST('0' AS bit(1)) > CAST('1' AS bit(1))", BOOLEAN, false);
        assertFunction("CAST('1101' AS bit(4)) > CAST('1101' AS bit(4))", BOOLEAN, false);
        assertFunction("CAST('1101' AS bit(4)) > CAST('1011' AS bit(4))", BOOLEAN, true);
        assertFunction("CAST('1001' AS bit(4)) > CAST('1101' AS bit(4))", BOOLEAN, false);
        assertFunction("CAST('1100001111' AS bit(10)) > CAST('1100001011' AS bit(10))", BOOLEAN, true);
    }

    @Test
    public void testGreaterThanOrEqual()
            throws Exception
    {
        assertFunction("CAST('1' AS bit(1)) >= CAST('1' AS bit(1))", BOOLEAN, true);
        assertFunction("CAST('0' AS bit(1)) >= CAST('0' AS bit(1))", BOOLEAN, true);
        assertFunction("CAST('1' AS bit(1)) >= CAST('0' AS bit(1))", BOOLEAN, true);
        assertFunction("CAST('0' AS bit(1)) >= CAST('1' AS bit(1))", BOOLEAN, false);
        assertFunction("CAST('1101' AS bit(4)) >= CAST('1101' AS bit(4))", BOOLEAN, true);
        assertFunction("CAST('1101' AS bit(4)) >= CAST('1011' AS bit(4))", BOOLEAN, true);
        assertFunction("CAST('1001' AS bit(4)) >= CAST('1101' AS bit(4))", BOOLEAN, false);
        assertFunction("CAST('1100001111' AS bit(10)) >= CAST('1100001011' AS bit(10))", BOOLEAN, true);
    }

    @Test
    public void testBetween()
            throws Exception
    {
        assertFunction("CAST('1' AS bit(1)) BETWEEN CAST('0' AS bit(1)) AND CAST('1' AS bit(1))", BOOLEAN, true);
        assertFunction("CAST('0' AS bit(1)) BETWEEN CAST('0' AS bit(1)) AND CAST('1' AS bit(1))", BOOLEAN, true);

        assertFunction("CAST('011' AS bit(3)) BETWEEN CAST('001' AS bit(3)) AND CAST('111' AS bit(3))", BOOLEAN, true);
        assertFunction("CAST('111' AS bit(3)) BETWEEN CAST('001' AS bit(3)) AND CAST('101' AS bit(3))", BOOLEAN, false);

        assertFunction("CAST('1011' AS bit(10)) BETWEEN CAST('001' AS bit(10)) AND CAST('1000000001' AS bit(10))", BOOLEAN, true);
    }

    @Test
    public void testIsDistinctFrom()
            throws Exception
    {
        assertFunction("CAST('1101' AS bit(4)) >= CAST('1101' AS bit(4))", BOOLEAN, true);

        assertFunction("CAST(NULL AS bit(3)) IS DISTINCT FROM CAST(NULL AS bit(3))", BOOLEAN, false);
        assertFunction("CAST(NULL AS bit(3)) IS DISTINCT FROM CAST(NULL AS bit(5))", BOOLEAN, false);
        assertFunction("CAST(NULL AS bit(5)) IS DISTINCT FROM CAST(NULL AS bit(3))", BOOLEAN, false);

        assertFunction("CAST('1101' AS bit(4)) IS DISTINCT FROM CAST('1101' AS bit(4))", BOOLEAN, false);
        assertFunction("CAST('1101' AS bit(4)) IS DISTINCT FROM CAST('1011' AS bit(4))", BOOLEAN, true);

        assertFunction("CAST('110' AS bit(4)) IS DISTINCT FROM CAST('110' AS bit(99))", BOOLEAN, false);
        assertFunction("CAST('110' AS bit(99)) IS DISTINCT FROM CAST('110' AS bit(4))", BOOLEAN, false);

        assertFunction("CAST('1100001111' AS bit(13)) IS DISTINCT FROM CAST('110' AS bit(4))", BOOLEAN, true);

        assertFunction("CAST(NULL AS bit(4)) IS DISTINCT FROM CAST('1101' AS bit(4))", BOOLEAN, true);
        assertFunction("CAST('1101' AS bit(4)) IS DISTINCT FROM CAST(NULL AS bit(4))", BOOLEAN, true);
    }

    @Test
    public void testBitToBitCast()
    {
        assertFunction("CAST(CAST('1101' AS bit(4)) AS bit(4))", bitType(4), "1101");
        assertFunction("CAST(CAST('1101' AS bit(4)) AS bit(3))", bitType(3), "101");
        assertFunction("CAST(CAST('1101' AS bit(4)) AS bit(5))", bitType(5), "01101");
        assertFunction("CAST(CAST('1101' AS bit(4)) AS bit(11))", bitType(11), "00000001101");
        assertFunction("CAST(CAST('110100111' AS bit(9)) AS bit(4))", bitType(4), "0111");
        assertFunction("CAST(CAST('110100111' AS bit(9)) AS bit(8))", bitType(8), "10100111");
    }
}
