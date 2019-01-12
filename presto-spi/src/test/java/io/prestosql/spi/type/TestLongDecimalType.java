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
package io.prestosql.spi.type;

import io.airlift.slice.Slice;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.VariableWidthBlockBuilder;
import org.testng.annotations.Test;

import java.math.BigDecimal;

import static io.prestosql.spi.type.Decimals.encodeScaledValue;
import static java.lang.Math.signum;
import static org.testng.Assert.assertEquals;

public class TestLongDecimalType
{
    private static final LongDecimalType TYPE = (LongDecimalType) LongDecimalType.createDecimalType(20, 10);

    @Test
    public void testCompareTo()
    {
        testCompare("0", "-1234567891.1234567890", 1);
        testCompare("1234567890.1234567890", "1234567890.1234567890", 0);
        testCompare("1234567890.1234567890", "1234567890.1234567891", -1);
        testCompare("1234567890.1234567890", "1234567890.1234567889", 1);
        testCompare("1234567890.1234567890", "1234567891.1234567890", -1);
        testCompare("1234567890.1234567890", "1234567889.1234567890", 1);
        testCompare("0", "1234567891.1234567890", -1);
        testCompare("1234567890.1234567890", "0", 1);
        testCompare("0", "0", 0);
        testCompare("-1234567890.1234567890", "-1234567890.1234567890", 0);
        testCompare("-1234567890.1234567890", "-1234567890.1234567891", 1);
        testCompare("-1234567890.1234567890", "-1234567890.1234567889", -1);
        testCompare("-1234567890.1234567890", "-1234567891.1234567890", 1);
        testCompare("-1234567890.1234567890", "-1234567889.1234567890", -1);
        testCompare("0", "-1234567891.1234567890", 1);
        testCompare("-1234567890.1234567890", "0", -1);
        testCompare("-1234567890.1234567890", "1234567890.1234567890", -1);
        testCompare("1234567890.1234567890", "-1234567890.1234567890", 1);
    }

    private void testCompare(String decimalA, String decimalB, int expected)
    {
        int actual = TYPE.compareTo(decimalAsBlock(decimalA), 0, decimalAsBlock(decimalB), 0);
        assertEquals((int) signum(actual), (int) signum(expected), "bad comparison result for " + decimalA + ", " + decimalB);
    }

    private Block decimalAsBlock(String value)
    {
        Slice slice = encodeScaledValue(new BigDecimal(value));
        BlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, 1, slice.length());
        TYPE.writeSlice(blockBuilder, slice);
        return blockBuilder.build();
    }
}
