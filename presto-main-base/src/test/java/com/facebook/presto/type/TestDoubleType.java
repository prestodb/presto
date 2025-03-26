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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.LongArrayBlockBuilder;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.DoubleType.OLD_NAN_DOUBLE;
import static com.facebook.presto.common.type.RealType.OLD_NAN_REAL;
import static java.lang.Double.doubleToLongBits;
import static java.lang.Double.doubleToRawLongBits;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class TestDoubleType
        extends AbstractTestType
{
    public TestDoubleType()
    {
        super(DOUBLE, Double.class, createTestBlock());
    }

    public static Block createTestBlock()
    {
        BlockBuilder blockBuilder = DOUBLE.createBlockBuilder(null, 15);
        DOUBLE.writeDouble(blockBuilder, 11.11);
        DOUBLE.writeDouble(blockBuilder, 11.11);
        DOUBLE.writeDouble(blockBuilder, 11.11);
        DOUBLE.writeDouble(blockBuilder, 22.22);
        DOUBLE.writeDouble(blockBuilder, 22.22);
        DOUBLE.writeDouble(blockBuilder, 22.22);
        DOUBLE.writeDouble(blockBuilder, 22.22);
        DOUBLE.writeDouble(blockBuilder, 22.22);
        DOUBLE.writeDouble(blockBuilder, 33.33);
        DOUBLE.writeDouble(blockBuilder, 33.33);
        DOUBLE.writeDouble(blockBuilder, 44.44);
        return blockBuilder.build();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        return ((Double) value) + 0.1;
    }

    @Test
    public void testNaNHash()
    {
        BlockBuilder blockBuilder = new LongArrayBlockBuilder(null, 4);
        blockBuilder.writeLong(doubleToLongBits(Double.NaN));
        blockBuilder.writeLong(doubleToRawLongBits(Double.NaN));
        // the following two are the long values of a double NaN
        blockBuilder.writeLong(-0x000fffffffffffffL);
        blockBuilder.writeLong(0x7ff8000000000000L);

        assertEquals(DOUBLE.hash(blockBuilder, 0), DOUBLE.hash(blockBuilder, 1));
        assertEquals(DOUBLE.hash(blockBuilder, 0), DOUBLE.hash(blockBuilder, 2));
        assertEquals(DOUBLE.hash(blockBuilder, 0), DOUBLE.hash(blockBuilder, 3));
    }

    @Test
    public void testLegacyDoubleHash()
    {
        BlockBuilder blockBuilder = new LongArrayBlockBuilder(null, 4);
        blockBuilder.writeLong(doubleToLongBits(Double.parseDouble("-0")));
        blockBuilder.writeLong(doubleToLongBits(Double.parseDouble("0")));
        assertNotEquals(OLD_NAN_DOUBLE.hash(blockBuilder, 0), OLD_NAN_REAL.hash(blockBuilder, 1));
    }

    @Test
    public void testDoubleHash()
    {
        BlockBuilder blockBuilder = new LongArrayBlockBuilder(null, 4);
        blockBuilder.writeLong(doubleToLongBits(Double.parseDouble("-0")));
        blockBuilder.writeLong(doubleToLongBits(Double.parseDouble("0")));
        assertEquals(DOUBLE.hash(blockBuilder, 0), DOUBLE.hash(blockBuilder, 1));
    }
}
