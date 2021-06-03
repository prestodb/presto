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
import static java.lang.Double.doubleToLongBits;
import static java.lang.Double.doubleToRawLongBits;
import static org.testng.Assert.assertEquals;

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
    public void testGetAndWrite()
    {
        BlockBuilder blockBuilder = DOUBLE.createFixedSizeBlockBuilder(5);
        DOUBLE.writeDouble(blockBuilder, 1.1);
        DOUBLE.writeObject(blockBuilder, 1.1);
        DOUBLE.writeDouble(blockBuilder, Double.NaN);
        DOUBLE.writeObject(blockBuilder, Double.NaN);
        // Test passing an integer.
        DOUBLE.writeObject(blockBuilder, 4);

        Block block = blockBuilder.build();
        assertEquals(DOUBLE.getDouble(block, 0), 1.1);
        assertEquals(DOUBLE.getObject(block, 0), 1.1);
        assertEquals(DOUBLE.getDouble(block, 1), 1.1);
        assertEquals(DOUBLE.getObject(block, 1), 1.1);
        assertEquals(DOUBLE.getDouble(block, 2), Double.NaN);
        assertEquals(DOUBLE.getObject(block, 2), Double.NaN);
        assertEquals(DOUBLE.getDouble(block, 3), Double.NaN);
        assertEquals(DOUBLE.getObject(block, 3), Double.NaN);
        assertEquals(DOUBLE.getObject(block, 4), 4.0);
    }
}
