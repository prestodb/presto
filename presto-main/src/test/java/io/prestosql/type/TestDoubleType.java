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

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.LongArrayBlockBuilder;
import org.testng.annotations.Test;

import static io.prestosql.spi.type.DoubleType.DOUBLE;
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
}
