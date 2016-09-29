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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;

import static com.facebook.presto.spi.type.RealType.REAL;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;

public class TestRealType
        extends AbstractTestType
{
    public TestRealType()
    {
        super(REAL, Float.class, createTestBlock());
    }

    public static Block createTestBlock()
    {
        BlockBuilder blockBuilder = REAL.createBlockBuilder(new BlockBuilderStatus(), 30);
        REAL.writeLong(blockBuilder, floatToRawIntBits(11.11F));
        REAL.writeLong(blockBuilder, floatToRawIntBits(11.11F));
        REAL.writeLong(blockBuilder, floatToRawIntBits(11.11F));
        REAL.writeLong(blockBuilder, floatToRawIntBits(22.22F));
        REAL.writeLong(blockBuilder, floatToRawIntBits(22.22F));
        REAL.writeLong(blockBuilder, floatToRawIntBits(22.22F));
        REAL.writeLong(blockBuilder, floatToRawIntBits(22.22F));
        REAL.writeLong(blockBuilder, floatToRawIntBits(22.22F));
        REAL.writeLong(blockBuilder, floatToRawIntBits(33.33F));
        REAL.writeLong(blockBuilder, floatToRawIntBits(33.33F));
        REAL.writeLong(blockBuilder, floatToRawIntBits(44.44F));
        return blockBuilder.build();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        int bits = ((Long) value).intValue();
        float greaterValue = intBitsToFloat(bits) + 0.1f;
        return Long.valueOf(floatToRawIntBits(greaterValue));
    }
}
