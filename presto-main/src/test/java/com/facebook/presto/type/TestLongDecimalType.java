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
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.LongDecimalType;
import com.facebook.presto.spi.type.SqlDecimal;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.math.BigDecimal;

public class TestLongDecimalType
        extends AbstractTestType
{
    private static final LongDecimalType LONG_DECIMAL_TYPE = (LongDecimalType) DecimalType.createDecimalType(30, 10);

    public TestLongDecimalType()
    {
        super(LONG_DECIMAL_TYPE, SqlDecimal.class, createTestBlock());
    }

    public static Block createTestBlock()
    {
        BlockBuilder blockBuilder = LONG_DECIMAL_TYPE.createBlockBuilder(new BlockBuilderStatus(), 15);
        LONG_DECIMAL_TYPE.writeBigDecimal(blockBuilder, new BigDecimal("-12345678901234567890.1234567890"));
        LONG_DECIMAL_TYPE.writeBigDecimal(blockBuilder, new BigDecimal("-12345678901234567890.1234567890"));
        LONG_DECIMAL_TYPE.writeBigDecimal(blockBuilder, new BigDecimal("-12345678901234567890.1234567890"));
        LONG_DECIMAL_TYPE.writeBigDecimal(blockBuilder, new BigDecimal("22345678901234567890.1234567890"));
        LONG_DECIMAL_TYPE.writeBigDecimal(blockBuilder, new BigDecimal("22345678901234567890.1234567890"));
        LONG_DECIMAL_TYPE.writeBigDecimal(blockBuilder, new BigDecimal("22345678901234567890.1234567890"));
        LONG_DECIMAL_TYPE.writeBigDecimal(blockBuilder, new BigDecimal("22345678901234567890.1234567890"));
        LONG_DECIMAL_TYPE.writeBigDecimal(blockBuilder, new BigDecimal("22345678901234567890.1234567890"));
        LONG_DECIMAL_TYPE.writeBigDecimal(blockBuilder, new BigDecimal("32345678901234567890.1234567890"));
        LONG_DECIMAL_TYPE.writeBigDecimal(blockBuilder, new BigDecimal("32345678901234567890.1234567890"));
        LONG_DECIMAL_TYPE.writeBigDecimal(blockBuilder, new BigDecimal("42345678901234567890.1234567890"));
        return blockBuilder.build();
    }

    @Override
    protected Object getNonNullValue()
    {
        return Slices.wrappedBuffer(new byte[16]);
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        Slice slice = (Slice) value;
        BigDecimal decimal = LongDecimalType.toBigDecimal(slice, 10);
        BigDecimal greaterDecimal = decimal.add(BigDecimal.ONE);
        return LONG_DECIMAL_TYPE.bigDecimalToSlice(greaterDecimal);
    }
}
