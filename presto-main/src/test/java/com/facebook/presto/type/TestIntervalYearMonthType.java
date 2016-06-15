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

import static com.facebook.presto.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;

public class TestIntervalYearMonthType
        extends AbstractTestType
{
    public TestIntervalYearMonthType()
    {
        super(INTERVAL_YEAR_MONTH, SqlIntervalYearMonth.class, createTestBlock());
    }

    public static Block createTestBlock()
    {
        BlockBuilder blockBuilder = INTERVAL_YEAR_MONTH.createBlockBuilder(new BlockBuilderStatus(), 15);
        INTERVAL_YEAR_MONTH.writeLong(blockBuilder, 1111);
        INTERVAL_YEAR_MONTH.writeLong(blockBuilder, 1111);
        INTERVAL_YEAR_MONTH.writeLong(blockBuilder, 1111);
        INTERVAL_YEAR_MONTH.writeLong(blockBuilder, 2222);
        INTERVAL_YEAR_MONTH.writeLong(blockBuilder, 2222);
        INTERVAL_YEAR_MONTH.writeLong(blockBuilder, 2222);
        INTERVAL_YEAR_MONTH.writeLong(blockBuilder, 2222);
        INTERVAL_YEAR_MONTH.writeLong(blockBuilder, 2222);
        INTERVAL_YEAR_MONTH.writeLong(blockBuilder, 3333);
        INTERVAL_YEAR_MONTH.writeLong(blockBuilder, 3333);
        INTERVAL_YEAR_MONTH.writeLong(blockBuilder, 4444);
        return blockBuilder.build();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        return ((Long) value) + 1;
    }
}
