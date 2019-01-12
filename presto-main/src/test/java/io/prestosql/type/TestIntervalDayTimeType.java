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

import static io.prestosql.type.IntervalDayTimeType.INTERVAL_DAY_TIME;

public class TestIntervalDayTimeType
        extends AbstractTestType
{
    public TestIntervalDayTimeType()
    {
        super(INTERVAL_DAY_TIME, SqlIntervalDayTime.class, createTestBlock());
    }

    public static Block createTestBlock()
    {
        BlockBuilder blockBuilder = INTERVAL_DAY_TIME.createBlockBuilder(null, 15);
        INTERVAL_DAY_TIME.writeLong(blockBuilder, 1111);
        INTERVAL_DAY_TIME.writeLong(blockBuilder, 1111);
        INTERVAL_DAY_TIME.writeLong(blockBuilder, 1111);
        INTERVAL_DAY_TIME.writeLong(blockBuilder, 2222);
        INTERVAL_DAY_TIME.writeLong(blockBuilder, 2222);
        INTERVAL_DAY_TIME.writeLong(blockBuilder, 2222);
        INTERVAL_DAY_TIME.writeLong(blockBuilder, 2222);
        INTERVAL_DAY_TIME.writeLong(blockBuilder, 2222);
        INTERVAL_DAY_TIME.writeLong(blockBuilder, 3333);
        INTERVAL_DAY_TIME.writeLong(blockBuilder, 3333);
        INTERVAL_DAY_TIME.writeLong(blockBuilder, 4444);
        return blockBuilder.build();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        return ((Long) value) + 1;
    }
}
