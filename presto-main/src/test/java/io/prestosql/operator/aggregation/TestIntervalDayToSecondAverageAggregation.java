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
package io.prestosql.operator.aggregation;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.type.SqlIntervalDayTime;

import java.util.List;

import static io.prestosql.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static java.lang.Math.round;

public class TestIntervalDayToSecondAverageAggregation
        extends AbstractTestAggregationFunction
{
    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        BlockBuilder blockBuilder = INTERVAL_DAY_TIME.createBlockBuilder(null, length);
        for (int i = start; i < start + length; i++) {
            INTERVAL_DAY_TIME.writeLong(blockBuilder, i * 250);
        }
        return new Block[] {blockBuilder.build()};
    }

    @Override
    public SqlIntervalDayTime getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }

        double sum = 0;
        for (int i = start; i < start + length; i++) {
            sum += i * 250;
        }
        return new SqlIntervalDayTime(round(sum / length));
    }

    @Override
    protected String getFunctionName()
    {
        return "avg";
    }

    @Override
    protected List<String> getFunctionParameterTypes()
    {
        return ImmutableList.of(StandardTypes.INTERVAL_DAY_TO_SECOND);
    }
}
