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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.SqlDecimal;

import java.math.BigDecimal;

import static java.math.BigDecimal.ROUND_DOWN;

public abstract class AbstractTestDecimalSumAggregation
        extends AbstractTestAggregationFunction
{
    protected abstract DecimalType getDecimalType();

    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        BlockBuilder blockBuilder = getDecimalType().createBlockBuilder(null, length);
        for (int i = start; i < start + length; i++) {
            writeDecimalToBlock(getBigDecimalForCounter(i), blockBuilder);
        }
        return new Block[] {blockBuilder.build()};
    }

    protected abstract void writeDecimalToBlock(BigDecimal decimal, BlockBuilder blockBuilder);

    private static BigDecimal getBigDecimalForCounter(int i)
    {
        String iAsString = String.valueOf(Math.abs(i));
        return new BigDecimal(String.valueOf(i) + "." + iAsString + iAsString).setScale(2, ROUND_DOWN);
    }

    @Override
    public SqlDecimal getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }
        BigDecimal sum = BigDecimal.ZERO;
        for (int i = start; i < start + length; i++) {
            sum = sum.add(getBigDecimalForCounter(i));
        }
        return new SqlDecimal(sum.unscaledValue(), sum.precision(), sum.scale());
    }

    @Override
    protected String getFunctionName()
    {
        return "sum";
    }
}
