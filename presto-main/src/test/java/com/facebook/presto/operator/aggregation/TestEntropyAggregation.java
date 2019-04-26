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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.type.IntegerType.INTEGER;

public class TestEntropyAggregation
        extends AbstractTestAggregationFunction
{
    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        BlockBuilder blockBuilder = INTEGER.createBlockBuilder(null, length);
        for (int i = start; i < start + length; i++) {
            INTEGER.writeInteger(i);
        }
        return new Block[] {blockBuilder.build()};
    }

    @Override
    public Number getExpectedValue(int start, int length)
    {
        final ArrayList<Integer> counts = IntStream.range(start, start + length).toArray();;
        final double sum = IntStream.of(counts).sum();
        final ArrayList<Double> entropies = counts.stream()
                .filter(c -> c > 0)
                .map(c -> (c / sum) * Math.log(sum / c))
                .collect(Collectors.toList());
        return entropies.isEmpty()? 0: entropies.stream().sum();
    }

    @Override
    protected String getFunctionName()
    {
        return "entropy";
    }

    @Override
    protected List<String> getFunctionParameterTypes()
    {
        return ImmutableList.of(StandardTypes.INTEGER);
    }
}
