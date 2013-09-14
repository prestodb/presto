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

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.operator.AggregationOperator.Aggregator;
import com.facebook.presto.operator.Page;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.operator.AggregationFunctionDefinition.aggregation;
import static com.facebook.presto.operator.AggregationOperator.createAggregator;
import static io.airlift.testing.Assertions.assertLessThan;
import static org.testng.Assert.assertEquals;

public abstract class AbstractTestApproximateCountDistinct
{
    public abstract ApproximateCountDistinctAggregation getAggregationFunction();

    public abstract TupleInfo.Type getValueType();

    public abstract Object randomValue();

    @Test
    public void testNoPositions()
            throws Exception
    {
        assertCount(ImmutableList.<Object>of(), 0);
    }

    @Test
    public void testSinglePosition()
            throws Exception
    {
        assertCount(ImmutableList.<Object>of(randomValue()), 1);
    }

    @Test
    public void testAllPositionsNull()
            throws Exception
    {
        assertCount(Collections.<Object>nCopies(100, null), 0);
    }

    @Test
    public void testMixedNullsAndNonNulls()
            throws Exception
    {
        List<Object> baseline = createRandomSample(10000, 15000);

        List<Object> mixed = new ArrayList<>(baseline);
        mixed.addAll(Collections.<Long>nCopies(baseline.size(), null));
        Collections.shuffle(mixed);

        assertCount(mixed, estimateCount(baseline, 0));
    }

    @Test
    public void testMultiplePositions()
            throws Exception
    {
        DescriptiveStatistics stats = new DescriptiveStatistics();

        for (int i = 0; i < 500; ++i) {
            int uniques = ThreadLocalRandom.current().nextInt(20000) + 1;

            List<Object> values = createRandomSample(uniques, (int) (uniques * 1.5));

            long actual = estimateCount(values, 0);
            double error = (actual - uniques) * 1.0 / uniques;

            stats.addValue(error);
        }

        assertLessThan(stats.getMean(), 1e-2);
        assertLessThan(Math.abs(stats.getStandardDeviation() - getAggregationFunction().getStandardError()), 1e-2);
    }

    @Test
    public void testMultiplePositionsPartial()
            throws Exception
    {
        for (int i = 0; i < 100; ++i) {
            int uniques = ThreadLocalRandom.current().nextInt(20000) + 1;
            List<Object> values = createRandomSample(uniques, (int) (uniques * 1.5));
            assertEquals(estimateCountPartial(values, 0), estimateCount(values, 0));
        }
    }

    private void assertCount(List<Object> values, long expectedCount)
    {
        assertCount(values, expectedCount, 0);
        assertCount(values, expectedCount, 1);
    }

    private void assertCount(List<Object> values, long expectedCount, int field)
    {
        assertEquals(estimateCount(values, field), expectedCount);
        assertEquals(estimateCountVectorized(values, field), expectedCount);
        assertEquals(estimateCountPartial(values, field), expectedCount);
    }

    private long estimateCount(List<Object> values, int field)
    {
        Aggregator aggregator = createAggregator(aggregation(getAggregationFunction(), new Input(0, field)), AggregationNode.Step.SINGLE);

        if (!values.isEmpty()) {
            BlockCursor cursor = createBlock(values, field + 1).cursor();
            while (cursor.advanceNextPosition()) {
                aggregator.addValue(cursor);
            }
        }

        return (long) BlockAssertions.toValues(aggregator.getResult()).get(0).get(0);
    }

    private long estimateCountVectorized(List<Object> values, int field)
    {
        Aggregator aggregator = createAggregator(aggregation(getAggregationFunction(), new Input(0, field)), AggregationNode.Step.SINGLE);

        if (!values.isEmpty()) {
            aggregator.addValue(new Page(createBlock(values, field + 1)));
        }

        return (long) BlockAssertions.toValues(aggregator.getResult()).get(0).get(0);
    }

    private long estimateCountPartial(List<Object> values, int field)
    {
        int size = Math.min(values.size(), Math.max(values.size() / 2, 1));

        Block first = aggregatePartial(values.subList(0, size), field);
        Block second = aggregatePartial(values.subList(size, values.size()), field);

        Aggregator aggregator = createAggregator(aggregation(getAggregationFunction(), new Input(0, field)), AggregationNode.Step.FINAL);

        BlockCursor cursor = first.cursor();
        while (cursor.advanceNextPosition()) {
            aggregator.addValue(cursor);
        }

        cursor = second.cursor();
        while (cursor.advanceNextPosition()) {
            aggregator.addValue(cursor);
        }

        return (long) BlockAssertions.toValues(aggregator.getResult()).get(0).get(0);
    }

    private Block aggregatePartial(List<Object> values, int field)
    {
        Aggregator aggregator = createAggregator(aggregation(getAggregationFunction(), new Input(0, field)), AggregationNode.Step.PARTIAL);

        if (!values.isEmpty()) {
            BlockCursor cursor = createBlock(values, field + 1).cursor();
            while (cursor.advanceNextPosition()) {
                aggregator.addValue(cursor);
            }
        }

        return aggregator.getResult();
    }

    /**
     * Produce a block with the given number of fields and the values in the last field. All other fields are set to null.
     */
    private Block createBlock(List<Object> values, int numberOfFields)
    {
        TupleInfo.Type[] types = new TupleInfo.Type[numberOfFields];
        Arrays.fill(types, getValueType());
        types[numberOfFields - 1] = getValueType();

        BlockBuilder blockBuilder = new BlockBuilder(new TupleInfo(types));

        for (Object value : values) {
            for (int i = 0; i < numberOfFields - 1; i++) {
                blockBuilder.appendNull();
            }

            if (value == null) {
                blockBuilder.appendNull();
            }
            else {
                switch (getValueType()) {
                    case FIXED_INT_64:
                        blockBuilder.append((Long) value);
                        break;
                    case VARIABLE_BINARY:
                        blockBuilder.append((Slice) value);
                        break;
                    case DOUBLE:
                        blockBuilder.append((Double) value);
                        break;
                    default:
                        throw new UnsupportedOperationException("not yet implemented");
                }
            }
        }

        return blockBuilder.build();
    }

    private List<Object> createRandomSample(int uniques, int total)
    {
        Preconditions.checkArgument(uniques <= total, "uniques (%s) must be <= total (%s)", uniques, total);

        List<Object> result = new ArrayList<>(total);
        result.addAll(makeRandomSet(uniques));

        Random random = ThreadLocalRandom.current();
        while (result.size() < total) {
            int index = random.nextInt(result.size());
            result.add(result.get(index));
        }

        return result;
    }

    private Set<Object> makeRandomSet(int count)
    {
        Set<Object> result = new HashSet<>();
        while (result.size() < count) {
            result.add(randomValue());
        }

        return result;
    }
}
