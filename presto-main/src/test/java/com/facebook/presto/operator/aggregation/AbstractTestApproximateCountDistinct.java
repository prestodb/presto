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
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.operator.Page;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static io.airlift.testing.Assertions.assertLessThan;
import static org.testng.Assert.assertEquals;

public abstract class AbstractTestApproximateCountDistinct
{
    public abstract AggregationFunction getAggregationFunction();

    public abstract Type getValueType();

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

        assertCount(mixed, estimateGroupByCount(baseline));
    }

    @Test
    public void testMultiplePositions()
            throws Exception
    {
        DescriptiveStatistics stats = new DescriptiveStatistics();

        for (int i = 0; i < 500; ++i) {
            int uniques = ThreadLocalRandom.current().nextInt(20000) + 1;

            List<Object> values = createRandomSample(uniques, (int) (uniques * 1.5));

            long actual = estimateGroupByCount(values);
            double error = (actual - uniques) * 1.0 / uniques;

            stats.addValue(error);
        }

        assertLessThan(stats.getMean(), 1.0e-2);
        assertLessThan(Math.abs(stats.getStandardDeviation() - ApproximateCountDistinctAggregation.getStandardError()), 1.0e-2);
    }

    @Test
    public void testMultiplePositionsPartial()
            throws Exception
    {
        for (int i = 0; i < 100; ++i) {
            int uniques = ThreadLocalRandom.current().nextInt(20000) + 1;
            List<Object> values = createRandomSample(uniques, (int) (uniques * 1.5));
            assertEquals(estimateCountPartial(values), estimateGroupByCount(values));
        }
    }

    private void assertCount(List<Object> values, long expectedCount)
    {
        if (!values.isEmpty()) {
            assertEquals(estimateGroupByCount(values), expectedCount);
        }
        assertEquals(estimateCount(values), expectedCount);
        assertEquals(estimateCountPartial(values), expectedCount);
    }

    private long estimateGroupByCount(List<Object> values)
    {
        Object result = AggregationTestUtils.groupedAggregation(getAggregationFunction(), 1.0, createPage(values));
        return (long) result;
    }

    private long estimateCount(List<Object> values)
    {
        Object result = AggregationTestUtils.aggregation(getAggregationFunction(), 1.0, createPage(values));
        return (long) result;
    }

    private long estimateCountPartial(List<Object> values)
    {
        Object result = AggregationTestUtils.partialAggregation(getAggregationFunction(), 1.0, createPage(values));
        return (long) result;
    }

    private Page createPage(List<Object> values)
    {
        Page page;
        if (values.isEmpty()) {
            page = new Page(0);
        }
        else {
            page = new Page(values.size(), createBlock(values));
        }
        return page;
    }

    /**
     * Produce a block with the given values in the last field.
     */
    private Block createBlock(List<Object> values)
    {
        BlockBuilder blockBuilder = getValueType().createBlockBuilder(new BlockBuilderStatus());

        for (Object value : values) {
            if (value == null) {
                blockBuilder.appendNull();
            }
            else {
                switch (getValueType().toColumnType()) {
                    case BOOLEAN:
                        blockBuilder.append((Boolean) value);
                        break;
                    case LONG:
                        blockBuilder.append((Long) value);
                        break;
                    case DOUBLE:
                        blockBuilder.append((Double) value);
                        break;
                    case STRING:
                        blockBuilder.append((Slice) value);
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
