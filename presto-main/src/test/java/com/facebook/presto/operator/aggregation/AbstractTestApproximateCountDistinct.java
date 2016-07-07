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

import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static io.airlift.testing.Assertions.assertLessThan;
import static org.testng.Assert.assertEquals;

public abstract class AbstractTestApproximateCountDistinct
{
    public abstract InternalAggregationFunction getAggregationFunction();

    public abstract Type getValueType();

    public abstract Object randomValue();

    protected static final MetadataManager metadata = MetadataManager.createTestMetadataManager();

    @DataProvider(name = "provideStandardErrors")
    public Object[][] provideStandardErrors()
    {
        return new Object[][] {
                { 0.0230 }, // 2k buckets
                { 0.0115 }, // 8k buckets
        };
    }

    @Test(dataProvider = "provideStandardErrors")
    public void testNoPositions(double maxStandardError)
            throws Exception
    {
        assertCount(ImmutableList.of(), maxStandardError, 0);
    }

    @Test(dataProvider = "provideStandardErrors")
    public void testSinglePosition(double maxStandardError)
            throws Exception
    {
        assertCount(ImmutableList.of(randomValue()), maxStandardError, 1);
    }

    @Test(dataProvider = "provideStandardErrors")
    public void testAllPositionsNull(double maxStandardError)
            throws Exception
    {
        assertCount(Collections.nCopies(100, null), maxStandardError, 0);
    }

    @Test(dataProvider = "provideStandardErrors")
    public void testMixedNullsAndNonNulls(double maxStandardError)
            throws Exception
    {
        List<Object> baseline = createRandomSample(10000, 15000);

        // Randomly insert nulls
        // We need to retain the preexisting order to ensure that the HLL can generate the same estimates.
        Iterator<Object> iterator = baseline.iterator();
        List<Object> mixed = new ArrayList<>();
        while (iterator.hasNext()) {
            mixed.add(ThreadLocalRandom.current().nextBoolean() ? null : iterator.next());
        }

        assertCount(mixed, maxStandardError, estimateGroupByCount(baseline, maxStandardError));
    }

    @Test(dataProvider = "provideStandardErrors")
    public void testMultiplePositions(double maxStandardError)
            throws Exception
    {
        DescriptiveStatistics stats = new DescriptiveStatistics();

        for (int i = 0; i < 500; ++i) {
            int uniques = ThreadLocalRandom.current().nextInt(20000) + 1;

            List<Object> values = createRandomSample(uniques, (int) (uniques * 1.5));

            long actual = estimateGroupByCount(values, maxStandardError);
            double error = (actual - uniques) * 1.0 / uniques;

            stats.addValue(error);
        }

        assertLessThan(stats.getMean(), 1.0e-2);
        assertLessThan(Math.abs(stats.getStandardDeviation() - maxStandardError), 1.0e-2);
    }

    @Test(dataProvider = "provideStandardErrors")
    public void testMultiplePositionsPartial(double maxStandardError)
            throws Exception
    {
        for (int i = 0; i < 100; ++i) {
            int uniques = ThreadLocalRandom.current().nextInt(20000) + 1;
            List<Object> values = createRandomSample(uniques, (int) (uniques * 1.5));
            assertEquals(estimateCountPartial(values, maxStandardError), estimateGroupByCount(values, maxStandardError));
        }
    }

    private void assertCount(List<Object> values, double maxStandardError, long expectedCount)
    {
        if (!values.isEmpty()) {
            assertEquals(estimateGroupByCount(values, maxStandardError), expectedCount);
        }
        assertEquals(estimateCount(values, maxStandardError), expectedCount);
        assertEquals(estimateCountPartial(values, maxStandardError), expectedCount);
    }

    private long estimateGroupByCount(List<Object> values, double maxStandardError)
    {
        Object result = AggregationTestUtils.groupedAggregation(getAggregationFunction(), 1.0, createPage(values, maxStandardError));
        return (long) result;
    }

    private long estimateCount(List<Object> values, double maxStandardError)
    {
        Object result = AggregationTestUtils.aggregation(getAggregationFunction(), 1.0, createPage(values, maxStandardError));
        return (long) result;
    }

    private long estimateCountPartial(List<Object> values, double maxStandardError)
    {
        Object result = AggregationTestUtils.partialAggregation(getAggregationFunction(), 1.0, createPage(values, maxStandardError));
        return (long) result;
    }

    private Page createPage(List<Object> values, double maxStandardError)
    {
        if (values.isEmpty()) {
            return new Page(0);
        }
        else {
            return new Page(values.size(),
                    createBlock(getValueType(), values),
                    createBlock(DOUBLE, ImmutableList.copyOf(Collections.nCopies(values.size(), maxStandardError))));
        }
    }

    /**
     * Produce a block with the given values in the last field.
     */
    private static Block createBlock(Type type, List<Object> values)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), values.size());

        for (Object value : values) {
            Class<?> javaType = type.getJavaType();
            if (value == null) {
                blockBuilder.appendNull();
            }
            else if (javaType == boolean.class) {
                type.writeBoolean(blockBuilder, (Boolean) value);
            }
            else if (javaType == long.class) {
                type.writeLong(blockBuilder, (Long) value);
            }
            else if (javaType == double.class) {
                type.writeDouble(blockBuilder, (Double) value);
            }
            else if (javaType == Slice.class) {
                Slice slice = (Slice) value;
                type.writeSlice(blockBuilder, slice, 0, slice.length());
            }
            else {
                throw new UnsupportedOperationException("not yet implemented: " + javaType);
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
