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
package com.facebook.presto.operator.aggregation.reservoirsample;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.IntStream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestReservoirSample
{
    private static final int NUM_TRIALS = 12_000;

    /**
     * Each trial merges two full samples of 2 values. The first saw the values 0 and 1, and the
     * second saw the values 2 and 3. Merging should return each of the 6 pairs of 0 to 3 from
     * about the same number of trials.
     */
    @Test
    public void testMergeIsUniform()
    {
        Random random = new Random(0);
        Map<List<Long>, Integer> pairCounts = new HashMap<>();
        for (int trial = 0; trial < NUM_TRIALS; trial++) {
            ReservoirSample sample = createFullSample(0, 1);
            sample.merge(createFullSample(2, 3), random);
            pairCounts.merge(sortedValues(sample), 1, Integer::sum);
        }
        assertUniformPairs(pairCounts, 4);
    }

    // Creates a sample that saw only 'values' and kept all of them.
    private static ReservoirSample createFullSample(long... values)
    {
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, values.length);
        for (long value : values) {
            BIGINT.writeLong(blockBuilder, value);
        }
        return new ReservoirSample(BIGINT, values.length, values.length, blockBuilder.build(), null, -1);
    }

    private static List<Long> sortedValues(ReservoirSample sample)
    {
        Block values = sample.getSampleBlockBuilder().build().getBlock(0);
        return IntStream.range(0, values.getPositionCount())
                .mapToObj(position -> BIGINT.getLong(values, position))
                .sorted()
                .collect(toImmutableList());
    }

    // Checks that every pair of the values 0 to numValues - 1 came back from about the same
    // number of trials. For a uniform sampler, each pair's count follows a binomial distribution.
    // The seed repeats the same counts on every run. If a code change alters the random sequence,
    // a uniform sampler still stays within 6 standard deviations in all but 1 in 10 million runs.
    private static void assertUniformPairs(Map<List<Long>, Integer> pairCounts, int numValues)
    {
        int numPairs = numValues * (numValues - 1) / 2;
        double expected = (double) NUM_TRIALS / numPairs;
        double tolerance = 6 * Math.sqrt(expected * (1 - 1.0 / numPairs));
        long minCount = (long) Math.ceil(expected - tolerance);
        long maxCount = (long) Math.floor(expected + tolerance);

        boolean uniform = true;
        int totalCount = 0;
        StringBuilder counts = new StringBuilder();
        for (long first = 0; first < numValues; first++) {
            for (long second = first + 1; second < numValues; second++) {
                int count = pairCounts.getOrDefault(ImmutableList.of(first, second), 0);
                totalCount += count;
                boolean inRange = count >= minCount && count <= maxCount;
                uniform &= inRange;
                counts.append(format("%n  {%s, %s}: %s%s", first, second, count, inRange ? "" : "  <-- out of range"));
            }
        }
        assertEquals(totalCount, NUM_TRIALS, "Every trial must produce a pair of distinct input values");
        assertTrue(uniform, format("Each pair should come back %s to %s times out of %s:%s", minCount, maxCount, NUM_TRIALS, counts));
    }
}
