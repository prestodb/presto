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
package com.facebook.presto.spark.planner.optimizers;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.spi.BucketFunction;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.hive.HiveBucketFunction.createHiveCompatibleBucketFunction;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static com.facebook.presto.spark.planner.optimizers.AssignTableWriterPartitionCount.assignBucketsToPartitions;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Collections.max;
import static java.util.Collections.nCopies;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestAssignTableWriterPartitionCount
{
    // the number of virtual buckets of the Hive partitioning for a partitioned table
    private static final int PARTITIONED_TABLE_BUCKET_COUNT = 1009;

    @Test
    public void testAssignBucketsToPartitions()
    {
        assertBalanced(assignBucketsToPartitions(13, 2, 4), 2);
        assertBalanced(assignBucketsToPartitions(1009, 7, 4), 7);
        assertBalanced(assignBucketsToPartitions(1009, 250, 1), 250);
        assertBalanced(assignBucketsToPartitions(512, 512, 8), 512);
        assertEquals(assignBucketsToPartitions(5, 1, 4), new int[] {0, 0, 0, 0, 0});
        // a bucket cannot be split between partitions
        assertBalanced(assignBucketsToPartitions(4, 100, 4), 4);
        assertEquals(assignBucketsToPartitions(1009, 7, 4), assignBucketsToPartitions(1009, 7, 4));
    }

    @Test
    public void testBucketsOfPartitionAreSpreadOverWriterDrivers()
    {
        // the Java local exchange of a writer task sends bucket b to writer driver b % writerCount
        int[][] cases = {{1009, 7, 4}, {1009, 250, 4}, {1024, 256, 4}, {13, 4, 4}, {13, 2, 4}, {512, 64, 8}, {1009, 2, 8}, {1009, 250, 16}};
        for (int[] testCase : cases) {
            int bucketCount = testCase[0];
            int partitionCount = testCase[1];
            int writerCount = testCase[2];
            int[] bucketToPartition = assignBucketsToPartitions(bucketCount, partitionCount, writerCount);
            int[][] bucketsPerDriver = new int[partitionCount][writerCount];
            for (int bucket = 0; bucket < bucketCount; bucket++) {
                bucketsPerDriver[bucketToPartition[bucket]][bucket % writerCount]++;
            }
            for (int partition = 0; partition < partitionCount; partition++) {
                IntSummaryStatistics drivers = IntStream.of(bucketsPerDriver[partition]).summaryStatistics();
                assertTrue(
                        drivers.getMax() - drivers.getMin() <= 1,
                        format("%s buckets, %s partitions, %s writers: buckets per driver of partition %s are %s", bucketCount, partitionCount, writerCount, partition, Arrays.toString(bucketsPerDriver[partition])));
            }
        }
    }

    @Test
    public void testSequentialPartitionValuesAreSpread()
    {
        // values that differ only in their last characters, such as consecutive dates, hash to nearby buckets
        List<String> days = IntStream.rangeClosed(1, 30)
                .mapToObj(day -> format("2026-10-%02d", day))
                .collect(toImmutableList());
        assertSpread(getHiveBuckets(ImmutableList.of(days)), 7);
        assertSpread(getHiveBuckets(ImmutableList.of(days)), 16);

        List<String> hours = IntStream.range(0, 24)
                .mapToObj(hour -> format("%02d", hour))
                .collect(toImmutableList());
        assertSpread(getHiveBuckets(ImmutableList.of(hours)), 24);

        // a week of hourly partitions of a table partitioned on (ds, hr)
        ImmutableList.Builder<String> weekDays = ImmutableList.builder();
        ImmutableList.Builder<String> weekHours = ImmutableList.builder();
        for (String day : days.subList(0, 7)) {
            weekDays.addAll(nCopies(hours.size(), day));
            weekHours.addAll(hours);
        }
        assertSpread(getHiveBuckets(ImmutableList.of(weekDays.build(), weekHours.build())), 4);
        assertSpread(getHiveBuckets(ImmutableList.of(weekDays.build(), weekHours.build())), 16);
    }

    private static void assertBalanced(int[] bucketToPartition, int partitionCount)
    {
        Map<Integer, Long> bucketsPerPartition = IntStream.of(bucketToPartition).boxed().collect(groupingBy(identity(), counting()));
        assertEquals(bucketsPerPartition.keySet(), IntStream.range(0, partitionCount).boxed().collect(toImmutableSet()), bucketsPerPartition.toString());
        long minimum = bucketToPartition.length / partitionCount;
        bucketsPerPartition.values().forEach(buckets -> assertTrue(buckets == minimum || buckets == minimum + 1, bucketsPerPartition.toString()));
    }

    /**
     * Checks, for several writer counts, that the values reach all partitions when there are at least twice as many
     * values as partitions, and at least half of them otherwise, and that no partition gets more than twice its share
     * of the values plus one. Dealing the buckets of each Java writer driver separately costs some spread, so with
     * more than one writer driver the bound is twice the share plus two.
     */
    private static void assertSpread(List<Integer> buckets, int partitionCount)
    {
        for (int writerCount : new int[] {1, 4, 8}) {
            int[] bucketToPartition = assignBucketsToPartitions(PARTITIONED_TABLE_BUCKET_COUNT, partitionCount, writerCount);
            Map<Integer, Long> valuesPerPartition = buckets.stream().collect(groupingBy(bucket -> bucketToPartition[bucket], counting()));
            String message = format("%s values on %s partitions with %s writers: %s", buckets.size(), partitionCount, writerCount, valuesPerPartition);
            int minimumPartitions = buckets.size() >= 2 * partitionCount ? partitionCount : (min(buckets.size(), partitionCount) + 1) / 2;
            assertTrue(valuesPerPartition.size() >= minimumPartitions, message);
            long share = (buckets.size() + partitionCount - 1) / partitionCount;
            assertTrue(max(valuesPerPartition.values()) <= 2 * share + (writerCount == 1 ? 1 : 2), message);
        }
    }

    private static List<Integer> getHiveBuckets(List<List<String>> columns)
    {
        Page page = new Page(columns.stream()
                .map(TestAssignTableWriterPartitionCount::createVarcharBlock)
                .toArray(Block[]::new));
        BucketFunction bucketFunction = createHiveCompatibleBucketFunction(PARTITIONED_TABLE_BUCKET_COUNT, nCopies(columns.size(), HIVE_STRING), false);
        return IntStream.range(0, page.getPositionCount())
                .mapToObj(position -> bucketFunction.getBucket(page, position))
                .collect(toImmutableList());
    }

    private static Block createVarcharBlock(List<String> values)
    {
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, values.size());
        values.forEach(value -> VARCHAR.writeString(blockBuilder, value));
        return blockBuilder.build();
    }
}
