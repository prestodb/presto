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
package com.facebook.presto.spark.planner;

import com.facebook.presto.Session;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spark.PrestoSparkConfig;
import com.facebook.presto.spark.PrestoSparkSessionProperties;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.spark.PrestoSparkSessionProperties.MAX_SPLITS_DATA_SIZE_PER_SPARK_PARTITION;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.MIN_SPARK_INPUT_PARTITION_COUNT_FOR_AUTO_TUNE;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_INITIAL_PARTITION_COUNT;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_PARTITION_COUNT_AUTO_TUNE_ENABLED;
import static com.facebook.presto.spark.planner.PrestoSparkRddFactory.assignSourceDistributionSplits;
import static com.facebook.presto.testing.TestingSession.TESTING_CATALOG;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.collect.Multimaps.asMap;
import static com.google.common.primitives.Longs.asList;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.assertEquals;

public class TestPrestoSparkRddFactory
{
    @Test
    public void testAssignSourceDistributionSplits()
    {
        // black box test
        testAssignSplitsToPartitionWithRandomSplitsSize(3);

        // auto tune partition + splits with mixed size
        List<Long> testSplitsSize = asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L);
        Collections.shuffle(testSplitsSize);
        Map<Integer, Set<Long>> actualResult = assignSplitsToPartition(true, 10, testSplitsSize);

        Map<Integer, Set<Long>> expectedResult = new HashMap<>();
        expectedResult.put(0, ImmutableSet.of(11L));
        expectedResult.put(1, ImmutableSet.of(10L));
        expectedResult.put(2, ImmutableSet.of(9L));
        expectedResult.put(3, ImmutableSet.of(8L, 1L));
        expectedResult.put(4, ImmutableSet.of(7L, 2L));
        expectedResult.put(5, ImmutableSet.of(6L, 3L));
        expectedResult.put(6, ImmutableSet.of(5L, 4L));
        assertEquals(actualResult, expectedResult);

        // enable auto tune partition + small splits
        testSplitsSize = asList(1L, 2L, 3L, 4L, 5L, 6L);
        Collections.shuffle(testSplitsSize);
        actualResult = assignSplitsToPartition(true, 10, testSplitsSize);

        expectedResult = new HashMap<>();
        expectedResult.put(0, ImmutableSet.of(6L, 3L));
        expectedResult.put(1, ImmutableSet.of(5L, 4L));
        expectedResult.put(2, ImmutableSet.of(2L, 1L));
        assertEquals(actualResult, expectedResult);

        // disable auto tune partition + small splits
        testSplitsSize = asList(1L, 2L, 3L, 4L, 5L, 6L);
        actualResult = assignSplitsToPartition(false, 10, testSplitsSize);

        expectedResult = new HashMap<>();
        expectedResult.put(0, ImmutableSet.of(6L, 1L));
        expectedResult.put(1, ImmutableSet.of(5L, 2L));
        expectedResult.put(2, ImmutableSet.of(4L, 3L));
        assertEquals(actualResult, expectedResult);

        // disable auto tune partition + large splits
        testSplitsSize = asList(5L, 6L, 7L, 8L, 9L, 10L);
        Collections.shuffle(testSplitsSize);
        actualResult = assignSplitsToPartition(false, 10, testSplitsSize);

        expectedResult = new HashMap<>();
        expectedResult.put(0, ImmutableSet.of(10L, 5L));
        expectedResult.put(1, ImmutableSet.of(9L, 6L));
        expectedResult.put(2, ImmutableSet.of(8L, 7L));
        assertEquals(actualResult, expectedResult);
    }

    private void testAssignSplitsToPartitionWithRandomSplitsSize(int repeatedTimes)
    {
        int maxSplitSizeInBytes = 2048;
        for (int i = 0; i < repeatedTimes; ++i) {
            List<Long> splitsSize = new ArrayList<>(1000);
            for (int j = 0; j < 1000; j++) {
                splitsSize.add(ThreadLocalRandom.current().nextLong((long) (maxSplitSizeInBytes * 1.2)));
            }
            assignSplitsToPartition(true, maxSplitSizeInBytes, splitsSize);
            assignSplitsToPartition(false, maxSplitSizeInBytes, splitsSize);
        }
    }

    private Map<Integer, Set<Long>> assignSplitsToPartition(
            boolean autoTunePartitionCount,
            long maxPartitionSize,
            List<Long> splitsSize)
    {
        Session session = testSessionBuilder(new SessionPropertyManager(new PrestoSparkSessionProperties(new PrestoSparkConfig()).getSessionProperties()))
                .setCatalog(TESTING_CATALOG)
                .setSchema("tpch")
                .setSystemProperty(SPARK_PARTITION_COUNT_AUTO_TUNE_ENABLED, Boolean.toString(autoTunePartitionCount))
                .setSystemProperty(SPARK_INITIAL_PARTITION_COUNT, "3")
                .setSystemProperty(MAX_SPLITS_DATA_SIZE_PER_SPARK_PARTITION, maxPartitionSize + "B")
                .setSystemProperty(MIN_SPARK_INPUT_PARTITION_COUNT_FOR_AUTO_TUNE, "1")
                .build();

        List<ScheduledSplit> splits = new ArrayList<>();
        for (int i = 0; i < splitsSize.size(); ++i) {
            MockSplit mockSplit = new MockSplit(splitsSize.get(i));
            Split testSplit = new Split(new ConnectorId("test" + i), TestingTransactionHandle.create(), mockSplit);
            ScheduledSplit scheduledSplit = new ScheduledSplit(i, new PlanNodeId("source"), testSplit);
            splits.add(scheduledSplit);
        }

        SetMultimap<Integer, ScheduledSplit> assignedSplits = assignSourceDistributionSplits(session, splits);
        Map<Integer, Set<Long>> partitionToSplitsSize = new HashMap<>();

        asMap(assignedSplits).forEach((partitionId, scheduledSplits) -> {
            if (autoTunePartitionCount) {
                if (scheduledSplits.size() > 1) {
                    long totalPartitionSize = scheduledSplits.stream()
                            .mapToLong(split -> split.getSplit().getConnectorSplit().getSplitSizeInBytes().getAsLong())
                            .sum();
                }
                else {
                    assertEquals(scheduledSplits.size(), 1, "A partition should hold at least one split");
                }
            }
            Set<Long> splitsSizeInBytes = scheduledSplits.stream()
                    .map(split -> split.getSplit().getConnectorSplit().getSplitSizeInBytes().getAsLong())
                    .collect(toSet());
            partitionToSplitsSize.put(partitionId, splitsSizeInBytes);
        });

        long expectedSizeInBytes = splitsSize.stream()
                .mapToLong(Long::longValue)
                .sum();
        long actualTotalSizeInBytes = assignedSplits.values().stream()
                .mapToLong(split -> split.getSplit().getConnectorSplit().getSplitSizeInBytes().getAsLong())
                .sum();

        assertEquals(expectedSizeInBytes, actualTotalSizeInBytes);

        return partitionToSplitsSize;
    }

    private static class MockSplit
            implements ConnectorSplit
    {
        long splitSizeInBytes;

        public MockSplit(long splitSizeInBytes)
        {
            this.splitSizeInBytes = splitSizeInBytes;
        }

        @Override
        public NodeSelectionStrategy getNodeSelectionStrategy()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object getInfo()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public OptionalLong getSplitSizeInBytes()
        {
            return OptionalLong.of(splitSizeInBytes);
        }
    }
}
