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

import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.facebook.presto.split.SplitSource;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.primitives.Ints.min;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestPrestoSparkSourceDistributionSplitAssigner
{
    @Test
    public void testSplitAssignmentWithAutoTuneEnabled()
    {
        assertSplitAssignmentWithAutoTuneEnabled(
                new DataSize(10, BYTE),
                2,
                4,
                ImmutableList.of(),
                ImmutableMap.of());
        assertSplitAssignmentWithAutoTuneEnabled(
                new DataSize(10, BYTE),
                2,
                4,
                ImmutableList.of(1L),
                ImmutableMap.of(
                        0, ImmutableList.of(1L)));
        assertSplitAssignmentWithAutoTuneEnabled(
                new DataSize(10, BYTE),
                2,
                4,
                ImmutableList.of(1L, 1L),
                ImmutableMap.of(
                        0, ImmutableList.of(1L),
                        1, ImmutableList.of(1L)));
        assertSplitAssignmentWithAutoTuneEnabled(
                new DataSize(10, BYTE),
                2,
                4,
                ImmutableList.of(10L, 11L, 12L, 13L, 9L),
                ImmutableMap.of(
                        0, ImmutableList.of(13L),
                        1, ImmutableList.of(12L),
                        2, ImmutableList.of(11L),
                        3, ImmutableList.of(10L, 9L)));
        assertSplitAssignmentWithAutoTuneEnabled(
                new DataSize(10, BYTE),
                1,
                4,
                ImmutableList.of(3L, 4L, 5L),
                ImmutableMap.of(
                        0, ImmutableList.of(5L, 4L),
                        1, ImmutableList.of(3L)));
        assertSplitAssignmentWithAutoTuneEnabled(
                new DataSize(10, BYTE),
                1,
                10,
                ImmutableList.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L),
                ImmutableMap.<Integer, List<Long>>builder()
                        .put(0, ImmutableList.of(11L))
                        .put(1, ImmutableList.of(10L))
                        .put(2, ImmutableList.of(9L))
                        .put(3, ImmutableList.of(8L, 1L))
                        .put(4, ImmutableList.of(7L, 2L))
                        .put(5, ImmutableList.of(6L, 3L))
                        .put(6, ImmutableList.of(5L, 4L))
                        .build());
        assertSplitAssignmentWithAutoTuneEnabled(
                new DataSize(10, BYTE),
                1,
                10,
                ImmutableList.of(1L, 2L, 3L, 4L, 5L, 6L),
                ImmutableMap.<Integer, List<Long>>builder()
                        .put(0, ImmutableList.of(6L, 3L))
                        .put(1, ImmutableList.of(5L, 4L))
                        .put(2, ImmutableList.of(2L, 1L))
                        .build());
    }

    private static void assertSplitAssignmentWithAutoTuneEnabled(
            DataSize maxSplitsDataSizePerSparkPartition,
            int minSparkInputPartitionCountForAutoTune,
            int maxSparkInputPartitionCountForAutoTune,
            List<Long> splitSizes,
            Map<Integer, List<Long>> expectedAssignment)
    {
        assertSplitAssignment(
                true,
                maxSplitsDataSizePerSparkPartition,
                // doesn't matter with auto tune enabled
                1,
                minSparkInputPartitionCountForAutoTune,
                maxSparkInputPartitionCountForAutoTune,
                splitSizes,
                expectedAssignment);
    }

    @Test
    public void testSplitAssignmentWithAutoTuneDisabled()
    {
        assertSplitAssignmentWithAutoTuneDisabled(
                1,
                ImmutableList.of(),
                ImmutableMap.of());
        assertSplitAssignmentWithAutoTuneDisabled(
                1,
                ImmutableList.of(1L),
                ImmutableMap.of(0, ImmutableList.of(1L)));
        assertSplitAssignmentWithAutoTuneDisabled(
                1,
                ImmutableList.of(1L, 1L),
                ImmutableMap.of(0, ImmutableList.of(1L, 1L)));
        assertSplitAssignmentWithAutoTuneDisabled(
                2,
                ImmutableList.of(1L, 1L),
                ImmutableMap.of(
                        0, ImmutableList.of(1L),
                        1, ImmutableList.of(1L)));
        assertSplitAssignmentWithAutoTuneDisabled(
                2,
                ImmutableList.of(1L, 1L, 2L),
                ImmutableMap.of(
                        0, ImmutableList.of(2L),
                        1, ImmutableList.of(1L, 1L)));
        assertSplitAssignmentWithAutoTuneDisabled(
                2,
                ImmutableList.of(2L, 1L, 1L, 1L),
                ImmutableMap.of(
                        0, ImmutableList.of(2L, 1L),
                        1, ImmutableList.of(1L, 1L)));
        assertSplitAssignmentWithAutoTuneDisabled(
                2,
                ImmutableList.of(2L, 1L, 1L, 1L, 3L),
                ImmutableMap.of(
                        0, ImmutableList.of(3L, 1L),
                        1, ImmutableList.of(2L, 1L, 1L)));
        assertSplitAssignmentWithAutoTuneDisabled(
                3,
                ImmutableList.of(1L, 2L, 3L, 4L, 5L, 6L),
                ImmutableMap.of(
                        0, ImmutableList.of(6L, 1L),
                        1, ImmutableList.of(5L, 2L),
                        2, ImmutableList.of(4L, 3L)));
        assertSplitAssignmentWithAutoTuneDisabled(
                3,
                ImmutableList.of(5L, 6L, 7L, 8L, 9L, 10L),
                ImmutableMap.of(
                        0, ImmutableList.of(10L, 5L),
                        1, ImmutableList.of(9L, 6L),
                        2, ImmutableList.of(8L, 7L)));
    }

    private static void assertSplitAssignmentWithAutoTuneDisabled(
            int initialPartitionCount,
            List<Long> splitSizes,
            Map<Integer, List<Long>> expectedAssignment)
    {
        assertSplitAssignment(
                false,
                // doesn't matter with auto tune disabled
                new DataSize(1, BYTE),
                initialPartitionCount,
                // doesn't matter with auto tune disabled
                1,
                // doesn't matter with auto tune disabled
                2,
                splitSizes,
                expectedAssignment);
    }

    private static void assertSplitAssignment(
            boolean autoTuneEnabled,
            DataSize maxSplitsDataSizePerSparkPartition,
            int initialPartitionCount,
            int minSparkInputPartitionCountForAutoTune,
            int maxSparkInputPartitionCountForAutoTune,
            List<Long> splitSizes,
            Map<Integer, List<Long>> expectedAssignment)
    {
        // assign splits in one shot
        {
            PrestoSparkSplitAssigner assigner = new PrestoSparkSourceDistributionSplitAssigner(
                    new PlanNodeId("test"),
                    createSplitSource(splitSizes),
                    Integer.MAX_VALUE,
                    maxSplitsDataSizePerSparkPartition.toBytes(),
                    initialPartitionCount,
                    autoTuneEnabled,
                    minSparkInputPartitionCountForAutoTune,
                    maxSparkInputPartitionCountForAutoTune);

            Optional<SetMultimap<Integer, ScheduledSplit>> actualAssignment = assigner.getNextBatch();
            if (!splitSizes.isEmpty()) {
                assertThat(actualAssignment).isPresent();
                assertAssignedSplits(actualAssignment.get(), expectedAssignment);
            }
            else {
                assertThat(actualAssignment).isNotPresent();
            }
        }

        // assign splits iteratively
        for (int splitBatchSize = 1; splitBatchSize < splitSizes.size(); splitBatchSize *= 2) {
            HashMultimap<Integer, ScheduledSplit> actualAssignment = HashMultimap.create();

            // sort splits to make assignment match the assignment done in one shot
            List<Long> sortedSplits = new ArrayList<>(splitSizes);
            sortedSplits.sort(Comparator.<Long>naturalOrder().reversed());

            PrestoSparkSplitAssigner assigner = new PrestoSparkSourceDistributionSplitAssigner(
                    new PlanNodeId("test"),
                    createSplitSource(sortedSplits),
                    splitBatchSize,
                    maxSplitsDataSizePerSparkPartition.toBytes(),
                    initialPartitionCount,
                    autoTuneEnabled,
                    minSparkInputPartitionCountForAutoTune,
                    maxSparkInputPartitionCountForAutoTune);

            while (true) {
                Optional<SetMultimap<Integer, ScheduledSplit>> assignment = assigner.getNextBatch();
                if (!assignment.isPresent()) {
                    break;
                }
                actualAssignment.putAll(assignment.get());
            }

            assertAssignedSplits(actualAssignment, expectedAssignment);
        }
    }

    @Test
    public void testAssignSplitsToPartitionWithRandomSplitSizes()
    {
        DataSize maxSplitDataSizePerPartition = new DataSize(2048, BYTE);
        int initialPartitionCount = 3;
        int minSparkInputPartitionCountForAutoTune = 2;
        int maxSparkInputPartitionCountForAutoTune = 5;
        int maxSplitSizeInBytes = 2048;
        AtomicInteger sequenceId = new AtomicInteger();
        for (int i = 0; i < 3; ++i) {
            List<Long> splitSizes = new ArrayList<>(1000);
            for (int j = 0; j < 1000; j++) {
                splitSizes.add(ThreadLocalRandom.current().nextLong((long) (maxSplitSizeInBytes * 1.2)));
            }

            PrestoSparkSplitAssigner assigner = new PrestoSparkSourceDistributionSplitAssigner(
                    new PlanNodeId("test"),
                    createSplitSource(splitSizes),
                    333,
                    maxSplitDataSizePerPartition.toBytes(),
                    initialPartitionCount,
                    true,
                    minSparkInputPartitionCountForAutoTune,
                    maxSparkInputPartitionCountForAutoTune);

            HashMultimap<Integer, ScheduledSplit> actualAssignment = HashMultimap.create();

            while (true) {
                Optional<SetMultimap<Integer, ScheduledSplit>> assignment = assigner.getNextBatch();
                if (!assignment.isPresent()) {
                    break;
                }
                actualAssignment.putAll(assignment.get());
            }

            long expectedSizeInBytes = splitSizes.stream()
                    .mapToLong(Long::longValue)
                    .sum();
            long actualTotalSizeInBytes = actualAssignment.values().stream()
                    .mapToLong(split -> split.getSplit().getConnectorSplit().getSplitSizeInBytes().orElseThrow(() -> new IllegalArgumentException("split size is expected to be present")))
                    .sum();

            // check if all splits got assigned
            assertEquals(expectedSizeInBytes, actualTotalSizeInBytes);
        }
    }

    private static void assertAssignedSplits(SetMultimap<Integer, ScheduledSplit> actual, Map<Integer, List<Long>> expected)
    {
        Map<Integer, List<Long>> actualAssignment = getAssignedSplitSizes(actual);
        assertThat(actualAssignment.keySet()).isEqualTo(expected.keySet());
        for (Integer partition : actualAssignment.keySet()) {
            assertThat(actualAssignment.get(partition)).containsExactlyInAnyOrder(expected.get(partition).toArray(new Long[] {}));
        }
    }

    private static Map<Integer, List<Long>> getAssignedSplitSizes(SetMultimap<Integer, ScheduledSplit> assignedSplits)
    {
        return assignedSplits.asMap().entrySet().stream()
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().stream()
                                .map(split -> split.getSplit().getConnectorSplit().getSplitSizeInBytes().orElseThrow(() -> new IllegalArgumentException("split size is expected to be present")))
                                .collect(toImmutableList())));
    }

    private static SplitSource createSplitSource(List<Long> splitSizes)
    {
        List<Split> splits = splitSizes.stream()
                .map(size -> new Split(new ConnectorId("test"), TestingTransactionHandle.create(), new MockSplit(size)))
                .collect(toImmutableList());
        return new MockSplitSource(splits);
    }

    private static class MockSplit
            implements ConnectorSplit
    {
        private final long splitSizeInBytes;

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

    private static class MockSplitSource
            implements SplitSource
    {
        private final List<Split> splits;

        private int position;
        private boolean closed;

        private MockSplitSource(List<Split> splits)
        {
            this.splits = ImmutableList.copyOf(requireNonNull(splits, "splits is null"));
        }

        @Override
        public ConnectorId getConnectorId()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConnectorTransactionHandle getTransactionHandle()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ListenableFuture<SplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, Lifespan lifespan, int maxSize)
        {
            checkState(!closed, "split source is closed");
            checkState(!isFinished(), "split source is finished");
            checkArgument(partitionHandle.equals(NOT_PARTITIONED), "unexpected partition handle: %s", partitionHandle);
            checkArgument(lifespan.equals(Lifespan.taskWide()), "unexpected lifespan: %s", lifespan);

            int remaining = splits.size() - position;
            int batchSize = min(remaining, maxSize);
            List<Split> batch = ImmutableList.copyOf(splits.subList(position, position + batchSize));
            position += batchSize;

            return immediateFuture(new SplitBatch(batch, isFinished()));
        }

        @Override
        public void rewind(ConnectorPartitionHandle partitionHandle)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public boolean isFinished()
        {
            return position >= splits.size();
        }
    }
}
