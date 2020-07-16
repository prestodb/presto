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
package com.facebook.presto.spark;

import com.facebook.presto.Session;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.tests.AbstractTestQueries;
import com.google.common.collect.SetMultimap;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.spark.PrestoSparkSessionProperties.MAX_SPLITS_DATA_SIZE_PER_SPARK_PARTITION;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_INITIAL_PARTITION_COUNT;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_PARTITION_COUNT_AUTO_TUNE_ENABLED;
import static com.facebook.presto.spark.planner.PrestoSparkRddFactory.assignSourceDistributionSplits;
import static com.google.common.collect.Multimaps.asMap;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestPrestoSparkAbstractTestQueries
        extends AbstractTestQueries
{
    protected TestPrestoSparkAbstractTestQueries()
    {
        super(PrestoSparkQueryRunner::createHivePrestoSparkQueryRunner);
    }

    @Override
    public void testDescribeInput()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testDescribeInputNoParameters()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testDescribeInputWithAggregation()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testDescribeInputNoSuchQuery()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testDescribeOutput()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testDescribeOutputNamedAndUnnamed()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testDescribeOutputNonSelect()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testDescribeOutputShowTables()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testDescribeOutputOnAliasedColumnsAndExpressions()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testDescribeOutputNoSuchQuery()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testExecute()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testExecuteUsing()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testExecuteUsingComplexJoinCriteria()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testExecuteUsingWithSubquery()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testExecuteUsingWithSubqueryInJoin()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testExecuteWithParametersInGroupBy()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testExplainExecute()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testExplainExecuteWithUsing()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testExplainSetSessionWithUsing()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testPreparedStatementWithSubqueries()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testExplainDdl()
    {
        // DDL statements are not supported by Presto on Spark
    }

    @Test
    public void testAssignSourceDistributionSplits()
    {
        // black box test
        testAssignSplitsToPartitionWithRandomSplitsSize(3);

        // auto tune partition + splits with mixed size
        List<Long> testSplitsSize = new ArrayList(Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L));
        Collections.shuffle(testSplitsSize);
        Map<Integer, List<Long>> actualResult = assignSplitsToPartition(true, 10, testSplitsSize);

        Map<Integer, List<Long>> expectedResult = new HashMap<>();
        expectedResult.put(0, new ArrayList(Arrays.asList(11L)));
        expectedResult.put(1, new ArrayList(Arrays.asList(10L)));
        expectedResult.put(2, new ArrayList(Arrays.asList(9L)));
        expectedResult.put(3, new ArrayList(Arrays.asList(8L, 1L)));
        expectedResult.put(4, new ArrayList(Arrays.asList(7L, 2L)));
        expectedResult.put(5, new ArrayList(Arrays.asList(6L, 3L)));
        expectedResult.put(6, new ArrayList(Arrays.asList(5L, 4L)));
        assertEquals(actualResult, expectedResult);

        // enable auto tune partition + small splits
        testSplitsSize = new ArrayList(Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L));
        Collections.shuffle(testSplitsSize);
        actualResult = assignSplitsToPartition(true, 10, testSplitsSize);

        expectedResult = new HashMap<>();
        expectedResult.put(0, new ArrayList(Arrays.asList(6L, 3L)));
        expectedResult.put(1, new ArrayList(Arrays.asList(5L, 4L)));
        expectedResult.put(2, new ArrayList(Arrays.asList(2L, 1L)));
        assertEquals(actualResult, expectedResult);

        // disable auto tune partition + small splits
        testSplitsSize = new ArrayList(Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L));
        actualResult = assignSplitsToPartition(false, 10, testSplitsSize);

        expectedResult = new HashMap<>();
        expectedResult.put(0, new ArrayList(Arrays.asList(6L, 1L)));
        expectedResult.put(1, new ArrayList(Arrays.asList(5L, 2L)));
        expectedResult.put(2, new ArrayList(Arrays.asList(4L, 3L)));
        assertEquals(actualResult, expectedResult);

        // disable auto tune partition + large splits
        testSplitsSize = new ArrayList(Arrays.asList(5L, 6L, 7L, 8L, 9L, 10L));
        Collections.shuffle(testSplitsSize);
        actualResult = assignSplitsToPartition(false, 10, testSplitsSize);

        expectedResult = new HashMap<>();
        expectedResult.put(0, new ArrayList(Arrays.asList(10L, 5L)));
        expectedResult.put(1, new ArrayList(Arrays.asList(9L, 6L)));
        expectedResult.put(2, new ArrayList(Arrays.asList(8L, 7L)));
        assertEquals(actualResult, expectedResult);
    }

    private void testAssignSplitsToPartitionWithRandomSplitsSize(int repeatedTimes)
    {
        int maxSplitSizeInBytes = 2048;
        for (int i = 0; i < repeatedTimes; ++i) {
            List<Long> splitsSize = new ArrayList<>(1000);
            for (int j = 0; j < splitsSize.size(); j++) {
                splitsSize.set(j, ThreadLocalRandom.current().nextLong((long) (maxSplitSizeInBytes * 1.2)));
            }
            assignSplitsToPartition(true, maxSplitSizeInBytes, splitsSize);
            assignSplitsToPartition(false, maxSplitSizeInBytes, splitsSize);
        }
    }

    private Map<Integer, List<Long>> assignSplitsToPartition(
            boolean autoTunePartitionCount,
            long maxPartitionSize,
            List<Long> splitsSize)
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(SPARK_PARTITION_COUNT_AUTO_TUNE_ENABLED, Boolean.toString(autoTunePartitionCount))
                .setSystemProperty(SPARK_INITIAL_PARTITION_COUNT, "3")
                .setSystemProperty(MAX_SPLITS_DATA_SIZE_PER_SPARK_PARTITION, maxPartitionSize + "B")
                .build();

        List<ScheduledSplit> splits = new ArrayList<>();
        for (int i = 0; i < splitsSize.size(); ++i) {
            MockSplit mockSplit = new MockSplit(splitsSize.get(i));
            Split testSplit = new Split(new ConnectorId("test" + i), TestingTransactionHandle.create(), mockSplit);
            ScheduledSplit scheduledSplit = new ScheduledSplit(i, new PlanNodeId("source"), testSplit);
            splits.add(scheduledSplit);
        }

        SetMultimap<Integer, ScheduledSplit> assignedSplits = assignSourceDistributionSplits(session, splits);
        Map<Integer, List<Long>> partitionToSplitsSize = new HashMap<>();

        asMap(assignedSplits).forEach((partitionId, scheduledSplits) -> {
            if (autoTunePartitionCount) {
                if (scheduledSplits.size() > 1) {
                    long totalPartitionSize = scheduledSplits.stream()
                            .mapToLong(split -> split.getSplit().getConnectorSplit().getSplitSizeInBytes().getAsLong())
                            .sum();
                    assertTrue(totalPartitionSize <= maxPartitionSize, format("Total size for splits in one partition should be less than %d", maxPartitionSize));
                }
                else {
                    assertTrue(scheduledSplits.size() == 1, "A partition should hold at least one split");
                }
            }
            List<Long> splitsSizeInBytes = scheduledSplits.stream()
                    .map(split -> split.getSplit().getConnectorSplit().getSplitSizeInBytes().getAsLong())
                    .collect(toList());
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

    private class MockSplit
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
