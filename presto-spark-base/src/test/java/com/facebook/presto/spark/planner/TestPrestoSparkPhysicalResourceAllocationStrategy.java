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
import com.facebook.presto.metadata.AbstractMockMetadata;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spark.PhysicalResourceSettings;
import com.facebook.presto.spark.PrestoSparkPhysicalResourceCalculator;
import com.facebook.presto.spark.PrestoSparkSourceStatsCollector;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.testing.TestingMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.SystemSessionProperties.HASH_PARTITION_COUNT;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_AVERAGE_INPUT_DATA_SIZE_PER_EXECUTOR;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_AVERAGE_INPUT_DATA_SIZE_PER_PARTITION;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_EXECUTOR_ALLOCATION_STRATEGY_ENABLED;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_HASH_PARTITION_COUNT_ALLOCATION_STRATEGY_ENABLED;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_MAX_EXECUTOR_COUNT;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_MAX_HASH_PARTITION_COUNT;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_MIN_EXECUTOR_COUNT;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_MIN_HASH_PARTITION_COUNT;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_RESOURCE_ALLOCATION_STRATEGY_ENABLED;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestPrestoSparkPhysicalResourceAllocationStrategy
{
    // mocked metadata with table statistics generating random estimate count for the purpose of testing
    // no other method is stubbed so will likely throw UnsupportedOperationException
    private static class MockedMetadata
            extends AbstractMockMetadata
    {
        @Override
        public TableStatistics getTableStatistics(Session session, TableHandle tableHandle, List<ColumnHandle> columnHandles, Constraint<ColumnHandle> constraint)
        {
            return TableStatistics.builder().setRowCount(Estimate.of(100)).setTotalSize(Estimate.of(1000)).build();
        }
    }

    // default properties passed as part of system property
    private static final PropertyMetadata<?>[] defaultPropertyMetadata = new PropertyMetadata[] {
            PropertyMetadata.integerProperty(SPARK_MIN_EXECUTOR_COUNT, "SPARK_MIN_EXECUTOR_COUNT", 10, false),
            PropertyMetadata.integerProperty(SPARK_MAX_EXECUTOR_COUNT, "SPARK_MAX_EXECUTOR_COUNT", 1000, false),
            PropertyMetadata.integerProperty(SPARK_MIN_HASH_PARTITION_COUNT, "SPARK_MIN_HASH_PARTITION_COUNT", 10, false),
            PropertyMetadata.integerProperty(SPARK_MAX_HASH_PARTITION_COUNT, "SPARK_MAX_HASH_PARTITION_COUNT", 1000, false),
            PropertyMetadata.dataSizeProperty(SPARK_AVERAGE_INPUT_DATA_SIZE_PER_EXECUTOR, "SPARK_AVERAGE_INPUT_DATA_SIZE_PER_EXECUTOR", new DataSize(200, DataSize.Unit.BYTE), false),
            PropertyMetadata.dataSizeProperty(SPARK_AVERAGE_INPUT_DATA_SIZE_PER_PARTITION, "SPARK_AVERAGE_INPUT_DATA_SIZE_PER_PARTITION", new DataSize(100, DataSize.Unit.BYTE), false),
            PropertyMetadata.integerProperty(HASH_PARTITION_COUNT, "HASH_PARTITION_COUNT", 150, false)
    };
    // system property with allocation based tuning enabled
    private static final Session testSessionWithAllocation = testSessionBuilder(new SessionPropertyManager(
            new ImmutableList.Builder<PropertyMetadata<?>>().add(defaultPropertyMetadata).add(
                    PropertyMetadata.booleanProperty(SPARK_RESOURCE_ALLOCATION_STRATEGY_ENABLED, "SPARK_RESOURCE_ALLOCATION_STRATEGY_ENABLED", true, false)
            ).build())).build();
    // system property with allocation based tuning disabled
    private static final Session testSessionWithoutAllocation = testSessionBuilder(new SessionPropertyManager(
            new ImmutableList.Builder<PropertyMetadata<?>>().add(defaultPropertyMetadata).add(
                    PropertyMetadata.booleanProperty(SPARK_RESOURCE_ALLOCATION_STRATEGY_ENABLED, "SPARK_RESOURCE_ALLOCATION_STRATEGY_ENABLED", false, false),
                    PropertyMetadata.booleanProperty(SPARK_HASH_PARTITION_COUNT_ALLOCATION_STRATEGY_ENABLED, "SPARK_HASH_PARTITION_COUNT_ALLOCATION_STRATEGY_ENABLED", false, false),
                    PropertyMetadata.booleanProperty(SPARK_EXECUTOR_ALLOCATION_STRATEGY_ENABLED, "SPARK_EXECUTOR_ALLOCATION_STRATEGY_ENABLED", false, false)
            ).build())).build();
    private static final Metadata mockedMetadata = new MockedMetadata();

    /**
     * Return any plan node, the node does not even need to be "correct",
     * only used for the purpose of traversing and estimating the source stats
     */
    private PlanNode getPlanToTest(Session session, Metadata metadata)
    {
        PlanBuilder planBuilder = new PlanBuilder(session, new PlanNodeIdAllocator(), metadata);
        VariableReferenceExpression sourceJoin = planBuilder.variable("sourceJoin");

        TableScanNode a = planBuilder.tableScan(ImmutableList.of(sourceJoin), ImmutableMap.of(sourceJoin, new TestingMetadata.TestingColumnHandle("sourceJoin")));
        VariableReferenceExpression filteringSource = planBuilder.variable("filteringSource");
        TableScanNode b = planBuilder.tableScan(ImmutableList.of(filteringSource), ImmutableMap.of(filteringSource, new TestingMetadata.TestingColumnHandle("filteringSource")));

        return planBuilder.join(JoinNode.Type.LEFT, a, b);
    }

    @Test
    public void testHashPartitionCountAllocationStrategy()
    {
        PrestoSparkSourceStatsCollector prestoSparkSourceStatsCollector = new PrestoSparkSourceStatsCollector(mockedMetadata, testSessionWithAllocation);
        PlanNode nodeToTest = getPlanToTest(testSessionWithAllocation, mockedMetadata);

        PhysicalResourceSettings settingsHolder = new PrestoSparkPhysicalResourceCalculator()
                .calculate(nodeToTest, prestoSparkSourceStatsCollector, testSessionWithAllocation);
        assertEquals(settingsHolder.getHashPartitionCount(), 20);
        assertEquals(settingsHolder.getMaxExecutorCount().getAsInt(), 10);
    }

    @Test
    public void testHashPartitionCountWithoutAllocationStrategy()
    {
        PrestoSparkSourceStatsCollector prestoSparkSourceStatsCollector = new PrestoSparkSourceStatsCollector(mockedMetadata, testSessionWithoutAllocation);
        PlanNode nodeToTest = getPlanToTest(testSessionWithoutAllocation, mockedMetadata);

        PhysicalResourceSettings settingsHolder = new PrestoSparkPhysicalResourceCalculator()
                .calculate(nodeToTest, prestoSparkSourceStatsCollector, testSessionWithoutAllocation);
        assertEquals(settingsHolder.getHashPartitionCount(), 150);
        assertFalse(settingsHolder.getMaxExecutorCount().isPresent());
    }
}
