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
package com.facebook.presto.operator;

import com.facebook.presto.RowPagesBuilder;
import com.facebook.presto.operator.HashPartitionMaskOperator.HashPartitionMaskOperatorFactory;
import com.facebook.presto.spi.Page;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.type.BigintOperators;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.slice.XxHash64;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.stream.IntStream;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test
public class TestHashPartitionMaskOperator
{
    private static final int PARTITION_COUNT = 5;
    private static final int ROW_COUNT = 100;
    private ExecutorService executor;

    @BeforeClass
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-%s"));
    }

    @AfterClass
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @DataProvider(name = "hashEnabledValues")
    public static Object[][] hashEnabledValuesProvider()
    {
        return new Object[][] { { true }, { false } };
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testHashPartitionMask(boolean hashEnabled)
            throws Exception
    {
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, Ints.asList(0), BIGINT);
        List<Page> input = rowPagesBuilder
                .addSequencePage(ROW_COUNT, 0)
                .build();

        OperatorFactory operatorFactory = new HashPartitionMaskOperatorFactory(
                0,
                new PlanNodeId("test"),
                PARTITION_COUNT,
                rowPagesBuilder.getTypes(),
                ImmutableList.of(),
                ImmutableList.of(0),
                rowPagesBuilder.getHashChannel());

        int[] rowPartition = new int[ROW_COUNT];
        Arrays.fill(rowPartition, -1);
        for (int partition = 0; partition < PARTITION_COUNT; partition++) {
            MaterializedResult.Builder expected = resultBuilder(TEST_SESSION, BIGINT, BOOLEAN);
            for (int i = 0; i < ROW_COUNT; i++) {
                long rawHash = BigintOperators.hashCode(i);
                // mix the bits so we don't use the same hash used to distribute between stages
                rawHash = XxHash64.hash(Long.reverse(rawHash));
                rawHash &= Long.MAX_VALUE;

                boolean active = (rawHash % PARTITION_COUNT == partition);
                expected.row((long) i, active);

                if (active) {
                    assertEquals(rowPartition[i], -1);
                    rowPartition[i] = partition;
                }
            }

            OperatorAssertion.assertOperatorEqualsIgnoreOrder(operatorFactory, createDriverContext(), input, expected.build(), hashEnabled, Optional.of(1));
        }
        assertTrue(IntStream.of(rowPartition).noneMatch(partition -> partition == -1));
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testHashPartitionMaskWithMask(boolean hashEnabled)
            throws Exception
    {
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, Ints.asList(0), BIGINT, BOOLEAN, BOOLEAN);
        List<Page> input = rowPagesBuilder
                .addSequencePage(ROW_COUNT, 0, 0, 1)
                .build();

        OperatorFactory operatorFactory = new HashPartitionMaskOperatorFactory(
                0,
                new PlanNodeId("test"),
                PARTITION_COUNT,
                rowPagesBuilder.getTypes(),
                ImmutableList.of(1, 2),
                ImmutableList.of(0),
                rowPagesBuilder.getHashChannel());

        int[] rowPartition = new int[ROW_COUNT];
        Arrays.fill(rowPartition, -1);
        for (int partition = 0; partition < PARTITION_COUNT; partition++) {
            MaterializedResult.Builder expected = resultBuilder(TEST_SESSION, BIGINT, BOOLEAN, BOOLEAN, BOOLEAN);
            for (int i = 0; i < ROW_COUNT; i++) {
                long rawHash = BigintOperators.hashCode(i);
                // mix the bits so we don't use the same hash used to distribute between stages
                rawHash = XxHash64.hash(Long.reverse(rawHash));
                rawHash &= Long.MAX_VALUE;

                boolean active = (rawHash % PARTITION_COUNT == partition);
                boolean maskValue = i % 2 == 0;
                expected.row((long) i, active && maskValue, active && !maskValue, active);

                if (active) {
                    assertEquals(rowPartition[i], -1);
                    rowPartition[i] = partition;
                }
            }

            OperatorAssertion.assertOperatorEqualsIgnoreOrder(operatorFactory, createDriverContext(), input, expected.build(), hashEnabled, Optional.of(3));
        }
        assertTrue(IntStream.of(rowPartition).noneMatch(partition -> partition == -1));
    }

    public DriverContext createDriverContext()
    {
        return createTaskContext(executor, TEST_SESSION)
                .addPipelineContext(0, true, true)
                .addDriverContext();
    }
}
