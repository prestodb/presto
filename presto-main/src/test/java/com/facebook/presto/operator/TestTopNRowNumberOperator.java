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
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.TestingTaskContext;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.testing.Assertions.assertGreaterThan;
import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.operator.BenchmarkInMemoryGroupedTopNBuilder.createSequentialInputPages;
import static com.facebook.presto.operator.GroupByHashYieldAssertion.createPagesWithDistinctHashKeys;
import static com.facebook.presto.operator.GroupByHashYieldAssertion.finishOperatorWithYieldingGroupByHash;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEqualsIgnoreOrder;
import static com.facebook.presto.operator.OperatorAssertion.toMaterializedResult;
import static com.facebook.presto.operator.TopNRowNumberOperator.TopNRowNumberOperatorFactory;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestTopNRowNumberOperator
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private DriverContext driverContext;
    private JoinCompiler joinCompiler;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
        driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
        joinCompiler = new JoinCompiler(MetadataManager.createTestMetadataManager(), new FeaturesConfig());
    }

    @AfterMethod
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @DataProvider(name = "hashEnabledValues")
    public static Object[][] hashEnabledValuesProvider()
    {
        return new Object[][] {{true}, {false}};
    }

    @DataProvider
    public Object[][] partial()
    {
        return new Object[][] {{true}, {false}};
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testPartitioned(boolean hashEnabled)
    {
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, Ints.asList(0), BIGINT, DOUBLE);
        List<Page> input = rowPagesBuilder
                .row(1L, 0.3)
                .row(2L, 0.2)
                .row(3L, 0.1)
                .row(3L, 0.91)
                .pageBreak()
                .row(1L, 0.4)
                .pageBreak()
                .row(1L, 0.5)
                .row(1L, 0.6)
                .row(2L, 0.7)
                .row(2L, 0.8)
                .pageBreak()
                .row(2L, 0.9)
                .build();

        TopNRowNumberOperatorFactory operatorFactory = new TopNRowNumberOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, DOUBLE),
                Ints.asList(1, 0),
                Ints.asList(0),
                ImmutableList.of(BIGINT),
                Ints.asList(1),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                3,
                false,
                Optional.empty(),
                10,
                0,
                joinCompiler,
                null,
                false);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), DOUBLE, BIGINT, BIGINT)
                .row(0.3, 1L, 1L)
                .row(0.4, 1L, 2L)
                .row(0.5, 1L, 3L)
                .row(0.2, 2L, 1L)
                .row(0.7, 2L, 2L)
                .row(0.8, 2L, 3L)
                .row(0.1, 3L, 1L)
                .row(0.91, 3L, 2L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test(dataProvider = "partial")
    public void testUnPartitioned(boolean partial)
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(1L, 0.3)
                .row(2L, 0.2)
                .row(3L, 0.1)
                .row(3L, 0.91)
                .pageBreak()
                .row(1L, 0.4)
                .pageBreak()
                .row(1L, 0.5)
                .row(1L, 0.6)
                .row(2L, 0.7)
                .row(2L, 0.8)
                .pageBreak()
                .row(2L, 0.9)
                .build();

        TopNRowNumberOperatorFactory operatorFactory = new TopNRowNumberOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, DOUBLE),
                Ints.asList(1, 0),
                Ints.asList(),
                ImmutableList.of(),
                Ints.asList(1),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                3,
                partial,
                Optional.empty(),
                10,
                0,
                joinCompiler,
                null,
                false);

        MaterializedResult expected;
        if (partial) {
            expected = resultBuilder(driverContext.getSession(), DOUBLE, BIGINT)
                    .row(0.1, 3L)
                    .row(0.2, 2L)
                    .row(0.3, 1L)
                    .build();
        }
        else {
            expected = resultBuilder(driverContext.getSession(), DOUBLE, BIGINT, BIGINT)
                    .row(0.1, 3L, 1L)
                    .row(0.2, 2L, 2L)
                    .row(0.3, 1L, 3L)
                    .build();
        }

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testMemoryReservationYield()
    {
        Type type = BIGINT;
        List<Page> input = createPagesWithDistinctHashKeys(type, 6_000, 600);

        OperatorFactory operatorFactory = new TopNRowNumberOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(type),
                ImmutableList.of(0),
                ImmutableList.of(0),
                ImmutableList.of(type),
                Ints.asList(0),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                3,
                false,
                Optional.empty(),
                10,
                0,
                joinCompiler,
                null,
                false);

        // get result with yield; pick a relatively small buffer for heaps
        GroupByHashYieldAssertion.GroupByHashYieldResult result = finishOperatorWithYieldingGroupByHash(
                input,
                type,
                operatorFactory,
                operator -> ((TopNRowNumberOperator) operator).getCapacity(),
                1_000_000);
        assertGreaterThan(result.getYieldCount(), 3);
        assertGreaterThan(result.getMaxReservedBytes(), 5L << 20);

        int count = 0;
        for (Page page : result.getOutput()) {
            assertEquals(page.getChannelCount(), 2);
            for (int i = 0; i < page.getPositionCount(); i++) {
                assertEquals(page.getBlock(1).getByte(i), 1);
                count++;
            }
        }
        assertEquals(count, 6_000 * 600);
    }

    @Test
    public void testSpillableTopNRowNumberOperatorProducesCorrectOutputIFSpilledDuringAddInput()
    {
        List<Page> input = createSequentialInputPages(1000, ImmutableList.of(BIGINT, DOUBLE, DOUBLE, VARCHAR, DOUBLE), 200, 300, 42);

        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
        driverContext = TestingTaskContext.builder(executor, scheduledExecutor, TEST_SESSION)
                .setQueryMaxMemory(new DataSize(200, DataSize.Unit.KILOBYTE))
                .setQueryMaxTotalMemory(new DataSize(200, DataSize.Unit.KILOBYTE))
                .setMaxRevocableMemory(new DataSize(200, DataSize.Unit.KILOBYTE))
                .build()
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
        joinCompiler = new JoinCompiler(MetadataManager.createTestMetadataManager(), new FeaturesConfig());

        TopNRowNumberOperatorFactory operatorFactory = new TopNRowNumberOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, DOUBLE, DOUBLE, VARCHAR, DOUBLE),
                ImmutableList.of(0, 1, 2, 3, 4),
                ImmutableList.of(0),
                ImmutableList.of(BIGINT),
                ImmutableList.of(1),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                3,
                true,
                Optional.empty(),
                10,
                10000,
                joinCompiler,
                new DummySpillerFactory(),
                true);

        assertOperatorEqualsIgnoreOrder(operatorFactory, driverContext, input, toMaterializedResult(driverContext.getSession(), ImmutableList.of(BIGINT, DOUBLE, DOUBLE, VARCHAR, DOUBLE), input), true);
    }

    @Test
    public void testSpillableTopNRowNumberOperatorProducesCorrectOutputIfNOSPILLDuringAddInput()
    {
        List<Page> input = createSequentialInputPages(1000, ImmutableList.of(BIGINT, DOUBLE, DOUBLE, VARCHAR, DOUBLE), 200, 300, 42);

        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
        driverContext = TestingTaskContext.builder(executor, scheduledExecutor, TEST_SESSION)
                .setQueryMaxMemory(new DataSize(200, DataSize.Unit.KILOBYTE))
                .setQueryMaxTotalMemory(new DataSize(200, DataSize.Unit.KILOBYTE))
                .setMaxRevocableMemory(new DataSize(200, DataSize.Unit.KILOBYTE))
                .build()
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
        joinCompiler = new JoinCompiler(MetadataManager.createTestMetadataManager(), new FeaturesConfig());

        TopNRowNumberOperatorFactory operatorFactory = new TopNRowNumberOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, DOUBLE, DOUBLE, VARCHAR, DOUBLE),
                ImmutableList.of(0, 1, 2, 3, 4),
                ImmutableList.of(0),
                ImmutableList.of(BIGINT),
                ImmutableList.of(1),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                3,
                true,
                Optional.empty(),
                10,
                10000,
                joinCompiler,
                new DummySpillerFactory(),
                true);

        assertOperatorEqualsIgnoreOrder(operatorFactory, driverContext, input, toMaterializedResult(driverContext.getSession(), ImmutableList.of(BIGINT, DOUBLE, DOUBLE, VARCHAR, DOUBLE), input));
    }
}
