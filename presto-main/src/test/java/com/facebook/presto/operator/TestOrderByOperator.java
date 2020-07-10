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

import com.facebook.presto.ExceededMemoryLimitException;
import com.facebook.presto.common.Page;
import com.facebook.presto.operator.OrderByOperator.OrderByOperatorFactory;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.block.SortOrder.ASC_NULLS_LAST;
import static com.facebook.presto.common.block.SortOrder.DESC_NULLS_LAST;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.operator.OperatorAssertion.toPages;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

@Test(singleThreaded = true)
public class TestOrderByOperator
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private DriverContext driverContext;

    @DataProvider
    public static Object[][] spillEnabled()
    {
        return new Object[][] {{false}, {true}};
    }

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
        driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }

    @AfterMethod
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test(dataProvider = "spillEnabled")
    public void testSingleFieldKey(boolean spillEnabled)
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(1L, 0.1)
                .row(2L, 0.2)
                .pageBreak()
                .row(-1L, -0.1)
                .row(4L, 0.4)
                .build();

        OrderByOperatorFactory operatorFactory = new OrderByOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, DOUBLE),
                ImmutableList.of(1),
                10,
                ImmutableList.of(0),
                ImmutableList.of(ASC_NULLS_LAST),
                new PagesIndex.TestingFactory(false),
                spillEnabled,
                Optional.of(new DummySpillerFactory()));

        MaterializedResult expected = resultBuilder(driverContext.getSession(), DOUBLE)
                .row(-0.1)
                .row(0.1)
                .row(0.2)
                .row(0.4)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test(dataProvider = "spillEnabled")
    public void testMultiFieldKey(boolean spillEnabled)
    {
        List<Page> input = rowPagesBuilder(VARCHAR, BIGINT)
                .row("a", 1L)
                .row("b", 2L)
                .pageBreak()
                .row("b", 3L)
                .row("a", 4L)
                .build();

        OrderByOperatorFactory operatorFactory = new OrderByOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(VARCHAR, BIGINT),
                ImmutableList.of(0, 1),
                10,
                ImmutableList.of(0, 1),
                ImmutableList.of(ASC_NULLS_LAST, DESC_NULLS_LAST),
                new PagesIndex.TestingFactory(false),
                spillEnabled,
                Optional.of(new DummySpillerFactory()));

        MaterializedResult expected = MaterializedResult.resultBuilder(driverContext.getSession(), VARCHAR, BIGINT)
                .row("a", 4L)
                .row("a", 1L)
                .row("b", 3L)
                .row("b", 2L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test(dataProvider = "spillEnabled")
    public void testReverseOrder(boolean spillEnabled)
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(1L, 0.1)
                .row(2L, 0.2)
                .pageBreak()
                .row(-1L, -0.1)
                .row(4L, 0.4)
                .build();

        OrderByOperatorFactory operatorFactory = new OrderByOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, DOUBLE),
                ImmutableList.of(0),
                10,
                ImmutableList.of(0),
                ImmutableList.of(DESC_NULLS_LAST),
                new PagesIndex.TestingFactory(false),
                spillEnabled,
                Optional.of(new DummySpillerFactory()));

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT)
                .row(4L)
                .row(2L)
                .row(1L)
                .row(-1L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test(expectedExceptions = ExceededMemoryLimitException.class, expectedExceptionsMessageRegExp = "Query exceeded per-node user memory limit of 10B.*")
    public void testMemoryLimit()
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(1L, 0.1)
                .row(2L, 0.2)
                .pageBreak()
                .row(-1L, -0.1)
                .row(4L, 0.4)
                .build();

        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION, new DataSize(10, Unit.BYTE))
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        OrderByOperatorFactory operatorFactory = new OrderByOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, DOUBLE),
                ImmutableList.of(1),
                10,
                ImmutableList.of(0),
                ImmutableList.of(ASC_NULLS_LAST),
                new PagesIndex.TestingFactory(false),
                false,
                Optional.of(new DummySpillerFactory()));

        toPages(operatorFactory, driverContext, input);
    }
}
