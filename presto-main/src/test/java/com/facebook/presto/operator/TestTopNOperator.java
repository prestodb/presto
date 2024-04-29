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
import com.facebook.presto.operator.TopNOperator.TopNOperatorFactory;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
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
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestTopNOperator
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private DriverContext driverContext;

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

    @Test
    public void testSingleFieldKey()
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(1L, 0.1)
                .row(2L, 0.2)
                .pageBreak()
                .row(-1L, -0.1)
                .row(4L, 0.4)
                .pageBreak()
                .row(5L, 0.5)
                .row(4L, 0.41)
                .row(6L, 0.6)
                .pageBreak()
                .build();

        TopNOperatorFactory operatorFactory = new TopNOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, DOUBLE),
                2,
                ImmutableList.of(0),
                ImmutableList.of(DESC_NULLS_LAST));

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, DOUBLE)
                .row(6L, 0.6)
                .row(5L, 0.5)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testMultiFieldKey()
    {
        List<Page> input = rowPagesBuilder(VARCHAR, BIGINT)
                .row("a", 1L)
                .row("b", 2L)
                .pageBreak()
                .row("f", 3L)
                .row("a", 4L)
                .pageBreak()
                .row("d", 5L)
                .row("d", 7L)
                .row("e", 6L)
                .build();

        TopNOperatorFactory operatorFactory = new TopNOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(VARCHAR, BIGINT),
                3,
                ImmutableList.of(0, 1),
                ImmutableList.of(DESC_NULLS_LAST, DESC_NULLS_LAST));

        MaterializedResult expected = MaterializedResult.resultBuilder(driverContext.getSession(), VARCHAR, BIGINT)
                .row("f", 3L)
                .row("e", 6L)
                .row("d", 7L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testReverseOrder()
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(1L, 0.1)
                .row(2L, 0.2)
                .pageBreak()
                .row(-1L, -0.1)
                .row(4L, 0.4)
                .pageBreak()
                .row(5L, 0.5)
                .row(4L, 0.41)
                .row(6L, 0.6)
                .pageBreak()
                .build();

        TopNOperatorFactory operatorFactory = new TopNOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, DOUBLE),
                2,
                ImmutableList.of(0),
                ImmutableList.of(ASC_NULLS_LAST));

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, DOUBLE)
                .row(-1L, -0.1)
                .row(1L, 0.1)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testLimitZero()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(BIGINT).row(1L).build();

        TopNOperatorFactory factory = new TopNOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT),
                0,
                ImmutableList.of(0),
                ImmutableList.of(DESC_NULLS_LAST));

        try (Operator operator = factory.createOperator(driverContext)) {
            assertEquals(operator.isFinished(), true);
            assertEquals(operator.needsInput(), false);
            assertEquals(operator.getOutput(), null);
        }
    }

    @Test
    public void testExceedMemoryLimit()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(BIGINT)
                .row(1L)
                .build();

        DriverContext smallDiverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION, new DataSize(1, BYTE))
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        TopNOperatorFactory operatorFactory = new TopNOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT),
                100,
                ImmutableList.of(0),
                ImmutableList.of(ASC_NULLS_LAST));
        try (Operator operator = operatorFactory.createOperator(smallDiverContext)) {
            operator.addInput(input.get(0));
            fail("must fail because of exceeding local memory limit");
        }
        catch (ExceededMemoryLimitException ignore) {
        }
    }
}
