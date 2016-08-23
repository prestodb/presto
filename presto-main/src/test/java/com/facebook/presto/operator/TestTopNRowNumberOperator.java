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
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.operator.TopNRowNumberOperator.TopNRowNumberOperatorFactory;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

@Test(singleThreaded = true)
public class TestTopNRowNumberOperator
{
    private ExecutorService executor;
    private DriverContext driverContext;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-%s"));
        driverContext = createTaskContext(executor, TEST_SESSION)
                .addPipelineContext(true, true)
                .addDriverContext();
    }

    @AfterMethod
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
    public void testTopNRowNumberPartitioned(boolean hashEnabled)
            throws Exception
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
                10);

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

    @Test
    public void testTopNRowNumberUnPartitioned()
            throws Exception
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
                ImmutableList.<Type>of(),
                Ints.asList(1),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                3,
                false,
                Optional.empty(),
                10);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), DOUBLE, BIGINT, BIGINT)
                .row(0.1, 3L, 1L)
                .row(0.2, 2L, 2L)
                .row(0.3, 1L, 3L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }
}
