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
import com.facebook.presto.sql.gen.JoinCompiler;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

@Test(singleThreaded = true)
public class TestDistinctLimitOperator
{
    private ExecutorService executor;
    private DriverContext driverContext;
    private JoinCompiler joinCompiler;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-%s"));
        driverContext = createTaskContext(executor, TEST_SESSION)
                .addPipelineContext(0, true, true)
                .addDriverContext();
        joinCompiler = new JoinCompiler();
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
    public void testDistinctLimit(boolean hashEnabled)
            throws Exception
    {
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, Ints.asList(0), BIGINT);
        List<Page> input = rowPagesBuilder
                .addSequencePage(3, 1)
                .addSequencePage(5, 2)
                .build();

        OperatorFactory operatorFactory = new DistinctLimitOperator.DistinctLimitOperatorFactory(0, new PlanNodeId("test"), ImmutableList.of(BIGINT), Ints.asList(0), 5, rowPagesBuilder.getHashChannel(), joinCompiler);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT)
                .row(1L)
                .row(2L)
                .row(3L)
                .row(4L)
                .row(5L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testDistinctLimitWithPageAlignment(boolean hashEnabled)
            throws Exception
    {
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, Ints.asList(0), BIGINT);
        List<Page> input = rowPagesBuilder
                .addSequencePage(3, 1)
                .addSequencePage(3, 2)
                .build();

        OperatorFactory operatorFactory = new DistinctLimitOperator.DistinctLimitOperatorFactory(0, new PlanNodeId("test"), ImmutableList.of(BIGINT), Ints.asList(0), 3, rowPagesBuilder.getHashChannel(), joinCompiler);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT)
                .row(1L)
                .row(2L)
                .row(3L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testDistinctLimitValuesLessThanLimit(boolean hashEnabled)
            throws Exception
    {
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, Ints.asList(0), BIGINT);
        List<Page> input = rowPagesBuilder
                .addSequencePage(3, 1)
                .addSequencePage(3, 2)
                .build();

        OperatorFactory operatorFactory = new DistinctLimitOperator.DistinctLimitOperatorFactory(0, new PlanNodeId("test"), ImmutableList.of(BIGINT), Ints.asList(0), 5, rowPagesBuilder.getHashChannel(), joinCompiler);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT)
                .row(1L)
                .row(2L)
                .row(3L)
                .row(4L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }
}
