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
import com.facebook.presto.operator.MarkDistinctOperator.MarkDistinctOperatorFactory;
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
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

@Test(singleThreaded = true)
public class TestMarkDistinctOperator
{
    private ExecutorService executor;
    private DriverContext driverContext;
    private JoinCompiler joinCompiler = new JoinCompiler();

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-%s"));
        driverContext = createTaskContext(executor, TEST_SESSION)
                .addPipelineContext(0, true, true)
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
    public void testMarkDistinct(boolean hashEnabled)
            throws Exception
    {
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, Ints.asList(0), BIGINT);
        List<Page> input = rowPagesBuilder
                .addSequencePage(100, 0)
                .addSequencePage(100, 0)
                .build();

        OperatorFactory operatorFactory = new MarkDistinctOperatorFactory(0, new PlanNodeId("test"), rowPagesBuilder.getTypes(), ImmutableList.of(0), rowPagesBuilder.getHashChannel(), joinCompiler);

        MaterializedResult.Builder expected = resultBuilder(driverContext.getSession(), BIGINT, BOOLEAN);
        for (long i = 0; i < 100; i++) {
            expected.row(i, true);
            expected.row(i, false);
        }

        OperatorAssertion.assertOperatorEqualsIgnoreOrder(operatorFactory, driverContext, input, expected.build(), hashEnabled, Optional.of(1));
    }
}
