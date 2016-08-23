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
import com.facebook.presto.operator.GroupIdOperator.GroupIdOperatorFactory;
import com.facebook.presto.spi.Page;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEqualsIgnoreOrder;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

@Test(singleThreaded = true)
public class TestGroupIdOperator
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

    public void testGroupId()
            throws Exception
    {
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(false, ImmutableList.of(), BIGINT, VARCHAR, BOOLEAN, BIGINT);
        List<Page> input = rowPagesBuilder
                .addSequencePage(3, 100, 400, 0, 1000)
                .addSequencePage(3, 200, 500, 0, 1100)
                .build();

        GroupIdOperatorFactory operatorFactory =
                new GroupIdOperatorFactory(0,
                        new PlanNodeId("test"),
                        ImmutableList.of(VARCHAR, BOOLEAN, BIGINT, BIGINT, BIGINT),
                        ImmutableList.of(ImmutableList.of(1, 2), ImmutableList.of(3)),
                        ImmutableList.of(1, 2, 3),
                        ImmutableList.of(0));

        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, BOOLEAN, BIGINT, BIGINT, BIGINT)
                .row("400", true, null, 100L, 0L)
                .row("401", false, null, 101L, 0L)
                .row("402", true, null, 102L, 0L)
                .row("500", true, null, 200L, 0L)
                .row("501", false, null, 201L, 0L)
                .row("502", true, null, 202L, 0L)
                .row(null, null, 1000L, 100L, 1L)
                .row(null, null, 1001L, 101L, 1L)
                .row(null, null, 1002L, 102L, 1L)
                .row(null, null, 1100L, 200L, 1L)
                .row(null, null, 1101L, 201L, 1L)
                .row(null, null, 1102L, 202L, 1L)
                .build();

        assertOperatorEqualsIgnoreOrder(operatorFactory, driverContext, input, expected);
    }
}
