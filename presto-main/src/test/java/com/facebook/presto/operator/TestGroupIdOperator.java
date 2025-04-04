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
import com.facebook.presto.operator.GroupIdOperator.GroupIdOperatorFactory;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEqualsIgnoreOrder;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

@Test(singleThreaded = true)
public class TestGroupIdOperator
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

    public void testGroupId()
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
                        ImmutableList.of(ImmutableMap.of(0, 1, 1, 2, 3, 0), ImmutableMap.of(2, 3, 3, 0)));

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
