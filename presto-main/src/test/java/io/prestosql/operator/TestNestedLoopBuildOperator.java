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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import io.prestosql.execution.Lifespan;
import io.prestosql.operator.NestedLoopBuildOperator.NestedLoopBuildOperatorFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.testing.TestingTaskContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.block.BlockAssertions.createLongSequenceBlock;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestNestedLoopBuildOperator
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;

    @BeforeClass
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testNestedLoopBuild()
            throws Exception
    {
        TaskContext taskContext = createTaskContext();
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        JoinBridgeManager<NestedLoopJoinBridge> nestedLoopJoinBridgeManager = new JoinBridgeManager<>(
                false,
                PipelineExecutionStrategy.UNGROUPED_EXECUTION,
                PipelineExecutionStrategy.UNGROUPED_EXECUTION,
                lifespan -> new NestedLoopJoinPagesSupplier(),
                buildTypes);
        NestedLoopBuildOperatorFactory nestedLoopBuildOperatorFactory = new NestedLoopBuildOperatorFactory(3, new PlanNodeId("test"), nestedLoopJoinBridgeManager);
        DriverContext driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
        NestedLoopBuildOperator nestedLoopBuildOperator = (NestedLoopBuildOperator) nestedLoopBuildOperatorFactory.createOperator(driverContext);
        NestedLoopJoinBridge nestedLoopJoinBridge = nestedLoopJoinBridgeManager.getJoinBridge(Lifespan.taskWide());

        assertFalse(nestedLoopJoinBridge.getPagesFuture().isDone());

        // build pages
        Page buildPage1 = new Page(3, createLongSequenceBlock(11, 14));
        Page buildPageEmpty = new Page(0);
        Page buildPage2 = new Page(3000, createLongSequenceBlock(4000, 7000));

        nestedLoopBuildOperator.addInput(buildPage1);
        nestedLoopBuildOperator.addInput(buildPageEmpty);
        nestedLoopBuildOperator.addInput(buildPage2);
        nestedLoopBuildOperator.finish();

        assertTrue(nestedLoopJoinBridge.getPagesFuture().isDone());
        List<Page> buildPages = nestedLoopJoinBridge.getPagesFuture().get().getPages();

        assertEquals(buildPages.get(0), buildPage1);
        assertEquals(buildPages.get(1), buildPage2);
        assertEquals(buildPages.size(), 2);
    }

    @Test
    public void testNestedLoopBuildNoBlock()
            throws Exception
    {
        TaskContext taskContext = createTaskContext();
        List<Type> buildTypes = ImmutableList.of();
        JoinBridgeManager<NestedLoopJoinBridge> nestedLoopJoinBridgeManager = new JoinBridgeManager<>(
                false,
                PipelineExecutionStrategy.UNGROUPED_EXECUTION,
                PipelineExecutionStrategy.UNGROUPED_EXECUTION,
                lifespan -> new NestedLoopJoinPagesSupplier(),
                buildTypes);
        NestedLoopBuildOperatorFactory nestedLoopBuildOperatorFactory = new NestedLoopBuildOperatorFactory(3, new PlanNodeId("test"), nestedLoopJoinBridgeManager);
        DriverContext driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
        NestedLoopBuildOperator nestedLoopBuildOperator = (NestedLoopBuildOperator) nestedLoopBuildOperatorFactory.createOperator(driverContext);
        NestedLoopJoinBridge nestedLoopJoinBridge = nestedLoopJoinBridgeManager.getJoinBridge(Lifespan.taskWide());

        assertFalse(nestedLoopJoinBridge.getPagesFuture().isDone());

        // build pages
        Page buildPage1 = new Page(3);
        Page buildPageEmpty = new Page(0);
        Page buildPage2 = new Page(3000);

        nestedLoopBuildOperator.addInput(buildPage1);
        nestedLoopBuildOperator.addInput(buildPageEmpty);
        nestedLoopBuildOperator.addInput(buildPage2);
        nestedLoopBuildOperator.finish();

        assertTrue(nestedLoopJoinBridge.getPagesFuture().isDone());
        List<Page> buildPages = nestedLoopJoinBridge.getPagesFuture().get().getPages();

        assertEquals(buildPages.get(0), buildPage1);
        assertEquals(buildPages.get(1), buildPage2);
        assertEquals(buildPages.size(), 2);
    }

    private TaskContext createTaskContext()
    {
        return TestingTaskContext.createTaskContext(executor, scheduledExecutor, TEST_SESSION);
    }
}
