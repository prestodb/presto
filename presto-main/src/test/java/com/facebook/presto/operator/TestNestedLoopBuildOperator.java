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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.operator.NestedLoopBuildOperator.NestedLoopBuildOperatorFactory;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.testing.TestingTaskContext;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.block.BlockAssertions.createLongSequenceBlock;
import static com.facebook.presto.common.type.BigintType.BIGINT;
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
                NestedLoopJoinPagesSupplier::new,
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
                NestedLoopJoinPagesSupplier::new,
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

        assertEquals(buildPages.size(), 1);
        assertEquals(buildPages.get(0).getPositionCount(), 3003);
    }

    @Test
    public void testNestedLoopNoBlocksMaxSizeLimit()
            throws Exception
    {
        TaskContext taskContext = createTaskContext();
        List<Type> buildTypes = ImmutableList.of();
        JoinBridgeManager<NestedLoopJoinBridge> nestedLoopJoinBridgeManager = new JoinBridgeManager<>(
                false,
                PipelineExecutionStrategy.UNGROUPED_EXECUTION,
                PipelineExecutionStrategy.UNGROUPED_EXECUTION,
                NestedLoopJoinPagesSupplier::new,
                buildTypes);
        NestedLoopBuildOperatorFactory nestedLoopBuildOperatorFactory = new NestedLoopBuildOperatorFactory(3, new PlanNodeId("test"), nestedLoopJoinBridgeManager);
        DriverContext driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
        NestedLoopBuildOperator nestedLoopBuildOperator = (NestedLoopBuildOperator) nestedLoopBuildOperatorFactory.createOperator(driverContext);
        NestedLoopJoinBridge nestedLoopJoinBridge = nestedLoopJoinBridgeManager.getJoinBridge(Lifespan.taskWide());

        assertFalse(nestedLoopJoinBridge.getPagesFuture().isDone());

        // build pages
        Page massivePage = new Page(PageProcessor.MAX_BATCH_SIZE + 100);

        nestedLoopBuildOperator.addInput(massivePage);
        nestedLoopBuildOperator.finish();

        assertTrue(nestedLoopJoinBridge.getPagesFuture().isDone());
        List<Page> buildPages = nestedLoopJoinBridge.getPagesFuture().get().getPages();
        assertEquals(buildPages.size(), 2);
        assertEquals(buildPages.get(0).getPositionCount(), PageProcessor.MAX_BATCH_SIZE);
        assertEquals(buildPages.get(1).getPositionCount(), 100);
    }

    private TaskContext createTaskContext()
    {
        return TestingTaskContext.createTaskContext(executor, scheduledExecutor, TEST_SESSION);
    }
}
