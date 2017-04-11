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

import com.facebook.presto.operator.NestedLoopBuildOperator.NestedLoopBuildOperatorFactory;
import com.facebook.presto.spi.Page;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.testing.TestingTaskContext;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.block.BlockAssertions.createLongSequenceBlock;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestNestedLoopBuildOperator
{
    private ExecutorService executor;

    @BeforeClass
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-%s"));
    }

    @AfterClass
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testNestedLoopBuild()
            throws Exception
    {
        TaskContext taskContext = createTaskContext();
        NestedLoopBuildOperatorFactory nestedLoopBuildOperatorFactory = new NestedLoopBuildOperatorFactory(3, new PlanNodeId("test"), ImmutableList.of(BIGINT));
        DriverContext driverContext = taskContext.addPipelineContext(0, true, true).addDriverContext();
        NestedLoopBuildOperator nestedLoopBuildOperator = (NestedLoopBuildOperator) nestedLoopBuildOperatorFactory.createOperator(driverContext);
        NestedLoopJoinPagesSupplier nestedLoopJoinPagesSupplier = nestedLoopBuildOperatorFactory.getNestedLoopJoinPagesSupplier();

        assertFalse(nestedLoopJoinPagesSupplier.getPagesFuture().isDone());

        // build pages
        Page buildPage1 = new Page(3, createLongSequenceBlock(11, 14));
        Page buildPageEmpty = new Page(0);
        Page buildPage2 = new Page(3000, createLongSequenceBlock(4000, 7000));

        nestedLoopBuildOperator.addInput(buildPage1);
        nestedLoopBuildOperator.addInput(buildPageEmpty);
        nestedLoopBuildOperator.addInput(buildPage2);
        nestedLoopBuildOperator.finish();

        assertTrue(nestedLoopJoinPagesSupplier.getPagesFuture().isDone());
        List<Page> buildPages = nestedLoopJoinPagesSupplier.getPagesFuture().get().getPages();

        assertEquals(buildPages.get(0), buildPage1);
        assertEquals(buildPages.get(1), buildPage2);
        assertEquals(buildPages.size(), 2);
    }

    @Test
    public void testNestedLoopBuildNoBlock()
            throws Exception
    {
        TaskContext taskContext = createTaskContext();
        NestedLoopBuildOperatorFactory nestedLoopBuildOperatorFactory = new NestedLoopBuildOperatorFactory(3, new PlanNodeId("test"), ImmutableList.of());
        DriverContext driverContext = taskContext.addPipelineContext(0, true, true).addDriverContext();
        NestedLoopBuildOperator nestedLoopBuildOperator = (NestedLoopBuildOperator) nestedLoopBuildOperatorFactory.createOperator(driverContext);
        NestedLoopJoinPagesSupplier nestedLoopJoinPagesSupplier = nestedLoopBuildOperatorFactory.getNestedLoopJoinPagesSupplier();

        assertFalse(nestedLoopJoinPagesSupplier.getPagesFuture().isDone());

        // build pages
        Page buildPage1 = new Page(3);
        Page buildPageEmpty = new Page(0);
        Page buildPage2 = new Page(3000);

        nestedLoopBuildOperator.addInput(buildPage1);
        nestedLoopBuildOperator.addInput(buildPageEmpty);
        nestedLoopBuildOperator.addInput(buildPage2);
        nestedLoopBuildOperator.finish();

        assertTrue(nestedLoopJoinPagesSupplier.getPagesFuture().isDone());
        List<Page> buildPages = nestedLoopJoinPagesSupplier.getPagesFuture().get().getPages();

        assertEquals(buildPages.get(0), buildPage1);
        assertEquals(buildPages.get(1), buildPage2);
        assertEquals(buildPages.size(), 2);
    }

    private TaskContext createTaskContext()
    {
        return TestingTaskContext.createTaskContext(executor, TEST_SESSION);
    }
}
