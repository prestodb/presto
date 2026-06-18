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
package com.facebook.presto.spark.execution;

import com.facebook.presto.Session;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.operator.UpdateMemory;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.testing.TestingSession;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

@Test(singleThreaded = true)
public class TestPrestoSparkRemoteSourceOperator
{
    private Executor executor;
    private ScheduledExecutorService scheduledExecutor;
    private TaskContext taskContext;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
        Session session = TestingSession.testSessionBuilder().build();
        taskContext = createTaskContext(executor, scheduledExecutor, session);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        if (taskContext != null) {
            taskContext.failed(new Exception("Cleaning up"));
        }
        if (executor != null) {
            ((java.util.concurrent.ExecutorService) executor).shutdownNow();
        }
        if (scheduledExecutor != null) {
            scheduledExecutor.shutdownNow();
        }
    }

    @Test
    public void testRecordsShuffleReadStatistics()
    {
        // Create a test page with known size and position count
        Page testPage1 = createTestPage(100, 10); // 10 rows
        Page testPage2 = createTestPage(200, 20); // 20 rows

        // Create a mock page input that provides test pages
        TestPrestoSparkPageInput pageInput = new TestPrestoSparkPageInput(testPage1, testPage2);

        // Create operator context
        DriverContext driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
        OperatorContext operatorContext = driverContext.addOperatorContext(
                0,
                new PlanNodeId("test"),
                PrestoSparkRemoteSourceOperator.class.getSimpleName());

        // Create the operator
        PrestoSparkRemoteSourceOperator operator = new PrestoSparkRemoteSourceOperator(
                new PlanNodeId("test"),
                operatorContext,
                pageInput,
                true);

        // Initially, no raw input should be recorded
        OperatorStats initialStats = operatorContext.getOperatorStats();
        assertEquals(initialStats.getRawInputPositions(), 0);
        assertEquals(initialStats.getRawInputDataSizeInBytes(), 0);

        // Get first page
        Page page1 = operator.getOutput();
        assertNotNull(page1);
        assertEquals(page1.getPositionCount(), 10);

        // Verify that raw input statistics are recorded for first page
        OperatorStats statsAfterPage1 = operatorContext.getOperatorStats();
        assertEquals(statsAfterPage1.getRawInputPositions(), 10);
        assertEquals(statsAfterPage1.getRawInputDataSizeInBytes(), testPage1.getSizeInBytes());

        // Get second page
        Page page2 = operator.getOutput();
        assertNotNull(page2);
        assertEquals(page2.getPositionCount(), 20);

        // Verify that raw input statistics are accumulated for both pages
        OperatorStats statsAfterPage2 = operatorContext.getOperatorStats();
        assertEquals(statsAfterPage2.getRawInputPositions(), 30); // 10 + 20
        assertEquals(statsAfterPage2.getRawInputDataSizeInBytes(), testPage1.getSizeInBytes() + testPage2.getSizeInBytes());

        // Get third page (should be null - no more pages)
        Page page3 = operator.getOutput();
        assertNull(page3);

        // Statistics should remain the same after getting null
        OperatorStats finalStats = operatorContext.getOperatorStats();
        assertEquals(finalStats.getRawInputPositions(), 30);
        assertEquals(finalStats.getRawInputDataSizeInBytes(), testPage1.getSizeInBytes() + testPage2.getSizeInBytes());

        operator.close();
    }

    @Test
    public void testNoStatisticsWhenNoPages()
    {
        // Create a page input with no pages
        TestPrestoSparkPageInput pageInput = new TestPrestoSparkPageInput();

        // Create operator context
        DriverContext driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
        OperatorContext operatorContext = driverContext.addOperatorContext(
                0,
                new PlanNodeId("test"),
                PrestoSparkRemoteSourceOperator.class.getSimpleName());

        // Create the operator
        PrestoSparkRemoteSourceOperator operator = new PrestoSparkRemoteSourceOperator(
                new PlanNodeId("test"),
                operatorContext,
                pageInput,
                true);

        // Get page (should be null immediately)
        Page page = operator.getOutput();
        assertNull(page);

        // No statistics should be recorded
        OperatorStats stats = operatorContext.getOperatorStats();
        assertEquals(stats.getRawInputPositions(), 0);
        assertEquals(stats.getRawInputDataSizeInBytes(), 0);

        operator.close();
    }

    /**
     * Helper method to create a test page with specified size and position count
     */
    private Page createTestPage(long targetSize, int positionCount)
    {
        BlockBuilder blockBuilder = BigintType.BIGINT.createBlockBuilder(null, positionCount);
        for (int i = 0; i < positionCount; i++) {
            BigintType.BIGINT.writeLong(blockBuilder, i);
        }
        return new Page(blockBuilder.build());
    }

    /**
     * Test implementation of PrestoSparkPageInput for testing purposes
     */
    private static class TestPrestoSparkPageInput
            implements PrestoSparkPageInput
    {
        private final Page[] pages;
        private int currentIndex;

        public TestPrestoSparkPageInput(Page... pages)
        {
            this.pages = pages;
            this.currentIndex = 0;
        }

        @Override
        public Page getNextPage(UpdateMemory updateMemory)
        {
            if (currentIndex >= pages.length) {
                return null;
            }
            return pages[currentIndex++];
        }
    }
}
