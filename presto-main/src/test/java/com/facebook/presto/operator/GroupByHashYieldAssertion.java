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

import com.facebook.airlift.stats.TestingGcMonitor;
import com.facebook.presto.RowPagesBuilder;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.memory.MemoryPool;
import com.facebook.presto.memory.QueryContext;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spiller.SpillSpaceTracker;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.facebook.airlift.testing.Assertions.assertBetweenInclusive;
import static com.facebook.airlift.testing.Assertions.assertGreaterThan;
import static com.facebook.airlift.testing.Assertions.assertLessThan;
import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.operator.OperatorAssertion.finishOperator;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public final class GroupByHashYieldAssertion
{
    private static final ExecutorService EXECUTOR = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
    private static final ScheduledExecutorService SCHEDULED_EXECUTOR = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));

    private GroupByHashYieldAssertion() {}

    public static List<Page> createPagesWithDistinctHashKeys(Type type, int pageCount, int positionCountPerPage)
    {
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(true, ImmutableList.of(0), type);
        for (int i = 0; i < pageCount; i++) {
            rowPagesBuilder.addSequencePage(positionCountPerPage, positionCountPerPage * i);
        }
        return rowPagesBuilder.build();
    }

    /**
     * @param operatorFactory creates an Operator that should directly or indirectly contain GroupByHash
     * @param getHashCapacity returns the hash table capacity for the input operator
     * @param additionalMemoryInBytes the memory used in addition to the GroupByHash in the operator (e.g., aggregator)
     */
    public static GroupByHashYieldResult finishOperatorWithYieldingGroupByHash(List<Page> input, Type hashKeyType, OperatorFactory operatorFactory, Function<Operator, Integer> getHashCapacity, long additionalMemoryInBytes)
    {
        assertLessThan(additionalMemoryInBytes, 1L << 21, "additionalMemoryInBytes should be a relatively small number");
        List<Page> result = new LinkedList<>();

        // mock an adjustable memory pool
        QueryId queryId1 = new QueryId("test_query1");
        QueryId queryId2 = new QueryId("test_query2");
        MemoryPool memoryPool = new MemoryPool(new MemoryPoolId("test"), new DataSize(1, GIGABYTE));
        QueryContext queryContext = new QueryContext(
                queryId2,
                new DataSize(512, MEGABYTE),
                new DataSize(1024, MEGABYTE),
                new DataSize(512, MEGABYTE),
                new DataSize(1, GIGABYTE),
                memoryPool,
                new TestingGcMonitor(),
                EXECUTOR,
                SCHEDULED_EXECUTOR,
                new DataSize(512, MEGABYTE),
                new SpillSpaceTracker(new DataSize(512, MEGABYTE)),
                listJsonCodec(TaskMemoryReservationSummary.class));

        DriverContext driverContext = createTaskContext(queryContext, EXECUTOR, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
        Operator operator = operatorFactory.createOperator(driverContext);

        // run operator
        int yieldCount = 0;
        long expectedReservedExtraBytes = 0;
        for (Page page : input) {
            // unblocked
            assertTrue(operator.needsInput());

            // saturate the pool with a tiny memory left
            long reservedMemoryInBytes = memoryPool.getFreeBytes() - additionalMemoryInBytes;
            memoryPool.reserve(queryId1, "test", reservedMemoryInBytes);

            long oldMemoryUsage = operator.getOperatorContext().getDriverContext().getMemoryUsage();
            int oldCapacity = getHashCapacity.apply(operator);

            // add a page and verify different behaviors
            operator.addInput(page);

            // get output to consume the input
            Page output = operator.getOutput();
            if (output != null) {
                result.add(output);
            }

            long newMemoryUsage = operator.getOperatorContext().getDriverContext().getMemoryUsage();

            // Skip if the memory usage is not large enough since we cannot distinguish
            // between rehash and memory used by aggregator
            if (newMemoryUsage < new DataSize(4, MEGABYTE).toBytes()) {
                // free the pool for the next iteration
                memoryPool.free(queryId1, "test", reservedMemoryInBytes);
                // this required in case input is blocked
                operator.getOutput();
                continue;
            }

            long actualIncreasedMemory = newMemoryUsage - oldMemoryUsage;

            if (operator.needsInput()) {
                // We have successfully added a page

                // Assert we are not blocked
                assertTrue(operator.getOperatorContext().isWaitingForMemory().isDone());

                // assert the hash capacity is not changed; otherwise, we should have yielded
                assertTrue(oldCapacity == getHashCapacity.apply(operator));

                // We are not going to rehash; therefore, assert the memory increase only comes from the aggregator
                assertLessThan(actualIncreasedMemory, additionalMemoryInBytes);

                // free the pool for the next iteration
                memoryPool.free(queryId1, "test", reservedMemoryInBytes);
            }
            else {
                // We failed to finish the page processing i.e. we yielded
                yieldCount++;

                // Assert we are blocked
                assertFalse(operator.getOperatorContext().isWaitingForMemory().isDone());

                // Hash table capacity should not change
                assertEquals(oldCapacity, (long) getHashCapacity.apply(operator));

                // Increased memory is no smaller than the hash table size and no greater than the hash table size + the memory used by aggregator
                if (hashKeyType == BIGINT) {
                    // groupIds and values double by hashCapacity; while valuesByGroupId double by maxFill = hashCapacity / 0.75
                    expectedReservedExtraBytes = oldCapacity * (long) (Long.BYTES * 1.75 + Integer.BYTES) + page.getRetainedSizeInBytes();
                }
                else {
                    // groupAddressByHash, groupIdsByHash, and rawHashByHashPosition double by hashCapacity; while groupAddressByGroupId double by maxFill = hashCapacity / 0.75
                    expectedReservedExtraBytes = oldCapacity * (long) (Long.BYTES * 1.75 + Integer.BYTES + Byte.BYTES) + page.getRetainedSizeInBytes();
                }
                assertBetweenInclusive(actualIncreasedMemory, expectedReservedExtraBytes, expectedReservedExtraBytes + additionalMemoryInBytes);

                // Output should be blocked as well
                assertNull(operator.getOutput());

                // Free the pool to unblock
                memoryPool.free(queryId1, "test", reservedMemoryInBytes);

                // Trigger a process through getOutput() or needsInput()
                output = operator.getOutput();
                if (output != null) {
                    result.add(output);
                }
                assertTrue(operator.needsInput());

                // Hash table capacity has increased
                assertGreaterThan(getHashCapacity.apply(operator), oldCapacity);

                // Assert the estimated reserved memory before rehash is very close to the one after rehash
                long rehashedMemoryUsage = operator.getOperatorContext().getDriverContext().getMemoryUsage();
                assertBetweenInclusive(rehashedMemoryUsage * 1.0 / newMemoryUsage, 0.99, 1.01);

                // unblocked
                assertTrue(operator.needsInput());
            }
        }

        result.addAll(finishOperator(operator));
        return new GroupByHashYieldResult(yieldCount, expectedReservedExtraBytes, result);
    }

    public static final class GroupByHashYieldResult
    {
        private final int yieldCount;
        private final long maxReservedBytes;
        private final List<Page> output;

        public GroupByHashYieldResult(int yieldCount, long maxReservedBytes, List<Page> output)
        {
            this.yieldCount = yieldCount;
            this.maxReservedBytes = maxReservedBytes;
            this.output = requireNonNull(output, "output is null");
        }

        public int getYieldCount()
        {
            return yieldCount;
        }

        public long getMaxReservedBytes()
        {
            return maxReservedBytes;
        }

        public List<Page> getOutput()
        {
            return output;
        }
    }
}
