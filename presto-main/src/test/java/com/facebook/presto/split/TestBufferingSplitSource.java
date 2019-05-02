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
package com.facebook.presto.split;

import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.split.SplitSource.SplitBatch;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.testng.annotations.Test;

import java.util.concurrent.Future;

import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.facebook.presto.split.MockSplitSource.Action.FAIL;
import static com.facebook.presto.split.MockSplitSource.Action.FINISH;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.airlift.testing.Assertions.assertContains;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestBufferingSplitSource
{
    @Test
    public void testSlowSource()
    {
        MockSplitSource mockSource = new MockSplitSource()
                .setBatchSize(1)
                .increaseAvailableSplits(25)
                .atSplitCompletion(FINISH);
        try (SplitSource source = new BufferingSplitSource(mockSource, 10)) {
            requireFutureValue(getNextBatch(source, 20))
                    .assertSize(10)
                    .assertNoMoreSplits(false);
            requireFutureValue(getNextBatch(source, 6))
                    .assertSize(6)
                    .assertNoMoreSplits(false);
            requireFutureValue(getNextBatch(source, 20))
                    .assertSize(9)
                    .assertNoMoreSplits(true);
            assertTrue(source.isFinished());
            assertEquals(mockSource.getNextBatchInvocationCount(), 25);
        }
    }

    @Test
    public void testFastSource()
    {
        MockSplitSource mockSource = new MockSplitSource()
                .setBatchSize(11)
                .increaseAvailableSplits(22)
                .atSplitCompletion(FINISH);
        try (SplitSource source = new BufferingSplitSource(mockSource, 10)) {
            requireFutureValue(getNextBatch(source, 200))
                    .assertSize(11)
                    .assertNoMoreSplits(false);
            requireFutureValue(getNextBatch(source, 200))
                    .assertSize(11)
                    .assertNoMoreSplits(true);
            assertTrue(source.isFinished());
            assertEquals(mockSource.getNextBatchInvocationCount(), 2);
        }
    }

    @Test
    public void testEmptySource()
    {
        MockSplitSource mockSource = new MockSplitSource()
                .setBatchSize(1)
                .atSplitCompletion(FINISH);
        try (SplitSource source = new BufferingSplitSource(mockSource, 100)) {
            requireFutureValue(getNextBatch(source, 200))
                    .assertSize(0)
                    .assertNoMoreSplits(true);
            assertTrue(source.isFinished());
            assertEquals(mockSource.getNextBatchInvocationCount(), 1);
        }
    }

    @Test
    public void testBlocked()
    {
        MockSplitSource mockSource = new MockSplitSource()
                .setBatchSize(1);
        try (SplitSource source = new BufferingSplitSource(mockSource, 10)) {
            // Source has 0 out of 10 needed.
            ListenableFuture<NextBatchResult> nextBatchFuture = getNextBatch(source, 10);
            assertFalse(nextBatchFuture.isDone());
            mockSource.increaseAvailableSplits(9);
            assertFalse(nextBatchFuture.isDone());
            mockSource.increaseAvailableSplits(1);
            requireFutureValue(nextBatchFuture)
                    .assertSize(10)
                    .assertNoMoreSplits(false);

            // Source is completed after getNextBatch invocation.
            nextBatchFuture = getNextBatch(source, 10);
            assertFalse(nextBatchFuture.isDone());
            mockSource.atSplitCompletion(FINISH);
            requireFutureValue(nextBatchFuture)
                    .assertSize(0)
                    .assertNoMoreSplits(true);
            assertTrue(source.isFinished());
        }

        mockSource = new MockSplitSource()
                .setBatchSize(1);
        try (SplitSource source = new BufferingSplitSource(mockSource, 10)) {
            // Source has 1 out of 10 needed.
            mockSource.increaseAvailableSplits(1);
            ListenableFuture<NextBatchResult> nextBatchFuture = getNextBatch(source, 10);
            assertFalse(nextBatchFuture.isDone());
            mockSource.increaseAvailableSplits(9);
            requireFutureValue(nextBatchFuture)
                    .assertSize(10)
                    .assertNoMoreSplits(false);

            // Source is completed with 5 last splits after getNextBatch invocation.
            nextBatchFuture = getNextBatch(source, 10);
            mockSource.increaseAvailableSplits(5);
            assertFalse(nextBatchFuture.isDone());
            mockSource.atSplitCompletion(FINISH);
            requireFutureValue(nextBatchFuture)
                    .assertSize(5)
                    .assertNoMoreSplits(true);
            assertTrue(source.isFinished());
        }

        mockSource = new MockSplitSource()
                .setBatchSize(1);
        try (SplitSource source = new BufferingSplitSource(mockSource, 10)) {
            // Source has 9 out of 10 needed.
            mockSource.increaseAvailableSplits(9);
            ListenableFuture<NextBatchResult> nextBatchFuture = getNextBatch(source, 10);
            assertFalse(nextBatchFuture.isDone());
            mockSource.increaseAvailableSplits(1);
            requireFutureValue(nextBatchFuture)
                    .assertSize(10)
                    .assertNoMoreSplits(false);

            // Source failed after getNextBatch invocation.
            nextBatchFuture = getNextBatch(source, 10);
            mockSource.increaseAvailableSplits(5);
            assertFalse(nextBatchFuture.isDone());
            mockSource.atSplitCompletion(FAIL);
            assertFutureFailsWithMockFailure(nextBatchFuture);
            assertFalse(source.isFinished());
        }

        // Fast source: source produce 8 before, and 8 after invocation. BufferedSource should return all 16 at once.
        mockSource = new MockSplitSource()
                .setBatchSize(8);
        try (SplitSource source = new BufferingSplitSource(mockSource, 10)) {
            mockSource.increaseAvailableSplits(8);
            ListenableFuture<NextBatchResult> nextBatchFuture = getNextBatch(source, 20);
            assertFalse(nextBatchFuture.isDone());
            mockSource.increaseAvailableSplits(8);
            requireFutureValue(nextBatchFuture)
                    .assertSize(16)
                    .assertNoMoreSplits(false);
        }
    }

    @Test
    public void testFinishedSetWithoutIndicationFromSplitBatch()
    {
        MockSplitSource mockSource = new MockSplitSource()
                .setBatchSize(1)
                .increaseAvailableSplits(1);
        try (SplitSource source = new BufferingSplitSource(mockSource, 100)) {
            requireFutureValue(getNextBatch(source, 1))
                    .assertSize(1)
                    .assertNoMoreSplits(false);
            assertFalse(source.isFinished());
            // Most of the time, mockSource.isFinished() returns the same value as
            // the SplitBatch.noMoreSplits field of the preceding mockSource.getNextBatch() call.
            // However, this is NOT always the case.
            // In this case, the preceding getNextBatch() indicates the noMoreSplits is false,
            // but the next isFinished call will return true.
            mockSource.atSplitCompletion(FINISH);
            requireFutureValue(getNextBatch(source, 1))
                    .assertSize(0)
                    .assertNoMoreSplits(true);
            assertTrue(source.isFinished());
            assertEquals(mockSource.getNextBatchInvocationCount(), 2);
        }
    }

    @Test
    public void testFailImmediate()
    {
        MockSplitSource mockSource = new MockSplitSource()
                .setBatchSize(1)
                .atSplitCompletion(FAIL);
        try (SplitSource source = new BufferingSplitSource(mockSource, 100)) {
            assertFutureFailsWithMockFailure(getNextBatch(source, 200));
            assertEquals(mockSource.getNextBatchInvocationCount(), 1);
        }
    }

    @Test
    public void testFail()
    {
        MockSplitSource mockSource = new MockSplitSource()
                .setBatchSize(1)
                .increaseAvailableSplits(1)
                .atSplitCompletion(FAIL);
        try (SplitSource source = new BufferingSplitSource(mockSource, 100)) {
            assertFutureFailsWithMockFailure(getNextBatch(source, 2));
            assertEquals(mockSource.getNextBatchInvocationCount(), 2);
        }
    }

    private static void assertFutureFailsWithMockFailure(ListenableFuture<?> future)
    {
        assertTrue(future.isDone());
        try {
            future.get();
            fail();
        }
        catch (Exception e) {
            assertContains(e.getMessage(), "Mock failure");
        }
    }

    private static <T> T requireFutureValue(Future<T> future)
    {
        return tryGetFutureValue(future).orElseThrow(AssertionError::new);
    }

    private static ListenableFuture<NextBatchResult> getNextBatch(SplitSource splitSource, int maxSize)
    {
        ListenableFuture<SplitBatch> future = splitSource.getNextBatch(NOT_PARTITIONED, Lifespan.taskWide(), maxSize);
        return Futures.transform(future, NextBatchResult::new, directExecutor());
    }

    @SuppressWarnings("UnusedReturnValue")
    private static class NextBatchResult
    {
        private final SplitBatch splitBatch;

        public NextBatchResult(SplitBatch splitBatch)
        {
            this.splitBatch = requireNonNull(splitBatch, "splits is null");
        }

        public NextBatchResult assertSize(int expectedSize)
        {
            assertEquals(splitBatch.getSplits().size(), expectedSize);
            return this;
        }

        public NextBatchResult assertNoMoreSplits(boolean expectedNoMoreSplits)
        {
            assertEquals(splitBatch.isLastBatch(), expectedNoMoreSplits);
            return this;
        }
    }
}
