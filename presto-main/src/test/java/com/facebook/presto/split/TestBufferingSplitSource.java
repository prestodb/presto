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

import com.facebook.presto.metadata.Split;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.split.MockSplitSource.Action.FAIL;
import static com.facebook.presto.split.MockSplitSource.Action.FINISH;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.airlift.testing.Assertions.assertContains;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@SuppressWarnings("ConstantConditions") // Optional.get without isPresent call
public class TestBufferingSplitSource
{
    @DataProvider
    public static Object[][] nextBatchAsserters()
    {
        return new Object[][]{{new SingleArgumentSplitSourceAsserter()}};
    }

    @Test(dataProvider = "nextBatchAsserters")
    public void testSlowSource(NextBatchAsserter nextBatchAsserter)
            throws Exception
    {
        MockSplitSource mockSource = new MockSplitSource()
                .setBatchSize(1)
                .increaseAvailableSplits(25)
                .atSplitCompletion(FINISH);
        try (SplitSource source = new BufferingSplitSource(mockSource, 10)) {
            tryGetFutureValue(nextBatchAsserter.invokeNextBatch(source, 20)).get()
                    .assertSize(10)
                    .assertNoMoreSplits(false);
            tryGetFutureValue(nextBatchAsserter.invokeNextBatch(source, 6)).get()
                    .assertSize(6)
                    .assertNoMoreSplits(false);
            tryGetFutureValue(nextBatchAsserter.invokeNextBatch(source, 20)).get()
                    .assertSize(9)
                    .assertNoMoreSplits(true);
            assertTrue(source.isFinished());
            assertEquals(mockSource.getNextBatchInvocationCount(), 25);
        }
    }

    @Test(dataProvider = "nextBatchAsserters")
    public void testFastSource(NextBatchAsserter nextBatchAsserter)
            throws Exception
    {
        MockSplitSource mockSource = new MockSplitSource()
                .setBatchSize(11)
                .increaseAvailableSplits(22)
                .atSplitCompletion(FINISH);
        try (SplitSource source = new BufferingSplitSource(mockSource, 10)) {
            tryGetFutureValue(nextBatchAsserter.invokeNextBatch(source, 200)).get()
                    .assertSize(11)
                    .assertNoMoreSplits(false);
            tryGetFutureValue(nextBatchAsserter.invokeNextBatch(source, 200)).get()
                    .assertSize(11)
                    .assertNoMoreSplits(true);
            assertTrue(source.isFinished());
            assertEquals(mockSource.getNextBatchInvocationCount(), 2);
        }
    }

    @Test(dataProvider = "nextBatchAsserters")
    public void testEmptySource(NextBatchAsserter nextBatchAsserter)
            throws Exception
    {
        MockSplitSource mockSource = new MockSplitSource()
                .setBatchSize(1)
                .atSplitCompletion(FINISH);
        try (SplitSource source = new BufferingSplitSource(mockSource, 100)) {
            tryGetFutureValue(nextBatchAsserter.invokeNextBatch(source, 200)).get()
                    .assertSize(0)
                    .assertNoMoreSplits(true);
            assertTrue(source.isFinished());
            assertEquals(mockSource.getNextBatchInvocationCount(), 0);
        }
    }

    @Test(dataProvider = "nextBatchAsserters")
    public void testBlocked(NextBatchAsserter nextBatchAsserter)
            throws Exception
    {
        MockSplitSource mockSource = new MockSplitSource()
                .setBatchSize(1);
        try (SplitSource source = new BufferingSplitSource(mockSource, 10)) {
            // Source has 0 out of 10 needed.
            ListenableFuture<NextBatchResult> nextBatchFuture = nextBatchAsserter.invokeNextBatch(source, 10);
            assertFalse(nextBatchFuture.isDone());
            mockSource.increaseAvailableSplits(9);
            assertFalse(nextBatchFuture.isDone());
            mockSource.increaseAvailableSplits(1);
            tryGetFutureValue(nextBatchFuture).get()
                    .assertSize(10)
                    .assertNoMoreSplits(false);

            // Source is completed after getNextBatch invocation.
            nextBatchFuture = nextBatchAsserter.invokeNextBatch(source, 10);
            assertFalse(nextBatchFuture.isDone());
            mockSource.atSplitCompletion(FINISH);
            tryGetFutureValue(nextBatchFuture).get()
                    .assertSize(0)
                    .assertNoMoreSplits(true);
            assertTrue(source.isFinished());
        }

        mockSource = new MockSplitSource()
                .setBatchSize(1);
        try (SplitSource source = new BufferingSplitSource(mockSource, 10)) {
            // Source has 1 out of 10 needed.
            mockSource.increaseAvailableSplits(1);
            ListenableFuture<NextBatchResult> nextBatchFuture = nextBatchAsserter.invokeNextBatch(source, 10);
            assertFalse(nextBatchFuture.isDone());
            mockSource.increaseAvailableSplits(9);
            tryGetFutureValue(nextBatchFuture).get()
                    .assertSize(10)
                    .assertNoMoreSplits(false);

            // Source is completed with 5 last splits after getNextBatch invocation.
            nextBatchFuture = nextBatchAsserter.invokeNextBatch(source, 10);
            mockSource.increaseAvailableSplits(5);
            assertFalse(nextBatchFuture.isDone());
            mockSource.atSplitCompletion(FINISH);
            tryGetFutureValue(nextBatchFuture).get()
                    .assertSize(5)
                    .assertNoMoreSplits(true);
            assertTrue(source.isFinished());
        }

        mockSource = new MockSplitSource()
                .setBatchSize(1);
        try (SplitSource source = new BufferingSplitSource(mockSource, 10)) {
            // Source has 9 out of 10 needed.
            mockSource.increaseAvailableSplits(9);
            ListenableFuture<NextBatchResult> nextBatchFuture = nextBatchAsserter.invokeNextBatch(source, 10);
            assertFalse(nextBatchFuture.isDone());
            mockSource.increaseAvailableSplits(1);
            tryGetFutureValue(nextBatchFuture).get()
                    .assertSize(10)
                    .assertNoMoreSplits(false);

            // Source failed after getNextBatch invocation.
            nextBatchFuture = nextBatchAsserter.invokeNextBatch(source, 10);
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
            ListenableFuture<NextBatchResult> nextBatchFuture = nextBatchAsserter.invokeNextBatch(source, 20);
            assertFalse(nextBatchFuture.isDone());
            mockSource.increaseAvailableSplits(8);
            tryGetFutureValue(nextBatchFuture).get()
                    .assertSize(16)
                    .assertNoMoreSplits(false);
        }
    }

    @Test(dataProvider = "nextBatchAsserters")
    public void testFailImmediate(NextBatchAsserter nextBatchAsserter)
            throws Exception
    {
        MockSplitSource mockSource = new MockSplitSource()
                .setBatchSize(1)
                .atSplitCompletion(FAIL);
        try (SplitSource source = new BufferingSplitSource(mockSource, 100)) {
            assertFutureFailsWithMockFailure(nextBatchAsserter.invokeNextBatch(source, 200));
            assertEquals(mockSource.getNextBatchInvocationCount(), 1);
        }
    }

    @Test(dataProvider = "nextBatchAsserters")
    public void testFail(NextBatchAsserter nextBatchAsserter)
            throws Exception
    {
        MockSplitSource mockSource = new MockSplitSource()
                .setBatchSize(1)
                .increaseAvailableSplits(1)
                .atSplitCompletion(FAIL);
        try (SplitSource source = new BufferingSplitSource(mockSource, 100)) {
            assertFutureFailsWithMockFailure(nextBatchAsserter.invokeNextBatch(source, 2));
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

    private interface NextBatchAsserter
    {
        ListenableFuture<NextBatchResult> invokeNextBatch(SplitSource splitSource, int maxSize);
    }

    private interface NextBatchResult
    {
        NextBatchResult assertSize(int expectedSize);

        NextBatchResult assertNoMoreSplits(boolean expectedNoMoreSplits);
    }

    private static class SingleArgumentSplitSourceAsserter
            implements NextBatchAsserter
    {
        @Override
        public ListenableFuture<NextBatchResult> invokeNextBatch(SplitSource splitSource, int maxSize)
        {
            return Futures.transform(splitSource.getNextBatch(maxSize), SingleArgumentNextBatchResult::new);
        }
    }

    private static class SingleArgumentNextBatchResult
            implements NextBatchResult
    {
        private final List<Split> splits;

        public SingleArgumentNextBatchResult(List<Split> splits)
        {
            this.splits = requireNonNull(splits, "splits is null");
        }

        @Override
        public NextBatchResult assertSize(int expectedSize)
        {
            assertEquals(splits.size(), expectedSize);
            return this;
        }

        @Override
        public NextBatchResult assertNoMoreSplits(boolean expectedNoMoreSplits)
        {
            // do nothing
            return this;
        }
    }
}
