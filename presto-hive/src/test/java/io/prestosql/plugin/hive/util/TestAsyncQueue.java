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

package io.prestosql.plugin.hive.util;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.Threads;
import io.prestosql.plugin.hive.util.AsyncQueue.BorrowResult;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.testing.Assertions.assertContains;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestAsyncQueue
{
    private ExecutorService executor;

    @BeforeClass
    public void setUpClass()
    {
        executor = Executors.newFixedThreadPool(8, Threads.daemonThreadsNamed("test-async-queue-%s"));
    }

    @AfterClass(alwaysRun = true)
    public void tearDownClass()
    {
        executor.shutdownNow();
    }

    @Test(timeOut = 10_000)
    public void testGetPartial()
            throws Exception
    {
        AsyncQueue<String> queue = new AsyncQueue<>(4, executor);

        queue.offer("1");
        queue.offer("2");
        queue.offer("3");
        assertEquals(queue.getBatchAsync(100).get(), ImmutableList.of("1", "2", "3"));

        queue.finish();
        assertTrue(queue.isFinished());
    }

    @Test(timeOut = 10_000)
    public void testFullQueue()
            throws Exception
    {
        AsyncQueue<String> queue = new AsyncQueue<>(4, executor);

        assertTrue(queue.offer("1").isDone());
        assertTrue(queue.offer("2").isDone());
        assertTrue(queue.offer("3").isDone());

        assertFalse(queue.offer("4").isDone());
        assertFalse(queue.offer("5").isDone());
        ListenableFuture<?> offerFuture = queue.offer("6");
        assertFalse(offerFuture.isDone());

        assertEquals(queue.getBatchAsync(2).get(), ImmutableList.of("1", "2"));
        assertFalse(offerFuture.isDone());

        assertEquals(queue.getBatchAsync(1).get(), ImmutableList.of("3"));
        offerFuture.get();

        offerFuture = queue.offer("7");
        assertFalse(offerFuture.isDone());

        queue.finish();
        offerFuture.get();
        assertFalse(queue.isFinished());
        assertEquals(queue.getBatchAsync(4).get(), ImmutableList.of("4", "5", "6", "7"));
        assertTrue(queue.isFinished());
    }

    @Test(timeOut = 10_000)
    public void testEmptyQueue()
            throws Exception
    {
        AsyncQueue<String> queue = new AsyncQueue<>(4, executor);

        assertTrue(queue.offer("1").isDone());
        assertTrue(queue.offer("2").isDone());
        assertTrue(queue.offer("3").isDone());
        assertEquals(queue.getBatchAsync(2).get(), ImmutableList.of("1", "2"));
        assertEquals(queue.getBatchAsync(2).get(), ImmutableList.of("3"));
        ListenableFuture<?> batchFuture = queue.getBatchAsync(2);
        assertFalse(batchFuture.isDone());

        assertTrue(queue.offer("4").isDone());
        assertEquals(batchFuture.get(), ImmutableList.of("4"));

        batchFuture = queue.getBatchAsync(2);
        assertFalse(batchFuture.isDone());
        queue.finish();
        batchFuture.get();
        assertTrue(queue.isFinished());
    }

    @Test(timeOut = 10_000)
    public void testOfferAfterFinish()
            throws Exception
    {
        AsyncQueue<String> queue = new AsyncQueue<>(4, executor);

        assertTrue(queue.offer("1").isDone());
        assertTrue(queue.offer("2").isDone());
        assertTrue(queue.offer("3").isDone());
        assertFalse(queue.offer("4").isDone());

        queue.finish();
        assertTrue(queue.offer("5").isDone());
        assertTrue(queue.offer("6").isDone());
        assertTrue(queue.offer("7").isDone());
        assertFalse(queue.isFinished());

        assertEquals(queue.getBatchAsync(100).get(), ImmutableList.of("1", "2", "3", "4"));
        assertTrue(queue.isFinished());
    }

    @Test
    public void testBorrow()
            throws Exception
    {
        // The numbers are chosen so that depletion of elements can happen.
        // Size is 5. Two threads each borrowing 3 can deplete the queue.
        // The third thread may try to borrow when the queue is already empty.
        // We also want to confirm that isFinished won't return true even if queue is depleted.

        AsyncQueue<Integer> queue = new AsyncQueue<>(4, executor);
        queue.offer(1);
        queue.offer(2);
        queue.offer(3);
        queue.offer(4);
        queue.offer(5);

        // Repeatedly remove up to 3 elements and re-insert them.
        Runnable runnable = () -> {
            for (int i = 0; i < 700; i++) {
                getFutureValue(queue.borrowBatchAsync(3, elements -> new BorrowResult<>(elements, null)));
            }
        };

        Future<?> future1 = executor.submit(runnable);
        Future<?> future2 = executor.submit(runnable);
        Future<?> future3 = executor.submit(runnable);
        future1.get();
        future2.get();
        future3.get();

        queue.finish();
        assertFalse(queue.isFinished());

        AtomicBoolean done = new AtomicBoolean();
        executor.submit(() -> {
            while (!done.get()) {
                assertFalse(queue.isFinished() || done.get());
            }
        });

        future1 = executor.submit(runnable);
        future2 = executor.submit(runnable);
        future3 = executor.submit(runnable);
        future1.get();
        future2.get();
        future3.get();
        done.set(true);

        assertFalse(queue.isFinished());
        ArrayList<Integer> list = new ArrayList<>(queue.getBatchAsync(100).get());
        list.sort(Integer::compare);
        assertEquals(list, ImmutableList.of(1, 2, 3, 4, 5));
        assertTrue(queue.isFinished());
    }

    @Test
    public void testBorrowThrows()
            throws Exception
    {
        // It doesn't matter the exact behavior when the caller-supplied function to borrow fails.
        // However, it must not block pending futures.

        AsyncQueue<Integer> queue = new AsyncQueue<>(4, executor);
        queue.offer(1);
        queue.offer(2);
        queue.offer(3);
        queue.offer(4);
        queue.offer(5);

        ListenableFuture<?> future1 = queue.offer(6);
        assertFalse(future1.isDone());

        Runnable runnable = () -> {
            getFutureValue(queue.borrowBatchAsync(1, elements -> {
                throw new RuntimeException("test fail");
            }));
        };

        try {
            executor.submit(runnable).get();
            fail("expected failure");
        }
        catch (ExecutionException e) {
            assertContains(e.getMessage(), "test fail");
        }

        ListenableFuture<?> future2 = queue.offer(7);
        assertFalse(future1.isDone());
        assertFalse(future2.isDone());
        queue.finish();
        future1.get();
        future2.get();
        assertTrue(queue.offer(8).isDone());

        try {
            executor.submit(runnable).get();
            fail("expected failure");
        }
        catch (ExecutionException e) {
            assertContains(e.getMessage(), "test fail");
        }

        assertTrue(queue.offer(9).isDone());

        assertFalse(queue.isFinished());
        ArrayList<Integer> list = new ArrayList<>(queue.getBatchAsync(100).get());
        // 1 and 2 were removed by borrow call; 8 and 9 were never inserted because insertion happened after finish.
        assertEquals(list, ImmutableList.of(3, 4, 5, 6, 7));
        assertTrue(queue.isFinished());
    }
}
