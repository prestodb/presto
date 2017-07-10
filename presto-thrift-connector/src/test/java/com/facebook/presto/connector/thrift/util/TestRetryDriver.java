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
package com.facebook.presto.connector.thrift.util;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.units.Duration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.connector.thrift.util.TestRetryDriver.AttemptStatus.ASYNC_FAILURE;
import static com.facebook.presto.connector.thrift.util.TestRetryDriver.AttemptStatus.SUCCESS;
import static com.facebook.presto.connector.thrift.util.TestRetryDriver.AttemptStatus.SYNC_FAILURE;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.concurrent.Threads.threadsNamed;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestRetryDriver
{
    private static final Duration MIN_DURATION = new Duration(10, MILLISECONDS);
    private static final Duration MAX_DURATION = new Duration(20, MILLISECONDS);
    private static final Duration MAX_RETRY_TIME = new Duration(60, SECONDS);
    private static final double BACK_OFF_SCALE_FACTOR = 1.5;
    private static final int MAX_RETRY_ATTEMPTS = 10;
    private static final Integer RESULT = 123;

    private final ListeningScheduledExecutorService retryExecutor;
    private final RetryDriver retry;

    public TestRetryDriver()
    {
        retryExecutor = MoreExecutors.listeningDecorator(newScheduledThreadPool(4, threadsNamed("test-retry-retry-%s")));
        this.retry = RetryDriver.retry(retryExecutor)
                .exponentialBackoff(MIN_DURATION, MAX_DURATION, MAX_RETRY_TIME, BACK_OFF_SCALE_FACTOR)
                .maxAttempts(MAX_RETRY_ATTEMPTS);
    }

    @AfterClass(alwaysRun = true)
    public void close()
    {
        retryExecutor.shutdownNow();
    }

    enum AttemptStatus
    {
        SUCCESS,
        ASYNC_FAILURE,
        SYNC_FAILURE,
    }

    @Test
    void testNoRetry()
            throws Exception
    {
        SyncTaskImpl task = new SyncTaskImpl(SUCCESS);
        Integer result = retry.run("test", task);

        assertSyncSuccess(result, task, 1);
    }

    @Test
    void testRetrySuccess()
            throws Exception
    {
        SyncTaskImpl task = new SyncTaskImpl(SYNC_FAILURE, SYNC_FAILURE, SYNC_FAILURE, SYNC_FAILURE, SUCCESS);
        Integer result = retry.run("test", task);

        assertSyncSuccess(result, task, 5);
    }

    @Test
    void testOutOfRetries()
    {
        SyncTaskImpl task = new SyncTaskImpl(SYNC_FAILURE, SYNC_FAILURE, SYNC_FAILURE);

        try {
            retry.maxAttempts(3).run("test", task);
            fail("Call didn't fail as expected");
        }
        catch (Exception e) {
            if (!(e instanceof SyncException)) {
                fail("Expected " + SyncException.class + ", but got " + e.getClass());
            }
        }
        assertEquals(task.totalAttemptsMade(), 3);
    }

    @Test
    void testAsyncNoRetry()
            throws Exception
    {
        AsyncTaskImpl task = new AsyncTaskImpl(SUCCESS);
        ListenableFuture<Integer> result = retry.runAsync("test", task);

        assertAsyncSuccess(result, task, 1);
    }

    @Test
    void testAsyncMixedRetries()
            throws Exception
    {
        AsyncTaskImpl task = new AsyncTaskImpl(SYNC_FAILURE, ASYNC_FAILURE, SYNC_FAILURE, ASYNC_FAILURE, SUCCESS);
        ListenableFuture<Integer> result = retry.runAsync("test", task);

        assertAsyncSuccess(result, task, 5);
    }

    @Test
    void testAsyncMultipleAsyncFailures()
            throws Exception
    {
        AsyncTaskImpl task = new AsyncTaskImpl(ASYNC_FAILURE, ASYNC_FAILURE, ASYNC_FAILURE, ASYNC_FAILURE, SUCCESS);
        ListenableFuture<Integer> result = retry.runAsync("test", task);

        assertAsyncSuccess(result, task, 5);
    }

    @Test
    void testAsyncOutOfRetries()
            throws Exception
    {
        AsyncTaskImpl task = new AsyncTaskImpl(ASYNC_FAILURE, ASYNC_FAILURE, ASYNC_FAILURE, ASYNC_FAILURE);
        ListenableFuture<Integer> result = retry.maxAttempts(4).runAsync("test", task);

        try {
            result.get();
            fail("Future didn't fail as expected");
        }
        catch (Exception e) {
            if (!AsyncException.class.equals(e.getCause().getClass())) {
                fail("Expected " + AsyncException.class + ", but got " + e.getClass());
            }
        }
        assertEquals(task.totalAttemptsMade(), 4);
    }

    private static void assertSyncSuccess(Integer result, SyncTaskImpl task, int expectedAttempts)
            throws Exception
    {
        assertEquals(result, RESULT);
        assertEquals(task.totalAttemptsMade(), expectedAttempts);
    }

    private static void assertAsyncSuccess(ListenableFuture<Integer> result, AsyncTaskImpl task, int expectedAttempts)
            throws Exception
    {
        assertEquals(result.get(), RESULT);
        assertEquals(task.totalAttemptsMade(), expectedAttempts);
    }

    private static class TaskTracker
    {
        private final List<AttemptStatus> attempts;
        private final AtomicInteger nextAttempt;

        public TaskTracker(AttemptStatus... attempts)
        {
            this.attempts = ImmutableList.copyOf(attempts);
            this.nextAttempt = new AtomicInteger(0);
        }

        public int totalAttemptsMade()
        {
            return nextAttempt.get();
        }

        public AttemptStatus getNextAttemptStatus()
        {
            return attempts.get(nextAttempt.getAndIncrement());
        }
    }

    private static class SyncTaskImpl
            implements Callable<Integer>
    {
        private final TaskTracker taskTracker;

        public SyncTaskImpl(AttemptStatus... attempts)
        {
            taskTracker = new TaskTracker(attempts);
        }

        public int totalAttemptsMade()
        {
            return taskTracker.totalAttemptsMade();
        }

        @Override
        public Integer call()
        {
            switch (taskTracker.getNextAttemptStatus()) {
                case SUCCESS:
                    return RESULT;
                case SYNC_FAILURE:
                    throw new SyncException("Failed on call " + taskTracker.totalAttemptsMade());
                default:
                    throw new RuntimeException("Unexpected exception type");
            }
        }
    }

    private static class AsyncTaskImpl
            implements Callable<ListenableFuture<Integer>>
    {
        private final TaskTracker taskTracker;

        public AsyncTaskImpl(AttemptStatus... attempts)
        {
            taskTracker = new TaskTracker(attempts);
        }

        public int totalAttemptsMade()
        {
            return taskTracker.totalAttemptsMade();
        }

        @Override
        public ListenableFuture<Integer> call()
                throws Exception
        {
            switch (taskTracker.getNextAttemptStatus()) {
                case SUCCESS:
                    return immediateFuture(RESULT);
                case SYNC_FAILURE:
                    throw new SyncException("Failed on call " + taskTracker.totalAttemptsMade());
                case ASYNC_FAILURE:
                    return immediateFailedFuture(new AsyncException("Failed on call " + taskTracker.totalAttemptsMade()));
                default:
                    throw new RuntimeException("Unexpected exception type");
            }
        }
    }

    private static final class AsyncException
            extends RuntimeException
    {
        public AsyncException()
        {
        }

        public AsyncException(String message)
        {
            super(message);
        }
    }

    private static final class SyncException
            extends RuntimeException
    {
        public SyncException()
        {
        }

        public SyncException(String message)
        {
            super(message);
        }
    }
}
