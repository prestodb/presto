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
package com.facebook.presto.hive.util;

import com.google.common.util.concurrent.MoreExecutors;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;

public class TestSuspendingExecutor
{
    @Test
    public void testSanity()
            throws Exception
    {
        final AtomicInteger count = new AtomicInteger();
        SuspendingExecutor suspendingExecutor = new SuspendingExecutor(MoreExecutors.sameThreadExecutor());

        Runnable incrementTask = new Runnable()
        {
            @Override
            public void run()
            {
                count.incrementAndGet();
            }
        };

        suspendingExecutor.execute(incrementTask);
        assertEquals(count.get(), 1);

        suspendingExecutor.execute(incrementTask);
        assertEquals(count.get(), 2);

        suspendingExecutor.suspend();
        suspendingExecutor.execute(incrementTask);
        // Count should still be one because task was executed after suspending
        assertEquals(count.get(), 2);

        suspendingExecutor.execute(incrementTask);
        // Still suspended
        assertEquals(count.get(), 2);

        suspendingExecutor.resume();
        // Now all suspended tasks should execute
        assertEquals(count.get(), 4);

        suspendingExecutor.execute(incrementTask);
        assertEquals(count.get(), 5);
    }

    @Test
    public void testSelfReference()
    {
        final SuspendingExecutor suspendingExecutor = new SuspendingExecutor(MoreExecutors.sameThreadExecutor());
        suspendingExecutor.suspend();
        suspendingExecutor.execute(new Runnable()
        {
            @Override
            public void run()
            {
                suspendingExecutor.resume();
            }
        });
        suspendingExecutor.resume();
        // This should complete successfully without any exceptions or infinite loops as forward progress will always be made on resume
    }

    @Test
    public void testConcurrency()
            throws Exception
    {
        ExecutorService executorService = Executors.newFixedThreadPool(20);

        int iterations = 500_000;
        final AtomicInteger count = new AtomicInteger();
        final SuspendingExecutor suspendingExecutor = new SuspendingExecutor(executorService);
        for (int i = 0; i < iterations; i++) {
            suspendingExecutor.execute(new Runnable()
            {
                @Override
                public void run()
                {
                    if (ThreadLocalRandom.current().nextBoolean()) {
                        suspendingExecutor.suspend();
                    }
                    count.incrementAndGet();
                }
            });
        }

        while (count.get() < iterations) {
            if (ThreadLocalRandom.current().nextBoolean()) {
                suspendingExecutor.resume();
            }
        }

        executorService.shutdownNow();
        executorService.awaitTermination(1, TimeUnit.MINUTES);

        // Make sure that despite some concurrent sequence of suspend and resumes, each task was executed once
        assertEquals(count.get(), iterations);
    }
}
