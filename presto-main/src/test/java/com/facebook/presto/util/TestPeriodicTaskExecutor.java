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
package com.facebook.presto.util;

import com.google.common.base.Stopwatch;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestPeriodicTaskExecutor
{
    private ScheduledExecutorService executorService;

    @BeforeMethod
    public void setUp()
    {
        executorService = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterMethod
    public void tearDown()
    {
        executorService.shutdownNow();
    }

    @Test(enabled = false)
    public void testTick()
            throws Exception
    {
        int numberOfTicks = 2;
        long durationBetweenTicksInSeconds = 2;

        CountDownLatch latch = new CountDownLatch(3);
        Runnable runnable = latch::countDown;

        try (PeriodicTaskExecutor executor = new PeriodicTaskExecutor(SECONDS.toMillis(durationBetweenTicksInSeconds), 500, executorService, runnable, i -> i)) {
            executor.start();
            Stopwatch stopwatch = Stopwatch.createStarted();
            latch.await(10, SECONDS);
            stopwatch.stop();

            assertEquals((numberOfTicks * durationBetweenTicksInSeconds), stopwatch.elapsed(SECONDS));
            assertEquals(latch.getCount(), 0); // latch was counted down 3 times
        }
    }
}
