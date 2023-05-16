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

import com.google.common.util.concurrent.MoreExecutors;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestPeriodicTaskExecutor
{
    private ExecutorService executorService;

    @BeforeMethod
    public void setUp()
    {
        executorService = MoreExecutors.newDirectExecutorService();
    }

    @AfterMethod
    public void tearDown()
    {
        executorService.shutdownNow();
    }

    @Test
    public void testTick()
            throws Exception
    {
        AtomicInteger counter = new AtomicInteger();
        Runnable runnable = counter::incrementAndGet;

        PeriodicTaskExecutor executor = new PeriodicTaskExecutor(0, 0, executorService, Optional.empty(), runnable, i -> i);
        try {
            executor.start();
            executor.tick();
            executor.tick();

            assertEquals(counter.get(), 3); // counter was incremented 3 times
        }
        finally {
            executor.close();
        }
        executor.tick();
        assertEquals(counter.get(), 3); // no-op after executor is closed
    }
}
