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
package com.facebook.presto.execution;

import com.facebook.airlift.units.Duration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestTimeoutThread
{
    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor();

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test(timeOut = 60_000)
    public void testExpiresWhenItInterrupts()
    {
        try (TimeoutThread timeoutThread = new TimeoutThread(Thread.currentThread(), executor, new Duration(10, MILLISECONDS))) {
            assertThrows(InterruptedException.class, () -> new CountDownLatch(1).await());
            assertTrue(timeoutThread.hasExpired());
        }
    }

    @Test(timeOut = 60_000)
    public void testNotExpiredBeforeTimeout()
    {
        try (TimeoutThread timeoutThread = new TimeoutThread(Thread.currentThread(), executor, new Duration(1, HOURS))) {
            assertFalse(timeoutThread.hasExpired());
        }
    }
}
