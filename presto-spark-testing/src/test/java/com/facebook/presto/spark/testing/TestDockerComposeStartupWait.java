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
package com.facebook.presto.spark.testing;

import org.testng.annotations.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestDockerComposeStartupWait
{
    private static final Duration TIMEOUT = Duration.ofSeconds(60);

    @Test
    public void testWaitsForRegistrationAndAddress()
            throws Exception
    {
        TestProcess process = new TestProcess();
        TestTime time = new TestTime();
        AtomicInteger lookups = new AtomicInteger();
        String address = waitForAddress(process, () -> {
            int lookup = lookups.incrementAndGet();
            if (lookup == 1) {
                return Optional.empty();
            }
            return Optional.of(lookup == 2 ? "" : "172.18.0.2");
        }, time);
        assertEquals(address, "172.18.0.2");
        assertEquals(lookups.get(), 3);
        assertEquals(time.nanos, Duration.ofMillis(200).toNanos());
    }

    @Test
    public void testRejectsExitedComposeBeforeLookup()
    {
        TestProcess process = new TestProcess();
        process.exit(17);
        AtomicInteger lookups = new AtomicInteger();
        assertThatThrownBy(() -> waitForAddress(process, () -> {
            lookups.incrementAndGet();
            return Optional.empty();
        }, new TestTime()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("code 17")
                .hasMessageContaining("hadoop-master");
        assertEquals(lookups.get(), 0);
    }

    @Test
    public void testRejectsComposeExitDuringLookup()
    {
        TestProcess process = new TestProcess();
        TestTime time = new TestTime();
        assertThatThrownBy(() -> waitForAddress(process, () -> {
            process.exit(1);
            return Optional.of("172.18.0.2");
        }, time))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("code 1");
        assertEquals(time.nanos, 0);
    }

    @Test
    public void testRejectsComposeExitWhileWaiting()
    {
        TestProcess process = new TestProcess();
        TestTime time = new TestTime();
        AtomicInteger lookups = new AtomicInteger();
        assertThatThrownBy(() -> DockerCompose.waitForContainerAddress("hadoop-master", process, TIMEOUT, () -> {
            lookups.incrementAndGet();
            return Optional.empty();
        }, time::nanoTime, nanos -> {
                time.sleep(nanos);
                process.exit(3);
            }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("code 3");
        assertEquals(lookups.get(), 1);
        assertEquals(time.nanos, Duration.ofMillis(100).toNanos());
    }

    @Test
    public void testTimeoutUsesMonotonicElapsedTime()
    {
        TestTime time = new TestTime();
        time.nanos = Long.MAX_VALUE - Duration.ofSeconds(1).toNanos();
        long started = time.nanos;
        assertThatThrownBy(() -> waitForAddress(new TestProcess(), Optional::empty, time))
                .isInstanceOf(TimeoutException.class)
                .hasMessageContaining("PT1M")
                .hasMessageContaining("hadoop-master");
        assertEquals(time.nanos - started, TIMEOUT.toNanos());
    }

    @Test
    public void testLookupFailureIsNotRetried()
    {
        RuntimeException failure = new IllegalStateException("docker inspect failed");
        AtomicInteger lookups = new AtomicInteger();
        TestTime time = new TestTime();
        assertThatThrownBy(() -> waitForAddress(new TestProcess(), () -> {
            lookups.incrementAndGet();
            throw failure;
        }, time)).isSameAs(failure);
        assertEquals(lookups.get(), 1);
        assertEquals(time.nanos, 0);
    }

    @Test
    public void testInterruptionIsPropagated()
    {
        InterruptedException interruption = new InterruptedException("interrupted");
        TestTime time = new TestTime();
        assertThatThrownBy(() -> DockerCompose.waitForContainerAddress("hadoop-master", new TestProcess(), TIMEOUT, Optional::empty, time::nanoTime, nanos -> {
            throw interruption;
        })).isSameAs(interruption);
        assertEquals(time.nanos, 0);
    }

    private static String waitForAddress(TestProcess process, DockerCompose.AddressLookup addressLookup, TestTime time)
            throws InterruptedException, TimeoutException
    {
        return DockerCompose.waitForContainerAddress("hadoop-master", process, TIMEOUT, addressLookup, time::nanoTime, time::sleep);
    }

    private static class TestTime
    {
        private long nanos;

        public long nanoTime()
        {
            return nanos;
        }

        public void sleep(long nanos)
        {
            this.nanos += nanos;
        }
    }

    private static class TestProcess
            extends Process
    {
        private boolean alive = true;
        private int exitCode;

        public void exit(int exitCode)
        {
            this.exitCode = exitCode;
            alive = false;
        }

        @Override
        public boolean isAlive()
        {
            return alive;
        }

        @Override
        public int exitValue()
        {
            if (alive) {
                throw new IllegalThreadStateException();
            }
            return exitCode;
        }

        @Override
        public OutputStream getOutputStream()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public InputStream getInputStream()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public InputStream getErrorStream()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int waitFor()
        {
            return exitValue();
        }

        @Override
        public void destroy()
        {
            exit(0);
        }
    }
}
