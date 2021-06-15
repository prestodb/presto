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
package com.facebook.presto.testng.services;

import com.facebook.airlift.log.Logger;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.IClassListener;
import org.testng.IExecutionListener;
import org.testng.IInvokedMethod;
import org.testng.IInvokedMethodListener;
import org.testng.ITestClass;
import org.testng.ITestResult;

import javax.annotation.concurrent.GuardedBy;

import java.lang.management.ThreadInfo;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.String.format;
import static java.lang.management.ManagementFactory.getThreadMXBean;
import static java.util.stream.Collectors.joining;

public class LogTestDurationListener
        implements IExecutionListener, IClassListener, IInvokedMethodListener
{
    private static final Logger LOG = Logger.get(LogTestDurationListener.class);

    private static final Duration SINGLE_TEST_LOGGING_THRESHOLD = Duration.valueOf("30s");
    private static final Duration CLASS_LOGGING_THRESHOLD = Duration.valueOf("1m");
    private static final Duration GLOBAL_IDLE_LOGGING_THRESHOLD = Duration.valueOf("8m");

    private final ScheduledExecutorService scheduledExecutorService;

    private final Map<String, Long> started = new ConcurrentHashMap<>();
    private final AtomicLong lastChange = new AtomicLong(System.nanoTime());
    private final AtomicBoolean hangLogged = new AtomicBoolean();
    private final AtomicBoolean finished = new AtomicBoolean();
    @GuardedBy("this")
    private ScheduledFuture<?> monitorHangTask;

    public LogTestDurationListener()
    {
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(daemonThreadsNamed("TestHangMonitor"));
    }

    @Override
    public synchronized void onExecutionStart()
    {
        resetHangMonitor();
        finished.set(false);
        if (monitorHangTask == null) {
            monitorHangTask = scheduledExecutorService.scheduleWithFixedDelay(this::checkForTestHang, 5, 5, TimeUnit.SECONDS);
        }
    }

    @Override
    public synchronized void onExecutionFinish()
    {
        resetHangMonitor();
        finished.set(true);
        // do not stop hang task so notification of hung test JVM will fire
        // Note: since the monitor uses daemon threads it will not prevent JVM shutdown
    }

    private void checkForTestHang()
    {
        if (hangLogged.get()) {
            return;
        }

        Duration duration = nanosSince(lastChange.get());
        if (duration.compareTo(GLOBAL_IDLE_LOGGING_THRESHOLD) < 0) {
            return;
        }

        if (!hangLogged.compareAndSet(false, true)) {
            return;
        }

        Map<String, Long> runningTests = ImmutableMap.copyOf(started);
        if (!runningTests.isEmpty()) {
            String testDetails = runningTests.entrySet().stream()
                    .map(entry -> format("%s running for %s", entry.getKey(), nanosSince(entry.getValue())))
                    .collect(joining("\n\t", "\n\t", ""));
            dumpAllThreads(format("No test started or completed in %s. Running tests:%s.", GLOBAL_IDLE_LOGGING_THRESHOLD, testDetails));
        }
        else if (finished.get()) {
            dumpAllThreads(format("Tests finished, but JVM did not shutdown in %s.", GLOBAL_IDLE_LOGGING_THRESHOLD));
        }
        else {
            dumpAllThreads(format("No test started in %s", GLOBAL_IDLE_LOGGING_THRESHOLD));
        }
    }

    private static void dumpAllThreads(String message)
    {
        LOG.warn("%s\n\nFull Thread Dump:\n%s", message,
                Arrays.stream(getThreadMXBean().dumpAllThreads(true, true))
                        .map(ThreadInfo::toString)
                        .collect(joining("\n")));
    }

    private void resetHangMonitor()
    {
        lastChange.set(System.nanoTime());
        hangLogged.set(false);
    }

    @Override
    public void onBeforeClass(ITestClass testClass)
    {
        beginExecution(getName(testClass));
    }

    @Override
    public void onAfterClass(ITestClass testClass)
    {
        String name = getName(testClass);
        Duration duration = endExecution(name);
        if (duration.compareTo(CLASS_LOGGING_THRESHOLD) > 0) {
            LOG.warn("Tests from %s took %s", name, duration);
        }
    }

    @Override
    public void beforeInvocation(IInvokedMethod method, ITestResult testResult)
    {
        beginExecution(getName(method));
    }

    @Override
    public void afterInvocation(IInvokedMethod method, ITestResult testResult)
    {
        String name = getName(method);
        Duration duration = endExecution(name);
        if (duration.compareTo(SINGLE_TEST_LOGGING_THRESHOLD) > 0) {
            LOG.info("Test %s took %s", name, duration);
        }
    }

    private void beginExecution(String name)
    {
        resetHangMonitor();
        Long existingEntry = started.putIfAbsent(name, System.nanoTime());
        // You can get concurrent tests with the same name when using @Factory.  Instead of adding complex support for
        // having multiple running tests with the same name, we simply don't use @Factory.
        checkState(existingEntry == null, "There already is a start record for test: %s", name);
    }

    private Duration endExecution(String name)
    {
        resetHangMonitor();
        Long startTime = started.remove(name);
        checkState(startTime != null, "There is no start record for test: %s", name);
        return nanosSince(startTime);
    }

    private static String getName(ITestClass testClass)
    {
        return testClass.getName();
    }

    private static String getName(IInvokedMethod method)
    {
        return format("%s::%s", method.getTestMethod().getTestClass().getName(), method.getTestMethod().getMethodName());
    }
}
