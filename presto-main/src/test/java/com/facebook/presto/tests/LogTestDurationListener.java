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
package com.facebook.presto.tests;

import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.testng.IClassListener;
import org.testng.IInvokedMethod;
import org.testng.IInvokedMethodListener;
import org.testng.ITestClass;
import org.testng.ITestResult;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.String.format;

public class LogTestDurationListener
        implements IClassListener, IInvokedMethodListener
{
    private static final Logger LOG = Logger.get(LogTestDurationListener.class);

    private static final Duration SINGLE_TEST_LOGGING_THRESHOLD = Duration.valueOf("30s");
    private static final Duration CLASS_LOGGING_THRESHOLD = Duration.valueOf("1m");

    private final Map<String, Long> started = new ConcurrentHashMap<>();

    @Override
    public void onBeforeClass(ITestClass testClass)
    {
        started.put(getName(testClass), System.nanoTime());
    }

    @Override
    public void onAfterClass(ITestClass testClass)
    {
        String name = getName(testClass);
        Duration duration = Duration.nanosSince(started.remove(name));
        if (duration.compareTo(CLASS_LOGGING_THRESHOLD) > 0) {
            LOG.warn("Tests from %s took %s", name, duration);
        }
    }

    @Override
    public void beforeInvocation(IInvokedMethod method, ITestResult testResult)
    {
        started.put(getName(method), System.nanoTime());
    }

    @Override
    public void afterInvocation(IInvokedMethod method, ITestResult testResult)
    {
        String name = getName(method);
        Duration duration = Duration.nanosSince(started.remove(name));
        if (duration.compareTo(SINGLE_TEST_LOGGING_THRESHOLD) > 0) {
            LOG.info("Test %s took %s", name, duration);
        }
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
