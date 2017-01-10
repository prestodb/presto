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
import org.testng.ITestResult;
import org.testng.TestListenerAdapter;

public class TestLoggingListener
        extends TestListenerAdapter
{
    private static final Logger log = Logger.get(TestLoggingListener.class);

    @Override
    public void onTestStart(ITestResult testResult)
    {
        log.info("Starting: %s", getTestName(testResult));
    }

    @Override
    public void onTestSuccess(ITestResult testResult)
    {
        log.info("Finished: %s", getTestName(testResult));
    }

    @Override
    public void onTestFailure(ITestResult testResult)
    {
        log.info("Failed: %s", getTestName(testResult));
    }

    @Override
    public void onTestSkipped(ITestResult testResult)
    {
        log.info("Skipped: %s", getTestName(testResult));
    }

    private static String getTestName(ITestResult testResult)
    {
        return testResult.getInstanceName() + "#" + testResult.getName();
    }
}
