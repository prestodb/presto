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

import com.google.common.annotations.VisibleForTesting;
import org.testng.IClassListener;
import org.testng.ITestClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.xml.XmlSuite.ParallelMode;
import org.testng.xml.XmlTest;

import java.lang.reflect.Method;

import static com.facebook.presto.testng.services.Listeners.reportListenerFailure;
import static com.google.common.base.Throwables.getStackTraceAsString;
import static java.lang.String.format;

public class ReportMultiThreadedBeforeOrAfterMethod
        implements IClassListener
{
    @Override
    public void onBeforeClass(ITestClass testClass)
    {
        try {
            if (!isParallel(testClass.getXmlTest())) {
                return;
            }

            reportMultiThreadedBeforeOrAfterMethod(testClass.getRealClass());
        }
        catch (RuntimeException | Error e) {
            reportListenerFailure(
                    ReportMultiThreadedBeforeOrAfterMethod.class,
                    "Failed to process %s: \n%s",
                    testClass,
                    getStackTraceAsString(e));
        }
    }

    private boolean isParallel(XmlTest xmlTest)
    {
        if (xmlTest.getThreadCount() == 1) {
            return false;
        }

        ParallelMode parallel = xmlTest.getParallel();
        return parallel.isParallel();
    }

    @VisibleForTesting
    static void reportMultiThreadedBeforeOrAfterMethod(Class<?> testClass)
    {
        Test testAnnotation = testClass.getAnnotation(Test.class);
        if (testAnnotation != null && testAnnotation.singleThreaded()) {
            return;
        }

        Method[] methods = testClass.getMethods();
        for (Method method : methods) {
            if (method.getAnnotation(BeforeMethod.class) != null || method.getAnnotation(AfterMethod.class) != null) {
                throw new RuntimeException(format(
                        "Test class %s should be annotated as @Test(singleThreaded=true), if it contains mutable state as indicated by %s",
                        testClass.getName(),
                        method));
            }
        }
    }

    @Override
    public void onAfterClass(ITestClass iTestClass) {}
}
