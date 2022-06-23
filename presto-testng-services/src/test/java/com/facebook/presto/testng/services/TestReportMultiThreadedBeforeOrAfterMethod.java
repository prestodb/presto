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

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.facebook.presto.testng.services.ReportMultiThreadedBeforeOrAfterMethod.reportMultiThreadedBeforeOrAfterMethod;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestReportMultiThreadedBeforeOrAfterMethod
{
    @Test
    public void test()
    {
        // no @BeforeMethod
        reportMultiThreadedBeforeOrAfterMethod(ClassWithoutBeforeMethod.class);

        // @BeforeMethod but not @Test(singleThreaded)
        assertThatThrownBy(() -> reportMultiThreadedBeforeOrAfterMethod(ClassWithBeforeMethod.class))
                .hasMessageMatching("Test class \\S*\\Q.TestReportMultiThreadedBeforeOrAfterMethod$ClassWithBeforeMethod should be annotated as @Test(singleThreaded=true)\\E, " +
                        "if it contains mutable state as indicated by public void \\S*\\Q.TestReportMultiThreadedBeforeOrAfterMethod$ClassWithBeforeMethod.someBeforeMethod()");

        // @BeforeMethod and @Test(singleThreaded)
        reportMultiThreadedBeforeOrAfterMethod(SingleThreadedClassWithBeforeMethod.class);

        // inherited @BeforeMethod but not @Test(singleThreaded)
        assertThatThrownBy(() -> reportMultiThreadedBeforeOrAfterMethod(ClassWithInheritedBeforeMethod.class))
                .hasMessageMatching("Test class \\S*\\Q.TestReportMultiThreadedBeforeOrAfterMethod$ClassWithInheritedBeforeMethod should be annotated as @Test(singleThreaded=true)\\E, " +
                        "if it contains mutable state as indicated by public void \\S*\\Q.TestReportMultiThreadedBeforeOrAfterMethod$ClassWithBeforeMethod.someBeforeMethod()");

        // inherited @BeforeMethod and "inherited" @Test(singleThreaded) (this does not get propagated to child class yet, see https://github.com/cbeust/testng/issues/144)
        assertThatThrownBy(() -> reportMultiThreadedBeforeOrAfterMethod(ClassExtendingSingleThreadedClassWithBeforeMethod.class))
                .hasMessageMatching("Test class \\S*\\Q.TestReportMultiThreadedBeforeOrAfterMethod$ClassExtendingSingleThreadedClassWithBeforeMethod should be annotated as @Test(singleThreaded=true)\\E, " +
                        "if it contains mutable state as indicated by public void \\S*\\Q.TestReportMultiThreadedBeforeOrAfterMethod$SingleThreadedClassWithBeforeMethod.someBeforeMethod()");
    }

    public static class ClassWithoutBeforeMethod {}

    public static class ClassWithBeforeMethod
    {
        @BeforeMethod
        public void someBeforeMethod() {}
    }

    @Test(singleThreaded = true)
    public static class SingleThreadedClassWithBeforeMethod
    {
        @BeforeMethod
        public void someBeforeMethod() {}
    }

    public static class ClassWithInheritedBeforeMethod
            extends ClassWithBeforeMethod {}

    public static class ClassExtendingSingleThreadedClassWithBeforeMethod
            extends SingleThreadedClassWithBeforeMethod {}
}
