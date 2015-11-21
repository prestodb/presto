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
package com.facebook.presto.jdbc;

import org.eclipse.jetty.util.log.JavaUtilLog;
import org.eclipse.jetty.util.log.Log;

// TODO: fix this in Airlift
final class JettyLogging
{
    private JettyLogging() {}

    /**
     * Force Jetty to use java.util.logging instead of SLF4J
     */
    public static void useJavaUtilLogging()
    {
        Log.__logClass = JavaUtilLog.class.getName();
        Log.initialized();
    }
}
