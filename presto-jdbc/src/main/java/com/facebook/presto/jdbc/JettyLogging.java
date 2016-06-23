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

import java.lang.reflect.Field;

final class JettyLogging
{
    @SuppressWarnings({"StaticNonFinalField", "RedundantFieldInitialization"})
    private static boolean setup = false;

    private JettyLogging() {}

    /**
     * Force Jetty to use java.util.logging instead of SLF4J
     */
    public static synchronized void useJavaUtilLogging()
    {
        try {
            if (!setup) {
                setup = true;
                setup();
            }
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException("Error initializing Jetty logging", e);
        }
    }

    private static void setup()
            throws ReflectiveOperationException
    {
        ClassLoader classLoader = JettyLogging.class.getClassLoader();
        Class<?> jettyLog = classLoader.loadClass("org.eclipse.jetty.util.log.Log");
        Class<?> javaUtilLog = classLoader.loadClass("org.eclipse.jetty.util.log.JavaUtilLog");

        Field initialized = jettyLog.getDeclaredField("__initialized");
        initialized.setAccessible(true);
        if (initialized.getBoolean(null)) {
            return;
        }

        Field log = jettyLog.getDeclaredField("LOG");
        log.setAccessible(true);
        log.set(null, javaUtilLog.getConstructor().newInstance());

        initialized.setBoolean(null, true);
    }
}
