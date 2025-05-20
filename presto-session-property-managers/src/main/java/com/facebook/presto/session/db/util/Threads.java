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
package com.facebook.presto.session.db.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ThreadFactory;

import static java.util.concurrent.Executors.defaultThreadFactory;

public final class Threads
{
    private Threads() {}

    /**
     * Creates a {@link ThreadFactory} that creates named threads
     * using the specified naming format.
     *
     * @param nameFormat a {@link String#format(String, Object...)}-compatible
     * format string, to which a string will be supplied as the single
     * parameter. This string will be unique to this instance of the
     * ThreadFactory and will be assigned sequentially.
     * @return the created ThreadFactory
     */
    public static ThreadFactory threadsNamed(String nameFormat)
    {
        return new ThreadFactoryBuilder()
                .setNameFormat(nameFormat)
                .setThreadFactory(new ContextClassLoaderThreadFactory(Thread.currentThread().getContextClassLoader(), defaultThreadFactory()))
                .build();
    }

    /**
     * Creates a {@link ThreadFactory} that creates named daemon threads.
     * using the specified naming format.
     *
     * @param nameFormat see {@link #threadsNamed(String)}
     * @return the created ThreadFactory
     */
    public static ThreadFactory daemonThreadsNamed(String nameFormat)
    {
        return new ThreadFactoryBuilder()
                .setNameFormat(nameFormat)
                .setDaemon(true)
                .setThreadFactory(new ContextClassLoaderThreadFactory(Thread.currentThread().getContextClassLoader(), defaultThreadFactory()))
                .build();
    }

    private static class ContextClassLoaderThreadFactory
            implements ThreadFactory
    {
        private final ClassLoader classLoader;
        private final ThreadFactory delegate;

        public ContextClassLoaderThreadFactory(ClassLoader classLoader, ThreadFactory delegate)
        {
            this.classLoader = classLoader;
            this.delegate = delegate;
        }

        @Override
        public Thread newThread(Runnable runnable)
        {
            Thread thread = delegate.newThread(runnable);
            thread.setContextClassLoader(classLoader);
            return thread;
        }
    }
}
