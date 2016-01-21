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
package com.facebook.presto.util;

import com.facebook.presto.util.ClosingProvider.ClosableBindingBuilder;
import com.google.common.base.Strings;
import com.google.inject.Binder;

import java.lang.annotation.Annotation;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.presto.util.ExecutorBinder.ShutdownPolicy.GRACEFUL;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.concurrent.Threads.threadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class ExecutorBinder
{
    public enum ShutdownPolicy
    {
        GRACEFUL, IMMEDIATE
    }

    private final Binder binder;

    private ExecutorBinder(Binder binder)
    {
        this.binder = requireNonNull(binder, "binder is null").skipSources(getClass());
    }

    public static ExecutorBinder executorBinder(Binder binder)
    {
        return new ExecutorBinder(binder);
    }

    public <T extends ExecutorService> ClosableBindingBuilder<T> bind(
            Class<T> type,
            Class<? extends Annotation> annotation,
            ShutdownPolicy policy)
    {
        return new ClosableBindingBuilder<>(
                binder.bind(type).annotatedWith(annotation),
                (policy == GRACEFUL) ? ExecutorService::shutdown : ExecutorService::shutdownNow);
    }

    public void bindSingleExecutor(Class<? extends Annotation> annotation, String namePrefix, ShutdownPolicy policy)
    {
        bind(ExecutorService.class, annotation, policy)
                .to(newSingleThreadExecutor(threadsNamed(toFormat(namePrefix, 1))));
    }

    public void bindFixedExecutor(Class<? extends Annotation> annotation, String namePrefix, ShutdownPolicy policy, int threads)
    {
        bind(ExecutorService.class, annotation, policy)
                .to(newFixedThreadPool(threads, threadsNamed(toFormat(namePrefix, threads))));
    }

    public void bindCachedExecutor(Class<? extends Annotation> annotation, String namePrefix, ShutdownPolicy policy)
    {
        bind(ExecutorService.class, annotation, policy)
                .to(newCachedThreadPool(threadsNamed(toFormat(namePrefix, 1))));
    }

    public void bindSingleScheduled(Class<? extends Annotation> annotation, String namePrefix, ShutdownPolicy policy)
    {
        bind(ScheduledExecutorService.class, annotation, policy)
                .to(newSingleThreadScheduledExecutor(threadsNamed(toFormat(namePrefix, 1))));
    }

    public void bindFixedScheduled(Class<? extends Annotation> annotation, String namePrefix, ShutdownPolicy policy, int threads)
    {
        bind(ScheduledExecutorService.class, annotation, policy)
                .to(newScheduledThreadPool(threads, threadsNamed(toFormat(namePrefix, 1))));
    }

    public void bindDaemonSingleExecutor(Class<? extends Annotation> annotation, String namePrefix, ShutdownPolicy policy)
    {
        bind(ExecutorService.class, annotation, policy)
                .to(newSingleThreadExecutor(daemonThreadsNamed(toFormat(namePrefix, 1))));
    }

    public void bindDaemonFixedExecutor(Class<? extends Annotation> annotation, String namePrefix, ShutdownPolicy policy, int threads)
    {
        bind(ExecutorService.class, annotation, policy)
                .to(newFixedThreadPool(threads, daemonThreadsNamed(toFormat(namePrefix, threads))));
    }

    public void bindDaemonCachedExecutor(Class<? extends Annotation> annotation, String namePrefix, ShutdownPolicy policy)
    {
        bind(ExecutorService.class, annotation, policy)
                .to(newCachedThreadPool(daemonThreadsNamed(toFormat(namePrefix, 1))));
    }

    public void bindDaemonSingleScheduled(Class<? extends Annotation> annotation, String namePrefix, ShutdownPolicy policy)
    {
        bind(ScheduledExecutorService.class, annotation, policy)
                .to(newSingleThreadScheduledExecutor(daemonThreadsNamed(toFormat(namePrefix, 1))));
    }

    public void bindDaemonFixedScheduled(Class<? extends Annotation> annotation, String namePrefix, ShutdownPolicy policy, int threads)
    {
        bind(ScheduledExecutorService.class, annotation, policy)
                .to(newScheduledThreadPool(threads, daemonThreadsNamed(toFormat(namePrefix, 1))));
    }

    private static String toFormat(String namePrefix, int threads)
    {
        checkArgument(!Strings.isNullOrEmpty(namePrefix), "namePrefix is null or empty");
        checkArgument(!namePrefix.contains("%"), "namePrefix must not contain %");
        checkArgument(!namePrefix.endsWith("-"), "namePrefix must not end with -");
        checkArgument(threads >= 1, "threads must be at least one");
        return (threads == 1) ? namePrefix : (namePrefix + "-%s");
    }
}
