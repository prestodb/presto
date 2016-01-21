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
import com.google.inject.Binder;

import java.lang.annotation.Annotation;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.util.ExecutorBinder.ShutdownPolicy.GRACEFUL;
import static java.util.Objects.requireNonNull;

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
}
