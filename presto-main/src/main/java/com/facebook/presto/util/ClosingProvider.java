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

import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.binder.LinkedBindingBuilder;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public final class ClosingProvider<T>
        implements Provider<T>
{
    private final T object;
    private final Closer<T> closer;

    private final AtomicBoolean fetched = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    private ClosingProvider(T object, Closer<T> closer)
    {
        this.object = requireNonNull(object, "object is null");
        this.closer = requireNonNull(closer, "closer is null");
    }

    @Override
    public T get()
    {
        checkState(!fetched.getAndSet(true), "provider already used (must be bound in singleton scope)");
        return object;
    }

    @PreDestroy
    public void destroy()
            throws Exception
    {
        if (!closed.getAndSet(true)) {
            closer.close(object);
        }
    }

    public static <T> Provider<T> closingProvider(T object, Closer<T> closer)
    {
        return new ClosingProvider<>(object, closer);
    }

    public interface Closer<T>
    {
        void close(T object)
                throws Exception;
    }

    public static class ClosableBindingBuilder<T>
    {
        private final LinkedBindingBuilder<T> builder;
        private final Closer<T> closer;

        public ClosableBindingBuilder(LinkedBindingBuilder<T> builder, Closer<T> closer)
        {
            this.builder = requireNonNull(builder, "builder is null");
            this.closer = requireNonNull(closer, "closer is null");
        }

        @SuppressWarnings("InstanceMethodNamingConvention")
        public void to(T executor)
        {
            builder.toProvider(closingProvider(executor, closer)).in(Scopes.SINGLETON);
        }
    }
}
