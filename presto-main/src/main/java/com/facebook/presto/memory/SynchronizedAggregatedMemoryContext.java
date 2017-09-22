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
package com.facebook.presto.memory;

import com.google.common.io.Closer;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

@ThreadSafe
public final class SynchronizedAggregatedMemoryContext
        extends AbstractAggregatedMemoryContext
        implements AutoCloseable
{
    /**
     * Creates synchronized wrapper for the {@code delegate} context. The delegate won't be closed when returned instance is closed.
     */
    public static SynchronizedAggregatedMemoryContext synchronizedMemoryContext(AbstractAggregatedMemoryContext delegate)
    {
        return new SynchronizedAggregatedMemoryContext(delegate);
    }

    private final AbstractAggregatedMemoryContext delegate;
    private final Closer closer = Closer.create();

    private SynchronizedAggregatedMemoryContext(AggregatedMemoryContext delegate, boolean ownDelegate)
    {
        this(delegate);

        if (ownDelegate) {
            closer.register(delegate::close);
        }
    }

    private SynchronizedAggregatedMemoryContext(AbstractAggregatedMemoryContext delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    protected synchronized void updateBytes(long bytes)
    {
        delegate.updateBytes(bytes);
    }

    @Override
    public AggregatedMemoryContext newAggregatedMemoryContext()
    {
        // Due to concrete return type we cannot provide thread safe implementation
        throw new UnsupportedOperationException("Use newSynchronizedAggregatedMemoryContext() instead");
    }

    public SynchronizedAggregatedMemoryContext newSynchronizedAggregatedMemoryContext()
    {
        return new SynchronizedAggregatedMemoryContext(super.newAggregatedMemoryContext(), true);
    }

    @Override
    public void close()
    {
        try {
            closer.close();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
