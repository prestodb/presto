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
package com.facebook.presto.sql.planner;

import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.split.SplitSource;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.concurrent.GuardedBy;

import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class LazySplitSource
        implements SplitSource
{
    private final Supplier<SplitSource> supplier;

    @GuardedBy("this")
    private SplitSource delegate;
    @GuardedBy("this")
    private boolean closed;

    public LazySplitSource(Supplier<SplitSource> supplier)
    {
        this.supplier = requireNonNull(supplier, "supplier is null");
    }

    @Override
    public ConnectorId getConnectorId()
    {
        return getDelegate().getConnectorId();
    }

    @Override
    public ConnectorTransactionHandle getTransactionHandle()
    {
        return getDelegate().getTransactionHandle();
    }

    @Override
    public ListenableFuture<SplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, Lifespan lifespan, int maxSize)
    {
        return getDelegate().getNextBatch(partitionHandle, lifespan, maxSize);
    }

    @Override
    public void rewind(ConnectorPartitionHandle partitionHandle)
    {
        getDelegate().rewind(partitionHandle);
    }

    @Override
    public boolean isFinished()
    {
        return getDelegate().isFinished();
    }

    @Override
    public synchronized void close()
    {
        // already closed
        if (closed) {
            return;
        }
        closed = true;

        // not yet initialized
        if (delegate == null) {
            return;
        }

        delegate.close();
    }

    private synchronized SplitSource getDelegate()
    {
        checkState(!closed, "split source is closed");
        if (delegate == null) {
            delegate = supplier.get();
        }
        return delegate;
    }
}
