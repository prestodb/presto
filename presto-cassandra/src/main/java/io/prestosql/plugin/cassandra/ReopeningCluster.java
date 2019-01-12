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
package io.prestosql.plugin.cassandra;

import com.datastax.driver.core.CloseFuture;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DelegatingCluster;
import io.airlift.log.Logger;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class ReopeningCluster
        extends DelegatingCluster
{
    private static final Logger log = Logger.get(ReopeningCluster.class);

    @GuardedBy("this")
    private Cluster delegate;
    @GuardedBy("this")
    private boolean closed;

    private final Supplier<Cluster> supplier;

    public ReopeningCluster(Supplier<Cluster> supplier)
    {
        this.supplier = requireNonNull(supplier, "supplier is null");
    }

    @Override
    protected synchronized Cluster delegate()
    {
        checkState(!closed, "Cluster has been closed");

        if (delegate == null) {
            delegate = supplier.get();
        }

        if (delegate.isClosed()) {
            log.warn("Cluster has been closed internally");
            delegate = supplier.get();
        }

        verify(!delegate.isClosed(), "Newly created cluster has been immediately closed");

        return delegate;
    }

    @Override
    public synchronized void close()
    {
        closed = true;
        if (delegate != null) {
            delegate.close();
            delegate = null;
        }
    }

    @Override
    public synchronized boolean isClosed()
    {
        return closed;
    }

    @Override
    public synchronized CloseFuture closeAsync()
    {
        throw new UnsupportedOperationException();
    }
}
