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
package com.facebook.presto.cassandra;

import com.facebook.airlift.log.Logger;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;

import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * ReopeningCluster is no longer needed in driver 4.x as it used driver 3.x Cluster API.
 * This class is kept for backward compatibility but is deprecated.
 * Use ReopeningSession instead.
 *
 * @deprecated Use {@link ReopeningSession} instead
 */
@Deprecated
@ThreadSafe
public class ReopeningCluster
{
    private static final Logger log = Logger.get(ReopeningCluster.class);

    @GuardedBy("this")
    private Object delegate;
    @GuardedBy("this")
    private boolean closed;

    private final Supplier<Object> supplier;

    public ReopeningCluster(Supplier<Object> supplier)
    {
        this.supplier = requireNonNull(supplier, "supplier is null");
    }

    protected synchronized Object delegate()
    {
        checkState(!closed, "Cluster has been closed");

        if (delegate == null) {
            delegate = supplier.get();
        }

        // Note: In driver 4.x, we can't check if session is closed the same way
        // This class is deprecated and should not be used

        return delegate;
    }

    public synchronized void close()
    {
        closed = true;
        if (delegate != null) {
            // Delegate closing to the actual session implementation
            if (delegate instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) delegate).close();
                }
                catch (Exception e) {
                    log.warn(e, "Error closing delegate");
                }
            }
            delegate = null;
        }
    }

    public synchronized boolean isClosed()
    {
        return closed;
    }

    public synchronized void closeAsync()
    {
        throw new UnsupportedOperationException("closeAsync is not supported in driver 4.x");
    }
}
