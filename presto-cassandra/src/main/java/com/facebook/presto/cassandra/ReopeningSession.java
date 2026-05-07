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

import com.datastax.oss.driver.api.core.CqlSession;
import com.facebook.airlift.log.Logger;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;

import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

/**
 * Wrapper around CqlSession that automatically reopens the session if it gets closed.
 * This is useful for handling transient connection issues.
 */
@ThreadSafe
public class ReopeningSession
{
    private static final Logger log = Logger.get(ReopeningSession.class);

    @GuardedBy("this")
    private CqlSession delegate;

    @GuardedBy("this")
    private boolean closed;

    private final Supplier<CqlSession> sessionSupplier;

    public ReopeningSession(Supplier<CqlSession> sessionSupplier)
    {
        this.sessionSupplier = requireNonNull(sessionSupplier, "sessionSupplier is null");
    }

    public synchronized CqlSession get()
    {
        checkState(!closed, "Session has been closed");

        if (delegate == null) {
            delegate = sessionSupplier.get();
        }

        if (delegate.isClosed()) {
            log.warn("Session has been closed internally, reopening...");
            delegate = sessionSupplier.get();
        }

        verify(!delegate.isClosed(), "Newly created session has been immediately closed");

        return delegate;
    }

    /**
     * Alias for get() method to maintain compatibility with code expecting getSession()
     */
    public synchronized CqlSession getSession()
    {
        return get();
    }

    public synchronized void close()
    {
        closed = true;
        if (delegate != null && !delegate.isClosed()) {
            delegate.close();
            delegate = null;
        }
    }

    public synchronized boolean isClosed()
    {
        return closed;
    }
}

// Made with Bob
