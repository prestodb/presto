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
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.facebook.airlift.log.Logger;
import com.google.common.base.Supplier;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

/**
 * Wrapper around CqlSession that automatically reopens the session if it gets closed.
 * This is useful for handling connection failures and ensuring the session remains available.
 */
@ThreadSafe
public class ReopeningSession
{
    private static final Logger log = Logger.get(ReopeningSession.class);

    private final Supplier<CqlSession> sessionSupplier;
    private final AtomicReference<CqlSession> session = new AtomicReference<>();

    public ReopeningSession(Supplier<CqlSession> sessionSupplier)
    {
        this.sessionSupplier = requireNonNull(sessionSupplier, "sessionSupplier is null");
        this.session.set(sessionSupplier.get());
    }

    /**
     * Get the current session, reopening it if necessary.
     * This method is thread-safe and will only reopen the session once if multiple threads
     * detect it is closed simultaneously.
     */
    public CqlSession get()
    {
        CqlSession currentSession = session.get();
        if (currentSession.isClosed()) {
            synchronized (this) {
                currentSession = session.get();
                if (currentSession.isClosed()) {
                    log.info("Session closed, reopening...");
                    try {
                        currentSession = sessionSupplier.get();
                        session.set(currentSession);
                        log.info("Session reopened successfully");
                    }
                    catch (Exception e) {
                        log.error(e, "Failed to reopen session");
                        throw new RuntimeException("Failed to reopen Cassandra session", e);
                    }
                }
            }
        }
        return currentSession;
    }

    /**
     * Get the metadata from the current session.
     * Note: Metadata in driver 4.x is immutable and atomically updated.
     * Call this method each time you need fresh metadata.
     */
    public Metadata getMetadata()
    {
        return get().getMetadata();
    }

    /**
     * Close the current session.
     * This will trigger a reopen on the next call to get().
     */
    public void close()
    {
        CqlSession currentSession = session.get();
        if (currentSession != null && !currentSession.isClosed()) {
            try {
                currentSession.close();
                log.info("Session closed");
            }
            catch (Exception e) {
                log.warn(e, "Error closing session");
            }
        }
    }

    /**
     * Check if the current session is closed.
     */
    public boolean isClosed()
    {
        CqlSession currentSession = session.get();
        return currentSession == null || currentSession.isClosed();
    }
}
