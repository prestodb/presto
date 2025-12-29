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
        // Lazy initialization: Don't create session in constructor
        // This defers DNS resolution and connection until first actual use
        // Helps avoid startup failures in Docker environments where services may not be immediately available
    }

    /**
     * Get the current session, creating or reopening it if necessary.
     * This method is thread-safe and will only create/reopen the session once if multiple threads
     * detect it is null or closed simultaneously.
     */
    public CqlSession get()
    {
        CqlSession currentSession = session.get();

        // Handle lazy initialization (session is null) or reopening (session is closed)
        if (currentSession == null || currentSession.isClosed()) {
            synchronized (this) {
                currentSession = session.get();
                if (currentSession == null || currentSession.isClosed()) {
                    if (currentSession == null) {
                        log.info("Initializing Cassandra session (lazy initialization)...");
                    }
                    else {
                        log.info("Session closed, reopening...");
                    }
                    try {
                        currentSession = sessionSupplier.get();
                        session.set(currentSession);
                        log.info("Session initialized/reopened successfully");
                    }
                    catch (Exception e) {
                        log.error(e, "Failed to initialize/reopen session");
                        throw new RuntimeException("Failed to initialize/reopen Cassandra session", e);
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

    /**
     * Force a refresh of the driver's metadata cache.
     * In Driver 4.x, metadata is cached and needs explicit refresh after schema changes.
     * This method forces the driver to reload schema information from Cassandra.
     */
    public void forceMetadataRefresh()
    {
        CqlSession currentSession = get();
        try {
            // Driver 4.x: Use checkSchemaAgreement() to force metadata refresh
            // This method queries system tables and updates the driver's metadata cache
            currentSession.checkSchemaAgreement();

            // Additional refresh: Access metadata to ensure it's loaded
            currentSession.getMetadata().getKeyspaces();

            log.info("Forced metadata refresh completed");
        }
        catch (Exception e) {
            log.warn(e, "Error during forced metadata refresh");
            // Don't throw - metadata refresh failures shouldn't break the session
        }
    }

    /**
     * Get the underlying CqlSession.
     * Use with caution - prefer using get() for most operations.
     */
    public CqlSession getSession()
    {
        return get();
    }
}
