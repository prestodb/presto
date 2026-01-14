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
     *
     * IMPORTANT: Driver 4.x does NOT provide a public API to manually refresh schema.
     * Per official documentation (https://apache.github.io/cassandra-java-driver/4.19.0/core/metadata/schema/):
     * - The driver automatically refreshes schema when it detects changes via system events
     * - There is no public method to force a manual refresh
     *
     * This method closes and reopens the session to force the driver to fetch completely
     * fresh metadata from Cassandra. This is the only reliable way to ensure metadata is current.
     */
    /**
     * Force a refresh of the driver's metadata cache by querying system tables.
     *
     * Driver 4.x caches metadata aggressively and doesn't provide a public API to force refresh.
     * This method forces the driver to reload metadata by:
     * 1. Querying system schema tables (forces driver to check for changes)
     * 2. Waiting for schema agreement across the cluster
     * 3. Accessing metadata objects to trigger internal refresh
     *
     * This is the most reliable way to ensure metadata is current without closing the session.
     */
    public void forceMetadataRefresh()
    {
        try {
            CqlSession currentSession = get();
            log.info("Forcing metadata refresh by querying system tables");

            // Step 1: Query system schema tables to force driver to check for schema changes
            // This triggers the driver's internal metadata refresh mechanism
            try {
                currentSession.execute("SELECT * FROM system_schema.keyspaces LIMIT 1");
                currentSession.execute("SELECT * FROM system_schema.tables LIMIT 1");
                log.debug("System schema tables queried successfully");
            }
            catch (Exception e) {
                log.warn(e, "Failed to query system schema tables");
            }

            // Step 2: Wait for schema agreement with timeout (up to 15 seconds)
            // Schema agreement means all nodes in the cluster have the same schema version
            boolean agreed = false;
            for (int i = 0; i < 30; i++) {
                agreed = currentSession.checkSchemaAgreement();
                if (agreed) {
                    log.info("Schema agreement achieved after %d attempts", i + 1);
                    break;
                }
                if (i < 29) {  // Don't sleep on last iteration
                    Thread.sleep(500);
                }
            }

            if (!agreed) {
                log.warn("Schema agreement not achieved after 15 seconds - metadata may be stale");
            }

            // Step 3: Access metadata objects to force driver to refresh its internal cache
            // The driver updates its metadata cache when we access it after schema changes
            Metadata metadata = currentSession.getMetadata();
            int keyspaceCount = metadata.getKeyspaces().size();
            log.info("Metadata refresh completed - %d keyspaces visible, schema agreement: %s",
                     keyspaceCount, agreed);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted during metadata refresh");
        }
        catch (Exception e) {
            log.warn(e, "Error during metadata refresh");
            // Don't throw - metadata refresh failures shouldn't break operations
        }
    }

    /**
     * Force a refresh of metadata for a specific keyspace and table.
     * This is more targeted than forceMetadataRefresh() and can be faster.
     *
     * @param keyspace The keyspace name
     * @param table The table name (can be null to refresh entire keyspace)
     */
    public void forceMetadataRefresh(String keyspace, String table)
    {
        try {
            CqlSession currentSession = get();
            log.info("Forcing metadata refresh for keyspace=%s, table=%s", keyspace, table);

            // Query specific keyspace/table in system schema
            try {
                currentSession.execute(
                        "SELECT * FROM system_schema.keyspaces WHERE keyspace_name = ?",
                        keyspace);

                if (table != null) {
                    currentSession.execute(
                            "SELECT * FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
                            keyspace, table);
                }
                else {
                    currentSession.execute(
                            "SELECT * FROM system_schema.tables WHERE keyspace_name = ?",
                            keyspace);
                }
                log.debug("System schema queried for keyspace=%s, table=%s", keyspace, table);
            }
            catch (Exception e) {
                log.warn(e, "Failed to query system schema for keyspace=%s, table=%s", keyspace, table);
            }

            // Wait for schema agreement
            boolean agreed = false;
            for (int i = 0; i < 30; i++) {
                agreed = currentSession.checkSchemaAgreement();
                if (agreed) {
                    log.info("Schema agreement achieved after %d attempts for keyspace=%s", i + 1, keyspace);
                    break;
                }
                if (i < 29) {
                    Thread.sleep(500);
                }
            }

            // Access specific keyspace metadata to trigger refresh
            Metadata metadata = currentSession.getMetadata();
            metadata.getKeyspace(keyspace).ifPresent(ks -> {
                int tableCount = ks.getTables().size();
                log.info("Keyspace %s has %d tables visible after refresh", keyspace, tableCount);
            });

            log.info("Metadata refresh completed for keyspace=%s, schema agreement: %s", keyspace, agreed);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted during metadata refresh");
        }
        catch (Exception e) {
            log.warn(e, "Error during metadata refresh for keyspace=%s", keyspace);
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
