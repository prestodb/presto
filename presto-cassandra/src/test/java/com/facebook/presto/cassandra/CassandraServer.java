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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.units.Duration;
import com.google.common.io.Resources;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.Files.write;
import static com.google.common.io.Resources.getResource;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testcontainers.utility.MountableFile.forHostPath;

public class CassandraServer
        implements Closeable

{
    private static Logger log = Logger.get(CassandraServer.class);

    private static final int PORT = 9142;

    private static final Duration REFRESH_SIZE_ESTIMATES_TIMEOUT = new Duration(1, MINUTES);

    private final GenericContainer<?> dockerContainer;
    private final ReopeningSession reopeningSession;
    private final CassandraSession session;
    private final Metadata metadata;

    public CassandraServer()
            throws Exception
    {
        log.info("Starting cassandra...");

        GenericContainer<?> container = new GenericContainer<>("cassandra:3.11.19")
                .withExposedPorts(PORT)
                .withCopyFileToContainer(forHostPath(prepareCassandraYaml()), "/etc/cassandra/cassandra.yaml")
                // Wait for Cassandra to be ready - this ensures it's fully started before tests
                // Wait for the log message indicating CQL clients can connect
                .waitingFor(Wait.forLogMessage(".*Starting listening for CQL clients.*", 1)
                        .withStartupTimeout(java.time.Duration.ofSeconds(120)));
        this.dockerContainer = container;
        this.dockerContainer.start();

        // Additional readiness check after container is "up"
        // This ensures Cassandra is not just started but also ready for schema operations
        waitForCassandraReady();

        // Driver 4.x: Use CqlSession.builder() instead of Cluster.builder()
        // Note: Driver 4.x doesn't have METADATA_SCHEMA_AGREEMENT_WAIT - schema agreement is handled automatically
        // Use getHost() instead of getContainerIpAddress() when using port mapping
        // getHost() returns "localhost" for local Docker, which works with mapped ports
        String host = this.dockerContainer.getHost();
        int mappedPort = this.dockerContainer.getMappedPort(PORT);
        log.info("Connecting to Cassandra at %s:%d", host, mappedPort);

        // Configure connection pool to handle high concurrency during metadata refresh operations
        // The default pool size (1 connection) is too small for tests that perform many
        // concurrent metadata queries, leading to StacklessClosedChannelException errors
        // Reference: https://apache.github.io/cassandra-java-driver/4.19.0/core/pooling/
        ProgrammaticDriverConfigLoaderBuilder configBuilder = DriverConfigLoader.programmaticBuilder();
        configBuilder.withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, 4);  // Increase from default 1
        configBuilder.withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, 4);
        configBuilder.withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, java.time.Duration.ofSeconds(30));
        configBuilder.withDuration(DefaultDriverOption.REQUEST_TIMEOUT, java.time.Duration.ofSeconds(30));
        configBuilder.withDuration(DefaultDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT, java.time.Duration.ofSeconds(30));
        configBuilder.withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, 1024);  // Keep default
        configBuilder.withDuration(DefaultDriverOption.HEARTBEAT_INTERVAL, java.time.Duration.ofSeconds(30));
        configBuilder.withDuration(DefaultDriverOption.HEARTBEAT_TIMEOUT, java.time.Duration.ofMillis(500));

        log.info("Configuring Cassandra driver with increased connection pool size (4 connections per host) to handle concurrent metadata operations");

        this.reopeningSession = new ReopeningSession(() -> {
            return CqlSession.builder()
                    .addContactPoint(new InetSocketAddress(host, mappedPort))
                    .withLocalDatacenter("datacenter1")
                    .addTypeCodecs(TimestampCodec.INSTANCE)  // Register custom TIMESTAMP codec
                    .withConfigLoader(configBuilder.build())
                    .build();
        });
        CassandraSession session = new NativeCassandraSession(
                "EmbeddedCassandra",
                JsonCodec.listJsonCodec(ExtraColumnMetadata.class),
                reopeningSession,
                new Duration(1, MINUTES),
                false);
        this.metadata = this.reopeningSession.getMetadata();

        try {
            // Verify connectivity with the main session
            checkConnectivity(session);

            // Additional verification: ensure the session can perform operations
            // This confirms the connection is fully functional
            log.info("Verifying session connectivity with a test query...");
            ResultSet testResult = session.execute("SELECT now() FROM system.local");
            if (testResult.one() == null) {
                throw new RuntimeException("Session connectivity test failed - query returned no results");
            }
            log.info("Session connectivity verified successfully");
        }
        catch (RuntimeException e) {
            log.error(e, "Failed to establish connectivity with Cassandra");
            this.reopeningSession.close();
            this.dockerContainer.stop();
            throw e;
        }

        this.session = session;
    }

    private static String prepareCassandraYaml()
            throws IOException
    {
        String original = Resources.toString(getResource("cu-cassandra.yaml"), UTF_8);

        File tempDirFile = createTempDir();
        tempDirFile.deleteOnExit();
        Path tmpDirPath = tempDirFile.toPath();
        Path dataDir = tmpDirPath.resolve("data");
        Files.createDirectory(dataDir);

        Path yamlLocation = tmpDirPath.resolve("cu-cassandra.yaml");
        write(original, yamlLocation.toFile(), UTF_8);

        return yamlLocation.toAbsolutePath().toString();
    }

    public CassandraSession getSession()
    {
        return requireNonNull(session, "cluster is null");
    }

    public Metadata getMetadata()
    {
        return metadata;
    }

    public String getHost()
    {
        // Use getHost() instead of getContainerIpAddress() when using port mapping
        // getHost() returns "localhost" for local Docker, which works with mapped ports
        return dockerContainer.getHost();
    }

    public int getPort()
    {
        return dockerContainer.getMappedPort(PORT);
    }

    /**
     * Wait for Cassandra to be fully ready for operations.
     * This method verifies both basic connectivity and schema operation readiness.
     * Uses the driver's metadata API to check readiness, as recommended in:
     * https://apache.github.io/cassandra-java-driver/4.19.0/core/metadata/schema/
     */
    private void waitForCassandraReady()
            throws Exception
    {
        int maxAttempts = 60;  // Increased from 30 to allow more time for startup
        int delayMs = 1000;
        String host = this.dockerContainer.getHost();
        int mappedPort = this.dockerContainer.getMappedPort(PORT);

        log.info("Waiting for Cassandra to be ready at %s:%d...", host, mappedPort);

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                // Try to create a temporary session to verify readiness
                // Use getHost() for port-mapped containers
                try (CqlSession testSession = CqlSession.builder()
                        .addContactPoint(new InetSocketAddress(host, mappedPort))
                        .withLocalDatacenter("datacenter1")
                        .build()) {
                    // Verify basic connectivity by querying system.local
                    // This also ensures the network connection is fully established
                    ResultSet result = testSession.execute("SELECT release_version FROM system.local");
                    Row versionRow = result.one();
                    if (versionRow == null) {
                        throw new RuntimeException("Failed to get Cassandra version from system.local");
                    }
                    String version = versionRow.getString("release_version");
                    log.info("Cassandra version: %s", version);

                    // Verify schema metadata is available using the driver's metadata API
                    // This ensures Cassandra is ready for DDL operations and the driver can access schema
                    // Accessing getKeyspaces() triggers metadata refresh if needed and verifies schema readiness
                    // See: https://apache.github.io/cassandra-java-driver/4.19.0/core/metadata/schema/
                    java.util.Map<CqlIdentifier, KeyspaceMetadata> keyspaces = testSession.getMetadata().getKeyspaces();
                    if (keyspaces == null || keyspaces.isEmpty()) {
                        throw new RuntimeException("Schema metadata not available - keyspaces map is null or empty");
                    }

                    // Verify we can access at least the system keyspace (should always exist)
                    KeyspaceMetadata systemKeyspace = keyspaces.get(CqlIdentifier.fromCql("system"));
                    if (systemKeyspace == null) {
                        throw new RuntimeException("System keyspace not found in metadata - schema not fully ready");
                    }

                    // Additional verification: ensure we can perform a simple query
                    // This confirms the connection is fully functional, not just established
                    testSession.execute("SELECT now() FROM system.local").one();

                    log.info("Cassandra is ready after %d attempts (found %d keyspaces in metadata)", attempt, keyspaces.size());

                    // Add a small delay to ensure all internal Cassandra services are fully initialized
                    Thread.sleep(500);
                    return;
                }
            }
            catch (Exception e) {
                if (attempt < maxAttempts) {
                    log.debug("Cassandra not ready yet (attempt %d/%d): %s", attempt, maxAttempts, e.getMessage());
                    Thread.sleep(delayMs);
                }
                else {
                    throw new RuntimeException(
                            format("Cassandra failed to become ready after %d attempts (waited %d seconds) at %s:%d",
                                    maxAttempts, maxAttempts * delayMs / 1000, host, mappedPort), e);
                }
            }
        }
    }

    private static void checkConnectivity(CassandraSession session)
    {
        ResultSet result = session.execute("SELECT release_version FROM system.local");
        Row versionRow = result.one();
        if (versionRow == null) {
            throw new RuntimeException("Failed to get Cassandra version");
        }
        String version = versionRow.getString("release_version");
        log.info("Cassandra version: %s", version);
    }

    public void refreshSizeEstimates(String keyspace, String table)
            throws Exception
    {
        long deadline = System.nanoTime() + REFRESH_SIZE_ESTIMATES_TIMEOUT.roundTo(NANOSECONDS);
        while (System.nanoTime() - deadline < 0) {
            flushTable(keyspace, table);
            refreshSizeEstimates();
            List<SizeEstimate> sizeEstimates = getSession().getSizeEstimates(keyspace, table);
            if (!sizeEstimates.isEmpty()) {
                log.info("Size estimates for the table %s.%s have been refreshed successfully: %s", keyspace, table, sizeEstimates);
                return;
            }
            log.info("Size estimates haven't been refreshed as expected. Retrying ...");
            SECONDS.sleep(1);
        }
        throw new TimeoutException(format("Attempting to refresh size estimates for table %s.%s has timed out after %s", keyspace, table, REFRESH_SIZE_ESTIMATES_TIMEOUT));
    }

    private void flushTable(String keyspace, String table)
            throws Exception
    {
        dockerContainer.execInContainer("nodetool", "flush", keyspace, table);
    }

    /**
     * Flush a table to ensure data is written to disk.
     * This is useful after INSERT operations to ensure data is immediately visible.
     */
    public void flushTablePublic(String keyspace, String table)
            throws Exception
    {
        flushTable(keyspace, table);
    }

    private void refreshSizeEstimates()
            throws Exception
    {
        dockerContainer.execInContainer("nodetool", "refreshsizeestimates");
    }

    /**
     * Force refresh of the Cassandra driver's metadata cache.
     * Uses the ReopeningSession's improved metadata refresh mechanism that queries
     * system tables and waits for schema agreement.
     */
    public void refreshMetadata()
    {
        log.info("Forcing metadata refresh in Cassandra driver");
        reopeningSession.forceMetadataRefresh();
        log.info("Metadata refresh completed");
    }

    /**
     * Force refresh of metadata for a specific keyspace and table.
     * This is more targeted and faster than refreshing all metadata.
     *
     * @param keyspace The keyspace name
     * @param table The table name
     */
    public void refreshMetadata(String keyspace, String table)
    {
        log.info("Forcing metadata refresh for keyspace=%s, table=%s", keyspace, table);
        reopeningSession.forceMetadataRefresh(keyspace, table);
        log.info("Metadata refresh completed for keyspace=%s, table=%s", keyspace, table);
    }

    /**
     * Force a session reconnection to get completely fresh metadata from Cassandra.
     * This is more aggressive than refreshMetadata() and should only be used when
     * we detect that the driver's metadata cache is severely stale.
     *
     * NOTE: This method now uses the improved metadata refresh instead of closing the session.
     * Closing and reopening the session is expensive and disruptive. The new approach queries
     * system tables and waits for schema agreement, which is more reliable and faster.
     */
    public void forceSessionReconnect()
    {
        log.info("Forcing metadata refresh (no longer closes session)");
        // Use the improved metadata refresh instead of closing the session
        reopeningSession.forceMetadataRefresh();
        log.info("Metadata refresh completed");
    }

    @Override
    public void close()
    {
        // Close resources in reverse order of creation
        // First close the ReopeningSession which will close the underlying CqlSession
        if (reopeningSession != null) {
            try {
                log.info("Closing ReopeningSession and underlying CqlSession");
                reopeningSession.close();
            }
            catch (Exception e) {
                log.warn(e, "Error closing ReopeningSession");
            }
        }

        // Then close the Docker container
        dockerContainer.close();
    }
}
