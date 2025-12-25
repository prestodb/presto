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
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.units.Duration;
import com.google.common.io.Resources;
import org.testcontainers.containers.GenericContainer;

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

    private final CassandraSession session;
    private final Metadata metadata;

    public CassandraServer()
            throws Exception
    {
        log.info("Starting cassandra...");

        this.dockerContainer = new GenericContainer<>("cassandra:3.11.19")
                .withExposedPorts(PORT)
                .withCopyFileToContainer(forHostPath(prepareCassandraYaml()), "/etc/cassandra/cassandra.yaml");
        this.dockerContainer.start();

        // Driver 4.x: Use CqlSession.builder() instead of Cluster.builder()
        // Note: Driver 4.x doesn't have METADATA_SCHEMA_AGREEMENT_WAIT - schema agreement is handled automatically
        ReopeningSession reopeningSession = new ReopeningSession(() -> {
            return CqlSession.builder()
                    .addContactPoint(new InetSocketAddress(
                            this.dockerContainer.getContainerIpAddress(),
                            this.dockerContainer.getMappedPort(PORT)))
                    .withLocalDatacenter("datacenter1")
                    .build();
        });
        CassandraSession session = new NativeCassandraSession(
                "EmbeddedCassandra",
                JsonCodec.listJsonCodec(ExtraColumnMetadata.class),
                reopeningSession,
                new Duration(1, MINUTES),
                false);
        this.metadata = reopeningSession.getMetadata();

        try {
            checkConnectivity(session);
        }
        catch (RuntimeException e) {
            reopeningSession.close();
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
        return dockerContainer.getContainerIpAddress();
    }

    public int getPort()
    {
        return dockerContainer.getMappedPort(PORT);
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

    private void refreshSizeEstimates()
            throws Exception
    {
        dockerContainer.execInContainer("nodetool", "refreshsizeestimates");
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }
}
