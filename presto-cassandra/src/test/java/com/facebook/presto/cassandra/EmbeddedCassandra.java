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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.apache.cassandra.service.CassandraDaemon;

import javax.management.ObjectName;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static com.datastax.driver.core.ProtocolVersion.V3;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.Files.write;
import static com.google.common.io.Resources.getResource;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.testng.Assert.assertEquals;

public final class EmbeddedCassandra
{
    private static Logger log = Logger.get(EmbeddedCassandra.class);

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 9142;

    private static CassandraSession session;
    private static boolean initialized;

    private EmbeddedCassandra() {}

    public static synchronized void start()
            throws Exception
    {
        if (initialized) {
            return;
        }

        log.info("Starting cassandra...");

        System.setProperty("cassandra.config", "file:" + prepareCassandraYaml());
        System.setProperty("cassandra-foreground", "true");
        System.setProperty("cassandra.native.epoll.enabled", "false");

        CassandraDaemon cassandraDaemon = new CassandraDaemon();
        cassandraDaemon.activate();

        Cluster cluster = Cluster.builder()
                .withProtocolVersion(V3)
                .withClusterName("TestCluster")
                .addContactPointsWithPorts(ImmutableList.of(
                        new InetSocketAddress(HOST, PORT)))
                .build();

        CassandraSession session = new NativeCassandraSession(
                "EmbeddedCassandra",
                JsonCodec.listJsonCodec(ExtraColumnMetadata.class),
                cluster,
                new Duration(1, MINUTES));

        try {
            checkConnectivity(session);
        }
        catch (RuntimeException e) {
            cluster.close();
            cassandraDaemon.deactivate();
            throw e;
        }

        EmbeddedCassandra.session = session;
        initialized = true;
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

        String modified = original.replaceAll("\\$\\{data_directory\\}", dataDir.toAbsolutePath().toString());

        Path yamlLocation = tmpDirPath.resolve("cu-cassandra.yaml");
        write(modified, yamlLocation.toFile(), UTF_8);

        return yamlLocation.toAbsolutePath().toString();
    }

    public static synchronized CassandraSession getSession()
    {
        checkIsInitialized();
        return requireNonNull(session, "cluster is null");
    }

    public static synchronized String getHost()
    {
        checkIsInitialized();
        return HOST;
    }

    public static synchronized int getPort()
    {
        checkIsInitialized();
        return PORT;
    }

    private static void checkIsInitialized()
    {
        checkState(initialized, "EmbeddedCassandra must be started with #start() method before retrieving the cluster retrieval");
    }

    private static void checkConnectivity(CassandraSession session)
    {
        ResultSet result = session.execute("SELECT release_version FROM system.local");
        List<Row> rows = result.all();
        assertEquals(rows.size(), 1);
        String version = rows.get(0).getString(0);
        log.info("Cassandra version: %s", version);
    }

    public static void flush(String keyspace, String table)
            throws Exception
    {
        ManagementFactory
                .getPlatformMBeanServer()
                .invoke(
                        new ObjectName("org.apache.cassandra.db:type=StorageService"),
                        "forceKeyspaceFlush",
                        new Object[] {keyspace, new String[] {table}},
                        new String[] {"java.lang.String", "[Ljava.lang.String;"});
    }

    public static void refreshSizeEstimates()
            throws Exception
    {
        ManagementFactory
                .getPlatformMBeanServer()
                .invoke(
                        new ObjectName("org.apache.cassandra.db:type=StorageService"),
                        "refreshSizeEstimates",
                        new Object[] {},
                        new String[] {});
    }
}
