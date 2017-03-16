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
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;

import javax.management.ObjectName;

import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public final class EmbeddedCassandra
{
    private static Logger log = Logger.get(EmbeddedCassandra.class);

    private static Cluster cluster;
    private static boolean initialized;

    private EmbeddedCassandra() {}

    public static synchronized void start()
            throws Exception
    {
        if (initialized) {
            return;
        }
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();
        Cluster cluster = Cluster.builder()
                .withClusterName("TestCluster")
                .addContactPointsWithPorts(ImmutableList.of(
                        new InetSocketAddress(
                                EmbeddedCassandraServerHelper.getHost(),
                                EmbeddedCassandraServerHelper.getNativeTransportPort())))
                .build();
        try {
            checkConnectivity(cluster);
        }
        catch (RuntimeException e) {
            cluster.close();
            throw e;
        }
        EmbeddedCassandra.cluster = cluster;
        initialized = true;
    }

    public static synchronized Cluster getCluster()
    {
        checkIsInitialized();
        return requireNonNull(cluster, "cluster is null");
    }

    public static synchronized String getHost()
    {
        checkIsInitialized();
        return EmbeddedCassandraServerHelper.getHost();
    }

    public static synchronized int getPort()
    {
        checkIsInitialized();
        return EmbeddedCassandraServerHelper.getNativeTransportPort();
    }

    private static void checkIsInitialized()
    {
        checkState(initialized, "EmbeddedCassandra must be started with #start() method before retrieving the cluster retrieval");
    }

    private static void checkConnectivity(Cluster cluster)
    {
        try (Session session = cluster.connect()) {
            ResultSet result = session.execute("SELECT release_version FROM system.local");
            List<Row> rows = result.all();
            assertEquals(rows.size(), 1);
            String version = rows.get(0).getString(0);
            log.info("Cassandra version: %s", version);
        }
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
