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
package com.facebook.presto.accumulo;

import com.facebook.presto.Session;
import com.facebook.presto.accumulo.conf.AccumuloConfig;
import com.facebook.presto.accumulo.serializers.LexicoderRowSerializer;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import io.airlift.log.Logger;
import io.airlift.tpch.TpchTable;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
import org.intellij.lang.annotations.Language;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;

import static com.facebook.presto.accumulo.AccumuloErrorCode.MINI_ACCUMULO;
import static com.facebook.presto.accumulo.AccumuloErrorCode.UNEXPECTED_ACCUMULO_ERROR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class AccumuloQueryRunner
{
    private static final Logger LOG = Logger.get(AccumuloQueryRunner.class);
    private static final String MAC_PASSWORD = "secret";
    private static final String MAC_USER = "root";

    private static boolean tpchLoaded = false;
    private static Connector connector = getAccumuloConnector();

    private AccumuloQueryRunner() {}

    public static synchronized DistributedQueryRunner createAccumuloQueryRunner(Map<String, String> extraProperties)
            throws Exception
    {
        DistributedQueryRunner queryRunner =
                new DistributedQueryRunner(createSession(), 4, extraProperties);

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        queryRunner.installPlugin(new AccumuloPlugin());
        Map<String, String> accumuloProperties =
                ImmutableMap.<String, String>builder()
                        .put(AccumuloConfig.INSTANCE, connector.getInstance().getInstanceName())
                        .put(AccumuloConfig.ZOOKEEPERS, connector.getInstance().getZooKeepers())
                        .put(AccumuloConfig.USERNAME, MAC_USER)
                        .put(AccumuloConfig.PASSWORD, MAC_PASSWORD)
                        .put(AccumuloConfig.ZOOKEEPER_METADATA_ROOT, "/presto-accumulo-test")
                        .build();

        queryRunner.createCatalog("accumulo", "accumulo", accumuloProperties);

        if (!tpchLoaded) {
            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(), TpchTable.getTables());
            connector.tableOperations().addSplits("tpch.orders", ImmutableSortedSet.of(new Text(new LexicoderRowSerializer().encode(BIGINT, 7500L))));
            tpchLoaded = true;
        }

        return queryRunner;
    }

    private static void copyTpchTables(
            QueryRunner queryRunner,
            String sourceCatalog,
            String sourceSchema,
            Session session,
            Iterable<TpchTable<?>> tables)
    {
        LOG.info("Loading data from %s.%s...", sourceCatalog, sourceSchema);
        long startTime = System.nanoTime();
        for (TpchTable<?> table : tables) {
            copyTable(queryRunner, sourceCatalog, session, sourceSchema, table);
        }
        LOG.info("Loading from %s.%s complete in %s", sourceCatalog, sourceSchema, nanosSince(startTime).toString(SECONDS));
    }

    private static void copyTable(
            QueryRunner queryRunner,
            String catalog,
            Session session,
            String schema,
            TpchTable<?> table)
    {
        QualifiedObjectName source = new QualifiedObjectName(catalog, schema, table.getTableName());
        String target = table.getTableName();

        @Language("SQL")
        String sql;
        switch (target) {
            case "customer":
                sql = format("CREATE TABLE %s WITH (index_columns = 'mktsegment') AS SELECT * FROM %s", target, source);
                break;
            case "lineitem":
                sql = format("CREATE TABLE %s WITH (index_columns = 'quantity,discount,returnflag,shipdate,receiptdate,shipinstruct,shipmode') AS SELECT UUID() AS uuid, * FROM %s", target, source);
                break;
            case "orders":
                sql = format("CREATE TABLE %s WITH (index_columns = 'orderdate') AS SELECT * FROM %s", target, source);
                break;
            case "part":
                sql = format("CREATE TABLE %s WITH (index_columns = 'brand,type,size,container') AS SELECT * FROM %s", target, source);
                break;
            case "partsupp":
                sql = format("CREATE TABLE %s WITH (index_columns = 'partkey') AS SELECT UUID() AS uuid, * FROM %s", target, source);
                break;
            case "supplier":
                sql = format("CREATE TABLE %s WITH (index_columns = 'name') AS SELECT * FROM %s", target, source);
                break;
            default:
                sql = format("CREATE TABLE %s AS SELECT * FROM %s", target, source);
                break;
        }

        LOG.info("Running import for %s", target, sql);
        LOG.info("%s", sql);
        long start = System.nanoTime();
        long rows = queryRunner.execute(session, sql).getUpdateCount().getAsLong();
        LOG.info("Imported %s rows for %s in %s", rows, target, nanosSince(start));
    }

    public static Session createSession()
    {
        return testSessionBuilder().setCatalog("accumulo").setSchema("tpch").build();
    }

    /**
     * Gets the AccumuloConnector singleton, starting the MiniAccumuloCluster on initialization.
     * This singleton instance is required so all test cases access the same MiniAccumuloCluster.
     *
     * @return Accumulo connector
     */
    private static Connector getAccumuloConnector()
    {
        if (connector != null) {
            return connector;
        }

        try {
            MiniAccumuloCluster accumulo = createMiniAccumuloCluster();
            Instance instance = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers());
            connector = instance.getConnector(MAC_USER, new PasswordToken(MAC_PASSWORD));
            LOG.info("Connection to MAC instance %s at %s established, user %s password %s", accumulo.getInstanceName(), accumulo.getZooKeepers(), MAC_USER, MAC_PASSWORD);
            return connector;
        }
        catch (AccumuloException | AccumuloSecurityException | InterruptedException | IOException e) {
            throw new PrestoException(UNEXPECTED_ACCUMULO_ERROR, "Failed to get connector to Accumulo", e);
        }
    }

    /**
     * Creates and starts an instance of MiniAccumuloCluster, returning the new instance.
     *
     * @return New MiniAccumuloCluster
     */
    private static MiniAccumuloCluster createMiniAccumuloCluster()
            throws IOException, InterruptedException
    {
        // Create MAC directory
        File macDir = Files.createTempDirectory("mac-").toFile();
        LOG.info("MAC is enabled, starting MiniAccumuloCluster at %s", macDir);

        // Start MAC and connect to it
        MiniAccumuloCluster accumulo = new MiniAccumuloCluster(macDir, MAC_PASSWORD);
        accumulo.start();

        // Add shutdown hook to stop MAC and cleanup temporary files
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                LOG.info("Shutting down MAC");
                accumulo.stop();
            }
            catch (IOException | InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new PrestoException(MINI_ACCUMULO, "Failed to shut down MAC instance", e);
            }

            try {
                LOG.info("Cleaning up MAC directory");
                FileUtils.forceDelete(macDir);
            }
            catch (IOException e) {
                throw new PrestoException(MINI_ACCUMULO, "Failed to clean up MAC directory", e);
            }
        }));

        return accumulo;
    }
}
