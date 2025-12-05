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
package com.facebook.presto.flightshim;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.redis.RedisPlugin;
import com.facebook.presto.redis.RedisTableDescription;
import com.facebook.presto.redis.util.EmbeddedRedis;
import com.facebook.presto.redis.util.RedisTestUtils;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.TestingPrestoClient;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airlift.tpch.TpchTable;
import org.apache.arrow.flight.FlightServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.airlift.units.Duration.nanosSince;
import static com.facebook.presto.flightshim.NativeArrowFederationConnectorUtils.createJavaQueryRunner;
import static com.facebook.presto.flightshim.NativeArrowFederationConnectorUtils.createNativeQueryRunner;
import static com.facebook.presto.flightshim.NativeArrowFederationConnectorUtils.setUpFlightServer;
import static com.facebook.presto.redis.RedisQueryRunner.createTpchTableDescriptions;
import static com.facebook.presto.redis.util.EmbeddedRedis.createEmbeddedRedis;
import static com.facebook.presto.redis.util.RedisTestUtils.createEmptyTableDescriptions;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.Locale.ENGLISH;

@Test(singleThreaded = true)
public class TestArrowFederationNativeQueriesRedis
        extends AbstractTestArrowFederationNativeQueries
{
    private final EmbeddedRedis embeddedRedis;

    private static final String CONNECTOR_ID = "redis";
    private static final String PLUGIN_BUNDLES = "../presto-redis/pom.xml";
    private static final Logger log = Logger.get(TestArrowFederationNativeQueriesRedis.class);

    private final List<AutoCloseable> closeables = new ArrayList<>();
    private FlightServer server;

    public TestArrowFederationNativeQueriesRedis()
            throws IOException, URISyntaxException
    {
        embeddedRedis = createEmbeddedRedis();
        embeddedRedis.start();
        closeables.add(embeddedRedis);
    }

    @BeforeClass
    public void setUp()
            throws Exception
    {
        if (server != null) {
            return;
        }
        server = setUpFlightServer(
                ImmutableMap.of(
                        CONNECTOR_ID,
                        getConnectorProperties(embeddedRedis, createEmptyTableDescriptions())),
                PLUGIN_BUNDLES,
                closeables);
    }

    @AfterClass(alwaysRun = true)
    public void close()
            throws Exception
    {
        for (AutoCloseable closeable : Lists.reverse(closeables)) {
            closeable.close();
        }
    }

    @Override
    public Session getSession()
    {
        return testSessionBuilder()
                .setCatalog("redis")
                .setSchema("tpch")
                .build();
    }

    @Override
    protected void createTables()
    {
        // hack: need the java query runner to generate tables
        try {
            DistributedQueryRunner queryRunner = (DistributedQueryRunner) createJavaQueryRunner();
            installRedisPlugin(
                    embeddedRedis,
                    queryRunner,
                    createTpchTableDescriptions(queryRunner.getCoordinator().getMetadata(), TpchTable.getTables(), "string"));
            createTpchTables(embeddedRedis, queryRunner);
            queryRunner.close();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        setUp();
        DistributedQueryRunner queryRunner =
                (DistributedQueryRunner) createNativeQueryRunner(ImmutableList.of(CONNECTOR_ID), server.getPort());
        installRedisPlugin(embeddedRedis, queryRunner,
                createTpchTableDescriptions(queryRunner.getCoordinator().getMetadata(), TpchTable.getTables(), "string"));
        return queryRunner;
    }

    // todo: Test all the below overrides when ConnectorOutputTableHandle is implemented
    @Override
    public void testCreateTable()
    {
        // Create is currently unsupported
    }

    @Override
    public void testInsert()
    {
        // Insert is currently unsupported
    }

    @Override
    public void testDelete()
    {
        // Delete is currently unsupported
    }

    @Override
    public void testUpdate()
    {
        // Updates are not supported by the connector
    }

    @Override
    public void testNonAutoCommitTransactionWithRollback()
    {
        // multi-statement writes within transactions not supported
    }

    @Override
    public void testNonAutoCommitTransactionWithCommit()
    {
        // multi-statement writes within transactions not supported
    }

    @Override
    public void testNonAutoCommitTransactionWithFailAndRollback()
    {
        // multi-statement writes within transactions not supported
    }

    @Override
    public void testPayloadJoinApplicability()
    {
    }

    @Override
    public void testPayloadJoinCorrectness()
    {
    }

    @Override
    public void testRemoveRedundantCastToVarcharInJoinClause()
    {
    }

    @Override
    public void testSubfieldAccessControl()
    {
    }

    static void createTpchTables(EmbeddedRedis embeddedRedis, QueryRunner queryRunner)
    {
        TestingPrestoClient prestoClient = ((DistributedQueryRunner) queryRunner).getRandomClient();
        for (TpchTable<?> table : TpchTable.getTables()) {
            loadTpchTable(embeddedRedis, prestoClient, table, "string");
        }
        embeddedRedis.destroyJedisPool();
    }

    static void installRedisPlugin(EmbeddedRedis embeddedRedis, QueryRunner queryRunner, Map<SchemaTableName, RedisTableDescription> tableDescriptions)
    {
        RedisPlugin redisPlugin = new RedisPlugin();
        redisPlugin.setTableDescriptionSupplier(() -> tableDescriptions);
        queryRunner.installPlugin(redisPlugin);
        queryRunner.createCatalog("redis", "redis",
                getConnectorProperties(embeddedRedis, tableDescriptions));
    }

    static Map<String, String> getConnectorProperties(EmbeddedRedis embeddedRedis, Map<SchemaTableName, RedisTableDescription> tableDescriptions)
    {
        Map<String, String> connectorProperties = new HashMap<>();
        connectorProperties.putIfAbsent("redis.nodes", embeddedRedis.getConnectString() + ":" + embeddedRedis.getPort());
        connectorProperties.putIfAbsent("redis.table-names", Joiner.on(",").join(tableDescriptions.keySet()));
        connectorProperties.putIfAbsent("redis.default-schema", "default");
        connectorProperties.putIfAbsent("redis.hide-internal-columns", "true");
        connectorProperties.putIfAbsent("redis.key-prefix-schema-table", "true");
        return ImmutableMap.copyOf(connectorProperties);
    }

    private static void loadTpchTable(EmbeddedRedis embeddedRedis, TestingPrestoClient prestoClient, TpchTable<?> table, String dataFormat)
    {
        long start = System.nanoTime();
        log.info("Running import for %s", table.getTableName());
        RedisTestUtils.loadTpchTable(
                embeddedRedis,
                prestoClient,
                redisTableName(table),
                new QualifiedObjectName("tpch", "tiny", table.getTableName().toLowerCase(ENGLISH)),
                dataFormat);
        log.info("Imported %s in %s", table.getTableName(), nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    private static String redisTableName(TpchTable<?> table)
    {
        return "tpch" + ":" + table.getTableName().toLowerCase(ENGLISH);
    }
}
