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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.mongodb.MongoPlugin;
import com.facebook.presto.mongodb.MongoQueryRunner;
import com.facebook.presto.plugin.mysql.MySqlPlugin;
import com.facebook.presto.plugin.postgresql.PostgreSqlPlugin;
import com.facebook.presto.redis.util.EmbeddedRedis;
import com.facebook.presto.split.PageSourceManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.tpch.TpchTable;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static com.facebook.presto.flightshim.NativeArrowFederationConnectorUtils.createJavaQueryRunner;
import static com.facebook.presto.flightshim.NativeArrowFederationConnectorUtils.createJdbcConnectorProperties;
import static com.facebook.presto.flightshim.NativeArrowFederationConnectorUtils.createNativeQueryRunner;
import static com.facebook.presto.flightshim.NativeArrowFederationConnectorUtils.getFlightServerShimConfig;
import static com.facebook.presto.flightshim.TestArrowFederationNativeQueriesMongo.createMongoConnectorProperties;
import static com.facebook.presto.flightshim.TestArrowFederationNativeQueriesMongo.getMongoDbSeeds;
import static com.facebook.presto.flightshim.TestArrowFederationNativeQueriesMySql.getConnectionUrl;
import static com.facebook.presto.flightshim.TestArrowFederationNativeQueriesRedis.createRedisConnectorProperties;
import static com.facebook.presto.flightshim.TestArrowFederationNativeQueriesRedis.installRedisPlugin;
import static com.facebook.presto.mongodb.MongoQueryRunner.createMongoQueryRunner;
import static com.facebook.presto.redis.RedisQueryRunner.createTpchTableDescriptions;
import static com.facebook.presto.redis.util.EmbeddedRedis.createEmbeddedRedis;
import static com.facebook.presto.redis.util.RedisTestUtils.createEmptyTableDescriptions;
import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPlugin;
import static com.google.inject.util.Modules.override;
import static java.util.Objects.requireNonNull;

public class TestArrowFederationNativeQueriesMixedConnectors
        extends AbstractTestQueryFramework
{
    private static final String TEST_USER = "testuser";
    private static final String TEST_PASSWORD = "testpass";

    private static final String POSTGRES_CONNECTOR_ID = "postgresql";
    private static final String MYSQL_CONNECTOR_ID = "mysql";
    private static final String MONGO_CONNECTOR_ID = "mongodb";
    private static final String REDIS_CONNECTOR_ID = "redis";
    private static final String PLUGIN_BUNDLES = "../presto-postgresql/pom.xml,\n../presto-mysql/pom.xml,\n../presto-redis/pom.xml,\n../presto-mongodb/pom.xml";

    private final PostgreSQLContainer<?> postgresContainer;
    private final MySQLContainer<?> mysqlContainer;
    private final MongoQueryRunner mongoQueryRunner;
    private final EmbeddedRedis embeddedRedis;

    private BufferAllocator allocator;
    private FlightShimProducer producer;
    private FlightServer server;

    public TestArrowFederationNativeQueriesMixedConnectors()
            throws Exception
    {
        // setup postgres
        this.postgresContainer = new PostgreSQLContainer<>("postgres:14")
                .withDatabaseName("tpch")
                .withUsername(TEST_USER)
                .withPassword(TEST_PASSWORD);
        this.postgresContainer.start();

        // setup mySql
        this.mysqlContainer = new MySQLContainer<>("mysql:8.0")
                .withDatabaseName("tpch")
                .withUsername(TEST_USER)
                .withPassword(TEST_PASSWORD);
        mysqlContainer.start();

        // setup redis
        embeddedRedis = createEmbeddedRedis();
        embeddedRedis.start();

        //setup mongo
        this.mongoQueryRunner = createMongoQueryRunner(TpchTable.getTables(), ImmutableMap.of());
    }

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        Injector injector = FlightShimServer.initialize(override(new FlightShimModule()).with(new TestingMixedConnectorsModule()), getFlightServerShimConfig(PLUGIN_BUNDLES));
        server = FlightShimServer.start(injector, FlightServer.builder(), getCatalogPropertiesMap());
        allocator = injector.getInstance(BufferAllocator.class);
        producer = injector.getInstance(FlightShimProducer.class);
        super.init();
    }

    @AfterClass(alwaysRun = true)
    public void close()
            throws Exception
    {
        // need to explicitly add the shutdown call to close the mongo server and client
        if (mongoQueryRunner != null) {
            mongoQueryRunner.shutdown();
        }
        postgresContainer.close();
        mysqlContainer.close();
        embeddedRedis.close();

        if (server != null && producer != null) {
            server.shutdown();
            producer.shutdown();
        }
        if (server != null) {
            server.close();
            server = null;
        }
        if (producer != null) {
            producer.close();
            producer = null;
        }
        if (allocator != null) {
            allocator.close();
            allocator = null;
        }
        super.close();
    }

    @Override
    protected void createTables()
    {
        createTables((QueryRunner) getExpectedQueryRunner(), postgresContainer.getJdbcUrl(), embeddedRedis);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner =
                createNativeQueryRunner(
                        ImmutableList.of(MYSQL_CONNECTOR_ID, POSTGRES_CONNECTOR_ID, REDIS_CONNECTOR_ID, MONGO_CONNECTOR_ID), server.getPort());
        setupNativeSidecarPlugin(queryRunner);
        installPlugins(queryRunner, postgresContainer.getJdbcUrl(), mysqlContainer.getJdbcUrl(), embeddedRedis, mongoQueryRunner);
        return queryRunner;
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = createJavaQueryRunner();
        installPlugins(queryRunner, postgresContainer.getJdbcUrl(), mysqlContainer.getJdbcUrl(), embeddedRedis, mongoQueryRunner);
        return queryRunner;
    }

    private Map<String, Map<String, String>> getCatalogPropertiesMap()
    {
        return ImmutableMap.of(
                MYSQL_CONNECTOR_ID, createConnectorCatalog(MYSQL_CONNECTOR_ID, createJdbcConnectorProperties(getConnectionUrl(mysqlContainer.getJdbcUrl()))),
                POSTGRES_CONNECTOR_ID, createConnectorCatalog(POSTGRES_CONNECTOR_ID, createJdbcConnectorProperties(postgresContainer.getJdbcUrl())),
                REDIS_CONNECTOR_ID, createConnectorCatalog(REDIS_CONNECTOR_ID, createRedisConnectorProperties(embeddedRedis, createEmptyTableDescriptions())),
                MONGO_CONNECTOR_ID, createConnectorCatalog(MONGO_CONNECTOR_ID, createMongoConnectorProperties(getMongoDbSeeds(mongoQueryRunner))));
    }

    @Override
    protected FeaturesConfig createFeaturesConfig()
    {
        return new FeaturesConfig().setNativeExecutionEnabled(true);
    }

    @Test
    public void testBasicSanityTests()
    {
        assertQuery("select * from mysql.tpch.customer");
        assertQuery("select * from postgresql.tpch.orders");
        assertQuery("select orderkey, custkey from postgresql.tpch.orders");
        assertQuery("select extendedprice * quantity from postgresql.tpch.lineitem");
        assertQuery("select name from mysql.tpch.part where name like '%steel%'");
        assertQuery("select name from mysql.tpch.part where LENGTH(name) > 10");
    }

    @Test
    public void testJoins()
    {
        assertQuery("SELECT c.custkey, o.orderkey " +
                "FROM mysql.tpch.customer c " +
                "JOIN postgresql.tpch.orders o ON c.custkey = o.custkey " +
                "WHERE c.custkey BETWEEN 100 AND 200");
        assertQuery("SELECT p.partkey, ps.suppkey " +
                "FROM postgresql.tpch.part p " +
                "JOIN mysql.tpch.partsupp ps ON p.partkey = ps.partkey " +
                "WHERE ps.supplycost > 100 " +
                "ORDER BY p.partkey, ps.suppkey " +
                "LIMIT 100");
        assertQuery(
                "SELECT c.custkey, n.regionkey, o.orderkey, l.quantity " +
                        "FROM mysql.tpch.customer c " +
                        "JOIN postgresql.tpch.orders o ON c.custkey = o.custkey " +
                        "JOIN redis.tpch.nation n ON c.nationkey =  n.nationkey " +
                        "JOIN mongodb.tpch.lineitem l ON o.orderkey = l.orderkey");
        assertQuery(
                "SELECT c.custkey " +
                        "FROM mysql.tpch.customer c " +
                        "WHERE NOT EXISTS ( " +
                        "   SELECT 1 " +
                        "   FROM postgresql.tpch.orders o " +
                        "   WHERE o.custkey = c.custkey)");
        assertQuery(
                "SELECT DISTINCT c.custkey " +
                        "FROM mysql.tpch.customer c " +
                        "WHERE c.custkey IN ( " +
                        "   SELECT custkey " +
                        "   FROM postgresql.tpch.orders o)");

        // wide JOIN
        assertQuery("SELECT * " +
                "FROM mysql.tpch.lineitem l " +
                "JOIN postgresql.tpch.orders o ON l.orderkey = o.orderkey " +
                "ORDER BY l.orderkey, o.orderkey");
    }

    @Test
    public void testLargeQueries()
    {
        // tpch q3
        assertQuery(
                "SELECT l.orderkey, sum(l.extendedprice * (1 - l.discount)) as revenue, o.orderdate, o.shippriority " +
                        "FROM " +
                        "mysql.tpch.customer as c," +
                        "postgresql.tpch.orders as o," +
                        "mysql.tpch.lineitem as l " +
                        "WHERE " +
                        "c.mktsegment = 'BUILDING' " +
                        "AND c.custkey = o.custkey " +
                        "AND l.orderkey = o.orderkey " +
                        "AND o.orderdate < DATE '1995-03-15' " +
                        "AND l.shipdate > DATE '1995-03-15' " +
                        "GROUP BY l.orderkey,o.orderdate, o.shippriority " +
                        "ORDER BY revenue desc, o.orderdate " +
                        "LIMIT 10");

        assertQuery(
                "SELECT n.name, sum(l.extendedprice * (1 - l.discount)) as revenue " +
                        "FROM mysql.tpch.customer c " +
                        "JOIN mongodb.tpch.orders o ON c.custkey = o.custkey " +
                        "JOIN postgresql.tpch.lineitem l ON o.orderkey = l.orderkey " +
                        "JOIN redis.tpch.supplier s ON l.suppkey = s.suppkey " +
                        "JOIN mysql.tpch.nation n ON c.nationkey = n.nationkey " +
                        "GROUP BY 1 " +
                        "ORDER BY revenue DESC " +
                        "LIMIT 20");
    }

    @Test
    public void testGroupByHaving()
    {
        assertQuery(
                "SELECT o.custkey, COUNT(*) AS cnt " +
                        "FROM postgresql.tpch.orders o " +
                        "JOIN mysql.tpch.lineitem l ON o.orderkey = l.orderkey " +
                        "GROUP BY o.custkey " +
                        "HAVING COUNT(*) > 10 " +
                        "ORDER BY cnt DESC");
    }

    @Test
    public void testMultiAggregation()
    {
        assertQuery(
                "SELECT c.nationkey, COUNT(*) as orders, SUM(l.quantity) as qty, AVG(l.discount) as avg " +
                        "FROM mysql.tpch.customer c " +
                        "JOIN postgresql.tpch.orders o ON c.custkey = o.custkey " +
                        "JOIN mysql.tpch.lineitem l ON o.orderkey =  l.orderkey " +
                        "GROUP BY 1");
    }

    @Test
    public void testWindowFunctions()
    {
        assertQuery(
                "SELECT o.orderkey, c.custkey, " +
                        "RANK() OVER (PARTITION BY c.custkey ORDER BY o.totalprice DESC) AS rnk " +
                        "FROM mysql.tpch.customer c " +
                        "JOIN postgresql.tpch.orders o ON c.custkey = o.custkey " +
                        "ORDER BY o.orderkey, c.custkey " +
                        "LIMIT 100");
        assertQuery(
                "SELECT c.nationkey, l.orderkey, " +
                        "SUM(l.quantity) OVER (PARTITION BY c.nationkey ORDER BY l.orderkey) AS qty " +
                        "FROM mysql.tpch.customer c " +
                        "JOIN postgresql.tpch.orders o ON c.custkey = o.custkey " +
                        "JOIN mysql.tpch.lineitem l ON o.orderkey = l.orderkey");
        assertQuery("SELECT l.orderkey, l.quantity, " +
                "DENSE_RANK() OVER (ORDER BY l.quantity DESC) AS dr " +
                "FROM postgresql.tpch.lineitem l");
    }

    @Test
    public void testNullCases()
    {
        assertQuery(
                "SELECT * FROM mysql.tpch.part p " +
                        "LEFT JOIN postgresql.tpch.partsupp s ON p.partkey = s.partkey " +
                        "WHERE s.suppkey is NULL");
        assertQuery(
                "SELECT COALESCE(c.name, 'UNKNOWN') AS customer_name, o.orderkey " +
                        "FROM mysql.tpch.customer c " +
                        "JOIN postgresql.tpch.orders o ON c.custkey = o.custkey " +
                        "ORDER BY o.orderkey, customer_name");
    }

    @Test
    public void testSetOperators()
    {
        assertQuery(
                "SELECT nationkey FROM mysql.tpch.nation " +
                        "UNION ALL " +
                        "SELECT nationkey FROM postgresql.tpch.nation");
        assertQuery(
                "SELECT orderkey FROM mysql.tpch.orders " +
                        "INTERSECT " +
                        "SELECT orderkey FROM postgresql.tpch.orders");
        assertQuery(
                "SELECT custkey FROM mysql.tpch.customer " +
                        "EXCEPT " +
                        "SELECT custkey FROM postgresql.tpch.customer");
    }

    @Test(timeOut = 30_000)
    public void testRepeatedJoins()
    {
        for (int i = 0; i < 20; i++) {
            assertQuery(
                    "SELECT COUNT(*) " +
                            "FROM mysql.tpch.lineitem l " +
                            "JOIN postgresql.tpch.orders o ON l.orderkey = o.orderkey");
        }
    }

    private static void installPlugins(QueryRunner queryRunner, String postgresJdbcUrl, String mySqlJdbcUrl, EmbeddedRedis embeddedRedis, MongoQueryRunner mongoQueryRunner)
            throws Exception
    {
        queryRunner.installPlugin(new PostgreSqlPlugin());
        queryRunner.createCatalog(POSTGRES_CONNECTOR_ID, POSTGRES_CONNECTOR_ID, createJdbcConnectorProperties(postgresJdbcUrl));

        queryRunner.installPlugin(new MySqlPlugin());
        queryRunner.createCatalog(MYSQL_CONNECTOR_ID, MYSQL_CONNECTOR_ID, createJdbcConnectorProperties(getConnectionUrl(mySqlJdbcUrl)));

        installRedisPlugin(
                embeddedRedis,
                queryRunner,
                createTpchTableDescriptions(((DistributedQueryRunner) queryRunner).getCoordinator().getMetadata(), TpchTable.getTables(), "string"));

        queryRunner.installPlugin(new MongoPlugin());
        queryRunner.createCatalog(MONGO_CONNECTOR_ID, MONGO_CONNECTOR_ID, createMongoConnectorProperties(getMongoDbSeeds(mongoQueryRunner)));
    }

    private static void createTables(QueryRunner queryRunner, String postgresJdbcUrl, EmbeddedRedis embeddedRedis)
    {
        TestArrowFederationNativeQueriesPostgres.createTpchTables(queryRunner, postgresJdbcUrl, POSTGRES_CONNECTOR_ID);
        TestArrowFederationNativeQueriesMySql.createTpchTables(queryRunner);
        TestArrowFederationNativeQueriesRedis.createTpchTables(embeddedRedis, queryRunner);
    }

    private static Map<String, String> createConnectorCatalog(String connectorName, Map<String, String> connectorProperties)
    {
        ImmutableMap.Builder<String, String> propertyBuilder = ImmutableMap.builder();
        propertyBuilder.put("connector.name", connectorName).putAll(connectorProperties);
        return propertyBuilder.build();
    }

    // Override Producer to correct serialized type info
    private static class TestingMixedConnectorsProducer
            extends FlightShimProducer
    {
        private final JsonCodec<FlightShimRequest> requestCodec;

        @Inject
        public TestingMixedConnectorsProducer(BufferAllocator allocator, FlightShimPluginManager pluginManager, FlightShimConfig config, @ForFlightShimServer ExecutorService shimExecutor, PageSourceManager pageSourceManager, JsonCodec<FlightShimRequest> requestCodec, JsonCodec<ExecutionFailureInfo> executionFailureInfoCodec)
        {
            super(allocator, pluginManager, config, shimExecutor, pageSourceManager, requestCodec, executionFailureInfoCodec);
            this.requestCodec = requireNonNull(requestCodec, "requestCodec is null");
        }

        @Override
        public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener)
        {
            // Because the testing Presto server can serialize type information incorrectly, we must
            // correct "type" fields in the request handle bytes, see https://github.com/prestodb/presto/issues/28643
            FlightShimRequest request = requestCodec.fromJson(ticket.getBytes());
            FlightShimRequest correctedRequest = new FlightShimRequest(
                    request.getConnectorId(),
                    request.getFields(),
                    correctJsonTypeInfo(request.getConnectorId(), request.getSplitBytes()),
                    request.getColumnHandlesBytes().stream().map(columnHandleBytes -> correctJsonTypeInfo(request.getConnectorId(), columnHandleBytes)).collect(Collectors.toList()),
                    correctJsonTypeInfo(request.getConnectorId(), request.getTableHandleBytes()),
                    request.getTableLayoutHandleBytes().map(tableHandleBytes -> correctJsonTypeInfo(request.getConnectorId(), tableHandleBytes)),
                    correctJsonTypeInfo(request.getConnectorId(), request.getTransactionHandleBytes()));
            super.getStream(context, new Ticket(requestCodec.toBytes(correctedRequest)), listener);
        }

        private byte[] correctJsonTypeInfo(String connectorName, byte[] jsonBytes)
        {
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                JsonNode tree = objectMapper.readTree(jsonBytes);
                if (tree.isObject()) {
                    ObjectNode objectNode = (ObjectNode) tree;
                    if (objectNode.has("@type")) {
                        objectNode.put("@type", connectorName);
                        for (JsonNode columnDomain : objectNode.path("tupleDomain").path("columnDomains")) {
                            JsonNode column = columnDomain.path("column");
                            if (column.isObject() && column.has("@type")) {
                                ((ObjectNode) column).put("@type", connectorName);
                            }
                        }
                        if (objectNode.has("table")) {
                            ObjectNode tableNode = (ObjectNode) objectNode.get("table");
                            if (tableNode.has("@type")) {
                                tableNode.put("@type", connectorName);
                            }
                        }
                    }
                }
                else if (tree.isArray()) {
                    ArrayNode arrayNode = (ArrayNode) tree;
                    arrayNode.set(0, connectorName);
                }
                return objectMapper.writeValueAsBytes(tree);
            }
            catch (IOException e) {
                throw new IllegalArgumentException("Unable to correct JSON type info on Flight request", e);
            }
        }
    }

    private static class TestingMixedConnectorsModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.bind(FlightShimProducer.class).to(TestingMixedConnectorsProducer.class).in(Scopes.SINGLETON);
        }
    }
}
