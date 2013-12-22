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
package com.facebook.presto.server;

import com.facebook.presto.AbstractTestQueries;
import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.QueryError;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementClient;
import com.facebook.presto.metadata.AllNodes;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.QualifiedTablePrefix;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.facebook.presto.util.MaterializedResult;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closeables;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.netty.StandaloneNettyAsyncHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.sql.analyzer.Session.DEFAULT_CATALOG;
import static com.facebook.presto.tpch.TpchMetadata.TPCH_CATALOG_NAME;
import static com.facebook.presto.tpch.TpchMetadata.TPCH_SCHEMA_NAME;
import static com.facebook.presto.util.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.testing.Assertions.assertLessThan;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestDistributedQueries
        extends AbstractTestQueries
{
    private static final String ENVIRONMENT = "testing";
    private static final Logger log = Logger.get(TestDistributedQueries.class.getSimpleName());
    private final JsonCodec<QueryResults> queryResultsCodec = jsonCodec(QueryResults.class);

    private TestingPrestoServer coordinator;
    private List<TestingPrestoServer> servers;
    private AsyncHttpClient httpClient;
    private TestingDiscoveryServer discoveryServer;

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "statement is too large \\(stack overflow during analysis\\)")
    public void testLargeQueryFailure()
            throws Exception
    {
        assertQuery("SELECT " + Joiner.on(" AND ").join(nCopies(1000, "1 = 1")), "SELECT true");
    }

    @Test
    public void testLargeQuerySuccess()
            throws Exception
    {
        assertQuery("SELECT " + Joiner.on(" AND ").join(nCopies(500, "1 = 1")), "SELECT true");
    }

    @Test
    public void testTableSampleSystem()
            throws Exception
    {
        int total = computeActual("SELECT orderkey FROM orders").getMaterializedTuples().size();

        boolean sampleSizeFound = false;
        for (int i = 0; i < 100; i++) {
            int sampleSize = computeActual("SELECT orderkey FROM ORDERS TABLESAMPLE SYSTEM (50)").getMaterializedTuples().size();
            if (sampleSize > 0 && sampleSize < total) {
                sampleSizeFound = true;
                break;
            }
        }
        assertTrue(sampleSizeFound, "Table sample returned unexpected number of rows");
    }

    @Test
    public void testTableSampleSystemBoundaryValues()
            throws Exception
    {
        MaterializedResult fullSample = computeActual("SELECT orderkey FROM orders TABLESAMPLE SYSTEM (100)");
        MaterializedResult emptySample = computeActual("SELECT orderkey FROM orders TABLESAMPLE SYSTEM (0)");
        MaterializedResult all = computeActual("SELECT orderkey FROM orders");

        assertTrue(all.getMaterializedTuples().containsAll(fullSample.getMaterializedTuples()));
        assertEquals(emptySample.getMaterializedTuples().size(), 0);
    }

    @Test
    public void testShowCatalogs()
            throws Exception
    {
        MaterializedResult result = computeActual("SHOW CATALOGS");
        Set<String> catalogNames = ImmutableSet.copyOf(transform(result.getMaterializedTuples(), onlyColumnGetter()));
        assertEquals(catalogNames, ImmutableSet.of(TPCH_CATALOG_NAME, DEFAULT_CATALOG));
    }

    @Test
    public void testCreateTableAsSelect()
            throws Exception
    {
        @Language("SQL") String query = "SELECT orderkey, totalprice, orderdate FROM orders";

        assertQuery("CREATE TABLE test AS " + query, "SELECT count(*) FROM orders");

        assertQuery("SELECT * FROM test", query);

        // TODO: drop table in finally block when supported
    }

    @Test
    public void testCreateMaterializedView()
            throws Exception
    {
        assertQuery(
                "CREATE MATERIALIZED VIEW test_mview_orders AS SELECT * FROM " +
                        format("%s.%s.orders", TPCH_CATALOG_NAME, TPCH_SCHEMA_NAME),
                "SELECT count(*) FROM orders");

        // Materialized views have a race condition between writing data to the
        // native store and when the data is visible to be queried. This is a
        // brain dead work around for this race condition that doesn't really
        // fix the problem, but makes it very unlikely.
        // TODO: remove this when the materialized view flow is fixed
        MILLISECONDS.sleep(500);

        assertQuery("SELECT * FROM test_mview_orders", "SELECT * FROM orders");
    }

    @Override
    protected int getNodeCount()
    {
        return 3;
    }

    @Override
    protected void setUpQueryFramework(String catalog, String schema)
            throws Exception
    {
        try {
            discoveryServer = new TestingDiscoveryServer(ENVIRONMENT);
            coordinator = createTestingPrestoServer(discoveryServer.getBaseUrl(), true);
            servers = ImmutableList.<TestingPrestoServer>builder()
                    .add(coordinator)
                    .add(createTestingPrestoServer(discoveryServer.getBaseUrl(), false))
                    .add(createTestingPrestoServer(discoveryServer.getBaseUrl(), false))
                    .build();
        }
        catch (Exception e) {
            tearDownQueryFramework();
            throw e;
        }

        this.httpClient = new StandaloneNettyAsyncHttpClient("test",
                new HttpClientConfig()
                        .setConnectTimeout(new Duration(1, TimeUnit.DAYS))
                        .setReadTimeout(new Duration(10, TimeUnit.DAYS)));

        long start = System.nanoTime();
        while (!allNodesGloballyVisible()) {
            assertLessThan(nanosSince(start), new Duration(10, SECONDS));
            MILLISECONDS.sleep(10);
        }

        log.info("Loading data...");
        long startTime = System.nanoTime();
        distributeData(catalog, schema);
        log.info("Loading complete in %s", nanosSince(startTime).toString(SECONDS));
    }

    private boolean allNodesGloballyVisible()
    {
        for (TestingPrestoServer server : servers) {
            AllNodes allNodes = server.refreshNodes();
            if (!allNodes.getInactiveNodes().isEmpty() ||
                    (allNodes.getActiveNodes().size() != servers.size())) {
                return false;
            }
        }
        return true;
    }

    @SuppressWarnings("deprecation")
    @Override
    protected void tearDownQueryFramework()
            throws Exception
    {
        if (servers != null) {
            for (TestingPrestoServer server : servers) {
                Closeables.closeQuietly(server);
            }
        }
        Closeables.closeQuietly(discoveryServer);
    }

    private void distributeData(String catalog, String schema)
            throws Exception
    {
        for (QualifiedTableName table : coordinator.getMetadata().listTables(new QualifiedTablePrefix(catalog, schema))) {
            if (table.getTableName().equalsIgnoreCase("dual")) {
                continue;
            }
            log.info("Running import for %s", table.getTableName());
            @Language("SQL") String sql = format("CREATE TABLE %s AS SELECT * FROM %s", table.getTableName(), table);
            long rows = checkType(computeActual(sql).getMaterializedTuples().get(0).getField(0), Long.class, "rows");
            log.info("Imported %s rows for %s", rows, table.getTableName());
        }
    }

    @Override
    protected MaterializedResult computeActual(@Language("SQL") String sql)
    {
        ClientSession session = new ClientSession(coordinator.getBaseUrl(), "testuser", "test", "default", "default", true);

        try (StatementClient client = new StatementClient(httpClient, queryResultsCodec, session, sql)) {
            AtomicBoolean loggedUri = new AtomicBoolean(false);
            ImmutableList.Builder<Tuple> rows = ImmutableList.builder();
            TupleInfo tupleInfo = null;

            while (client.isValid()) {
                QueryResults results = client.current();
                if (!loggedUri.getAndSet(true)) {
                    log.info("Query %s: %s?pretty", results.getId(), results.getInfoUri());
                }

                if ((tupleInfo == null) && (results.getColumns() != null)) {
                    tupleInfo = getTupleInfo(results.getColumns());
                }
                if (results.getData() != null) {
                    rows.addAll(transform(results.getData(), dataToTuple(tupleInfo)));
                }

                client.advance();
            }

            if (!client.isFailed()) {
                return new MaterializedResult(rows.build(), tupleInfo);
            }

            QueryError error = client.finalResults().getError();
            assert error != null;
            if (error.getFailureInfo() != null) {
                throw error.getFailureInfo().toException();
            }
            throw new RuntimeException("Query failed: " + error.getMessage());

            // dump query info to console for debugging (NOTE: not pretty printed)
            // JsonCodec<QueryInfo> queryInfoJsonCodec = createCodecFactory().prettyPrint().jsonCodec(QueryInfo.class);
            // log.info("\n" + queryInfoJsonCodec.toJson(queryInfo));
        }
    }

    private static TupleInfo getTupleInfo(List<Column> columns)
    {
        return new TupleInfo(transform(transform(columns, Column.typeGetter()), tupleType()));
    }

    private static Function<String, Type> tupleType()
    {
        return new Function<String, Type>()
        {
            @Override
            public Type apply(String type)
            {
                switch (type) {
                    case "boolean":
                        return Type.BOOLEAN;
                    case "bigint":
                        return Type.FIXED_INT_64;
                    case "double":
                        return Type.DOUBLE;
                    case "varchar":
                        return Type.VARIABLE_BINARY;
                }
                throw new AssertionError("Unhandled type: " + type);
            }
        };
    }

    private static Function<List<Object>, Tuple> dataToTuple(final TupleInfo tupleInfo)
    {
        return new Function<List<Object>, Tuple>()
        {
            @Override
            public Tuple apply(List<Object> data)
            {
                checkArgument(data.size() == tupleInfo.getTypes().size(), "columns size does not match tuple info");
                TupleInfo.Builder tuple = tupleInfo.builder();
                for (int i = 0; i < data.size(); i++) {
                    Object value = data.get(i);
                    if (value == null) {
                        tuple.appendNull();
                        continue;
                    }
                    Type type = tupleInfo.getTypes().get(i);
                    switch (type) {
                        case BOOLEAN:
                            tuple.append((Boolean) value);
                            break;
                        case FIXED_INT_64:
                            tuple.append(((Number) value).longValue());
                            break;
                        case DOUBLE:
                            tuple.append(((Number) value).doubleValue());
                            break;
                        case VARIABLE_BINARY:
                            tuple.append((String) value);
                            break;
                        default:
                            throw new AssertionError("unhandled type: " + type);
                    }
                }
                return tuple.build();
            }
        };
    }

    private static TestingPrestoServer createTestingPrestoServer(URI discoveryUri, boolean coordinator)
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("query.client.timeout", "10m")
                .put("exchange.http-client.read-timeout", "1h")
                .put("datasources", "native,tpch")
                .build();

        return new TestingPrestoServer(coordinator, properties, ENVIRONMENT, discoveryUri);
    }
}
