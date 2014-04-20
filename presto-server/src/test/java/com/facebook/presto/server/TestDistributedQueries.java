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

import com.facebook.presto.AbstractTestSampledQueries;
import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.QueryError;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementClient;
import com.facebook.presto.metadata.AllNodes;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.QualifiedTablePrefix;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.Session;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.tpch.SampledTpchPlugin;
import com.facebook.presto.tpch.TpchMetadata;
import com.facebook.presto.tpch.TpchPlugin;
import com.facebook.presto.util.MaterializedResult;
import com.facebook.presto.util.MaterializedRow;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.testing.Closeables;
import io.airlift.units.Duration;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.net.URI;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.spi.Session.DEFAULT_CATALOG;
import static com.facebook.presto.spi.Session.DEFAULT_SCHEMA;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.DateTimeUtils.parseDate;
import static com.facebook.presto.util.DateTimeUtils.parseTime;
import static com.facebook.presto.util.DateTimeUtils.parseTimeWithTimeZone;
import static com.facebook.presto.util.DateTimeUtils.parseTimestamp;
import static com.facebook.presto.util.DateTimeUtils.parseTimestampWithTimeZone;
import static com.facebook.presto.util.MaterializedResult.DEFAULT_PRECISION;
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
        extends AbstractTestSampledQueries
{
    private static final Session SESSION = new Session("user", "test", DEFAULT_CATALOG, "test", UTC_KEY, Locale.ENGLISH, null, null);

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
        int total = computeActual("SELECT orderkey FROM orders").getMaterializedRows().size();

        boolean sampleSizeFound = false;
        for (int i = 0; i < 100; i++) {
            int sampleSize = computeActual("SELECT orderkey FROM ORDERS TABLESAMPLE SYSTEM (50)").getMaterializedRows().size();
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

        assertTrue(all.getMaterializedRows().containsAll(fullSample.getMaterializedRows()));
        assertEquals(emptySample.getMaterializedRows().size(), 0);
    }

    @Test
    public void testCreateTableAsSelect()
            throws Exception
    {
        assertCreateTable(
                "test_simple",
                "SELECT orderdate, orderkey, totalprice FROM orders",
                "SELECT count(*) FROM orders");
    }

    @Test
    public void testCreateTableAsSelectGroupBy()
            throws Exception
    {
        assertCreateTable(
                "test_group",
                "SELECT orderstatus, sum(totalprice) x FROM orders GROUP BY orderstatus",
                "SELECT count(DISTINCT orderstatus) FROM orders");
    }

    @Test
    public void testCreateTableAsSelectLimit()
            throws Exception
    {
        assertCreateTable(
                "test_limit",
                "SELECT orderkey FROM orders ORDER BY orderkey LIMIT 10",
                "SELECT 10");
    }

    @Test
    public void testCreateTableAsSelectJoin()
            throws Exception
    {
        assertCreateTable(
                "test_join",
                "SELECT count(*) x FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey",
                "SELECT 1");
    }

    @Test
    public void testCreateSampledTableAsSelectLimit()
            throws Exception
    {
        assertCreateTable(
                "test_limit_sampled",
                "SELECT orderkey FROM tpch_sampled.tiny.orders ORDER BY orderkey LIMIT 10",
                "SELECT orderkey FROM (SELECT orderkey FROM orders) UNION ALL (SELECT orderkey FROM orders) ORDER BY orderkey LIMIT 10",
                "SELECT 10");
    }

    private void assertCreateTable(String table, @Language("SQL") String query, @Language("SQL") String rowCountQuery)
            throws Exception
    {
        assertCreateTable(table, query, query, rowCountQuery);
    }

    private void assertCreateTable(String table, @Language("SQL") String query, @Language("SQL") String expectedQuery, @Language("SQL") String rowCountQuery)
           throws Exception
    {
        try {
            assertQuery("CREATE TABLE " +  table + " AS " + query, rowCountQuery);
            assertQuery("SELECT * FROM " + table, expectedQuery);
        }
        finally {
            QualifiedTableName name = new QualifiedTableName(DEFAULT_CATALOG, DEFAULT_SCHEMA, table);
            Optional<TableHandle> handle = coordinator.getMetadata().getTableHandle(name);
            if (handle.isPresent()) {
                coordinator.getMetadata().dropTable(handle.get());
            }
        }
    }

    @Override
    protected int getNodeCount()
    {
        return 3;
    }

    @Override
    protected Session setUpQueryFramework()
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

        this.httpClient = new JettyHttpClient(
                new HttpClientConfig()
                        .setConnectTimeout(new Duration(1, TimeUnit.DAYS))
                        .setReadTimeout(new Duration(10, TimeUnit.DAYS)));

        long start = System.nanoTime();
        while (!allNodesGloballyVisible()) {
            assertLessThan(nanosSince(start), new Duration(10, SECONDS));
            MILLISECONDS.sleep(10);
        }

        for (TestingPrestoServer server : servers) {
            server.getMetadata().addFunctions(CUSTOM_FUNCTIONS);
        }

        log.info("Loading data...");
        long startTime = System.nanoTime();
        distributeData("tpch", TpchMetadata.TINY_SCHEMA_NAME, getClientSession());
        distributeData("tpch_sampled", TpchMetadata.TINY_SCHEMA_NAME, getSampledClientSession());
        log.info("Loading complete in %s", nanosSince(startTime).toString(SECONDS));

        return SESSION;
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

    private void distributeData(String catalog, String schema, ClientSession session)
            throws Exception
    {
        for (QualifiedTableName table : coordinator.getMetadata().listTables(new QualifiedTablePrefix(catalog, schema))) {
            if (table.getTableName().equalsIgnoreCase("dual")) {
                continue;
            }
            log.info("Running import for %s", table.getTableName());
            @Language("SQL") String sql = format("CREATE TABLE %s AS SELECT * FROM %s", table.getTableName(), table);
            long rows = checkType(compute(sql, session).getMaterializedRows().get(0).getField(0), Long.class, "rows");
            log.info("Imported %s rows for %s", rows, table.getTableName());
        }
    }

    protected ClientSession getClientSession()
    {
        return new ClientSession(
                coordinator.getBaseUrl(),
                SESSION.getUser(),
                SESSION.getSource(),
                SESSION.getCatalog(),
                SESSION.getSchema(),
                SESSION.getTimeZoneKey().getTimeZoneId(),
                SESSION.getLocale(),
                true);
    }

    protected ClientSession getSampledClientSession()
    {
        return new ClientSession(
                coordinator.getBaseUrl(),
                SESSION.getUser(),
                SESSION.getSource(),
                SESSION.getCatalog(),
                "sampled",
                SESSION.getTimeZoneKey().getTimeZoneId(),
                SESSION.getLocale(),
                true);
    }

    @Override
    protected MaterializedResult computeActualSampled(@Language("SQL") String sql)
    {
        return compute(sql, getSampledClientSession());
    }

    @Override
    protected MaterializedResult computeActual(@Language("SQL") String sql)
    {
        return compute(sql, getClientSession());
    }

    private MaterializedResult compute(@Language("SQL") String sql, ClientSession session)
    {
        try (StatementClient client = new StatementClient(httpClient, queryResultsCodec, session, sql)) {
            AtomicBoolean loggedUri = new AtomicBoolean(false);
            ImmutableList.Builder<MaterializedRow> rows = ImmutableList.builder();
            List<Type> types = null;

            while (client.isValid()) {
                QueryResults results = client.current();
                if (!loggedUri.getAndSet(true)) {
                    log.info("Query %s: %s?pretty", results.getId(), results.getInfoUri());
                }

                if ((types == null) && (results.getColumns() != null)) {
                    types = getTypes(coordinator.getMetadata(), results.getColumns());
                }
                if (results.getData() != null) {
                    rows.addAll(transform(results.getData(), dataToRow(types)));
                }

                client.advance();
            }

            if (!client.isFailed()) {
                return new MaterializedResult(rows.build(), types);
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

    private static List<Type> getTypes(Metadata metadata, List<Column> columns)
    {
        return ImmutableList.copyOf(transform(columns, columnTypeGetter(metadata)));
    }

    private static Function<Column, Type> columnTypeGetter(final Metadata metadata)
    {
        return new Function<Column, Type>()
        {
            @Override
            public Type apply(Column column)
            {
                String typeName = column.getType();
                Type type = metadata.getType(typeName);
                if (type == null) {
                    throw new AssertionError("Unhandled type: " + typeName);
                }
                return type;
            }
        };
    }

    private Function<List<Object>, MaterializedRow> dataToRow(final List<Type> types)
    {
        return new Function<List<Object>, MaterializedRow>()
        {
            @Override
            public MaterializedRow apply(List<Object> data)
            {
                checkArgument(data.size() == types.size(), "columns size does not match types size");
                List<Object> row = new ArrayList<>();
                for (int i = 0; i < data.size(); i++) {
                    Object value = data.get(i);
                    if (value == null) {
                        row.add(null);
                        continue;
                    }

                    Type type = types.get(i);
                    if (BOOLEAN.equals(type)) {
                        row.add(value);
                    }
                    else if (BIGINT.equals(type)) {
                        row.add(((Number) value).longValue());
                    }
                    else if (DOUBLE.equals(type)) {
                        row.add(((Number) value).doubleValue());
                    }
                    else if (VARCHAR.equals(type)) {
                        row.add(value);
                    }
                    else if (DATE.equals(type)) {
                        row.add(new Date(parseDate((String) value)));
                    }
                    else if (TIME.equals(type)) {
                        row.add(new Time(parseTime(getSession().getTimeZoneKey(), (String) value)));
                    }
                    else if (TIME_WITH_TIME_ZONE.equals(type)) {
                        row.add(new Time(unpackMillisUtc(parseTimeWithTimeZone((String) value))));
                    }
                    else if (TIMESTAMP.equals(type)) {
                        row.add(new Timestamp(parseTimestamp(getSession().getTimeZoneKey(), (String) value)));
                    }
                    else if (TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
                        row.add(new Timestamp(unpackMillisUtc(parseTimestampWithTimeZone((String) value))));
                    }
                }
                return new MaterializedRow(DEFAULT_PRECISION, row);
            }
        };
    }

    private static TestingPrestoServer createTestingPrestoServer(URI discoveryUri, boolean coordinator)
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("query.client.timeout", "10m")
                .put("exchange.http-client.read-timeout", "1h")
                .put("datasources", "native,tpch,tpch_sampled")
                .build();

        TestingPrestoServer server = new TestingPrestoServer(coordinator, properties, ENVIRONMENT, discoveryUri, ImmutableList.<Module>of());
        server.installPlugin(new TpchPlugin(), "tpch", "tpch");
        server.installPlugin(new SampledTpchPlugin(), "tpch_sampled", "tpch_sampled");
        return server;
    }
}
