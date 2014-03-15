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

import com.facebook.presto.AbstractTestIndexedQueries;
import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.QueryError;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementClient;
import com.facebook.presto.metadata.AllNodes;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.Session;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.tpch.IndexedTpchPlugin;
import com.facebook.presto.tpch.TpchMetadata;
import com.facebook.presto.util.MaterializedResult;
import com.facebook.presto.util.MaterializedRow;
import com.google.common.base.Function;
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

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.MaterializedResult.DEFAULT_PRECISION;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.testing.Assertions.assertLessThan;
import static io.airlift.units.Duration.nanosSince;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestDistributedQueriesIndexed
        extends AbstractTestIndexedQueries
{
    private static final Session SESSION = new Session("user", "test", "tpch_indexed", TpchMetadata.TINY_SCHEMA_NAME, UTC_KEY, Locale.ENGLISH, null, null);

    private static final String ENVIRONMENT = "testing";
    private static final Logger log = Logger.get(TestDistributedQueries.class.getSimpleName());
    private final JsonCodec<QueryResults> queryResultsCodec = jsonCodec(QueryResults.class);

    private TestingPrestoServer coordinator;
    private List<TestingPrestoServer> servers;
    private AsyncHttpClient httpClient;
    private TestingDiscoveryServer discoveryServer;

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

    protected ClientSession getClientSession()
    {
        return new ClientSession(coordinator.getBaseUrl(),
                SESSION.getUser(),
                SESSION.getSource(),
                SESSION.getCatalog(),
                SESSION.getSchema(),
                SESSION.getTimeZoneKey().getTimeZoneId(),
                SESSION.getLocale(),
                true);
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
                    types = getTypes(results.getColumns());
                }
                if (results.getData() != null) {
                    rows.addAll(transform(results.getData(), dataToRows(types)));
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

    private static List<Type> getTypes(List<Column> columns)
    {
        return ImmutableList.copyOf(transform(columns, columnTypeGetter()));
    }

    private static Function<Column, Type> columnTypeGetter()
    {
        return new Function<Column, Type>()
        {
            @Override
            public Type apply(Column column)
            {
                String type = column.getType();
                switch (type) {
                    case "boolean":
                        return BOOLEAN;
                    case "bigint":
                        return BIGINT;
                    case "double":
                        return DOUBLE;
                    case "varchar":
                        return VARCHAR;
                }
                throw new AssertionError("Unhandled type: " + type);
            }
        };
    }

    private static Function<List<Object>, MaterializedRow> dataToRows(final List<Type> types)
    {
        return new Function<List<Object>, MaterializedRow>()
        {
            @Override
            public MaterializedRow apply(List<Object> data)
            {
                checkArgument(data.size() == types.size(), "columns size does not match tuple infos");
                List<Object> row = new ArrayList<>();
                for (int i = 0; i < data.size(); i++) {
                    Object value = data.get(i);
                    if (value == null) {
                        row.add(null);
                        continue;
                    }
                    Type type = types.get(i);
                    if (type.equals(BOOLEAN)) {
                        row.add(value);
                    }
                    else if (type.equals(BIGINT)) {
                        row.add(((Number) value).longValue());
                    }
                    else if (type.equals(DOUBLE)) {
                        row.add(((Number) value).doubleValue());
                    }
                    else if (type.equals(VARCHAR)) {
                        row.add(value);
                    }
                    else {
                        throw new AssertionError("unhandled type: " + type);
                    }
                }
                return new MaterializedRow(DEFAULT_PRECISION, row);
            }
        };
    }

    private TestingPrestoServer createTestingPrestoServer(URI discoveryUri, boolean coordinator)
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("query.client.timeout", "10m")
                .put("exchange.http-client.read-timeout", "1h")
                .put("datasources", "tpch_indexed")
                .build();

        TestingPrestoServer server = new TestingPrestoServer(coordinator, properties, ENVIRONMENT, discoveryUri, ImmutableList.<Module>of());
        server.installPlugin(new IndexedTpchPlugin(getTpchIndexSpec()), "tpch_indexed", "tpch_indexed");
        return server;
    }
}
