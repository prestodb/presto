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
package com.facebook.presto.tests;

import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.QueryError;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementClient;
import com.facebook.presto.metadata.AllNodes;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.QualifiedTablePrefix;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.testing.Assertions;
import io.airlift.testing.Closeables;
import io.airlift.units.Duration;
import org.intellij.lang.annotations.Language;

import java.io.Closeable;
import java.net.URI;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.DEFAULT_PRECISION;
import static com.facebook.presto.util.DateTimeUtils.parseDate;
import static com.facebook.presto.util.DateTimeUtils.parseTime;
import static com.facebook.presto.util.DateTimeUtils.parseTimeWithTimeZone;
import static com.facebook.presto.util.DateTimeUtils.parseTimestamp;
import static com.facebook.presto.util.DateTimeUtils.parseTimestampWithTimeZone;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.units.Duration.nanosSince;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class DistributedQueryRunner
    implements Closeable
{
    private static final Logger log = Logger.get("TestQueries");

    private static final String ENVIRONMENT = "testing";
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);

    private final TestingDiscoveryServer discoveryServer;
    private final TestingPrestoServer coordinator;
    private final List<TestingPrestoServer> servers;
    private final AsyncHttpClient httpClient;
    private final ConnectorSession session;

    public DistributedQueryRunner(ConnectorSession defaultSession, int workersCount)
            throws Exception
    {
        session = checkNotNull(defaultSession, "defaultSession is null");

        try {
            discoveryServer = new TestingDiscoveryServer(ENVIRONMENT);

            ImmutableList.Builder<TestingPrestoServer> servers = ImmutableList.builder();
            coordinator = createTestingPrestoServer(discoveryServer.getBaseUrl(), true);
            servers.add(coordinator);

            for (int i = 1; i < workersCount; i++) {
                servers.add(createTestingPrestoServer(discoveryServer.getBaseUrl(), false));
            }
            this.servers = servers.build();
        }
        catch (Exception e) {
            close();
            throw e;
        }

        this.httpClient = new JettyHttpClient(
                new HttpClientConfig()
                        .setConnectTimeout(new Duration(1, TimeUnit.DAYS))
                        .setReadTimeout(new Duration(10, TimeUnit.DAYS)));

        long start = System.nanoTime();
        while (!allNodesGloballyVisible()) {
            Assertions.assertLessThan(nanosSince(start), new Duration(10, SECONDS));
            MILLISECONDS.sleep(10);
        }

        for (TestingPrestoServer server : servers) {
            server.getMetadata().addFunctions(AbstractTestQueries.CUSTOM_FUNCTIONS);
        }
    }

    private static TestingPrestoServer createTestingPrestoServer(URI discoveryUri, boolean coordinator)
            throws Exception
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put("query.client.timeout", "10m")
                .put("exchange.http-client.read-timeout", "1h")
                .put("datasources", "system");
        if (coordinator) {
            properties.put("node-scheduler.include-coordinator", "false");
        }

        TestingPrestoServer server = new TestingPrestoServer(coordinator, properties.build(), ENVIRONMENT, discoveryUri, ImmutableList.<Module>of());

        return server;
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

    public int getNodeCount()
    {
        return servers.size();
    }

    public TestingPrestoServer getCoordinator()
    {
        return coordinator;
    }

    public void installPlugin(Plugin plugin)
    {
        for (TestingPrestoServer server : servers) {
            server.installPlugin(plugin);
        }
    }

    public void createCatalog(String catalogName, String connectorName)
    {
        createCatalog(catalogName, connectorName, ImmutableMap.<String, String>of());
    }

    public void createCatalog(String catalogName, String connectorName, Map<String, String> properties)
    {
        for (TestingPrestoServer server : servers) {
            server.createCatalog(catalogName, connectorName, properties);
        }

        // wait for all nodes to announce the new catalog
        long start = System.nanoTime();
        while (!isConnectionVisibleToAllNodes(catalogName)) {
            Assertions.assertLessThan(nanosSince(start), new Duration(100, SECONDS), "waiting form connector "  + connectorName + " to be initialized in every node");
            try {
                MILLISECONDS.sleep(10);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }
        }
    }

    private boolean isConnectionVisibleToAllNodes(String connectorId)
    {
        for (TestingPrestoServer server : servers) {
            server.refreshNodes();
            Set<Node> activeNodesWithConnector = server.getActiveNodesWithConnector(connectorId);
            if (activeNodesWithConnector.size() != servers.size()) {
                return false;
            }
        }
        return true;
    }

    public List<QualifiedTableName> listTables(ConnectorSession session, String catalog, String schema)
    {
        return coordinator.getMetadata().listTables(session, new QualifiedTablePrefix(catalog, schema));
    }

    public boolean tableExists(ConnectorSession session, String table)
    {
        QualifiedTableName name =  new QualifiedTableName(session.getCatalog(), session.getSchema(), table);
        Optional<TableHandle> handle = coordinator.getMetadata().getTableHandle(session, name);
        return handle.isPresent();
    }

    public MaterializedResult execute(@Language("SQL") String sql)
    {
        return execute(session, sql);
    }

    public MaterializedResult execute(ConnectorSession session, @Language("SQL") String sql)
    {
        try (StatementClient client = new StatementClient(httpClient, QUERY_RESULTS_CODEC, toClientSession(session), sql)) {
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
                    rows.addAll(transform(results.getData(), dataToRow(session.getTimeZoneKey(), types)));
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

    private ClientSession toClientSession(ConnectorSession connectorSession)
    {
        return new ClientSession(
                coordinator.getBaseUrl(),
                connectorSession.getUser(),
                connectorSession.getSource(),
                connectorSession.getCatalog(),
                connectorSession.getSchema(),
                connectorSession.getTimeZoneKey().getId(),
                connectorSession.getLocale(), true);
    }

    @Override
    public final void close()
    {
        if (servers != null) {
            for (TestingPrestoServer server : servers) {
                Closeables.closeQuietly(server);
            }
        }
        Closeables.closeQuietly(discoveryServer);
    }

    private static Function<List<Object>, MaterializedRow> dataToRow(final TimeZoneKey timeZoneKey, final List<Type> types)
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
                        row.add(new Time(parseTime(timeZoneKey, (String) value)));
                    }
                    else if (TIME_WITH_TIME_ZONE.equals(type)) {
                        row.add(new Time(unpackMillisUtc(parseTimeWithTimeZone((String) value))));
                    }
                    else if (TIMESTAMP.equals(type)) {
                        row.add(new Timestamp(parseTimestamp(timeZoneKey, (String) value)));
                    }
                    else if (TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
                        row.add(new Timestamp(unpackMillisUtc(parseTimestampWithTimeZone((String) value))));
                    }
                    else {
                        throw new AssertionError("unhandled type: " + type);
                    }
                }
                return new MaterializedRow(DEFAULT_PRECISION, row);
            }
        };
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
}
