/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.AbstractTestQueries;
import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.QueryError;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementClient;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.lang.String.format;
import static java.util.Collections.nCopies;

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
            coordinator = createTestingPrestoServer(discoveryServer.getBaseUrl());
            servers = ImmutableList.<TestingPrestoServer>builder()
                    .add(coordinator)
                    .add(createTestingPrestoServer(discoveryServer.getBaseUrl()))
                    .add(createTestingPrestoServer(discoveryServer.getBaseUrl()))
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

        for (TestingPrestoServer server : servers) {
            server.refreshServiceSelectors();
        }

        log.info("Loading data...");
        long startTime = System.nanoTime();
        distributeData(catalog, schema);
        log.info("Loading complete in %.2fs", Duration.nanosSince(startTime).getValue(TimeUnit.SECONDS));
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
        List<QualifiedTableName> qualifiedTableNames = coordinator.getMetadata().listTables(new QualifiedTablePrefix(catalog, schema));
        for (QualifiedTableName qualifiedTableName : qualifiedTableNames) {
            if (qualifiedTableName.getTableName().equalsIgnoreCase("dual")) {
                continue;
            }
            log.info("Running import for %s", qualifiedTableName.getTableName());
            MaterializedResult importResult = computeActual(format("CREATE MATERIALIZED VIEW default.default.%s AS SELECT * FROM %s",
                    qualifiedTableName.getTableName(),
                    qualifiedTableName));
            log.info("Imported %s rows for %s", importResult.getMaterializedTuples().get(0).getField(0), qualifiedTableName.getTableName());
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

    private static TestingPrestoServer createTestingPrestoServer(URI discoveryUri)
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("query.client.timeout", "10m")
                .put("exchange.http-client.read-timeout", "1h")
                .put("failure-detector.warmup-interval", "0ms")
                .put("failure-detector.enabled", "false") // todo enable failure detector
                .put("datasources", "native,tpch")
                .build();

        return new TestingPrestoServer(properties, ENVIRONMENT, discoveryUri);
    }
}
