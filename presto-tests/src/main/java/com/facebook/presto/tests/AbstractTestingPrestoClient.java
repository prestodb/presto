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

import com.facebook.presto.Session;
import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.QueryError;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementClient;
import com.facebook.presto.metadata.MetadataUtil;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.metadata.QualifiedTablePrefix;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import org.intellij.lang.annotations.Language;

import java.io.Closeable;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.util.Objects.requireNonNull;

public abstract class AbstractTestingPrestoClient<T>
        implements Closeable
{
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);

    private final TestingPrestoServer prestoServer;
    private final Session defaultSession;

    private final HttpClient httpClient;

    protected AbstractTestingPrestoClient(TestingPrestoServer prestoServer,
            Session defaultSession)
    {
        this.prestoServer = requireNonNull(prestoServer, "prestoServer is null");
        this.defaultSession = requireNonNull(defaultSession, "defaultSession is null");

        this.httpClient = new JettyHttpClient(
                new HttpClientConfig()
                        .setConnectTimeout(new Duration(1, TimeUnit.DAYS))
                        .setIdleTimeout(new Duration(10, TimeUnit.DAYS)));
    }

    @Override
    public void close()
    {
        this.httpClient.close();
    }

    protected abstract ResultsSession<T> getResultSession(Session session);

    public T execute(@Language("SQL") String sql)
    {
        return execute(defaultSession, sql);
    }

    public T execute(Session session, @Language("SQL") String sql)
    {
        ResultsSession<T> resultsSession = getResultSession(session);

        ClientSession clientSession = toClientSession(session, prestoServer.getBaseUrl(), true, new Duration(2, TimeUnit.MINUTES));

        try (StatementClient client = new StatementClient(httpClient, QUERY_RESULTS_CODEC, clientSession, sql)) {
            while (client.isValid()) {
                QueryResults results = client.current();

                resultsSession.addResults(results);
                client.advance();
            }

            if (!client.isFailed()) {
                QueryResults results = client.finalResults();
                if (results.getUpdateType() != null) {
                    resultsSession.setUpdateType(results.getUpdateType());
                }
                if (results.getUpdateCount() != null) {
                    resultsSession.setUpdateCount(results.getUpdateCount());
                }

                return resultsSession.build(client.getSetSessionProperties(), client.getResetSessionProperties());
            }

            QueryError error = client.finalResults().getError();
            verify(error != null, "no error");
            if (error.getFailureInfo() != null) {
                throw error.getFailureInfo().toException();
            }
            throw new RuntimeException("Query failed: " + error.getMessage());

            // dump query info to console for debugging (NOTE: not pretty printed)
            // JsonCodec<QueryInfo> queryInfoJsonCodec = createCodecFactory().prettyPrint().jsonCodec(QueryInfo.class);
            // log.info("\n" + queryInfoJsonCodec.toJson(queryInfo));
        }
    }

    private static ClientSession toClientSession(Session session, URI server, boolean debug, Duration clientRequestTimeout)
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        properties.putAll(session.getSystemProperties());
        for (Entry<String, Map<String, String>> connectorProperties : session.getUnprocessedCatalogProperties().entrySet()) {
            for (Entry<String, String> entry : connectorProperties.getValue().entrySet()) {
                properties.put(connectorProperties.getKey() + "." + entry.getKey(), entry.getValue());
            }
        }

        return new ClientSession(
                server,
                session.getIdentity().getUser(),
                session.getSource().orElse(null),
                session.getClientInfo().orElse(null),
                session.getCatalog().orElse(null),
                session.getSchema().orElse(null),
                session.getTimeZoneKey().getId(),
                session.getLocale(),
                properties.build(),
                session.getPreparedStatements(),
                session.getTransactionId().map(Object::toString).orElse(null),
                debug,
                clientRequestTimeout);
    }

    public List<QualifiedObjectName> listTables(Session session, String catalog, String schema)
    {
        return transaction(prestoServer.getTransactionManager(), prestoServer.getAccessControl())
                .readOnly()
                .execute(session, transactionSession -> {
                    return prestoServer.getMetadata().listTables(transactionSession, new QualifiedTablePrefix(catalog, schema));
                });
    }

    public boolean tableExists(Session session, String table)
    {
        return transaction(prestoServer.getTransactionManager(), prestoServer.getAccessControl())
                .readOnly()
                .execute(session, transactionSession -> {
                    return MetadataUtil.tableExists(prestoServer.getMetadata(), transactionSession, table);
                });
    }

    public Session getDefaultSession()
    {
        return defaultSession;
    }

    public TestingPrestoServer getServer()
    {
        return prestoServer;
    }

    protected List<Type> getTypes(List<Column> columns)
    {
        return ImmutableList.copyOf(transform(columns, columnTypeGetter()));
    }

    protected Function<Column, Type> columnTypeGetter()
    {
        return column -> {
            String typeName = column.getType();
            Type type = prestoServer.getMetadata().getType(parseTypeSignature(typeName));
            if (type == null) {
                throw new AssertionError("Unhandled type: " + typeName);
            }
            return type;
        };
    }
}
