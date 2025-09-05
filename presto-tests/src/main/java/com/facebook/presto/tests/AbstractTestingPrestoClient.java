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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.Session;
import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.QueryError;
import com.facebook.presto.client.QueryStatusInfo;
import com.facebook.presto.client.StatementClient;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.MetadataUtil;
import com.facebook.presto.metadata.QualifiedTablePrefix;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.session.ResourceEstimates;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import okhttp3.OkHttpClient;
import org.intellij.lang.annotations.Language;

import java.io.Closeable;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.client.StatementClientFactory.newStatementClient;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.session.ResourceEstimates.CPU_TIME;
import static com.facebook.presto.spi.session.ResourceEstimates.EXECUTION_TIME;
import static com.facebook.presto.spi.session.ResourceEstimates.PEAK_MEMORY;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toMap;

public abstract class AbstractTestingPrestoClient<T>
        implements Closeable
{
    private static final JsonCodec<SqlFunctionId> SQL_FUNCTION_ID_JSON_CODEC = jsonCodec(SqlFunctionId.class);
    private static final JsonCodec<SqlInvokedFunction> SQL_INVOKED_FUNCTION_JSON_CODEC = jsonCodec(SqlInvokedFunction.class);

    private final TestingPrestoServer prestoServer;
    private final Session defaultSession;

    private final OkHttpClient httpClient = new OkHttpClient();

    protected AbstractTestingPrestoClient(TestingPrestoServer prestoServer,
            Session defaultSession)
    {
        this.prestoServer = requireNonNull(prestoServer, "prestoServer is null");
        this.defaultSession = requireNonNull(defaultSession, "defaultSession is null");
    }

    @Override
    public void close()
    {
        httpClient.dispatcher().executorService().shutdown();
        httpClient.connectionPool().evictAll();
    }

    protected abstract ResultsSession<T> getResultSession(Session session);

    public ResultWithQueryId<T> execute(@Language("SQL") String sql)
    {
        return execute(defaultSession, sql);
    }

    public ResultWithQueryId<T> execute(Session session, @Language("SQL") String sql)
    {
        ResultsSession<T> resultsSession = getResultSession(session);

        ClientSession clientSession = toClientSession(session, prestoServer.getBaseUrl(), new Duration(2, TimeUnit.MINUTES));

        try (StatementClient client = newStatementClient(httpClient, clientSession, sql)) {
            while (client.isRunning()) {
                resultsSession.addResults(client.currentStatusInfo(), client.currentData());
                client.advance();
            }

            checkState(client.isFinished());
            QueryError error = client.finalStatusInfo().getError();

            if (error == null) {
                QueryStatusInfo results = client.finalStatusInfo();
                if (results.getUpdateType() != null) {
                    resultsSession.setUpdateType(results.getUpdateType());
                }
                if (results.getUpdateCount() != null) {
                    resultsSession.setUpdateCount(results.getUpdateCount());
                }

                resultsSession.setWarnings(results.getWarnings());

                T result = resultsSession.build(client.getSetSessionProperties(), client.getResetSessionProperties(), client.getStartedTransactionId(), client.isClearTransactionId());
                return new ResultWithQueryId<>(new QueryId(results.getId()), result);
            }

            if (error.getFailureInfo() != null) {
                RuntimeException remoteException = error.getFailureInfo().toException();
                throw new RuntimeException(Optional.ofNullable(remoteException.getMessage()).orElseGet(remoteException::toString), remoteException);
            }
            throw new RuntimeException("Query failed: " + error.getMessage());

            // dump query info to console for debugging (NOTE: not pretty printed)
            // JsonCodec<QueryInfo> queryInfoJsonCodec = createCodecFactory().prettyPrint().jsonCodec(QueryInfo.class);
            // log.info("\n" + queryInfoJsonCodec.toJson(queryInfo));
        }
    }

    private static ClientSession toClientSession(Session session, URI server, Duration clientRequestTimeout)
    {
        Map<String, String> properties = new HashMap<>();
        properties.putAll(session.getSystemProperties());
        for (Entry<String, Map<String, String>> connectorProperties : session.getUnprocessedCatalogProperties().entrySet()) {
            for (Entry<String, String> entry : connectorProperties.getValue().entrySet()) {
                properties.put(connectorProperties.getKey() + "." + entry.getKey(), entry.getValue());
            }
        }

        for (Entry<ConnectorId, Map<String, String>> connectorProperties : session.getConnectorProperties().entrySet()) {
            for (Entry<String, String> entry : connectorProperties.getValue().entrySet()) {
                properties.put(connectorProperties.getKey().getCatalogName() + "." + entry.getKey(), entry.getValue());
            }
        }

        ImmutableMap.Builder<String, String> resourceEstimates = ImmutableMap.builder();
        ResourceEstimates estimates = session.getResourceEstimates();
        estimates.getExecutionTime().ifPresent(e -> resourceEstimates.put(EXECUTION_TIME, e.toString()));
        estimates.getCpuTime().ifPresent(e -> resourceEstimates.put(CPU_TIME, e.toString()));
        estimates.getPeakMemory().ifPresent(e -> resourceEstimates.put(PEAK_MEMORY, e.toString()));

        Map<String, String> serializedSessionFunctions = session.getSessionFunctions().entrySet().stream()
                .collect(collectingAndThen(
                        toMap(e -> SQL_FUNCTION_ID_JSON_CODEC.toJson(e.getKey()), e -> SQL_INVOKED_FUNCTION_JSON_CODEC.toJson(e.getValue())),
                        ImmutableMap::copyOf));

        return new ClientSession(
                server,
                session.getIdentity().getUser(),
                session.getSource().orElse(null),
                session.getTraceToken(),
                session.getClientTags(),
                session.getClientInfo().orElse(null),
                session.getCatalog().orElse(null),
                session.getSchema().orElse(null),
                session.getTimeZoneKey().getId(),
                session.getLocale(),
                resourceEstimates.build(),
                ImmutableMap.copyOf(properties),
                session.getPreparedStatements(),
                session.getIdentity().getRoles(),
                session.getIdentity().getExtraCredentials(),
                session.getTransactionId().map(Object::toString).orElse(null),
                clientRequestTimeout,
                true,
                serializedSessionFunctions,
                ImmutableMap.of(),
                false);
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
