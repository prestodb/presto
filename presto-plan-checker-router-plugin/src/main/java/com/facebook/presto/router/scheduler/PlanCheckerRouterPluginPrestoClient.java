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
package com.facebook.presto.router.scheduler;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.stats.CounterStat;
import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.ErrorLocation;
import com.facebook.presto.client.FailureInfo;
import com.facebook.presto.client.QueryError;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.QueryStatusInfo;
import com.facebook.presto.client.StatementClient;
import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.common.ErrorType;
import com.facebook.presto.server.HttpRequestSessionContext;
import com.facebook.presto.server.SessionContext;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.session.ResourceEstimates;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import okhttp3.OkHttpClient;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.servlet.http.HttpServletRequest;

import java.net.URI;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TRANSACTION_ID;
import static com.facebook.presto.client.StatementClientFactory.newStatementClient;
import static com.facebook.presto.common.ErrorType.USER_ERROR;
import static com.facebook.presto.spi.session.ResourceEstimates.CPU_TIME;
import static com.facebook.presto.spi.session.ResourceEstimates.EXECUTION_TIME;
import static com.facebook.presto.spi.session.ResourceEstimates.PEAK_MEMORY;
import static com.google.common.base.Verify.verify;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toMap;

public class PlanCheckerRouterPluginPrestoClient
{
    private static final Logger log = Logger.get(PlanCheckerRouterPluginPrestoClient.class);
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);
    private static final JsonCodec<SqlFunctionId> SQL_FUNCTION_ID_JSON_CODEC = jsonCodec(SqlFunctionId.class);
    private static final JsonCodec<SqlInvokedFunction> SQL_INVOKED_FUNCTION_JSON_CODEC = jsonCodec(SqlInvokedFunction.class);
    private static final String ANALYZE_CALL = "EXPLAIN (TYPE DISTRIBUTED) ";
    private static final CounterStat javaClusterRedirectRequests = new CounterStat();
    private static final CounterStat nativeClusterRedirectRequests = new CounterStat();

    private final OkHttpClient httpClient = new OkHttpClient();
    private final URI planCheckerClusterURI;
    private final URI javaRouterURI;
    private final URI nativeRouterURI;
    private final Duration clientRequestTimeout;

    public PlanCheckerRouterPluginPrestoClient(URI planCheckerClusterURI, URI javaRouterURI, URI nativeRouterURI, Duration clientRequestTimeout)
    {
        this.planCheckerClusterURI = planCheckerClusterURI;
        this.javaRouterURI = javaRouterURI;
        this.nativeRouterURI = nativeRouterURI;
        this.clientRequestTimeout = clientRequestTimeout;
    }

    public Optional<URI> getCompatibleClusterURI(HttpServletRequest httpServletRequest, String statement)
    {
        String newSql = ANALYZE_CALL + statement;
        ClientSession clientSession = parseHeadersToClientSession(httpServletRequest);
        boolean isNativeCompatible = true;
        // submit initial query
        try (StatementClient client = newStatementClient(httpClient, clientSession, newSql)) {
            // read query output
            while (client.isRunning()) {
                log.debug((client.currentData().toString()));

                if (!client.advance()) {
                    break;
                }
            }

            // verify final state
            if (client.isClientAborted()) {
                throw new IllegalStateException("Query aborted by user");
            }

            if (client.isClientError()) {
                throw new IllegalStateException("Query is gone (server restarted?)");
            }

            verify(client.isFinished());
            QueryError resultsError = client.finalStatusInfo().getError();
            if (resultsError != null) {
                if (ErrorType.valueOf(resultsError.getErrorType()) == USER_ERROR) {
                    throw new PrestoException(
                            () -> new ErrorCode(
                                    resultsError.getErrorCode(),
                                    resultsError.getErrorName(),
                                    USER_ERROR,
                                    resultsError.isRetriable()),
                            newQueryResults(client));
                }
                isNativeCompatible = false;
                log.info(resultsError.getMessage());
            }
        }

        if (isNativeCompatible) {
            log.debug("Native compatible, routing to native-clusters router: [%s]", nativeRouterURI);
            nativeClusterRedirectRequests.update(1L);
            return Optional.of(nativeRouterURI);
        }
        log.debug("Native incompatible, routing to java-clusters router: [%s]", javaRouterURI);
        javaClusterRedirectRequests.update(1L);
        return Optional.of(javaRouterURI);
    }

    @Managed
    @Nested
    public CounterStat getJavaClusterRedirectRequests()
    {
        return javaClusterRedirectRequests;
    }

    @Managed
    @Nested
    public CounterStat getNativeClusterRedirectRequests()
    {
        return nativeClusterRedirectRequests;
    }

    private String newQueryResults(StatementClient client)
    {
        QueryStatusInfo queryStatusInfo = client.finalStatusInfo();
        QueryResults queryResults = new QueryResults(
                queryStatusInfo.getId(),
                queryStatusInfo.getInfoUri(),
                queryStatusInfo.getPartialCancelUri(),
                queryStatusInfo.getNextUri(),
                queryStatusInfo.getColumns(),
                null, // if error thrown no data is returned.
                null,
                queryStatusInfo.getStats(),
                newQueryError(queryStatusInfo.getError()),
                queryStatusInfo.getWarnings(),
                queryStatusInfo.getUpdateType(),
                queryStatusInfo.getUpdateCount());

        return QUERY_RESULTS_CODEC.toJson(queryResults);
    }

    private QueryError newQueryError(QueryError error)
    {
        // todo: the getMessage() calls will still return the previous error location
        ErrorLocation errorLocation = error.getErrorLocation();
        ErrorLocation newErrorLocation = null;
        FailureInfo failureInfo = error.getFailureInfo();
        if (errorLocation != null) {
            newErrorLocation = new ErrorLocation(
                    errorLocation.getLineNumber(),
                    errorLocation.getColumnNumber() - ANALYZE_CALL.length());
        }
        return new QueryError(
                error.getMessage(),
                error.getSqlState(),
                error.getErrorCode(),
                error.getErrorName(),
                error.getErrorType(),
                error.isRetriable(),
                newErrorLocation,
                new FailureInfo(
                        failureInfo.getType(),
                        failureInfo.getMessage(),
                        failureInfo.getCause(),
                        failureInfo.getSuppressed(),
                        failureInfo.getStack(),
                        newErrorLocation));
    }

    private ClientSession parseHeadersToClientSession(HttpServletRequest httpServletRequest)
    {
        SessionContext sessionContext = new HttpRequestSessionContext(
                httpServletRequest,
                new SqlParserOptions());

        return new ClientSession(
                planCheckerClusterURI,
                sessionContext.getIdentity().getUser(),
                sessionContext.getSource(),
                sessionContext.getTraceToken(),
                sessionContext.getClientTags(),
                sessionContext.getClientInfo(),
                sessionContext.getCatalog(),
                sessionContext.getSchema(),
                sessionContext.getTimeZoneId(),
                sessionContext.getLanguage() == null ? Locale.ENGLISH : Locale.forLanguageTag(sessionContext.getLanguage()),
                getResourceEstimates(sessionContext),
                sessionContext.getSystemProperties(),
                sessionContext.getPreparedStatements(),
                sessionContext.getIdentity().getRoles(),
                sessionContext.getIdentity().getExtraCredentials(),
                httpServletRequest.getHeader(PRESTO_TRANSACTION_ID),
                clientRequestTimeout,
                true,
                getSerializedSessionFunctions(sessionContext),
                ImmutableMap.of(), // todo: do we need custom headers?
                true);
    }

    private static Map<String, String> getResourceEstimates(SessionContext sessionContext)
    {
        ImmutableMap.Builder<String, String> resourceEstimates = ImmutableMap.builder();
        ResourceEstimates estimates = sessionContext.getResourceEstimates();
        estimates.getExecutionTime().ifPresent(e -> resourceEstimates.put(EXECUTION_TIME, e.toString()));
        estimates.getCpuTime().ifPresent(e -> resourceEstimates.put(CPU_TIME, e.toString()));
        estimates.getPeakMemory().ifPresent(e -> resourceEstimates.put(PEAK_MEMORY, e.toString()));
        return resourceEstimates.build();
    }

    private static Map<String, String> getSerializedSessionFunctions(SessionContext sessionContext)
    {
        return sessionContext.getSessionFunctions().entrySet().stream()
                .collect(collectingAndThen(
                        toMap(e -> SQL_FUNCTION_ID_JSON_CODEC.toJson(e.getKey()), e -> SQL_INVOKED_FUNCTION_JSON_CODEC.toJson(e.getValue())),
                        ImmutableMap::copyOf));
    }
}
