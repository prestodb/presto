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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import okhttp3.OkHttpClient;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.net.URI;
import java.security.Principal;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TRANSACTION_ID;
import static com.facebook.presto.client.StatementClientFactory.newStatementClient;
import static com.facebook.presto.common.ErrorType.USER_ERROR;
import static com.facebook.presto.router.scheduler.PlanCheckerPluginHttpRequestSessionContext.getResourceEstimates;
import static com.facebook.presto.router.scheduler.PlanCheckerPluginHttpRequestSessionContext.getSerializedSessionFunctions;
import static com.facebook.presto.spi.StandardErrorCode.ABANDONED_QUERY;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.PERMISSION_DENIED;
import static com.facebook.presto.spi.StandardErrorCode.SYNTAX_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.USER_CANCELED;
import static com.google.common.base.Verify.verify;

public class PlanCheckerRouterPluginPrestoClient
{
    private static final Logger log = Logger.get(PlanCheckerRouterPluginPrestoClient.class);
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);
    private static final String ANALYZE_CALL = "EXPLAIN (TYPE DISTRIBUTED) ";
    private static final CounterStat javaClusterRedirectRequests = new CounterStat();
    private static final CounterStat nativeClusterRedirectRequests = new CounterStat();
    private static final Set<StandardErrorCode> USER_ERRORS_SET =
            new HashSet<>(ImmutableList.of(GENERIC_USER_ERROR, SYNTAX_ERROR, ABANDONED_QUERY, USER_CANCELED, PERMISSION_DENIED));
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

    public Optional<URI> getCompatibleClusterURI(Map<String, List<String>> headers, String statement, Principal principal, String remoteUserAddr)
    {
        String newSql = ANALYZE_CALL + statement;
        ClientSession clientSession = parseHeadersToClientSession(headers, principal, remoteUserAddr);
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
                returnQueryErrorToClient(resultsError, client);
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

    private void returnQueryErrorToClient(QueryError resultsError, StatementClient client)
    {
        if (ErrorType.valueOf(resultsError.getErrorType()) == USER_ERROR) {
            if (USER_ERRORS_SET.stream()
                    .anyMatch((code -> code.toErrorCode().getName()
                                    .equalsIgnoreCase(resultsError.getErrorName())))) {
                throw new PrestoException(
                        () -> new ErrorCode(
                                resultsError.getErrorCode(),
                                resultsError.getErrorName(),
                                USER_ERROR,
                                resultsError.isRetriable()),
                        newQueryResults(client));
            }
        }
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

    private ClientSession parseHeadersToClientSession(Map<String, List<String>> headers, Principal principal, String remoteUserAddr)
    {
        PlanCheckerPluginHttpRequestSessionContext sessionContext =
                new PlanCheckerPluginHttpRequestSessionContext(
                        headers,
                        new SqlParserOptions(),
                        principal,
                        remoteUserAddr);

        return new ClientSession(
                planCheckerClusterURI,
                sessionContext.getIdentity().getUser(),
                sessionContext.getSource(),
                Optional.empty(),
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
                sessionContext.getHeader(PRESTO_TRANSACTION_ID),
                clientRequestTimeout,
                true,
                getSerializedSessionFunctions(sessionContext),
                ImmutableMap.of(), // todo: do we need custom headers?
                true);
    }
}
