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
package com.facebook.presto.router;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.ErrorLocation;
import com.facebook.presto.client.FailureInfo;
import com.facebook.presto.client.QueryError;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.QueryStatusInfo;
import com.facebook.presto.client.StatementClient;
import com.facebook.presto.server.HttpRequestSessionContext;
import com.facebook.presto.server.SessionContext;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import okhttp3.OkHttpClient;

import javax.servlet.http.HttpServletRequest;

import java.net.URI;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TRANSACTION_ID;
import static com.facebook.presto.client.StatementClientFactory.newStatementClient;
import static com.google.common.base.Verify.verify;
import static java.util.concurrent.TimeUnit.MINUTES;

public class SimplePrestoClient
{
    private static final Logger log = Logger.get(RouterResource.class);
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);
    private static final String ANALYZE_CALL = "EXPLAIN (TYPE DISTRIBUTED) ";

    private final OkHttpClient httpClient = new OkHttpClient();
    private final URI preonClusterURI;
    private final String clientTagForNativeClusterRouting;

    public SimplePrestoClient(URI preonClusterURI, String clientTagForNativeClusterRouting)
    {
        this.preonClusterURI = preonClusterURI;
        this.clientTagForNativeClusterRouting = clientTagForNativeClusterRouting;
    }

    public Optional<List<String>> getCompatibleClusterClientTags(HttpServletRequest httpServletRequest, String statement)
    {
        String newSql = ANALYZE_CALL + statement;
        ClientSession clientSession = parseHeadersToClientSession(httpServletRequest);
        boolean isNativeCompatible = true;
        // submit initial query
        try (StatementClient client = newStatementClient(httpClient, clientSession, newSql)) {
            // read query output
            while (client.isRunning()) {
                log.info((client.currentData().toString()));

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
                if (resultsError.getErrorType().equalsIgnoreCase("USER_ERROR")) {
                    throw new RuntimeException(newQueryResults(client));
                }
                isNativeCompatible = false;
                log.info(resultsError.getMessage());
            }
        }

        // todo: fix this hack, @aaneja will add the filter chain
        if (isNativeCompatible) {
            log.info("Native compatible, adding native cluster client tags: ");
            return Optional.of(ImmutableList.of(clientTagForNativeClusterRouting));
        }
        return Optional.empty();
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
        // todo: How to parse headers into a ClientSession object?
        SessionContext sessionContext = new HttpRequestSessionContext(
                httpServletRequest,
                new SqlParserOptions());
        return new ClientSession(
                preonClusterURI,
                sessionContext.getIdentity().getUser(),
                sessionContext.getSource(),
                sessionContext.getTraceToken(),
                sessionContext.getClientTags(),
                sessionContext.getClientInfo(),
                sessionContext.getCatalog(),
                sessionContext.getSchema(),
                sessionContext.getTimeZoneId(),
                Locale.ENGLISH,
                ImmutableMap.of(),
                sessionContext.getSystemProperties(),
                sessionContext.getPreparedStatements(),
                sessionContext.getIdentity().getRoles(),
                sessionContext.getIdentity().getExtraCredentials(),
                httpServletRequest.getHeader(PRESTO_TRANSACTION_ID),
                new Duration(2, MINUTES),
                true,
                ImmutableMap.of(),
                ImmutableMap.of(),
                true);
    }
}
