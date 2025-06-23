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

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.stats.CounterStat;
import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.QueryError;
import com.facebook.presto.client.StatementClient;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import okhttp3.OkHttpClient;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.inject.Inject;

import java.net.URI;
import java.security.Principal;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.client.PrestoHeaders.PRESTO_TRANSACTION_ID;
import static com.facebook.presto.client.StatementClientFactory.newStatementClient;
import static com.facebook.presto.router.scheduler.PlanCheckerPluginHttpRequestSessionContext.getResourceEstimates;
import static com.facebook.presto.router.scheduler.PlanCheckerPluginHttpRequestSessionContext.getSerializedSessionFunctions;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class PlanCheckerRouterPluginPrestoClient
{
    private static final Logger log = Logger.get(PlanCheckerRouterPluginPrestoClient.class);
    private static final String ANALYZE_CALL = "EXPLAIN (TYPE DISTRIBUTED) ";
    private final CounterStat javaClusterRedirectRequests;
    private final CounterStat nativeClusterRedirectRequests;
    private final OkHttpClient httpClient = new OkHttpClient();
    private final URI javaRouterURI;
    private final URI nativeRouterURI;
    private final Duration clientRequestTimeout;

    @Inject
    public PlanCheckerRouterPluginPrestoClient(
            @JavaClusterRedirectRequestsCounter CounterStat javaClusterRedirectRequests,
            @NativeClustersRedirectRequestsCounter CounterStat nativeClusterRedirectRequests,
            PlanCheckerRouterPluginConfig planCheckerRouterConfig)
    {
        requireNonNull(planCheckerRouterConfig, "PlanCheckerRouterPluginConfig is null");
        this.javaRouterURI =
                requireNonNull(planCheckerRouterConfig.getJavaRouterURI(), "javaRouterURI is null");
        this.nativeRouterURI =
                requireNonNull(planCheckerRouterConfig.getNativeRouterURI(), "nativeRouterURI is null");
        this.clientRequestTimeout = planCheckerRouterConfig.getClientRequestTimeout();
        this.javaClusterRedirectRequests = javaClusterRedirectRequests;
        this.nativeClusterRedirectRequests = nativeClusterRedirectRequests;
    }

    public Optional<URI> getCompatibleClusterURI(URI planCheckerClusterURI, Map<String, List<String>> headers, String statement, Principal principal, String remoteUserAddr)
    {
        String newSql = ANALYZE_CALL + statement;
        ClientSession clientSession = parseHeadersToClientSession(planCheckerClusterURI, headers, principal, remoteUserAddr);
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

    private ClientSession parseHeadersToClientSession(URI planCheckerClusterURI, Map<String, List<String>> headers, Principal principal, String remoteUserAddr)
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
