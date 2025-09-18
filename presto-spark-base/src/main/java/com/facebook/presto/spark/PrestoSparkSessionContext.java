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
package com.facebook.presto.spark;

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.transaction.TransactionId;
import com.facebook.presto.server.SessionContext;
import com.facebook.presto.spark.accesscontrol.PrestoSparkAuthenticatorProvider;
import com.facebook.presto.spark.accesscontrol.PrestoSparkCredentialsProvider;
import com.facebook.presto.spark.classloader_interface.PrestoSparkSession;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.TokenAuthenticator;
import com.facebook.presto.spi.session.ResourceEstimates;
import com.facebook.presto.spi.tracing.Tracer;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import jakarta.annotation.Nullable;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class PrestoSparkSessionContext
        implements SessionContext
{
    private final Identity identity;
    private final String catalog;
    private final String schema;
    private final String source;

    private final String sqlText;

    private final String userAgent;
    private final String clientInfo;
    private final Set<String> clientTags;
    private final String timeZoneId;
    private final String language;

    private final Map<String, String> systemProperties;
    private final Map<String, Map<String, String>> catalogSessionProperties;
    private final Optional<String> traceToken;
    private final RuntimeStats runtimeStats = new RuntimeStats();

    public static PrestoSparkSessionContext createFromSessionInfo(
            PrestoSparkSession prestoSparkSession,
            Set<PrestoSparkCredentialsProvider> credentialsProviders,
            Set<PrestoSparkAuthenticatorProvider> authenticatorProviders,
            String sqlQueryText)
    {
        ImmutableMap.Builder<String, String> extraCredentials = ImmutableMap.builder();
        extraCredentials.putAll(prestoSparkSession.getExtraCredentials());
        credentialsProviders.forEach(provider -> extraCredentials.putAll(provider.getCredentials()));

        ImmutableMap.Builder<String, TokenAuthenticator> extraTokenAuthenticators = ImmutableMap.builder();
        authenticatorProviders.forEach(provider -> extraTokenAuthenticators.putAll(provider.getTokenAuthenticators()));

        return new PrestoSparkSessionContext(
                new Identity(
                        prestoSparkSession.getUser(),
                        prestoSparkSession.getPrincipal(),
                        ImmutableMap.of(),  // presto on spark does not support role management
                        extraCredentials.build(),
                        extraTokenAuthenticators.build(),
                        Optional.empty(),
                        Optional.empty()),
                prestoSparkSession.getCatalog().orElse(null),
                prestoSparkSession.getSchema().orElse(null),
                prestoSparkSession.getSource().orElse(null),
                sqlQueryText,
                prestoSparkSession.getUserAgent().orElse(null),
                prestoSparkSession.getClientInfo().orElse(null),
                prestoSparkSession.getClientTags(),
                prestoSparkSession.getTimeZoneId().orElse(null),
                prestoSparkSession.getLanguage().orElse(null),
                prestoSparkSession.getSystemProperties(),
                prestoSparkSession.getCatalogSessionProperties(),
                prestoSparkSession.getTraceToken());
    }

    public PrestoSparkSessionContext(
            Identity identity,
            String catalog,
            String schema,
            String source,
            String sqlText,
            String userAgent,
            String clientInfo,
            Set<String> clientTags,
            String timeZoneId,
            String language,
            Map<String, String> systemProperties,
            Map<String, Map<String, String>> catalogSessionProperties,
            Optional<String> traceToken)
    {
        this.identity = requireNonNull(identity, "identity is null");
        this.catalog = catalog;
        this.schema = schema;
        this.source = source;
        this.sqlText = requireNonNull(sqlText, "sqlText is null");
        this.userAgent = userAgent;
        this.clientInfo = clientInfo;
        this.clientTags = ImmutableSet.copyOf(requireNonNull(clientTags, "clientTags is null"));
        this.timeZoneId = timeZoneId;
        this.language = language;
        this.systemProperties = ImmutableMap.copyOf(requireNonNull(systemProperties, "systemProperties is null"));
        this.catalogSessionProperties = ImmutableMap.copyOf(requireNonNull(catalogSessionProperties, "catalogSessionProperties is null"));
        this.traceToken = requireNonNull(traceToken, "traceToken is null");
    }

    @Override
    public Identity getIdentity()
    {
        return identity;
    }

    @Nullable
    @Override
    public String getCatalog()
    {
        return catalog;
    }

    @Nullable
    @Override
    public String getSchema()
    {
        return schema;
    }

    @Override
    public String getSqlText()
    {
        return sqlText;
    }

    @Nullable
    @Override
    public String getSource()
    {
        return source;
    }

    @Override
    public String getRemoteUserAddress()
    {
        return "localhost";
    }

    @Nullable
    @Override
    public String getUserAgent()
    {
        return userAgent;
    }

    @Nullable
    @Override
    public String getClientInfo()
    {
        return clientInfo;
    }

    @Override
    public Set<String> getClientTags()
    {
        return clientTags;
    }

    @Override
    public ResourceEstimates getResourceEstimates()
    {
        // presto on spark does not use resource groups
        return new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    @Nullable
    @Override
    public String getTimeZoneId()
    {
        return timeZoneId;
    }

    @Nullable
    @Override
    public String getLanguage()
    {
        return language;
    }

    @Override
    public Optional<Tracer> getTracer()
    {
        return Optional.empty();
    }

    @Override
    public Map<String, String> getSystemProperties()
    {
        return systemProperties;
    }

    @Override
    public Map<String, Map<String, String>> getCatalogSessionProperties()
    {
        return catalogSessionProperties;
    }

    @Override
    public Map<String, String> getPreparedStatements()
    {
        // presto on spark does not support prepared statements
        return ImmutableMap.of();
    }

    @Override
    public Optional<TransactionId> getTransactionId()
    {
        // presto on spark does not support explicit transaction management
        return Optional.empty();
    }

    @Override
    public Optional<String> getTraceToken()
    {
        return traceToken;
    }

    @Override
    public boolean supportClientTransaction()
    {
        return false;
    }

    @Override
    public Map<SqlFunctionId, SqlInvokedFunction> getSessionFunctions()
    {
        // presto on spark does not support session functions
        return ImmutableMap.of();
    }

    @Override
    public RuntimeStats getRuntimeStats()
    {
        return runtimeStats;
    }
}
