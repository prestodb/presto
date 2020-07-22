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
package com.facebook.presto;

import com.facebook.airlift.http.server.BasicPrincipal;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.SelectedRole;
import com.facebook.presto.spi.security.TokenAuthenticator;
import com.facebook.presto.spi.session.ResourceEstimates;
import com.facebook.presto.transaction.TransactionId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public final class SessionRepresentation
{
    private final String queryId;
    private final Optional<TransactionId> transactionId;
    private final boolean clientTransactionSupport;
    private final String user;
    private final Optional<String> principal;
    private final Optional<String> source;
    private final Optional<String> catalog;
    private final Optional<String> schema;
    private final Optional<String> traceToken;
    private final TimeZoneKey timeZoneKey;
    private final Locale locale;
    private final Optional<String> remoteUserAddress;
    private final Optional<String> userAgent;
    private final Optional<String> clientInfo;
    private final Set<String> clientTags;
    private final long startTime;
    private final ResourceEstimates resourceEstimates;
    private final Map<String, String> systemProperties;
    private final Map<ConnectorId, Map<String, String>> catalogProperties;
    private final Map<String, Map<String, String>> unprocessedCatalogProperties;
    private final Map<String, SelectedRole> roles;
    private final Map<String, String> preparedStatements;

    @JsonCreator
    public SessionRepresentation(
            @JsonProperty("queryId") String queryId,
            @JsonProperty("transactionId") Optional<TransactionId> transactionId,
            @JsonProperty("clientTransactionSupport") boolean clientTransactionSupport,
            @JsonProperty("user") String user,
            @JsonProperty("principal") Optional<String> principal,
            @JsonProperty("source") Optional<String> source,
            @JsonProperty("catalog") Optional<String> catalog,
            @JsonProperty("schema") Optional<String> schema,
            @JsonProperty("traceToken") Optional<String> traceToken,
            @JsonProperty("timeZoneKey") TimeZoneKey timeZoneKey,
            @JsonProperty("locale") Locale locale,
            @JsonProperty("remoteUserAddress") Optional<String> remoteUserAddress,
            @JsonProperty("userAgent") Optional<String> userAgent,
            @JsonProperty("clientInfo") Optional<String> clientInfo,
            @JsonProperty("clientTags") Set<String> clientTags,
            @JsonProperty("resourceEstimates") ResourceEstimates resourceEstimates,
            @JsonProperty("startTime") long startTime,
            @JsonProperty("systemProperties") Map<String, String> systemProperties,
            @JsonProperty("catalogProperties") Map<ConnectorId, Map<String, String>> catalogProperties,
            @JsonProperty("unprocessedCatalogProperties") Map<String, Map<String, String>> unprocessedCatalogProperties,
            @JsonProperty("roles") Map<String, SelectedRole> roles,
            @JsonProperty("preparedStatements") Map<String, String> preparedStatements)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.transactionId = requireNonNull(transactionId, "transactionId is null");
        this.clientTransactionSupport = clientTransactionSupport;
        this.user = requireNonNull(user, "user is null");
        this.principal = requireNonNull(principal, "principal is null");
        this.source = requireNonNull(source, "source is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.traceToken = requireNonNull(traceToken, "traceToken is null");
        this.timeZoneKey = requireNonNull(timeZoneKey, "timeZoneKey is null");
        this.locale = requireNonNull(locale, "locale is null");
        this.remoteUserAddress = requireNonNull(remoteUserAddress, "remoteUserAddress is null");
        this.userAgent = requireNonNull(userAgent, "userAgent is null");
        this.clientInfo = requireNonNull(clientInfo, "clientInfo is null");
        this.clientTags = requireNonNull(clientTags, "clientTags is null");
        this.resourceEstimates = requireNonNull(resourceEstimates, "resourceEstimates is null");
        this.startTime = startTime;
        this.systemProperties = ImmutableMap.copyOf(systemProperties);
        this.roles = ImmutableMap.copyOf(roles);
        this.preparedStatements = ImmutableMap.copyOf(preparedStatements);

        ImmutableMap.Builder<ConnectorId, Map<String, String>> catalogPropertiesBuilder = ImmutableMap.builder();
        for (Entry<ConnectorId, Map<String, String>> entry : catalogProperties.entrySet()) {
            catalogPropertiesBuilder.put(entry.getKey(), ImmutableMap.copyOf(entry.getValue()));
        }
        this.catalogProperties = catalogPropertiesBuilder.build();

        ImmutableMap.Builder<String, Map<String, String>> unprocessedCatalogPropertiesBuilder = ImmutableMap.builder();
        for (Entry<String, Map<String, String>> entry : unprocessedCatalogProperties.entrySet()) {
            unprocessedCatalogPropertiesBuilder.put(entry.getKey(), ImmutableMap.copyOf(entry.getValue()));
        }
        this.unprocessedCatalogProperties = unprocessedCatalogPropertiesBuilder.build();
    }

    @JsonProperty
    public String getQueryId()
    {
        return queryId;
    }

    @JsonProperty
    public Optional<TransactionId> getTransactionId()
    {
        return transactionId;
    }

    @JsonProperty
    public boolean isClientTransactionSupport()
    {
        return clientTransactionSupport;
    }

    @JsonProperty
    public String getUser()
    {
        return user;
    }

    @JsonProperty
    public Optional<String> getPrincipal()
    {
        return principal;
    }

    @JsonProperty
    public Optional<String> getSource()
    {
        return source;
    }

    @JsonProperty
    public Optional<String> getTraceToken()
    {
        return traceToken;
    }

    @JsonProperty
    public Optional<String> getCatalog()
    {
        return catalog;
    }

    @JsonProperty
    public Optional<String> getSchema()
    {
        return schema;
    }

    @JsonProperty
    public TimeZoneKey getTimeZoneKey()
    {
        return timeZoneKey;
    }

    @JsonProperty
    public Locale getLocale()
    {
        return locale;
    }

    @JsonProperty
    public Optional<String> getRemoteUserAddress()
    {
        return remoteUserAddress;
    }

    @JsonProperty
    public Optional<String> getUserAgent()
    {
        return userAgent;
    }

    @JsonProperty
    public Optional<String> getClientInfo()
    {
        return clientInfo;
    }

    @JsonProperty
    public Set<String> getClientTags()
    {
        return clientTags;
    }

    @JsonProperty
    public long getStartTime()
    {
        return startTime;
    }

    @JsonProperty
    public ResourceEstimates getResourceEstimates()
    {
        return resourceEstimates;
    }

    @JsonProperty
    public Map<String, String> getSystemProperties()
    {
        return systemProperties;
    }

    @JsonProperty
    public Map<ConnectorId, Map<String, String>> getCatalogProperties()
    {
        return catalogProperties;
    }

    @JsonProperty
    public Map<String, Map<String, String>> getUnprocessedCatalogProperties()
    {
        return unprocessedCatalogProperties;
    }

    @JsonProperty
    public Map<String, SelectedRole> getRoles()
    {
        return roles;
    }

    @JsonProperty
    public Map<String, String> getPreparedStatements()
    {
        return preparedStatements;
    }

    public Session toSession(SessionPropertyManager sessionPropertyManager)
    {
        return toSession(sessionPropertyManager, emptyMap(), emptyMap());
    }

    public Session toSession(SessionPropertyManager sessionPropertyManager, Map<String, String> extraCredentials)
    {
        return toSession(sessionPropertyManager, extraCredentials, emptyMap());
    }

    public Session toSession(SessionPropertyManager sessionPropertyManager, Map<String, String> extraCredentials, Map<String, TokenAuthenticator> extraAuthenticators)
    {
        return new Session(
                new QueryId(queryId),
                transactionId,
                clientTransactionSupport,
                new Identity(
                        user,
                        principal.map(BasicPrincipal::new),
                        roles,
                        extraCredentials,
                        extraAuthenticators),
                source,
                catalog,
                schema,
                traceToken,
                timeZoneKey,
                locale,
                remoteUserAddress,
                userAgent,
                clientInfo,
                clientTags,
                resourceEstimates,
                startTime,
                systemProperties,
                catalogProperties,
                unprocessedCatalogProperties,
                sessionPropertyManager,
                preparedStatements);
    }
}
