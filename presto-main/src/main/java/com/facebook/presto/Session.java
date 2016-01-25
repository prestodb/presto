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

import com.facebook.presto.client.ClientSession;
import com.facebook.presto.execution.QueryId;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.sql.tree.Execute;
import com.facebook.presto.transaction.TransactionId;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.airlift.units.Duration;

import java.net.URI;
import java.security.Principal;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.TimeZone;

import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public final class Session
{
    private final QueryId queryId;
    private final Optional<TransactionId> transactionId;
    private final boolean clientTransactionSupport;
    private final Identity identity;
    private final Optional<String> source;
    private final Optional<String> catalog;
    private final Optional<String> schema;
    private final TimeZoneKey timeZoneKey;
    private final Locale locale;
    private final Optional<String> remoteUserAddress;
    private final Optional<String> userAgent;
    private final long startTime;
    private final Map<String, String> systemProperties;
    private final Map<String, Map<String, String>> catalogProperties;
    private final SessionPropertyManager sessionPropertyManager;
    private final Map<String, String> preparedStatements;

    public Session(
            QueryId queryId,
            Optional<TransactionId> transactionId,
            boolean clientTransactionSupport,
            Identity identity,
            Optional<String> source,
            Optional<String> catalog,
            Optional<String> schema,
            TimeZoneKey timeZoneKey,
            Locale locale,
            Optional<String> remoteUserAddress,
            Optional<String> userAgent,
            long startTime,
            Map<String, String> systemProperties,
            Map<String, Map<String, String>> catalogProperties,
            SessionPropertyManager sessionPropertyManager,
            Map<String, String> preparedStatements)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.transactionId = requireNonNull(transactionId, "transactionId is null");
        this.clientTransactionSupport = clientTransactionSupport;
        this.identity = identity;
        this.source = requireNonNull(source, "source is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.timeZoneKey = requireNonNull(timeZoneKey, "timeZoneKey is null");
        this.locale = requireNonNull(locale, "locale is null");
        this.remoteUserAddress = requireNonNull(remoteUserAddress, "remoteUserAddress is null");
        this.userAgent = requireNonNull(userAgent, "userAgent is null");
        this.startTime = startTime;
        this.systemProperties = ImmutableMap.copyOf(requireNonNull(systemProperties, "systemProperties is null"));
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.preparedStatements = requireNonNull(preparedStatements, "preparedStatements is null");

        ImmutableMap.Builder<String, Map<String, String>> catalogPropertiesBuilder = ImmutableMap.<String, Map<String, String>>builder();
        catalogProperties.entrySet().stream()
                .map(entry -> Maps.immutableEntry(entry.getKey(), ImmutableMap.copyOf(entry.getValue())))
                .forEach(catalogPropertiesBuilder::put);
        this.catalogProperties = catalogPropertiesBuilder.build();

        checkArgument(catalog.isPresent() || !schema.isPresent(), "schema is set but catalog is not");
    }

    public QueryId getQueryId()
    {
        return queryId;
    }

    public String getUser()
    {
        return identity.getUser();
    }

    public Identity getIdentity()
    {
        return identity;
    }

    public Optional<String> getSource()
    {
        return source;
    }

    public Optional<String> getCatalog()
    {
        return catalog;
    }

    public Optional<String> getSchema()
    {
        return schema;
    }

    public TimeZoneKey getTimeZoneKey()
    {
        return timeZoneKey;
    }

    public Locale getLocale()
    {
        return locale;
    }

    public Optional<String> getRemoteUserAddress()
    {
        return remoteUserAddress;
    }

    public Optional<String> getUserAgent()
    {
        return userAgent;
    }

    public long getStartTime()
    {
        return startTime;
    }

    public Optional<TransactionId> getTransactionId()
    {
        return transactionId;
    }

    public TransactionId getRequiredTransactionId()
    {
        checkState(transactionId.isPresent(), "Not in a transaction");
        return transactionId.get();
    }

    public boolean isClientTransactionSupport()
    {
        return clientTransactionSupport;
    }

    public <T> T getProperty(String name, Class<T> type)
    {
        return sessionPropertyManager.decodeProperty(name, systemProperties.get(name), type);
    }

    public Map<String, Map<String, String>> getCatalogProperties()
    {
        return catalogProperties;
    }

    public Map<String, String> getCatalogProperties(String catalog)
    {
        return catalogProperties.getOrDefault(catalog, ImmutableMap.of());
    }

    public Map<String, String> getSystemProperties()
    {
        return systemProperties;
    }

    public Map<String, String> getPreparedStatements()
    {
        return preparedStatements;
    }

    public String getPreparedStatementFromExecute(Execute execute)
    {
        String name = execute.getName();
        String sql = preparedStatements.get(name);
        checkCondition(sql != null, NOT_FOUND, "Prepared statement not found: " + name);
        return sql;
    }

    public Session withTransactionId(TransactionId transactionId)
    {
        requireNonNull(transactionId, "transactionId is null");
        checkArgument(!this.transactionId.isPresent(), "Session already has an active transaction");
        return withTransactionId(Optional.of(transactionId));
    }

    public Session withoutTransactionId()
    {
        return withTransactionId(Optional.empty());
    }

    private Session withTransactionId(Optional<TransactionId> transactionId)
    {
        return new Session(
                queryId,
                transactionId,
                clientTransactionSupport,
                identity,
                source,
                catalog,
                schema,
                timeZoneKey,
                locale,
                remoteUserAddress,
                userAgent,
                startTime,
                systemProperties,
                catalogProperties,
                sessionPropertyManager,
                preparedStatements);
    }

    public Session withSystemProperty(String key, String value)
    {
        requireNonNull(key, "key is null");
        requireNonNull(value, "value is null");

        Map<String, String> systemProperties = new LinkedHashMap<>(this.systemProperties);
        systemProperties.put(key, value);

        return new Session(
                queryId,
                transactionId,
                clientTransactionSupport,
                identity,
                source,
                catalog,
                schema,
                timeZoneKey,
                locale,
                remoteUserAddress,
                userAgent,
                startTime,
                systemProperties,
                catalogProperties,
                sessionPropertyManager,
                preparedStatements);
    }

    public Session withCatalogProperty(String catalog, String key, String value)
    {
        requireNonNull(catalog, "catalog is null");
        requireNonNull(key, "key is null");
        requireNonNull(value, "value is null");

        Map<String, Map<String, String>> catalogProperties = new LinkedHashMap<>(this.catalogProperties);
        Map<String, String> properties = catalogProperties.get(catalog);
        if (properties == null) {
            properties = new LinkedHashMap<>();
        }
        else {
            properties = new LinkedHashMap<>(properties);
        }
        properties.put(key, value);
        catalogProperties.put(catalog, properties);

        return new Session(
                queryId,
                transactionId,
                clientTransactionSupport,
                identity,
                source,
                this.catalog,
                schema,
                timeZoneKey,
                locale,
                remoteUserAddress,
                userAgent,
                startTime,
                systemProperties,
                catalogProperties,
                sessionPropertyManager,
                preparedStatements);
    }

    public Session withPreparedStatement(String statementName, String query)
    {
        requireNonNull(statementName, "statementName is null");
        requireNonNull(query, "query is null");

        Map<String, String> preparedStatements = new HashMap<>(getPreparedStatements());
        preparedStatements.put(statementName, query);
        return new Session(
                queryId,
                transactionId,
                clientTransactionSupport,
                identity,
                source,
                catalog,
                schema,
                timeZoneKey,
                locale,
                remoteUserAddress,
                userAgent,
                startTime,
                systemProperties,
                catalogProperties,
                sessionPropertyManager,
                preparedStatements);
    }

    public ConnectorSession toConnectorSession()
    {
        return new FullConnectorSession(queryId.toString(), identity, timeZoneKey, locale, startTime);
    }

    public ConnectorSession toConnectorSession(String catalog)
    {
        requireNonNull(catalog, "catalog is null");
        return new FullConnectorSession(
                queryId.toString(),
                identity,
                timeZoneKey,
                locale,
                startTime,
                catalogProperties.getOrDefault(catalog, ImmutableMap.of()),
                catalog,
                sessionPropertyManager);
    }

    public ClientSession toClientSession(URI server, boolean debug, Duration clientRequestTimeout)
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        properties.putAll(systemProperties);
        for (Entry<String, Map<String, String>> catalogProperties : this.catalogProperties.entrySet()) {
            String catalog = catalogProperties.getKey();
            for (Entry<String, String> entry : catalogProperties.getValue().entrySet()) {
                properties.put(catalog + "." + entry.getKey(), entry.getValue());
            }
        }

        return new ClientSession(
                requireNonNull(server, "server is null"),
                identity.getUser(),
                source.orElse(null),
                catalog.orElse(null),
                schema.orElse(null),
                timeZoneKey.getId(),
                locale,
                properties.build(),
                preparedStatements,
                transactionId.map(TransactionId::toString).orElse(null),
                debug,
                clientRequestTimeout);
    }

    public SessionRepresentation toSessionRepresentation()
    {
        return new SessionRepresentation(
                queryId.toString(),
                transactionId,
                clientTransactionSupport,
                identity.getUser(),
                identity.getPrincipal().map(Principal::toString),
                source,
                catalog,
                schema,
                timeZoneKey,
                locale,
                remoteUserAddress,
                userAgent,
                startTime,
                systemProperties,
                catalogProperties,
                preparedStatements);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("queryId", queryId)
                .add("transactionId", transactionId)
                .add("user", getUser())
                .add("principal", getIdentity().getPrincipal().orElse(null))
                .add("source", source.orElse(null))
                .add("catalog", catalog.orElse(null))
                .add("schema", schema.orElse(null))
                .add("timeZoneKey", timeZoneKey)
                .add("locale", locale)
                .add("remoteUserAddress", remoteUserAddress.orElse(null))
                .add("userAgent", userAgent.orElse(null))
                .add("startTime", startTime)
                .omitNullValues()
                .toString();
    }

    public static SessionBuilder builder(SessionPropertyManager sessionPropertyManager)
    {
        return new SessionBuilder(sessionPropertyManager);
    }

    public static class SessionBuilder
    {
        private QueryId queryId;
        private TransactionId transactionId;
        private boolean clientTransactionSupport;
        private Identity identity;
        private String source;
        private String catalog;
        private String schema;
        private TimeZoneKey timeZoneKey = TimeZoneKey.getTimeZoneKey(TimeZone.getDefault().getID());
        private Locale locale = Locale.getDefault();
        private String remoteUserAddress;
        private String userAgent;
        private long startTime = System.currentTimeMillis();
        private Map<String, String> systemProperties = ImmutableMap.of();
        private final Map<String, Map<String, String>> catalogProperties = new HashMap<>();
        private final SessionPropertyManager sessionPropertyManager;
        private Map<String, String> preparedStatements = ImmutableMap.of();

        private SessionBuilder(SessionPropertyManager sessionPropertyManager)
        {
            this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        }

        public SessionBuilder setQueryId(QueryId queryId)
        {
            this.queryId = requireNonNull(queryId, "queryId is null");
            return this;
        }

        public SessionBuilder setTransactionId(TransactionId transactionId)
        {
            this.transactionId = transactionId;
            return this;
        }

        public SessionBuilder setClientTransactionSupport()
        {
            this.clientTransactionSupport = true;
            return this;
        }

        public SessionBuilder setCatalog(String catalog)
        {
            this.catalog = catalog;
            return this;
        }

        public SessionBuilder setLocale(Locale locale)
        {
            this.locale = locale;
            return this;
        }

        public SessionBuilder setRemoteUserAddress(String remoteUserAddress)
        {
            this.remoteUserAddress = remoteUserAddress;
            return this;
        }

        public SessionBuilder setSchema(String schema)
        {
            this.schema = schema;
            return this;
        }

        public SessionBuilder setSource(String source)
        {
            this.source = source;
            return this;
        }

        public SessionBuilder setStartTime(long startTime)
        {
            this.startTime = startTime;
            return this;
        }

        public SessionBuilder setTimeZoneKey(TimeZoneKey timeZoneKey)
        {
            this.timeZoneKey = timeZoneKey;
            return this;
        }

        public SessionBuilder setIdentity(Identity identity)
        {
            this.identity = identity;
            return this;
        }

        public SessionBuilder setUserAgent(String userAgent)
        {
            this.userAgent = userAgent;
            return this;
        }

        /**
         * Sets the system properties for the session.  The property names and
         * values must only contain characters from US-ASCII and must not be for '='.
         */
        public SessionBuilder setSystemProperties(Map<String, String> systemProperties)
        {
            this.systemProperties = ImmutableMap.copyOf(systemProperties);
            return this;
        }

        /**
         * Sets the properties for a catalog.  The catalog name, property names, and
         * values must only contain characters from US-ASCII and must not be for '='.
         */
        public SessionBuilder setCatalogProperties(String catalog, Map<String, String> properties)
        {
            requireNonNull(catalog, "catalog is null");
            checkArgument(!catalog.isEmpty(), "catalog is empty");

            catalogProperties.put(catalog, ImmutableMap.copyOf(properties));
            return this;
        }

        public void setPreparedStatements(Map<String, String> preparedStatements)
        {
            this.preparedStatements = ImmutableMap.copyOf(preparedStatements);
        }

        public Session build()
        {
            return new Session(
                    queryId,
                    Optional.ofNullable(transactionId),
                    clientTransactionSupport,
                    identity,
                    Optional.ofNullable(source),
                    Optional.ofNullable(catalog),
                    Optional.ofNullable(schema),
                    timeZoneKey,
                    locale,
                    Optional.ofNullable(remoteUserAddress),
                    Optional.ofNullable(userAgent),
                    startTime,
                    systemProperties,
                    catalogProperties,
                    sessionPropertyManager,
                    preparedStatements);
        }
    }
}
