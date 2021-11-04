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

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.security.AccessControlContext;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.SelectedRole;
import com.facebook.presto.spi.session.ResourceEstimates;
import com.facebook.presto.spi.session.SessionPropertyConfigurationManager.SystemSessionPropertyConfiguration;
import com.facebook.presto.sql.tree.Execute;
import com.facebook.presto.transaction.TransactionId;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.security.Principal;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.isLegacyMapSubscript;
import static com.facebook.presto.SystemSessionProperties.isLegacyRowFieldOrdinalAccessEnabled;
import static com.facebook.presto.SystemSessionProperties.isLegacyTimestamp;
import static com.facebook.presto.SystemSessionProperties.isLegacyTypeCoercionWarningEnabled;
import static com.facebook.presto.SystemSessionProperties.isParseDecimalLiteralsAsDouble;
import static com.facebook.presto.spi.ConnectorId.createInformationSchemaConnectorId;
import static com.facebook.presto.spi.ConnectorId.createSystemTablesConnectorId;
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
    private final Optional<String> clientInfo;
    private final Optional<String> traceToken;
    private final Set<String> clientTags;
    private final ResourceEstimates resourceEstimates;
    private final long startTime;
    private final Map<String, String> systemProperties;
    private final Map<ConnectorId, Map<String, String>> connectorProperties;
    private final Map<String, Map<String, String>> unprocessedCatalogProperties;
    private final SessionPropertyManager sessionPropertyManager;
    private final Map<String, String> preparedStatements;
    private final Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions;
    private final AccessControlContext context;

    private final RuntimeStats runtimeStats = new RuntimeStats();

    public Session(
            QueryId queryId,
            Optional<TransactionId> transactionId,
            boolean clientTransactionSupport,
            Identity identity,
            Optional<String> source,
            Optional<String> catalog,
            Optional<String> schema,
            Optional<String> traceToken,
            TimeZoneKey timeZoneKey,
            Locale locale,
            Optional<String> remoteUserAddress,
            Optional<String> userAgent,
            Optional<String> clientInfo,
            Set<String> clientTags,
            ResourceEstimates resourceEstimates,
            long startTime,
            Map<String, String> systemProperties,
            Map<ConnectorId, Map<String, String>> connectorProperties,
            Map<String, Map<String, String>> unprocessedCatalogProperties,
            SessionPropertyManager sessionPropertyManager,
            Map<String, String> preparedStatements,
            Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.transactionId = requireNonNull(transactionId, "transactionId is null");
        this.clientTransactionSupport = clientTransactionSupport;
        this.identity = requireNonNull(identity, "identity is null");
        this.source = requireNonNull(source, "source is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.traceToken = requireNonNull(traceToken, "traceToken is null");
        this.timeZoneKey = requireNonNull(timeZoneKey, "timeZoneKey is null");
        this.locale = requireNonNull(locale, "locale is null");
        this.remoteUserAddress = requireNonNull(remoteUserAddress, "remoteUserAddress is null");
        this.userAgent = requireNonNull(userAgent, "userAgent is null");
        this.clientInfo = requireNonNull(clientInfo, "clientInfo is null");
        this.clientTags = ImmutableSet.copyOf(requireNonNull(clientTags, "clientTags is null"));
        this.resourceEstimates = requireNonNull(resourceEstimates, "resourceEstimates is null");
        this.startTime = startTime;
        this.systemProperties = ImmutableMap.copyOf(requireNonNull(systemProperties, "systemProperties is null"));
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.preparedStatements = requireNonNull(preparedStatements, "preparedStatements is null");
        this.sessionFunctions = requireNonNull(sessionFunctions, "sessionFunctions is null");

        ImmutableMap.Builder<ConnectorId, Map<String, String>> catalogPropertiesBuilder = ImmutableMap.builder();
        connectorProperties.entrySet().stream()
                .map(entry -> Maps.immutableEntry(entry.getKey(), ImmutableMap.copyOf(entry.getValue())))
                .forEach(catalogPropertiesBuilder::put);
        this.connectorProperties = catalogPropertiesBuilder.build();

        ImmutableMap.Builder<String, Map<String, String>> unprocessedCatalogPropertiesBuilder = ImmutableMap.builder();
        unprocessedCatalogProperties.entrySet().stream()
                .map(entry -> Maps.immutableEntry(entry.getKey(), ImmutableMap.copyOf(entry.getValue())))
                .forEach(unprocessedCatalogPropertiesBuilder::put);
        this.unprocessedCatalogProperties = unprocessedCatalogPropertiesBuilder.build();

        checkArgument(!transactionId.isPresent() || unprocessedCatalogProperties.isEmpty(), "Catalog session properties cannot be set if there is an open transaction");

        checkArgument(catalog.isPresent() || !schema.isPresent(), "schema is set but catalog is not");
        this.context = new AccessControlContext(queryId, clientInfo, source);
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

    public Optional<String> getClientInfo()
    {
        return clientInfo;
    }

    public Set<String> getClientTags()
    {
        return clientTags;
    }

    public Optional<String> getTraceToken()
    {
        return traceToken;
    }

    public ResourceEstimates getResourceEstimates()
    {
        return resourceEstimates;
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

    public <T> T getSystemProperty(String name, Class<T> type)
    {
        return sessionPropertyManager.decodeSystemPropertyValue(name, systemProperties.get(name), type);
    }

    public Map<ConnectorId, Map<String, String>> getConnectorProperties()
    {
        return connectorProperties;
    }

    public Map<String, String> getConnectorProperties(ConnectorId connectorId)
    {
        return connectorProperties.getOrDefault(connectorId, ImmutableMap.of());
    }

    public Map<String, Map<String, String>> getUnprocessedCatalogProperties()
    {
        return unprocessedCatalogProperties;
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
        return getPreparedStatement(execute.getName().getValue());
    }

    public String getPreparedStatement(String name)
    {
        String sql = preparedStatements.get(name);
        checkCondition(sql != null, NOT_FOUND, "Prepared statement not found: " + name);
        return sql;
    }

    public Map<SqlFunctionId, SqlInvokedFunction> getSessionFunctions()
    {
        return sessionFunctions;
    }

    public AccessControlContext getAccessControlContext()
    {
        return context;
    }

    public RuntimeStats getRuntimeStats()
    {
        return runtimeStats;
    }

    public Session beginTransactionId(TransactionId transactionId, TransactionManager transactionManager, AccessControl accessControl)
    {
        requireNonNull(transactionId, "transactionId is null");
        checkArgument(!this.transactionId.isPresent(), "Session already has an active transaction");
        requireNonNull(transactionManager, "transactionManager is null");
        requireNonNull(accessControl, "accessControl is null");

        for (Entry<String, String> property : systemProperties.entrySet()) {
            // verify permissions
            accessControl.checkCanSetSystemSessionProperty(identity, context, property.getKey());

            // validate session property value
            sessionPropertyManager.validateSystemSessionProperty(property.getKey(), property.getValue());
        }

        // Now that there is a transaction, the catalog name can be resolved to a connector, and the catalog properties can be validated
        ImmutableMap.Builder<ConnectorId, Map<String, String>> connectorProperties = ImmutableMap.builder();
        for (Entry<String, Map<String, String>> catalogEntry : unprocessedCatalogProperties.entrySet()) {
            String catalogName = catalogEntry.getKey();
            Map<String, String> catalogProperties = catalogEntry.getValue();
            if (catalogProperties.isEmpty()) {
                continue;
            }
            ConnectorId connectorId = transactionManager.getOptionalCatalogMetadata(transactionId, catalogName)
                    .orElseThrow(() -> new PrestoException(NOT_FOUND, "Session property catalog does not exist: " + catalogName))
                    .getConnectorId();

            for (Entry<String, String> property : catalogProperties.entrySet()) {
                // verify permissions
                accessControl.checkCanSetCatalogSessionProperty(transactionId, identity, context, catalogName, property.getKey());

                // validate session property value
                sessionPropertyManager.validateCatalogSessionProperty(connectorId, catalogName, property.getKey(), property.getValue());
            }
            connectorProperties.put(connectorId, catalogProperties);
        }

        ImmutableMap.Builder<String, SelectedRole> roles = ImmutableMap.builder();
        for (Entry<String, SelectedRole> entry : identity.getRoles().entrySet()) {
            String catalogName = entry.getKey();
            SelectedRole role = entry.getValue();

            ConnectorId connectorId = transactionManager.getOptionalCatalogMetadata(transactionId, catalogName)
                    .orElseThrow(() -> new PrestoException(NOT_FOUND, "Catalog does not exist: " + catalogName))
                    .getConnectorId();

            if (role.getType() == SelectedRole.Type.ROLE) {
                accessControl.checkCanSetRole(transactionId, identity, context, role.getRole().get(), catalogName);
            }
            roles.put(connectorId.getCatalogName(), role);

            String informationSchemaCatalogName = createInformationSchemaConnectorId(connectorId).getCatalogName();
            if (transactionManager.getCatalogNames(transactionId).containsKey(informationSchemaCatalogName)) {
                roles.put(createInformationSchemaConnectorId(connectorId).getCatalogName(), role);
            }

            String systemTablesCatalogName = createSystemTablesConnectorId(connectorId).getCatalogName();
            if (transactionManager.getCatalogNames(transactionId).containsKey(systemTablesCatalogName)) {
                roles.put(createSystemTablesConnectorId(connectorId).getCatalogName(), role);
            }
        }

        return new Session(
                queryId,
                Optional.of(transactionId),
                clientTransactionSupport,
                new Identity(
                        identity.getUser(),
                        identity.getPrincipal(),
                        roles.build(),
                        identity.getExtraCredentials(),
                        identity.getExtraAuthenticators()),
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
                connectorProperties.build(),
                ImmutableMap.of(),
                sessionPropertyManager,
                preparedStatements,
                sessionFunctions);
    }

    public Session withDefaultProperties(
            SystemSessionPropertyConfiguration systemPropertyConfiguration,
            Map<String, Map<String, String>> catalogPropertyDefaults)
    {
        requireNonNull(systemPropertyConfiguration, "systemPropertyConfiguration is null");
        requireNonNull(catalogPropertyDefaults, "catalogPropertyDefaults is null");

        // to remove this check properties must be authenticated and validated as in beginTransactionId
        checkState(
                !this.transactionId.isPresent() && this.connectorProperties.isEmpty(),
                "Session properties cannot be overridden once a transaction is active");

        Map<String, String> systemProperties = new HashMap<>();
        systemProperties.putAll(systemPropertyConfiguration.systemPropertyDefaults);
        systemProperties.putAll(this.systemProperties);
        systemProperties.putAll(systemPropertyConfiguration.systemPropertyOverrides);

        Map<String, Map<String, String>> connectorProperties = catalogPropertyDefaults.entrySet().stream()
                .map(entry -> Maps.immutableEntry(entry.getKey(), new HashMap<>(entry.getValue())))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        for (Entry<String, Map<String, String>> catalogProperties : this.unprocessedCatalogProperties.entrySet()) {
            String catalog = catalogProperties.getKey();
            for (Entry<String, String> entry : catalogProperties.getValue().entrySet()) {
                connectorProperties.computeIfAbsent(catalog, id -> new HashMap<>())
                        .put(entry.getKey(), entry.getValue());
            }
        }

        return new Session(
                queryId,
                transactionId,
                clientTransactionSupport,
                identity,
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
                ImmutableMap.of(),
                connectorProperties,
                sessionPropertyManager,
                preparedStatements,
                sessionFunctions);
    }

    public ConnectorSession toConnectorSession()
    {
        return new FullConnectorSession(this, identity.toConnectorIdentity());
    }

    public SqlFunctionProperties getSqlFunctionProperties()
    {
        return SqlFunctionProperties.builder()
                .setTimeZoneKey(timeZoneKey)
                .setLegacyRowFieldOrdinalAccessEnabled(isLegacyRowFieldOrdinalAccessEnabled(this))
                .setLegacyTypeCoercionWarningEnabled(isLegacyTypeCoercionWarningEnabled(this))
                .setLegacyTimestamp(isLegacyTimestamp(this))
                .setLegacyMapSubscript(isLegacyMapSubscript(this))
                .setParseDecimalLiteralAsDouble(isParseDecimalLiteralsAsDouble(this))
                .setSessionStartTime(getStartTime())
                .setSessionLocale(getLocale())
                .setSessionUser(getUser())
                .build();
    }

    public ConnectorSession toConnectorSession(ConnectorId connectorId)
    {
        requireNonNull(connectorId, "connectorId is null");

        return new FullConnectorSession(
                this,
                identity.toConnectorIdentity(connectorId.getCatalogName()),
                connectorProperties.getOrDefault(connectorId, ImmutableMap.of()),
                connectorId,
                connectorId.getCatalogName(),
                sessionPropertyManager);
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
                connectorProperties,
                unprocessedCatalogProperties,
                identity.getRoles(),
                preparedStatements,
                sessionFunctions);
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
                .add("traceToken", traceToken.orElse(null))
                .add("timeZoneKey", timeZoneKey)
                .add("locale", locale)
                .add("remoteUserAddress", remoteUserAddress.orElse(null))
                .add("userAgent", userAgent.orElse(null))
                .add("clientInfo", clientInfo.orElse(null))
                .add("clientTags", clientTags)
                .add("resourceEstimates", resourceEstimates)
                .add("startTime", startTime)
                .omitNullValues()
                .toString();
    }

    public static SessionBuilder builder(SessionPropertyManager sessionPropertyManager)
    {
        return new SessionBuilder(sessionPropertyManager);
    }

    @VisibleForTesting
    public static SessionBuilder builder(Session session)
    {
        return new SessionBuilder(session);
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
        private Optional<String> traceToken = Optional.empty();
        private TimeZoneKey timeZoneKey = TimeZoneKey.getTimeZoneKey(TimeZone.getDefault().getID());
        private Locale locale = Locale.getDefault();
        private String remoteUserAddress;
        private String userAgent;
        private String clientInfo;
        private Set<String> clientTags = ImmutableSet.of();
        private ResourceEstimates resourceEstimates;
        private long startTime = System.currentTimeMillis();
        private final Map<String, String> systemProperties = new HashMap<>();
        private final Map<String, Map<String, String>> catalogSessionProperties = new HashMap<>();
        private final SessionPropertyManager sessionPropertyManager;
        private final Map<String, String> preparedStatements = new HashMap<>();
        private final Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions = new HashMap<>();

        private SessionBuilder(SessionPropertyManager sessionPropertyManager)
        {
            this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        }

        private SessionBuilder(Session session)
        {
            requireNonNull(session, "session is null");
            checkArgument(!session.getTransactionId().isPresent(), "Session builder cannot be created from a session in a transaction");
            this.sessionPropertyManager = session.sessionPropertyManager;
            this.queryId = session.queryId;
            this.transactionId = session.transactionId.orElse(null);
            this.clientTransactionSupport = session.clientTransactionSupport;
            this.identity = session.identity;
            this.source = session.source.orElse(null);
            this.catalog = session.catalog.orElse(null);
            this.schema = session.schema.orElse(null);
            this.traceToken = requireNonNull(session.traceToken, "traceToken is null");
            this.timeZoneKey = session.timeZoneKey;
            this.locale = session.locale;
            this.remoteUserAddress = session.remoteUserAddress.orElse(null);
            this.userAgent = session.userAgent.orElse(null);
            this.clientInfo = session.clientInfo.orElse(null);
            this.clientTags = ImmutableSet.copyOf(session.clientTags);
            this.startTime = session.startTime;
            this.systemProperties.putAll(session.systemProperties);
            session.unprocessedCatalogProperties.forEach((key, value) -> this.catalogSessionProperties.put(key, new HashMap<>(value)));
            this.preparedStatements.putAll(session.preparedStatements);
            this.sessionFunctions.putAll(session.sessionFunctions);
        }

        public SessionBuilder setQueryId(QueryId queryId)
        {
            this.queryId = requireNonNull(queryId, "queryId is null");
            return this;
        }

        public SessionBuilder setTransactionId(TransactionId transactionId)
        {
            checkArgument(catalogSessionProperties.isEmpty(), "Catalog session properties cannot be set if there is an open transaction");
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

        public SessionBuilder setTraceToken(Optional<String> traceToken)
        {
            this.traceToken = requireNonNull(traceToken, "traceToken is null");
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

        public SessionBuilder setClientInfo(String clientInfo)
        {
            this.clientInfo = clientInfo;
            return this;
        }

        public SessionBuilder setClientTags(Set<String> clientTags)
        {
            this.clientTags = ImmutableSet.copyOf(clientTags);
            return this;
        }

        public SessionBuilder setResourceEstimates(ResourceEstimates resourceEstimates)
        {
            this.resourceEstimates = resourceEstimates;
            return this;
        }

        /**
         * Sets a system property for the session.  The property name and value must
         * only contain characters from US-ASCII and must not be for '='.
         */
        public SessionBuilder setSystemProperty(String propertyName, String propertyValue)
        {
            systemProperties.put(propertyName, propertyValue);
            return this;
        }

        /**
         * Sets a catalog property for the session.  The property name and value must
         * only contain characters from US-ASCII and must not be for '='.
         */
        public SessionBuilder setCatalogSessionProperty(String catalogName, String propertyName, String propertyValue)
        {
            checkArgument(transactionId == null, "Catalog session properties cannot be set if there is an open transaction");
            catalogSessionProperties.computeIfAbsent(catalogName, id -> new HashMap<>()).put(propertyName, propertyValue);
            return this;
        }

        public SessionBuilder addPreparedStatement(String statementName, String query)
        {
            this.preparedStatements.put(statementName, query);
            return this;
        }

        public SessionBuilder addSessionFunction(SqlFunctionId functionSignature, SqlInvokedFunction functionDefinition)
        {
            this.sessionFunctions.put(functionSignature, functionDefinition);
            return this;
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
                    traceToken,
                    timeZoneKey,
                    locale,
                    Optional.ofNullable(remoteUserAddress),
                    Optional.ofNullable(userAgent),
                    Optional.ofNullable(clientInfo),
                    clientTags,
                    Optional.ofNullable(resourceEstimates).orElse(new ResourceEstimateBuilder().build()),
                    startTime,
                    systemProperties,
                    ImmutableMap.of(),
                    catalogSessionProperties,
                    sessionPropertyManager,
                    preparedStatements,
                    sessionFunctions);
        }
    }

    public static class ResourceEstimateBuilder
    {
        private Optional<Duration> executionTime = Optional.empty();
        private Optional<Duration> cpuTime = Optional.empty();
        private Optional<DataSize> peakMemory = Optional.empty();
        private Optional<DataSize> peakTaskMemory = Optional.empty();

        public ResourceEstimateBuilder setExecutionTime(Duration executionTime)
        {
            this.executionTime = Optional.of(executionTime);
            return this;
        }

        public ResourceEstimateBuilder setCpuTime(Duration cpuTime)
        {
            this.cpuTime = Optional.of(cpuTime);
            return this;
        }

        public ResourceEstimateBuilder setPeakMemory(DataSize peakMemory)
        {
            this.peakMemory = Optional.of(peakMemory);
            return this;
        }

        public ResourceEstimateBuilder setPeakTaskMemory(DataSize peakTaskMemory)
        {
            this.peakTaskMemory = Optional.of(peakTaskMemory);
            return this;
        }

        public ResourceEstimates build()
        {
            return new ResourceEstimates(executionTime, cpuTime, peakMemory, peakTaskMemory);
        }
    }
}
