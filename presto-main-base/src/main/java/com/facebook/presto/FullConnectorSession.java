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
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.google.common.collect.ImmutableMap;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.isExploitConstraints;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class FullConnectorSession
        implements ConnectorSession
{
    private final Session session;
    private final ConnectorIdentity identity;
    private final Map<String, String> properties;
    private final ConnectorId connectorId;
    private final String catalog;
    private final SessionPropertyManager sessionPropertyManager;
    private final SqlFunctionProperties sqlFunctionProperties;
    private final Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions;
    private final RuntimeStats runtimeStats;

    public FullConnectorSession(Session session, ConnectorIdentity identity)
    {
        this(builder(session, identity, null, null, null, null));
    }

    public FullConnectorSession(
            Session session,
            ConnectorIdentity identity,
            Map<String, String> properties,
            ConnectorId connectorId,
            String catalog,
            SessionPropertyManager sessionPropertyManager)
    {
        this(builder(session, identity, properties, connectorId, catalog, sessionPropertyManager));
    }

    private FullConnectorSession(Builder builder)
    {
        this.session = builder.getSession();
        this.identity = builder.getIdentity();
        this.properties = builder.getProperties();
        this.connectorId = builder.getConnectorId();
        this.catalog = builder.getCatalog();
        this.sessionPropertyManager = builder.getSessionPropertyManager();
        this.sqlFunctionProperties = builder.getSqlFunctionProperties() != null ? builder.getSqlFunctionProperties() : builder.getSession().getSqlFunctionProperties();
        this.sessionFunctions = builder.getSessionFunctions() != null ? builder.getSessionFunctions() : ImmutableMap.copyOf(builder.getSession().getSessionFunctions());
        this.runtimeStats = builder.getRuntimeStats() != null ? builder.getRuntimeStats() : builder.getSession().getRuntimeStats();
    }

    public static Builder builder(
            Session session,
            ConnectorIdentity identity,
            Map<String, String> properties,
            ConnectorId connectorId,
            String catalog,
            SessionPropertyManager sessionPropertyManager)
    {
        return new Builder(session, identity, properties, connectorId, catalog, sessionPropertyManager);
    }

    public static class Builder
    {
        private final Session session;
        private final ConnectorIdentity identity;
        private final Map<String, String> properties;
        private final ConnectorId connectorId;
        private final String catalog;
        private final SessionPropertyManager sessionPropertyManager;

        private SqlFunctionProperties sqlFunctionProperties;
        private Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions;
        private RuntimeStats runtimeStats;

        private Builder(Session session, ConnectorIdentity identity, Map<String, String> properties, ConnectorId connectorId, String catalog, SessionPropertyManager sessionPropertyManager)
        {
            this.session = requireNonNull(session, "session is null");
            this.identity = requireNonNull(identity, "identity is null");
            this.properties = properties;
            this.connectorId = connectorId;
            this.catalog = catalog;
            this.sessionPropertyManager = sessionPropertyManager;
        }

        public Session getSession()
        {
            return session;
        }

        public ConnectorIdentity getIdentity()
        {
            return identity;
        }

        public Map<String, String> getProperties()
        {
            return properties;
        }

        public ConnectorId getConnectorId()
        {
            return connectorId;
        }

        public String getCatalog()
        {
            return catalog;
        }

        public SessionPropertyManager getSessionPropertyManager()
        {
            return sessionPropertyManager;
        }

        public SqlFunctionProperties getSqlFunctionProperties()
        {
            return sqlFunctionProperties;
        }

        public Builder setSqlFunctionProperties(SqlFunctionProperties sqlFunctionProperties)
        {
            this.sqlFunctionProperties = sqlFunctionProperties;
            return this;
        }

        public Map<SqlFunctionId, SqlInvokedFunction> getSessionFunctions()
        {
            return sessionFunctions;
        }

        public Builder setSessionFunctions(Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions)
        {
            this.sessionFunctions = sessionFunctions;
            return this;
        }

        public RuntimeStats getRuntimeStats()
        {
            return runtimeStats;
        }

        public Builder setRuntimeStats(RuntimeStats runtimeStats)
        {
            this.runtimeStats = runtimeStats;
            return this;
        }

        public FullConnectorSession build()
        {
            return new FullConnectorSession(this);
        }
    }

    public Session getSession()
    {
        return session;
    }

    @Override
    public String getQueryId()
    {
        return session.getQueryId().toString();
    }

    @Override
    public Optional<String> getSource()
    {
        return session.getSource();
    }

    @Override
    public ConnectorIdentity getIdentity()
    {
        return identity;
    }

    @Override
    public TimeZoneKey getTimeZoneKey()
    {
        return session.getTimeZoneKey();
    }

    @Override
    public Locale getLocale()
    {
        return session.getLocale();
    }

    @Override
    public long getStartTime()
    {
        return session.getStartTime();
    }

    @Override
    public Optional<String> getTraceToken()
    {
        return session.getTraceToken();
    }

    @Override
    public Optional<String> getClientInfo()
    {
        return session.getClientInfo();
    }

    @Override
    public Set<String> getClientTags()
    {
        return session.getClientTags();
    }

    @Override
    public SqlFunctionProperties getSqlFunctionProperties()
    {
        return sqlFunctionProperties;
    }

    @Override
    public Map<SqlFunctionId, SqlInvokedFunction> getSessionFunctions()
    {
        return sessionFunctions;
    }

    @Override
    public <T> T getProperty(String propertyName, Class<T> type)
    {
        if (properties == null) {
            throw new PrestoException(INVALID_SESSION_PROPERTY, format("Unknown session property: %s.%s", catalog, propertyName));
        }

        return sessionPropertyManager.decodeCatalogPropertyValue(connectorId, catalog, propertyName, properties.get(propertyName), type);
    }

    @Override
    public Optional<String> getSchema()
    {
        return session.getSchema();
    }

    @Override
    public boolean isReadConstraints()
    {
        return isExploitConstraints(session);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("queryId", getQueryId())
                .add("user", getUser())
                .add("source", getSource().orElse(null))
                .add("traceToken", getTraceToken().orElse(null))
                .add("timeZoneKey", getTimeZoneKey())
                .add("locale", getLocale())
                .add("startTime", getStartTime())
                .add("properties", properties)
                .omitNullValues()
                .toString();
    }

    @Override
    public WarningCollector getWarningCollector()
    {
        return session.getWarningCollector();
    }

    @Override
    public RuntimeStats getRuntimeStats()
    {
        return runtimeStats;
    }

    @Override
    public ConnectorSession forConnectorId(ConnectorId connectorId)
    {
        return new FullConnectorSession(session, identity);
    }
}
