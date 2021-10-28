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

import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.google.common.collect.ImmutableMap;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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

    public FullConnectorSession(Session session, ConnectorIdentity identity)
    {
        this.session = requireNonNull(session, "session is null");
        this.identity = requireNonNull(identity, "identity is null");
        this.properties = null;
        this.connectorId = null;
        this.catalog = null;
        this.sessionPropertyManager = null;
        this.sqlFunctionProperties = session.getSqlFunctionProperties();
        this.sessionFunctions = ImmutableMap.copyOf(session.getSessionFunctions());
    }

    public FullConnectorSession(
            Session session,
            ConnectorIdentity identity,
            Map<String, String> properties,
            ConnectorId connectorId,
            String catalog,
            SessionPropertyManager sessionPropertyManager)
    {
        this.session = requireNonNull(session, "session is null");
        this.identity = requireNonNull(identity, "identity is null");
        this.properties = ImmutableMap.copyOf(requireNonNull(properties, "properties is null"));
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.sqlFunctionProperties = session.getSqlFunctionProperties();
        this.sessionFunctions = ImmutableMap.copyOf(session.getSessionFunctions());
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
    public String toString()
    {
        return toStringHelper(this)
                .add("queryId", getQueryId())
                .add("user", getUser())
                .add("source", getSource().orElse(null))
                .add("traceToken", getTraceToken().orElse(null))
                .add("locale", getLocale())
                .add("startTime", getStartTime())
                .add("properties", properties)
                .omitNullValues()
                .toString();
    }
}
