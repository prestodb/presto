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

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.google.common.collect.ImmutableMap;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class FullConnectorSession
        implements ConnectorSession
{
    private final Session session;
    private final Map<String, String> properties;
    private final ConnectorId connectorId;
    private final String catalog;
    private final SessionPropertyManager sessionPropertyManager;
    private final boolean isLegacyTimestamp;
    private final boolean isLegacyRoundNBigint;

    public FullConnectorSession(Session session)
    {
        this.session = requireNonNull(session, "session is null");
        this.properties = null;
        this.connectorId = null;
        this.catalog = null;
        this.sessionPropertyManager = null;
        this.isLegacyTimestamp = SystemSessionProperties.isLegacyTimestamp(session);
        this.isLegacyRoundNBigint = SystemSessionProperties.isLegacyRoundNBigint(session);
    }

    public FullConnectorSession(
            Session session,
            Map<String, String> properties,
            ConnectorId connectorId,
            String catalog,
            SessionPropertyManager sessionPropertyManager)
    {
        this.session = requireNonNull(session, "session is null");
        this.properties = ImmutableMap.copyOf(requireNonNull(properties, "properties is null"));
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.isLegacyTimestamp = SystemSessionProperties.isLegacyTimestamp(session);
        this.isLegacyRoundNBigint = SystemSessionProperties.isLegacyRoundNBigint(session);
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
    public String getPath()
    {
        return session.getPath().toString();
    }

    @Override
    public Identity getIdentity()
    {
        return session.getIdentity();
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
    public boolean isLegacyTimestamp()
    {
        return isLegacyTimestamp;
    }

    @Override
    public boolean isLegacyRoundNBigint()
    {
        return isLegacyRoundNBigint;
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
}
