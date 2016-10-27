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

import static com.facebook.presto.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class FullConnectorSession
        implements ConnectorSession
{
    private final String queryId;
    private final Identity identity;
    private final TimeZoneKey timeZoneKey;
    private final Locale locale;
    private final long startTime;
    private final Map<String, String> properties;
    private final ConnectorId connectorId;
    private final String catalog;
    private final SessionPropertyManager sessionPropertyManager;

    public FullConnectorSession(
            String queryId,
            Identity identity,
            TimeZoneKey timeZoneKey,
            Locale locale,
            long startTime)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.identity = requireNonNull(identity, "identity is null");
        this.timeZoneKey = requireNonNull(timeZoneKey, "timeZoneKey is null");
        this.locale = requireNonNull(locale, "locale is null");
        this.startTime = startTime;

        this.properties = null;
        this.connectorId = null;
        this.catalog = null;
        this.sessionPropertyManager = null;
    }

    public FullConnectorSession(
            String queryId,
            Identity identity,
            TimeZoneKey timeZoneKey,
            Locale locale,
            long startTime,
            Map<String, String> properties,
            ConnectorId connectorId,
            String catalog,
            SessionPropertyManager sessionPropertyManager)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.identity = requireNonNull(identity, "identity is null");
        this.timeZoneKey = requireNonNull(timeZoneKey, "timeZoneKey is null");
        this.locale = requireNonNull(locale, "locale is null");
        this.startTime = startTime;

        this.properties = ImmutableMap.copyOf(requireNonNull(properties, "properties is null"));
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
    }

    @Override
    public String getQueryId()
    {
        return queryId;
    }

    @Override
    public Identity getIdentity()
    {
        return identity;
    }

    @Override
    public TimeZoneKey getTimeZoneKey()
    {
        return timeZoneKey;
    }

    @Override
    public Locale getLocale()
    {
        return locale;
    }

    @Override
    public long getStartTime()
    {
        return startTime;
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
                .omitNullValues()
                .add("queryId", queryId)
                .add("user", getUser())
                .add("timeZoneKey", timeZoneKey)
                .add("locale", locale)
                .add("startTime", startTime)
                .add("properties", properties)
                .toString();
    }
}
