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

import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.google.common.collect.ImmutableMap;

import java.util.Locale;
import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class FullConnectorSession
        implements ConnectorSession
{
    private final String queryId;
    private final Identity identity;
    private final TimeZoneKey timeZoneKey;
    private final Locale locale;
    private final long startTime;
    private final Map<String, String> systemProperties;
    private final SessionPropertyManager sessionPropertyManager;
    private final Map<String, String> catalogProperties;
    private final String catalog;

    public FullConnectorSession(
            String queryId,
            Identity identity,
            TimeZoneKey timeZoneKey,
            Locale locale,
            long startTime,
            Map<String, String> systemProperties,
            SessionPropertyManager sessionPropertyManager)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.identity = requireNonNull(identity, "identity is null");
        this.timeZoneKey = requireNonNull(timeZoneKey, "timeZoneKey is null");
        this.locale = requireNonNull(locale, "locale is null");
        this.startTime = startTime;
        this.systemProperties = ImmutableMap.copyOf(requireNonNull(systemProperties, "systemProperties is null"));
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");

        this.catalogProperties = null;
        this.catalog = null;
    }

    public FullConnectorSession(
            String queryId,
            Identity identity,
            TimeZoneKey timeZoneKey,
            Locale locale,
            long startTime,
            Map<String, String> systemProperties,
            SessionPropertyManager sessionPropertyManager,
            Map<String, String> catalogProperties,
            String catalog)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.identity = requireNonNull(identity, "identity is null");
        this.timeZoneKey = requireNonNull(timeZoneKey, "timeZoneKey is null");
        this.locale = requireNonNull(locale, "locale is null");
        this.startTime = startTime;
        this.systemProperties = ImmutableMap.copyOf(requireNonNull(systemProperties, "properties is null"));
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");

        this.catalogProperties = requireNonNull(catalogProperties, "catalogProperties is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
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
    public <T> T getProperty(String name, Class<T> type)
    {
        if (sessionPropertyManager.containsSessionProperty(name)) {
            return sessionPropertyManager.decodeProperty(name, systemProperties.get(name), type);
        }

        if (catalogProperties == null) {
            throw new PrestoException(StandardErrorCode.INVALID_SESSION_PROPERTY, "Unknown session property " + name);
        }

        return sessionPropertyManager.decodeProperty(catalog + "." + name, catalogProperties.get(name), type);
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
                .add("systemProperties", systemProperties)
                .add("catalogProperties", catalogProperties)
                .add("catalog", catalog)
                .toString();
    }
}
