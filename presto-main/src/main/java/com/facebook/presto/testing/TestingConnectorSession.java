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
package com.facebook.presto.testing;

import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class TestingConnectorSession
        implements ConnectorSession
{
    private static final QueryIdGenerator queryIdGenerator = new QueryIdGenerator();
    public static final ConnectorSession SESSION = new TestingConnectorSession(ImmutableList.of());

    private final String queryId;
    private final ConnectorIdentity identity;
    private final Optional<String> source;
    private final Locale locale;
    private final Optional<String> traceToken;
    private final long startTime;
    private final Map<String, PropertyMetadata<?>> properties;
    private final Map<String, Object> propertyValues;
    private final Optional<String> clientInfo;
    private final Set<String> clientTags;
    private final SqlFunctionProperties sqlFunctionProperties;
    private final Optional<String> schema;
    private final Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions;

    public TestingConnectorSession(List<PropertyMetadata<?>> properties)
    {
        this("user", Optional.of("test"), Optional.empty(), UTC_KEY, ENGLISH, System.currentTimeMillis(), properties, ImmutableMap.of(), new FeaturesConfig().isLegacyTimestamp(), Optional.empty(), ImmutableSet.of(), Optional.empty(), ImmutableMap.of());
    }

    public TestingConnectorSession(List<PropertyMetadata<?>> properties, Map<String, Object> propertyValues)
    {
        this("user", Optional.of("test"), Optional.empty(), UTC_KEY, ENGLISH, System.currentTimeMillis(), properties, propertyValues, new FeaturesConfig().isLegacyTimestamp(), Optional.empty(), ImmutableSet.of(), Optional.empty(), ImmutableMap.of());
    }

    public TestingConnectorSession(List<PropertyMetadata<?>> properties, Optional<String> schema)
    {
        this("user", Optional.of("test"), Optional.empty(), UTC_KEY, ENGLISH, System.currentTimeMillis(), properties, ImmutableMap.of(), new FeaturesConfig().isLegacyTimestamp(), Optional.empty(), ImmutableSet.of(), schema, ImmutableMap.of());
    }

    public TestingConnectorSession(
            String user,
            Optional<String> source,
            Optional<String> traceToken,
            TimeZoneKey timeZoneKey,
            Locale locale,
            long startTime,
            List<PropertyMetadata<?>> propertyMetadatas,
            Map<String, Object> propertyValues,
            boolean isLegacyTimestamp,
            Optional<String> clientInfo,
            Set<String> clientTags,
            Optional<String> schema,
            Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions)
    {
        this.queryId = queryIdGenerator.createNextQueryId().toString();
        this.identity = new ConnectorIdentity(requireNonNull(user, "user is null"), Optional.empty(), Optional.empty());
        this.source = requireNonNull(source, "source is null");
        this.traceToken = requireNonNull(traceToken, "traceToken is null");
        this.locale = requireNonNull(locale, "locale is null");
        this.startTime = startTime;
        this.properties = Maps.uniqueIndex(propertyMetadatas, PropertyMetadata::getName);
        this.propertyValues = ImmutableMap.copyOf(propertyValues);
        this.clientInfo = clientInfo;
        this.clientTags = clientTags;
        this.sqlFunctionProperties = SqlFunctionProperties.builder()
                .setTimeZoneKey(requireNonNull(timeZoneKey, "timeZoneKey is null"))
                .setLegacyTimestamp(isLegacyTimestamp)
                .setSessionStartTime(startTime)
                .setSessionLocale(locale)
                .setSessionUser(user)
                .build();
        this.schema = requireNonNull(schema, "schema is null");
        this.sessionFunctions = sessionFunctions;
    }

    @Override
    public String getQueryId()
    {
        return queryId;
    }

    @Override
    public Optional<String> getSource()
    {
        return source;
    }

    @Override
    public ConnectorIdentity getIdentity()
    {
        return identity;
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
    public Optional<String> getTraceToken()
    {
        return traceToken;
    }

    @Override
    public Optional<String> getClientInfo()
    {
        return clientInfo;
    }

    @Override
    public Set<String> getClientTags()
    {
        return clientTags;
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
    public <T> T getProperty(String name, Class<T> type)
    {
        PropertyMetadata<?> metadata = properties.get(name);
        if (metadata == null) {
            throw new PrestoException(INVALID_SESSION_PROPERTY, "Unknown session property " + name);
        }
        Object value = propertyValues.get(name);
        if (value == null) {
            return type.cast(metadata.getDefaultValue());
        }
        return type.cast(metadata.decode(value));
    }

    @Override
    public Optional<String> getSchema()
    {
        return schema;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("user", getUser())
                .add("source", source.orElse(null))
                .add("traceToken", traceToken.orElse(null))
                .add("locale", locale)
                .add("startTime", startTime)
                .add("sqlFunctionProperties", sqlFunctionProperties)
                .add("properties", propertyValues)
                .add("clientInfo", clientInfo)
                .omitNullValues()
                .toString();
    }
}
