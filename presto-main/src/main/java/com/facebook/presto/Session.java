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
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import javax.annotation.Nullable;

import java.net.URI;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Objects.requireNonNull;

public final class Session
{
    private final String user;
    @Nullable
    private final String source;
    private final String catalog;
    private final String schema;
    private final TimeZoneKey timeZoneKey;
    private final Locale locale;
    @Nullable
    private final String remoteUserAddress;
    @Nullable
    private final String userAgent;
    private final long startTime;
    private final Map<String, String> systemProperties;
    private final Map<String, Map<String, String>> catalogProperties;

    @JsonCreator
    public Session(
            @JsonProperty("user") String user,
            @JsonProperty("source") @Nullable String source,
            @JsonProperty("catalog") String catalog,
            @JsonProperty("schema") String schema,
            @JsonProperty("timeZoneKey") TimeZoneKey timeZoneKey,
            @JsonProperty("locale") Locale locale,
            @JsonProperty("remoteUserAddress") @Nullable String remoteUserAddress,
            @JsonProperty("userAgent") @Nullable String userAgent,
            @JsonProperty("startTime") long startTime,
            @JsonProperty("systemProperties") Map<String, String> systemProperties,
            @JsonProperty("catalogProperties") Map<String, Map<String, String>> catalogProperties)
    {
        this.user = requireNonNull(user, "user is null");
        this.source = source;
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.timeZoneKey = requireNonNull(timeZoneKey, "timeZoneKey is null");
        this.locale = requireNonNull(locale, "locale is null");
        this.remoteUserAddress = remoteUserAddress;
        this.userAgent = userAgent;
        this.startTime = startTime;
        this.systemProperties = ImmutableMap.copyOf(systemProperties);

        ImmutableMap.Builder<String, Map<String, String>> catalogPropertiesBuilder = ImmutableMap.<String, Map<String, String>>builder();
        catalogProperties.entrySet().stream()
                .map(entry -> Maps.immutableEntry(entry.getKey(), ImmutableMap.copyOf(entry.getValue())))
                .forEach(catalogPropertiesBuilder::put);
        this.catalogProperties = catalogPropertiesBuilder.build();
    }

    @JsonProperty
    public String getUser()
    {
        return user;
    }

    @Nullable
    @JsonProperty
    public String getSource()
    {
        return source;
    }

    @JsonProperty
    public String getCatalog()
    {
        return catalog;
    }

    @JsonProperty
    public String getSchema()
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

    @Nullable
    @JsonProperty
    public String getRemoteUserAddress()
    {
        return remoteUserAddress;
    }

    @Nullable
    @JsonProperty
    public String getUserAgent()
    {
        return userAgent;
    }

    @JsonProperty
    public long getStartTime()
    {
        return startTime;
    }

    @JsonProperty
    public Map<String, String> getSystemProperties()
    {
        return systemProperties;
    }

    @JsonProperty
    public Map<String, Map<String, String>> getCatalogProperties()
    {
        return catalogProperties;
    }

    public Session withSystemProperty(String key, String value)
    {
        checkNotNull(key, "key is null");
        checkNotNull(value, "value is null");

        Map<String, String> systemProperties = new LinkedHashMap<>(this.systemProperties);
        systemProperties.put(key, value);

        return new Session(
                user,
                source,
                catalog,
                schema,
                timeZoneKey,
                locale,
                remoteUserAddress,
                userAgent,
                startTime,
                systemProperties,
                catalogProperties);
    }

    public Session withCatalogProperty(String catalog, String key, String value)
    {
        checkNotNull(catalog, "catalog is null");
        checkNotNull(key, "key is null");
        checkNotNull(value, "value is null");

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
                user,
                source,
                catalog,
                schema,
                timeZoneKey,
                locale,
                remoteUserAddress,
                userAgent,
                startTime,
                systemProperties,
                catalogProperties);
    }

    public ConnectorSession toConnectorSession()
    {
        return new ConnectorSession(user, timeZoneKey, locale, startTime, null);
    }

    public ConnectorSession toConnectorSession(String catalog)
    {
        return new ConnectorSession(user, timeZoneKey, locale, startTime, catalogProperties.get(checkNotNull(catalog, "catalog is null")));
    }

    public ClientSession toClientSession(URI server, boolean debug, String digest)
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
                checkNotNull(server, "server is null"),
                user,
                source,
                catalog,
                schema,
                timeZoneKey.getId(),
                locale,
                properties.build(),
                debug,
                digest);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("user", user)
                .add("source", source)
                .add("catalog", catalog)
                .add("schema", schema)
                .add("timeZoneKey", timeZoneKey)
                .add("locale", locale)
                .add("remoteUserAddress", remoteUserAddress)
                .add("userAgent", userAgent)
                .add("startTime", startTime)
                .toString();
    }

    public static SessionBuilder builder()
    {
        return new SessionBuilder();
    }

    public static class SessionBuilder
    {
        private String user;
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

        private SessionBuilder()
        {
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

        public SessionBuilder setUser(String user)
        {
            this.user = user;
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
            checkNotNull(catalog, "catalog is null");
            checkArgument(!catalog.isEmpty(), "catalog is empty");

            catalogProperties.put(catalog, ImmutableMap.copyOf(properties));
            return this;
        }

        public Session build()
        {
            return new Session(user, source, catalog, schema, timeZoneKey, locale, remoteUserAddress, userAgent, startTime, systemProperties, catalogProperties);
        }
    }
}
