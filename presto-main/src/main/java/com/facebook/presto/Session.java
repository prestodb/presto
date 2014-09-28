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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import javax.annotation.Nullable;

import java.util.Locale;
import java.util.TimeZone;

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
            @JsonProperty("startTime") long startTime)
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

    public ConnectorSession toConnectorSession()
    {
        return new ConnectorSession(user, schema, timeZoneKey, locale, startTime);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
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

        public Session build()
        {
            return new Session(user, source, catalog, schema, timeZoneKey, locale, remoteUserAddress, userAgent, startTime);
        }
    }
}
