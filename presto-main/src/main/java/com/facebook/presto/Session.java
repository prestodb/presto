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

    public Session(String user, String source, String catalog, String schema, TimeZoneKey timeZoneKey, Locale locale, String remoteUserAddress, String userAgent)
    {
        this(user, source, catalog, schema, timeZoneKey, locale, remoteUserAddress, userAgent, System.currentTimeMillis());
    }

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
}
