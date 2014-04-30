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
package com.facebook.presto.spi;

import com.facebook.presto.spi.type.TimeZoneKey;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Locale;

import static java.util.Objects.requireNonNull;

public class Session
{
    private final String user;
    private final String source;
    private final TimeZoneKey timeZoneKey;
    private final Locale locale;
    private final String remoteUserAddress;
    private final String userAgent;
    private final String catalog;
    private final String schema;
    private final long startTime;

    public Session(String user, String source, String catalog, String schema, TimeZoneKey timeZoneKey, Locale locale, String remoteUserAddress, String userAgent)
    {
        this(user, source, catalog, schema, timeZoneKey, locale, remoteUserAddress, userAgent, System.currentTimeMillis());
    }

    @JsonCreator
    public Session(
            @JsonProperty("user") String user,
            @JsonProperty("source") String source,
            @JsonProperty("catalog") String catalog,
            @JsonProperty("schema") String schema,
            @JsonProperty("timeZoneKey") TimeZoneKey timeZoneKey,
            @JsonProperty("locale") Locale locale,
            @JsonProperty("remoteUserAddress") String remoteUserAddress,
            @JsonProperty("userAgent") String userAgent,
            @JsonProperty("startTime") long startTime)
    {
        this.user = user;
        this.source = source;
        this.timeZoneKey = requireNonNull(timeZoneKey, "timeZoneKey is null");
        this.locale = locale;
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.remoteUserAddress = remoteUserAddress;
        this.userAgent = userAgent;
        this.startTime = startTime;
    }

    @JsonProperty
    public String getUser()
    {
        return user;
    }

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

    @JsonProperty
    public String getRemoteUserAddress()
    {
        return remoteUserAddress;
    }

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

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("Session{");
        builder.append("user='").append(user).append('\'');
        builder.append(", source='").append(source).append('\'');
        builder.append(", remoteUserAddress='").append(remoteUserAddress).append('\'');
        builder.append(", userAgent='").append(userAgent).append('\'');
        builder.append(", catalog='").append(catalog).append('\'');
        builder.append(", schema='").append(schema).append('\'');
        builder.append(", timeZoneKey=").append(timeZoneKey);
        builder.append(", locale=").append(locale);
        builder.append(", startTime=").append(startTime);
        builder.append('}');
        return builder.toString();
    }
}
