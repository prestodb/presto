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

public class ConnectorSession
{
    private final String user;
    private final TimeZoneKey timeZoneKey;
    private final Locale locale;
    private final String schema;
    private final long startTime;

    @JsonCreator
    public ConnectorSession(
            @JsonProperty("user") String user,
            @JsonProperty("schema") String schema,
            @JsonProperty("timeZoneKey") TimeZoneKey timeZoneKey,
            @JsonProperty("locale") Locale locale,
            @JsonProperty("startTime") long startTime)
    {
        this.user = requireNonNull(user, "user is null");
        this.timeZoneKey = requireNonNull(timeZoneKey, "timeZoneKey is null");
        this.locale = requireNonNull(locale, "locale is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.startTime = startTime;
    }

    @JsonProperty
    public String getUser()
    {
        return user;
    }

    /**
     * DO NOT CALL THIS FROM CONNECTORS. IT WILL BE REMOVED SOON.
     */
    @Deprecated
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
    public long getStartTime()
    {
        return startTime;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("Session{");
        builder.append("user='").append(user).append('\'');
        builder.append(", schema='").append(schema).append('\'');
        builder.append(", timeZoneKey=").append(timeZoneKey);
        builder.append(", locale=").append(locale);
        builder.append(", startTime=").append(startTime);
        builder.append('}');
        return builder.toString();
    }
}
