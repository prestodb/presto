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

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public class ConnectorSession
{
    private final String user;
    private final TimeZoneKey timeZoneKey;
    private final Locale locale;
    private final long startTime;
    private final Map<String, String> properties;

    public ConnectorSession(
            String user,
            TimeZoneKey timeZoneKey,
            Locale locale,
            long startTime,
            Map<String, String> properties)
    {
        this.user = requireNonNull(user, "user is null");
        this.timeZoneKey = requireNonNull(timeZoneKey, "timeZoneKey is null");
        this.locale = requireNonNull(locale, "locale is null");
        this.startTime = startTime;

        if (properties == null) {
            properties = new HashMap<>();
        }
        this.properties = unmodifiableMap(new HashMap<>(properties));
    }

    public String getUser()
    {
        return user;
    }

    public TimeZoneKey getTimeZoneKey()
    {
        return timeZoneKey;
    }

    public Locale getLocale()
    {
        return locale;
    }

    public long getStartTime()
    {
        return startTime;
    }

    public Map<String, String> getProperties()
    {
        return properties;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("Session{");
        builder.append("user='").append(user).append('\'');
        builder.append(", timeZoneKey=").append(timeZoneKey);
        builder.append(", locale=").append(locale);
        builder.append(", startTime=").append(startTime);
        builder.append(", properties=").append(properties);
        builder.append('}');
        return builder.toString();
    }
}
