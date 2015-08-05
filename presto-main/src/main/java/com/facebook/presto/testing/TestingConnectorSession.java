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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class TestingConnectorSession
        implements ConnectorSession
{
    public static final ConnectorSession SESSION = new TestingConnectorSession(
            "user",
            UTC_KEY,
            ENGLISH,
            System.currentTimeMillis(),
            ImmutableList.of(),
            ImmutableMap.of());

    private final String user;
    private final TimeZoneKey timeZoneKey;
    private final Locale locale;
    private final long startTime;
    private final Map<String, PropertyMetadata<?>> properties;
    private final Map<String, Object> propertyValues;

    public TestingConnectorSession(
            String user,
            TimeZoneKey timeZoneKey,
            Locale locale,
            long startTime,
            List<PropertyMetadata<?>> propertyMetadatas,
            Map<String, Object> propertyValues)
    {
        this.user = requireNonNull(user, "user is null");
        this.timeZoneKey = requireNonNull(timeZoneKey, "timeZoneKey is null");
        this.locale = requireNonNull(locale, "locale is null");
        this.startTime = startTime;

        ImmutableMap.Builder<String, PropertyMetadata<?>> builder = ImmutableMap.builder();
        for (PropertyMetadata<?> propertyMetadata : propertyMetadatas) {
            builder.put(propertyMetadata.getName(), propertyMetadata);
        }
        this.properties = builder.build();
        this.propertyValues = ImmutableMap.copyOf(propertyValues);
    }

    @Override
    public String getUser()
    {
        return user;
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
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("user", user)
                .add("timeZoneKey", timeZoneKey)
                .add("locale", locale)
                .add("startTime", startTime)
                .add("properties", properties)
                .toString();
    }
}
