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
package com.facebook.presto.common.type;

import com.fasterxml.jackson.annotation.JsonValue;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.type.TimeZoneKey.UTC_KEY;

public final class SqlTimestamp
{
    // This needs to be Locale-independent, Java Time's DateTimeFormatter compatible and should never change, as it defines the external API data format.
    private static final String JSON_MILLIS_FORMAT = "uuuu-MM-dd HH:mm:ss.SSS";
    public static final DateTimeFormatter JSON_MILLIS_FORMATTER = DateTimeFormatter.ofPattern(JSON_MILLIS_FORMAT);

    private final long value;
    private final Optional<TimeZoneKey> sessionTimeZoneKey;
    private final TimeUnit precision;

    public SqlTimestamp(long value, TimeUnit precision)
    {
        this.value = value;
        sessionTimeZoneKey = Optional.empty();
        this.precision = precision;
    }

    @Deprecated
    public SqlTimestamp(long millisUtc, TimeZoneKey sessionTimeZoneKey, TimeUnit precision)
    {
        this.value = millisUtc;
        this.sessionTimeZoneKey = Optional.of(sessionTimeZoneKey);
        this.precision = precision;
    }

    public long getMillis()
    {
        checkState(!isLegacyTimestamp(), "getMillis() can be called in new timestamp semantics only");
        return value;
    }

    @Deprecated
    public long getMillisUtc()
    {
        checkState(isLegacyTimestamp(), "getMillisUtc() can be called in legacy timestamp semantics only");
        return value;
    }

    @Deprecated
    public Optional<TimeZoneKey> getSessionTimeZoneKey()
    {
        return sessionTimeZoneKey;
    }

    @Deprecated
    public boolean isLegacyTimestamp()
    {
        return sessionTimeZoneKey.isPresent();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value, sessionTimeZoneKey);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SqlTimestamp other = (SqlTimestamp) obj;
        return Objects.equals(this.value, other.value) &&
                Objects.equals(this.sessionTimeZoneKey, other.sessionTimeZoneKey);
    }

    @JsonValue
    @Override
    public String toString()
    {
        if (isLegacyTimestamp()) {
            return Instant.ofEpochMilli(value).atZone(ZoneId.of(sessionTimeZoneKey.get().getId())).format(JSON_MILLIS_FORMATTER);
        }
        else {
            return Instant.ofEpochMilli(value).atZone(ZoneId.of(UTC_KEY.getId())).format(JSON_MILLIS_FORMATTER);
        }
    }

    private static void checkState(boolean condition, String message)
    {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }
}
