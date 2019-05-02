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
package com.facebook.presto.spi.type;

import com.fasterxml.jackson.annotation.JsonValue;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;

public final class SqlTimestamp
{
    // This needs to be Locale-independent, Java Time's DateTimeFormatter compatible and should never change, as it defines the external API data format.
    public static final String JSON_FORMAT = "uuuu-MM-dd HH:mm:ss.SSS";
    public static final DateTimeFormatter JSON_FORMATTER = DateTimeFormatter.ofPattern(JSON_FORMAT);

    private final long millis;
    private final Optional<TimeZoneKey> sessionTimeZoneKey;

    public SqlTimestamp(long millis)
    {
        this.millis = millis;
        sessionTimeZoneKey = Optional.empty();
    }

    @Deprecated
    public SqlTimestamp(long millisUtc, TimeZoneKey sessionTimeZoneKey)
    {
        this.millis = millisUtc;
        this.sessionTimeZoneKey = Optional.of(sessionTimeZoneKey);
    }

    public long getMillis()
    {
        checkState(!isLegacyTimestamp(), "getMillis() can be called in new timestamp semantics only");
        return millis;
    }

    @Deprecated
    public long getMillisUtc()
    {
        checkState(isLegacyTimestamp(), "getMillisUtc() can be called in legacy timestamp semantics only");
        return millis;
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
        return Objects.hash(millis, sessionTimeZoneKey);
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
        return Objects.equals(this.millis, other.millis) &&
                Objects.equals(this.sessionTimeZoneKey, other.sessionTimeZoneKey);
    }

    @JsonValue
    @Override
    public String toString()
    {
        if (isLegacyTimestamp()) {
            return Instant.ofEpochMilli(millis).atZone(ZoneId.of(sessionTimeZoneKey.get().getId())).format(JSON_FORMATTER);
        }
        else {
            return Instant.ofEpochMilli(millis).atZone(ZoneId.of(UTC_KEY.getId())).format(JSON_FORMATTER);
        }
    }

    private static void checkState(boolean condition, String message)
    {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }
}
