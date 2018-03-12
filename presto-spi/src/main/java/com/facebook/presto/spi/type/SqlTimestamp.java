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

import javax.annotation.Nullable;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

public final class SqlTimestamp
{
    // This needs to be Locale-independent, Java Time's DateTimeFormatter compatible and should never change, as it defines the external API data format.
    public static final String JSON_FORMAT = "uuuu-MM-dd HH:mm:ss.SSS";
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(JSON_FORMAT);

    private final long millis;
    @Nullable
    private final TimeZoneKey sessionTimeZoneKey;

    public static SqlTimestamp of(long millisLocal)
    {
        return new SqlTimestamp(millisLocal);
    }

    @Deprecated
    public static SqlTimestamp ofLegacy(long millisUtc, TimeZoneKey sessionTimeZoneKey)
    {
        return new SqlTimestamp(millisUtc, sessionTimeZoneKey);
    }

    public SqlTimestamp(long millisLocal)
    {
        this.millis = millisLocal;
        this.sessionTimeZoneKey = null;
    }

    @Deprecated
    public SqlTimestamp(long millisUtc, TimeZoneKey sessionTimeZoneKey)
    {
        this.millis = millisUtc;
        this.sessionTimeZoneKey = sessionTimeZoneKey;
    }

    public static SqlTimestamp of(LocalDateTime localDateTime)
    {
        return new SqlTimestamp(localDateTime.toEpochSecond(ZoneOffset.UTC) * 1000 + localDateTime.getNano() / 1_000_000);
    }

    public boolean isLegacy()
    {
        return sessionTimeZoneKey != null;
    }

    public long getMillis()
    {
        return millis;
    }

    @Deprecated
    public long getMillisUtc()
    {
        if (!isLegacy()) {
            throw new IllegalStateException("millisUtc is only present in legacy SqlTimestamp");
        }
        return millis;
    }

    @Deprecated
    public TimeZoneKey getSessionTimeZoneKey()
    {
        return sessionTimeZoneKey;
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
        if (isLegacy()) {
            return Instant.ofEpochMilli(millis).atZone(ZoneId.of(sessionTimeZoneKey.getId())).format(formatter);
        }
        else {
            return LocalDateTime.ofEpochSecond(Math.floorDiv(millis, 1000), 1_000_000 * (int) Math.floorMod(millis, 1000), ZoneOffset.UTC).format(formatter);
        }
    }
}
