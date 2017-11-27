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
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSS");

    private final long millisUtc;
    private final Optional<TimeZoneKey> sessionTimeZoneKey;

    public SqlTimestamp(long millisUtc)
    {
        this.millisUtc = millisUtc;
        sessionTimeZoneKey = Optional.empty();
    }

    @Deprecated
    public SqlTimestamp(long millisUtc, TimeZoneKey sessionTimeZoneKey)
    {
        this.millisUtc = millisUtc;
        this.sessionTimeZoneKey = Optional.of(sessionTimeZoneKey);
    }

    public long getMillisUtc()
    {
        return millisUtc;
    }

    @Deprecated
    public Optional<TimeZoneKey> getSessionTimeZoneKey()
    {
        return sessionTimeZoneKey;
    }

    @Deprecated
    private boolean isDeprecatedTimestamp()
    {
        return sessionTimeZoneKey.isPresent();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(millisUtc, sessionTimeZoneKey);
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
        return Objects.equals(this.millisUtc, other.millisUtc) &&
                Objects.equals(this.sessionTimeZoneKey, other.sessionTimeZoneKey);
    }

    @JsonValue
    @Override
    public String toString()
    {
        if (isDeprecatedTimestamp()) {
            return Instant.ofEpochMilli(millisUtc).atZone(ZoneId.of(sessionTimeZoneKey.get().getId())).format(formatter);
        }
        else {
            // FIXME this should be exact equivalent of TIMESTAMP to VARCHAR cast
            return Instant.ofEpochMilli(millisUtc).atZone(ZoneId.of(UTC_KEY.getId())).format(formatter);
        }
    }
}
