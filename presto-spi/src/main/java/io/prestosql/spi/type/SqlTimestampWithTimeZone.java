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
package io.prestosql.spi.type;

import com.fasterxml.jackson.annotation.JsonValue;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.TimeZone;

import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.type.DateTimeEncoding.unpackZoneKey;

public final class SqlTimestampWithTimeZone
{
    // This needs to be Locale-independent, Java Time's DateTimeFormatter compatible and should never change, as it defines the external API data format.
    public static final String JSON_FORMAT = "uuuu-MM-dd HH:mm:ss.SSS VV";
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(JSON_FORMAT);

    private final long millisUtc;
    private final TimeZoneKey timeZoneKey;

    public SqlTimestampWithTimeZone(long timestampWithTimeZone)
    {
        millisUtc = unpackMillisUtc(timestampWithTimeZone);
        timeZoneKey = unpackZoneKey(timestampWithTimeZone);
    }

    public SqlTimestampWithTimeZone(long millisUtc, TimeZoneKey timeZoneKey)
    {
        this.millisUtc = millisUtc;
        this.timeZoneKey = timeZoneKey;
    }

    public SqlTimestampWithTimeZone(long millisUtc, TimeZone timeZone)
    {
        this.millisUtc = millisUtc;
        this.timeZoneKey = TimeZoneKey.getTimeZoneKey(timeZone.getID());
    }

    public long getMillisUtc()
    {
        return millisUtc;
    }

    public TimeZoneKey getTimeZoneKey()
    {
        return timeZoneKey;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(millisUtc, timeZoneKey);
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
        SqlTimestampWithTimeZone other = (SqlTimestampWithTimeZone) obj;
        return this.millisUtc == other.millisUtc &&
                this.timeZoneKey == other.timeZoneKey;
    }

    @JsonValue
    @Override
    public String toString()
    {
        return Instant.ofEpochMilli(millisUtc).atZone(ZoneId.of(timeZoneKey.getId())).format(formatter);
    }
}
