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

public final class SqlTimeWithTimeZone
{
    // This needs to be Locale-independent, Java Time's DateTimeFormatter compatible and should never change, as it defines the external API data format.
    // TODO when support for political time zones is removed, change the pattern to "HH:mm:ss.SSS XXX" and reuse in TestingPrestoClient
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS VV");

    private final long millisUtc;
    private final TimeZoneKey timeZoneKey;

    public SqlTimeWithTimeZone(long timeWithTimeZone)
    {
        millisUtc = unpackMillisUtc(timeWithTimeZone);
        timeZoneKey = unpackZoneKey(timeWithTimeZone);
    }

    public SqlTimeWithTimeZone(long millisUtc, TimeZoneKey timeZoneKey)
    {
        this.millisUtc = millisUtc;
        this.timeZoneKey = timeZoneKey;
    }

    public SqlTimeWithTimeZone(long millisUtc, TimeZone timeZone)
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
        SqlTimeWithTimeZone other = (SqlTimeWithTimeZone) obj;
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
