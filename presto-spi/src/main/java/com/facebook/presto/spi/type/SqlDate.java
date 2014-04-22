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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;

import static com.facebook.presto.spi.type.TimeZoneIndex.getTimeZoneForKey;

public final class SqlDate
{
    private final long millisAtMidnight;
    private final TimeZoneKey sessionTimeZoneKey;

    public SqlDate(long millisAtMidnight, TimeZoneKey sessionTimeZoneKey)
    {
        this.millisAtMidnight = millisAtMidnight;
        this.sessionTimeZoneKey = sessionTimeZoneKey;
    }

    public long getMillisAtMidnight()
    {
        return millisAtMidnight;
    }

    public TimeZoneKey getSessionTimeZoneKey()
    {
        return sessionTimeZoneKey;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(millisAtMidnight, sessionTimeZoneKey);
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
        SqlDate other = (SqlDate) obj;
        return Objects.equals(this.millisAtMidnight, other.millisAtMidnight) &&
                Objects.equals(this.sessionTimeZoneKey, other.sessionTimeZoneKey);
    }

    @JsonValue
    @Override
    public String toString()
    {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        format.setTimeZone(getTimeZoneForKey(sessionTimeZoneKey));
        return format.format(new Date(millisAtMidnight));
    }
}
