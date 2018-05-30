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
package com.facebook.presto.type;

import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Objects;

import static com.facebook.presto.client.IntervalDayTime.formatMillis;
import static com.facebook.presto.client.IntervalDayTime.toMillis;

public class SqlIntervalDayTime
{
    private final long milliSeconds;

    public SqlIntervalDayTime(long milliSeconds)
    {
        this.milliSeconds = milliSeconds;
    }

    public SqlIntervalDayTime(int day, int hour, int minute, int second, int millis)
    {
        milliSeconds = toMillis(day, hour, minute, second, millis);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(milliSeconds);
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
        SqlIntervalDayTime other = (SqlIntervalDayTime) obj;
        return Objects.equals(this.milliSeconds, other.milliSeconds);
    }

    @JsonValue
    @Override
    public String toString()
    {
        return formatMillis(milliSeconds);
    }

    public long getMillis()
    {
        return milliSeconds;
    }
}
