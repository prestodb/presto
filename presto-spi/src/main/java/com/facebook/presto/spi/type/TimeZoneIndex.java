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

import java.util.TimeZone;

import static com.facebook.presto.spi.type.TimeZoneKey.MAX_TIME_ZONE_KEY;
import static java.util.Objects.requireNonNull;

public final class TimeZoneIndex
{
    private TimeZoneIndex()
    {
    }

    private static final TimeZone[] TIME_ZONES;

    static {
        TIME_ZONES = new TimeZone[MAX_TIME_ZONE_KEY + 1];
        for (TimeZoneKey timeZoneKey : TimeZoneKey.getTimeZoneKeys()) {
            String zoneId = timeZoneKey.getId();
            TimeZone timeZone;
            // zone class is totally broken...
            if (zoneId.charAt(0) == '-' || zoneId.charAt(0) == '+') {
                timeZone = TimeZone.getTimeZone("GMT" + zoneId);
            }
            else {
                timeZone = TimeZone.getTimeZone(zoneId);
            }
            TIME_ZONES[timeZoneKey.getKey()] = timeZone;
        }
    }

    public static TimeZone getTimeZoneForKey(TimeZoneKey timeZoneKey)
    {
        requireNonNull(timeZoneKey, "timeZoneKey is null");
        return (TimeZone) TIME_ZONES[timeZoneKey.getKey()].clone();
    }
}
