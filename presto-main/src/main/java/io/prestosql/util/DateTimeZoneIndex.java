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
package io.prestosql.util;

import io.prestosql.spi.type.DateTimeEncoding;
import io.prestosql.spi.type.TimeZoneKey;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;

import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.prestosql.spi.type.TimeZoneKey.MAX_TIME_ZONE_KEY;
import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKeys;

public final class DateTimeZoneIndex
{
    private DateTimeZoneIndex()
    {
    }

    private static final DateTimeZone[] DATE_TIME_ZONES;
    private static final ISOChronology[] CHRONOLOGIES;
    private static final int[] FIXED_ZONE_OFFSET;

    private static final int VARIABLE_ZONE = Integer.MAX_VALUE;

    static {
        DATE_TIME_ZONES = new DateTimeZone[MAX_TIME_ZONE_KEY + 1];
        CHRONOLOGIES = new ISOChronology[MAX_TIME_ZONE_KEY + 1];
        FIXED_ZONE_OFFSET = new int[MAX_TIME_ZONE_KEY + 1];
        for (TimeZoneKey timeZoneKey : getTimeZoneKeys()) {
            short zoneKey = timeZoneKey.getKey();
            DateTimeZone dateTimeZone = DateTimeZone.forID(timeZoneKey.getId());
            DATE_TIME_ZONES[zoneKey] = dateTimeZone;
            CHRONOLOGIES[zoneKey] = ISOChronology.getInstance(dateTimeZone);
            if (dateTimeZone.isFixed() && dateTimeZone.getOffset(0) % 60_000 == 0) {
                FIXED_ZONE_OFFSET[zoneKey] = dateTimeZone.getOffset(0) / 60_000;
            }
            else {
                FIXED_ZONE_OFFSET[zoneKey] = VARIABLE_ZONE;
            }
        }
    }

    public static ISOChronology getChronology(TimeZoneKey zoneKey)
    {
        return CHRONOLOGIES[zoneKey.getKey()];
    }

    public static ISOChronology unpackChronology(long timestampWithTimeZone)
    {
        return getChronology(unpackZoneKey(timestampWithTimeZone));
    }

    public static DateTimeZone getDateTimeZone(TimeZoneKey zoneKey)
    {
        return DATE_TIME_ZONES[zoneKey.getKey()];
    }

    public static DateTimeZone unpackDateTimeZone(long dateTimeWithTimeZone)
    {
        return getDateTimeZone(unpackZoneKey(dateTimeWithTimeZone));
    }

    public static long packDateTimeWithZone(DateTime dateTime)
    {
        return DateTimeEncoding.packDateTimeWithZone(dateTime.getMillis(), dateTime.getZone().getID());
    }

    public static int extractZoneOffsetMinutes(long dateTimeWithTimeZone)
    {
        short zoneKey = unpackZoneKey(dateTimeWithTimeZone).getKey();

        if (FIXED_ZONE_OFFSET[zoneKey] == VARIABLE_ZONE) {
            return DATE_TIME_ZONES[zoneKey].getOffset(unpackMillisUtc(dateTimeWithTimeZone)) / 60_000;
        }
        else {
            return FIXED_ZONE_OFFSET[zoneKey];
        }
    }
}
