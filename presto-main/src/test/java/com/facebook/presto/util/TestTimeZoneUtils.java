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
package com.facebook.presto.util;

import com.facebook.presto.common.type.TimeZoneKey;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.time.ZoneId;
import java.util.TreeSet;

import static com.facebook.presto.common.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.common.type.TimeZoneKey.isUtcZoneId;
import static com.facebook.presto.util.DateTimeZoneIndex.getDateTimeZone;
import static com.facebook.presto.util.DateTimeZoneIndex.packDateTimeWithZone;
import static com.facebook.presto.util.DateTimeZoneIndex.unpackDateTimeZone;
import static org.testng.Assert.assertEquals;

public class TestTimeZoneUtils
{
    @Test
    public void test()
    {
        TimeZoneKey.getTimeZoneKey("GMT-13:00");

        TreeSet<String> jdkZones = new TreeSet<>(ZoneId.getAvailableZoneIds());

        for (String zoneId : new TreeSet<>(jdkZones)) {
            if (zoneId.startsWith("Etc/") || zoneId.startsWith("GMT") || zoneId.startsWith("SystemV/")) {
                continue;
            }

            if (zoneId.equals("Canada/East-Saskatchewan")) {
                // TODO: remove once minimum Java version is increased to 8u161 and 9.0.4, see PrestoSystemRequirement.
                // Removed from tzdata since 2017c.
                // Java updated to 2017c since 8u161, 9.0.4.
                // All Java 10+ are on later versions
                continue;
            }

            DateTimeZone dateTimeZone = DateTimeZone.forID(zoneId);
            DateTimeZone indexedZone = getDateTimeZone(TimeZoneKey.getTimeZoneKey(zoneId));

            assertDateTimeZoneEquals(zoneId, indexedZone);
            assertTimeZone(zoneId, dateTimeZone);
        }

        for (int offsetHours = -13; offsetHours < 14; offsetHours++) {
            for (int offsetMinutes = 0; offsetMinutes < 60; offsetMinutes++) {
                DateTimeZone dateTimeZone = DateTimeZone.forOffsetHoursMinutes(offsetHours, offsetMinutes);
                assertTimeZone(dateTimeZone.getID(), dateTimeZone);
            }
        }
    }

    public static void assertTimeZone(String zoneId, DateTimeZone dateTimeZone)
    {
        long dateTimeWithTimeZone = packDateTimeWithZone(new DateTime(42, dateTimeZone));
        assertEquals(packDateTimeWithZone((long) 42, dateTimeZone.toTimeZone().getID()), dateTimeWithTimeZone);
        DateTimeZone unpackedZone = unpackDateTimeZone(dateTimeWithTimeZone);
        assertDateTimeZoneEquals(zoneId, unpackedZone);
    }

    public static void assertDateTimeZoneEquals(String zoneId, DateTimeZone actualTimeZone)
    {
        DateTimeZone expectedDateTimeZone;
        if (isUtcZoneId(zoneId)) {
            expectedDateTimeZone = DateTimeZone.UTC;
        }
        else {
            expectedDateTimeZone = DateTimeZone.forID(zoneId);
        }

        assertEquals(actualTimeZone, expectedDateTimeZone);
    }
}
