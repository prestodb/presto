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
package com.facebook.presto.common.type;

import com.google.common.collect.ImmutableSortedSet;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.SortedSet;

import static com.facebook.presto.common.type.TimeZoneKey.MAX_TIME_ZONE_KEY;
import static com.facebook.presto.common.type.TimeZoneKey.UTC_KEY;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestTimeZoneKey
{
    private static final TimeZoneKey PLUS_7_KEY = TimeZoneKey.getTimeZoneKeyForOffset(7 * 60);
    private static final TimeZoneKey MINUS_7_KEY = TimeZoneKey.getTimeZoneKeyForOffset(-7 * 60);

    @Test
    public void testUTC()
    {
        assertEquals(UTC_KEY.getKey(), 0);
        assertEquals(UTC_KEY.getId(), "UTC");

        assertSame(TimeZoneKey.getTimeZoneKey((short) 0), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("UTC"), UTC_KEY);

        // verify UTC equivalent zones map to UTC
        assertTimeZoneNotSupported("Z");
        assertTimeZoneNotSupported("Zulu");
        assertTimeZoneNotSupported("zulu");
        assertTimeZoneNotSupported("ZULU");
        assertTimeZoneNotSupported("UT");
        assertTimeZoneNotSupported("UCT");
        assertTimeZoneNotSupported("Universal");
        assertTimeZoneNotSupported("GMT");
        assertTimeZoneNotSupported("GMT0");
        assertTimeZoneNotSupported("GMT+0");
        assertTimeZoneNotSupported("GMT-0");
        assertTimeZoneNotSupported("GMT+00:00");
        assertTimeZoneNotSupported("GMT-00:00");
        assertTimeZoneNotSupported("+00:00");
        assertTimeZoneNotSupported("-00:00");
        assertTimeZoneNotSupported("etc/utc");
        assertTimeZoneNotSupported("etc/gmt");
        assertSame(TimeZoneKey.getTimeZoneKey("etc/gmt+0"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("etc/gmt+00:00"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("etc/gmt-00:00"), UTC_KEY);
        assertTimeZoneNotSupported("etc/ut");
        assertTimeZoneNotSupported("etc/UT");
        assertTimeZoneNotSupported("etc/UCT");
        assertTimeZoneNotSupported("etc/Universal");
    }

    @Test
    public void testHourOffsetZone()
    {
        assertTimeZoneNotSupported("GMT0");
        assertTimeZoneNotSupported("GMT+0");
        assertTimeZoneNotSupported("GMT-0");
        assertTimeZoneNotSupported("GMT+0");
        assertTimeZoneNotSupported("GMT-0");
        assertTimeZoneNotSupported("GMT7");
        assertTimeZoneNotSupported("GMT+7");
        assertTimeZoneNotSupported("GMT-7");
        assertTimeZoneNotSupported("GMT+7");
        assertTimeZoneNotSupported("GMT-7");

        assertTimeZoneNotSupported("UT0");
        assertTimeZoneNotSupported("UT+0");
        assertTimeZoneNotSupported("UT-0");
        assertTimeZoneNotSupported("UT+0");
        assertTimeZoneNotSupported("UT-0");
        assertTimeZoneNotSupported("UT7");
        assertTimeZoneNotSupported("UT+7");
        assertTimeZoneNotSupported("UT-7");
        assertTimeZoneNotSupported("UT+7");
        assertTimeZoneNotSupported("UT-7");

        assertTimeZoneNotSupported("UTC0");
        assertTimeZoneNotSupported("UTC+0");
        assertTimeZoneNotSupported("UTC-0");
        assertTimeZoneNotSupported("UTC+0");
        assertTimeZoneNotSupported("UTC-0");
        assertTimeZoneNotSupported("UTC7");
        assertTimeZoneNotSupported("UTC+7");
        assertTimeZoneNotSupported("UTC-7");
        assertTimeZoneNotSupported("UTC+7");
        assertTimeZoneNotSupported("UTC-7");

        assertTimeZoneNotSupported("Etc/GMT0");
        assertSame(TimeZoneKey.getTimeZoneKey("Etc/GMT+0"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("Etc/GMT-0"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("Etc/GMT+0"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("Etc/GMT-0"), UTC_KEY);
        assertTimeZoneNotSupported("Etc/GMT7");
        assertSame(TimeZoneKey.getTimeZoneKey("Etc/GMT+7"), PLUS_7_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("Etc/GMT-7"), MINUS_7_KEY);

        assertTimeZoneNotSupported("Etc/UT0");
        assertTimeZoneNotSupported("Etc/UT+0");
        assertTimeZoneNotSupported("Etc/UT-0");
        assertTimeZoneNotSupported("Etc/UT+0");
        assertTimeZoneNotSupported("Etc/UT-0");
        assertTimeZoneNotSupported("Etc/UT7");
        assertTimeZoneNotSupported("Etc/UT+7");
        assertTimeZoneNotSupported("Etc/UT-7");
        assertTimeZoneNotSupported("Etc/UT+7");
        assertTimeZoneNotSupported("Etc/UT-7");

        assertTimeZoneNotSupported("Etc/UTC0");
        assertSame(TimeZoneKey.getTimeZoneKey("Etc/UTC+0"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("Etc/UTC-0"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("Etc/UTC+0"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("Etc/UTC-0"), UTC_KEY);
        assertTimeZoneNotSupported("Etc/UTC7");
        assertSame(TimeZoneKey.getTimeZoneKey("Etc/UTC+7"), PLUS_7_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("Etc/UTC-7"), MINUS_7_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("Etc/UTC+7"), PLUS_7_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("Etc/UTC-7"), MINUS_7_KEY);
    }

    @Test
    public void testZoneKeyLookup()
    {
        for (TimeZoneKey timeZoneKey : TimeZoneKey.getTimeZoneKeys()) {
            assertSame(TimeZoneKey.getTimeZoneKey(timeZoneKey.getKey()), timeZoneKey);
            assertSame(TimeZoneKey.getTimeZoneKey(timeZoneKey.getId()), timeZoneKey);
            assertSame(TimeZoneKey.getTimeZoneKey(timeZoneKey.getId().toUpperCase(ENGLISH)), timeZoneKey);
            assertSame(TimeZoneKey.getTimeZoneKey(timeZoneKey.getId().toLowerCase(ENGLISH)), timeZoneKey);
        }
    }

    @Test
    public void testMaxTimeZoneKey()
    {
        boolean foundMax = false;
        for (TimeZoneKey timeZoneKey : TimeZoneKey.getTimeZoneKeys()) {
            assertTrue(timeZoneKey.getKey() <= MAX_TIME_ZONE_KEY, timeZoneKey + " key is larger than max key " + MAX_TIME_ZONE_KEY);
            foundMax = foundMax || (timeZoneKey.getKey() == MAX_TIME_ZONE_KEY);
        }
        assertTrue(foundMax, "Did not find a time zone with the MAX_TIME_ZONE_KEY");
    }

    @Test
    public void testZoneKeyIdRange()
    {
        boolean[] hasValue = new boolean[MAX_TIME_ZONE_KEY + 1];

        for (TimeZoneKey timeZoneKey : TimeZoneKey.getTimeZoneKeys()) {
            short key = timeZoneKey.getKey();
            assertTrue(key >= 0, timeZoneKey + " has a negative time zone key");
            assertFalse(hasValue[key], "Another time zone has the same zone key as " + timeZoneKey);
            hasValue[key] = true;
        }

        // previous spot for Canada/East-Saskatchewan
        assertFalse(hasValue[2040]);
        hasValue[2040] = true;
        // previous spot for EST
        assertFalse(hasValue[2180]);
        hasValue[2180] = true;
        // previous spot for HST
        assertFalse(hasValue[2186]);
        hasValue[2186] = true;
        // previous spot for MST
        assertFalse(hasValue[2196]);
        hasValue[2196] = true;
        // US/Pacific-New removed http://mm.icann.org/pipermail/tz-announce/2020-October/000059.html
        assertFalse(hasValue[2174]);
        hasValue[2174] = true;

        for (int i = 0; i < hasValue.length; i++) {
            assertTrue(hasValue[i], "There is no time zone with key " + i);
        }
    }

    @Test
    public void testZoneKeyData()
    {
        Hasher hasher = Hashing.murmur3_128().newHasher();

        SortedSet<TimeZoneKey> timeZoneKeysSortedByKey = ImmutableSortedSet.copyOf(new Comparator<TimeZoneKey>()
        {
            @Override
            public int compare(TimeZoneKey left, TimeZoneKey right)
            {
                return Short.compare(left.getKey(), right.getKey());
            }
        }, TimeZoneKey.getTimeZoneKeys());

        for (TimeZoneKey timeZoneKey : timeZoneKeysSortedByKey) {
            hasher.putShort(timeZoneKey.getKey());
            hasher.putString(timeZoneKey.getId(), StandardCharsets.UTF_8);
        }
        // Zone file should not (normally) be changed, so let's make this more difficult
        assertEquals(hasher.hash().asLong(), 4825838578917475630L, "zone-index.properties file contents changed!");
    }

    public void assertTimeZoneNotSupported(String zoneId)
    {
        try {
            TimeZoneKey.getTimeZoneKey(zoneId);
            fail("expect TimeZoneNotSupportedException");
        }
        catch (TimeZoneNotSupportedException e) {
            // expected
        }
    }
}
