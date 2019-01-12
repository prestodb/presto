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

import com.google.common.collect.ImmutableSortedSet;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.SortedSet;

import static io.prestosql.spi.type.TimeZoneKey.MAX_TIME_ZONE_KEY;
import static io.prestosql.spi.type.TimeZoneKey.UTC_KEY;
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
        assertSame(TimeZoneKey.getTimeZoneKey("Z"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("Zulu"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("zulu"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("ZULU"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("UT"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("UCT"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("Universal"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("GMT"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("GMT0"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("GMT+0"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("GMT-0"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("GMT+00:00"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("GMT-00:00"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("+00:00"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("-00:00"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("etc/utc"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("etc/gmt"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("etc/gmt+0"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("etc/gmt+00:00"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("etc/gmt-00:00"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("etc/ut"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("etc/UT"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("etc/UCT"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("etc/Universal"), UTC_KEY);
    }

    @Test
    public void testHourOffsetZone()
    {
        assertSame(TimeZoneKey.getTimeZoneKey("GMT0"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("GMT+0"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("GMT-0"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("GMT+0"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("GMT-0"), UTC_KEY);
        assertTimeZoneNotSupported("GMT7");
        assertSame(TimeZoneKey.getTimeZoneKey("GMT+7"), PLUS_7_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("GMT-7"), MINUS_7_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("GMT+7"), PLUS_7_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("GMT-7"), MINUS_7_KEY);

        assertTimeZoneNotSupported("UT0");
        assertSame(TimeZoneKey.getTimeZoneKey("UT+0"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("UT-0"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("UT+0"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("UT-0"), UTC_KEY);
        assertTimeZoneNotSupported("UT7");
        assertSame(TimeZoneKey.getTimeZoneKey("UT+7"), PLUS_7_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("UT-7"), MINUS_7_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("UT+7"), PLUS_7_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("UT-7"), MINUS_7_KEY);

        assertTimeZoneNotSupported("UTC0");
        assertSame(TimeZoneKey.getTimeZoneKey("UTC+0"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("UTC-0"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("UTC+0"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("UTC-0"), UTC_KEY);
        assertTimeZoneNotSupported("UTC7");
        assertSame(TimeZoneKey.getTimeZoneKey("UTC+7"), PLUS_7_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("UTC-7"), MINUS_7_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("UTC+7"), PLUS_7_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("UTC-7"), MINUS_7_KEY);

        assertSame(TimeZoneKey.getTimeZoneKey("Etc/GMT0"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("Etc/GMT+0"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("Etc/GMT-0"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("Etc/GMT+0"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("Etc/GMT-0"), UTC_KEY);
        assertTimeZoneNotSupported("Etc/GMT7");
        assertSame(TimeZoneKey.getTimeZoneKey("Etc/GMT+7"), MINUS_7_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("Etc/GMT-7"), PLUS_7_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("Etc/GMT+7"), MINUS_7_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("Etc/GMT-7"), PLUS_7_KEY);

        assertTimeZoneNotSupported("Etc/UT0");
        assertSame(TimeZoneKey.getTimeZoneKey("Etc/UT+0"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("Etc/UT-0"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("Etc/UT+0"), UTC_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("Etc/UT-0"), UTC_KEY);
        assertTimeZoneNotSupported("Etc/UT7");
        assertSame(TimeZoneKey.getTimeZoneKey("Etc/UT+7"), PLUS_7_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("Etc/UT-7"), MINUS_7_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("Etc/UT+7"), PLUS_7_KEY);
        assertSame(TimeZoneKey.getTimeZoneKey("Etc/UT-7"), MINUS_7_KEY);

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
        assertEquals(hasher.hash().asLong(), -4582158485614614451L, "zone-index.properties file contents changed!");
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
