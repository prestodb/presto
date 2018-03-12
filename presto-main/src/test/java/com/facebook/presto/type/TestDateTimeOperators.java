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

import com.facebook.presto.spi.type.SqlTimestamp;
import org.testng.annotations.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;

public class TestDateTimeOperators
         extends TestDateTimeOperatorsBase
{
    public TestDateTimeOperators()
    {
        super(false);
    }

    @Test
    public void testTimeZoneGap()
    {
        assertFunction("TIMESTAMP '2013-03-31 00:05' + INTERVAL '1' hour", TIMESTAMP, toTimestamp(LocalDateTime.of(2013, 3, 31, 1, 5, 0, 0)));
        assertFunction("TIMESTAMP '2013-03-31 00:05' + INTERVAL '2' hour", TIMESTAMP, toTimestamp(LocalDateTime.of(2013, 3, 31, 2, 5, 0, 0)));
        assertFunction("TIMESTAMP '2013-03-31 00:05' + INTERVAL '3' hour", TIMESTAMP, toTimestamp(LocalDateTime.of(2013, 3, 31, 3, 5, 0, 0)));

        assertFunction("TIMESTAMP '2013-03-31 04:05' - INTERVAL '3' hour", TIMESTAMP, toTimestamp(LocalDateTime.of(2013, 3, 31, 1, 5, 0, 0)));
        assertFunction("TIMESTAMP '2013-03-31 03:05' - INTERVAL '2' hour", TIMESTAMP, toTimestamp(LocalDateTime.of(2013, 3, 31, 1, 5, 0, 0)));
        assertFunction("TIMESTAMP '2013-03-31 01:05' - INTERVAL '1' hour", TIMESTAMP, toTimestamp(LocalDateTime.of(2013, 3, 31, 0, 5, 0, 0)));
    }

    @Test
    public void testTimeZoneDuplicate()
    {
        assertFunction("TIMESTAMP '2013-10-27 00:05' + INTERVAL '1' hour",
                TIMESTAMP,
                toTimestamp(LocalDateTime.of(2013, 10, 27, 1, 5, 0, 0)));
        assertFunction("TIMESTAMP '2013-10-27 00:05' + INTERVAL '2' hour",
                TIMESTAMP,
                toTimestamp(LocalDateTime.of(2013, 10, 27, 2, 5, 0, 0)));
        assertFunction("TIMESTAMP '2013-10-27 00:05' + INTERVAL '3' hour",
                TIMESTAMP,
                toTimestamp(LocalDateTime.of(2013, 10, 27, 3, 5, 0, 0)));
        assertFunction("TIMESTAMP '2013-10-27 00:05' + INTERVAL '4' hour",
                TIMESTAMP,
                toTimestamp(LocalDateTime.of(2013, 10, 27, 4, 5, 0, 0)));

        assertFunction("TIMESTAMP '2013-10-27 03:05' - INTERVAL '4' hour",
                TIMESTAMP,
                toTimestamp(LocalDateTime.of(2013, 10, 26, 23, 5, 0, 0)));
        assertFunction("TIMESTAMP '2013-10-27 02:05' - INTERVAL '2' hour",
                TIMESTAMP,
                toTimestamp(LocalDateTime.of(2013, 10, 27, 0, 5, 0, 0)));
        assertFunction("TIMESTAMP '2013-10-27 01:05' - INTERVAL '1' hour",
                TIMESTAMP,
                toTimestamp(LocalDateTime.of(2013, 10, 27, 0, 5, 0, 0)));

        assertFunction("TIMESTAMP '2013-10-27 03:05' - INTERVAL '1' hour",
                TIMESTAMP,
                toTimestamp(LocalDateTime.of(2013, 10, 27, 2, 5, 0, 0)));
        assertFunction("TIMESTAMP '2013-10-27 03:05' - INTERVAL '2' hour",
                TIMESTAMP,
                toTimestamp(LocalDateTime.of(2013, 10, 27, 1, 5, 0, 0)));
    }

    @Override
    protected SqlTimestamp toTimestamp(LocalDateTime localDateTime)
    {
        return new SqlTimestamp(localDateTime.toEpochSecond(ZoneOffset.UTC) * 1000 + localDateTime.getNano() / 1_000_000);
    }
}
