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
package com.facebook.presto.tests;

import com.facebook.presto.Session;
import com.facebook.presto.spi.type.TimeZoneKey;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.testng.Assert.assertEquals;

public abstract class AbstractTestEngineOnlyQueries
        extends AbstractTestQueryFramework
{
    protected AbstractTestEngineOnlyQueries(QueryRunnerSupplier supplier)
    {
        super(supplier);
    }

    @Test
    public void testTimeLiterals()
    {
        Session chicago = Session.builder(getSession()).setTimeZoneKey(TimeZoneKey.getTimeZoneKey("America/Chicago")).build();
        Session kathmandu = Session.builder(getSession()).setTimeZoneKey(TimeZoneKey.getTimeZoneKey("Asia/Kathmandu")).build();

        assertEquals(computeScalar("SELECT DATE '2013-03-22'"), LocalDate.of(2013, 3, 22));
        assertQuery("SELECT DATE '2013-03-22'");
        assertQuery(chicago, "SELECT DATE '2013-03-22'");
        assertQuery(kathmandu, "SELECT DATE '2013-03-22'");

        assertEquals(computeScalar("SELECT TIME '3:04:05'"), LocalTime.of(3, 4, 5, 0));
        assertEquals(computeScalar("SELECT TIME '3:04:05.123'"), LocalTime.of(3, 4, 5, 123_000_000));
        assertQuery("SELECT TIME '3:04:05'");
        // TODO #7122 assertQuery(chicago, "SELECT TIME '3:04:05'");
        // TODO #7122 assertQuery(kathmandu, "SELECT TIME '3:04:05'");

        assertEquals(computeScalar("SELECT TIME '01:02:03.400 Z'"), OffsetTime.of(1, 2, 3, 400_000_000, ZoneOffset.UTC));
        assertEquals(computeScalar("SELECT TIME '01:02:03.400 UTC'"), OffsetTime.of(1, 2, 3, 400_000_000, ZoneOffset.UTC));
        assertEquals(computeScalar("SELECT TIME '3:04:05 +06:00'"), OffsetTime.of(3, 4, 5, 0, ZoneOffset.ofHoursMinutes(6, 0)));
        assertEquals(computeScalar("SELECT TIME '3:04:05 +0507'"), OffsetTime.of(3, 4, 5, 0, ZoneOffset.ofHoursMinutes(5, 7)));
        assertEquals(computeScalar("SELECT TIME '3:04:05 +03'"), OffsetTime.of(3, 4, 5, 0, ZoneOffset.ofHoursMinutes(3, 0)));

        assertEquals(computeScalar("SELECT TIMESTAMP '1960-01-22 3:04:05'"), LocalDateTime.of(1960, 1, 22, 3, 4, 5));
        assertEquals(computeScalar("SELECT TIMESTAMP '1960-01-22 3:04:05.123'"), LocalDateTime.of(1960, 1, 22, 3, 4, 5, 123_000_000));
        assertQuery("SELECT TIMESTAMP '1960-01-22 3:04:05'");
        assertQuery("SELECT TIMESTAMP '1960-01-22 3:04:05.123'");
        // TODO #7122 assertQuery(chicago, "SELECT TIMESTAMP '1960-01-22 3:04:05.123'");
        // TODO #7122 assertQuery(kathmandu, "SELECT TIMESTAMP '1960-01-22 3:04:05.123'");

        assertEquals(computeScalar("SELECT TIMESTAMP '1960-01-22 3:04:05 +06:00'"), ZonedDateTime.of(1960, 1, 22, 3, 4, 5, 0, ZoneOffset.ofHoursMinutes(6, 0)));
    }
}
