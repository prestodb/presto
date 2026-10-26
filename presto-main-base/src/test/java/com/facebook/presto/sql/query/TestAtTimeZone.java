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
package com.facebook.presto.sql.query;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.TimeZoneKey;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.LEGACY_TIMESTAMP;
import static com.facebook.presto.SystemSessionProperties.LEGACY_TIMESTAMP_WITH_TIMEZONE;
import static com.facebook.presto.common.type.TimeZoneKey.getTimeZoneKey;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestAtTimeZone
{
    // Distinct from both the operand zone and the target zone, so a result computed in the session zone is visible.
    private static final TimeZoneKey SESSION_TIME_ZONE = getTimeZoneKey("Europe/Warsaw");

    private QueryAssertions assertions;

    @BeforeClass
    public void init()
    {
        assertions = new QueryAssertions(session(true));
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testTimestampWithTimeZoneKeepsZone()
    {
        assertions.assertQuery(
                session(true),
                "SELECT TIMESTAMP '2020-06-15 12:00:00 UTC' AT TIME ZONE 'America/Los_Angeles'",
                "SELECT TIMESTAMP '2020-06-15 05:00:00 America/Los_Angeles'");
    }

    @Test
    public void testTimestampWithTimeZoneBecomesWallClock()
    {
        // A literal operand is constant folded on the coordinator, so this covers the Java at_timezone_convert body.
        // 14:00 here would mean the wall clock was read in the session zone instead of America/Los_Angeles.
        assertions.assertQuery(
                session(false),
                "SELECT TIMESTAMP '2020-06-15 12:00:00 UTC' AT TIME ZONE 'America/Los_Angeles'",
                "SELECT TIMESTAMP '2020-06-15 05:00:00'");
    }

    @Test
    public void testTimestampWithTimeZoneBecomesWallClockForColumn()
    {
        // A column operand is not constant folded, so this covers the compiled expression instead.
        assertions.assertQuery(
                session(false),
                "SELECT ts AT TIME ZONE 'America/Los_Angeles' FROM (VALUES TIMESTAMP '2020-06-15 12:00:00 UTC') t(ts)",
                "SELECT TIMESTAMP '2020-06-15 05:00:00'");
    }

    @Test
    public void testTimestampWithTimeZoneAtZoneOffset()
    {
        assertions.assertQuery(
                session(false),
                "SELECT TIMESTAMP '2020-06-15 12:00:00 UTC' AT TIME ZONE INTERVAL '-07:00' HOUR TO MINUTE",
                "SELECT TIMESTAMP '2020-06-15 05:00:00'");
    }

    @Test
    public void testTimestampWithTimeZoneBecomesWallClockUnderLegacyTimestamp()
    {
        // Every Java coordinator at Meta pins legacy_timestamp=true, so this is the deployed configuration.
        Session session = testSessionBuilder()
                .setCatalog("local")
                .setSchema("default")
                .setTimeZoneKey(SESSION_TIME_ZONE)
                .setSystemProperty(LEGACY_TIMESTAMP_WITH_TIMEZONE, "false")
                .setSystemProperty(LEGACY_TIMESTAMP, "true")
                .build();
        assertions.assertQuery(
                session,
                "SELECT TIMESTAMP '2020-06-15 12:00:00 UTC' AT TIME ZONE 'America/Los_Angeles'",
                "SELECT TIMESTAMP '2020-06-15 05:00:00'");
        // The column case covers the compiled expression path under the same production settings.
        assertions.assertQuery(
                session,
                "SELECT ts AT TIME ZONE 'America/Los_Angeles' FROM (VALUES TIMESTAMP '2020-06-15 12:00:00 UTC') t(ts)",
                "SELECT TIMESTAMP '2020-06-15 05:00:00'");
        // Match literal and native encoding at the Warsaw overlap and gap.
        assertions.assertQuery(
                session,
                "SELECT TIMESTAMP '2020-10-25 02:30:00 UTC' AT TIME ZONE 'UTC'",
                "SELECT TIMESTAMP '2020-10-25 02:30:00'");
        assertions.assertQuery(
                session,
                "SELECT TIMESTAMP '2020-03-29 02:30:00 UTC' AT TIME ZONE 'UTC'",
                "SELECT TIMESTAMP '2020-03-29 03:30:00'");
    }

    @Test
    public void testTimestampIsUnaffected()
    {
        // The operand is read in the session zone (12:00 +02:00 is 03:00 in America/Los_Angeles) under both settings.
        String query = "SELECT TIMESTAMP '2020-06-15 12:00:00' AT TIME ZONE 'America/Los_Angeles'";
        String expected = "SELECT TIMESTAMP '2020-06-15 03:00:00 America/Los_Angeles'";
        assertions.assertQuery(session(true), query, expected);
        assertions.assertQuery(session(false), query, expected);
    }

    @Test
    public void testTimeWithTimeZoneIsUnaffected()
    {
        String query = "SELECT TIME '10:00:00 +00:00' AT TIME ZONE '+02:00'";
        String expected = "SELECT TIME '12:00:00 +02:00'";
        assertions.assertQuery(session(true), query, expected);
        assertions.assertQuery(session(false), query, expected);
    }

    @Test
    public void testDateIsRejected()
    {
        String query = "SELECT DATE '2020-06-15' AT TIME ZONE 'America/Los_Angeles'";
        String expectedMessage = ".*Type of value must be a time or timestamp with or without time zone \\(actual date\\).*";
        assertions.assertFails(session(true), query, expectedMessage);
        assertions.assertFails(session(false), query, expectedMessage);
    }

    private static Session session(boolean legacyTimestampWithTimezone)
    {
        return testSessionBuilder()
                .setCatalog("local")
                .setSchema("default")
                .setTimeZoneKey(SESSION_TIME_ZONE)
                .setSystemProperty(LEGACY_TIMESTAMP_WITH_TIMEZONE, String.valueOf(legacyTimestampWithTimezone))
                .build();
    }
}
