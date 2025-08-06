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

import org.testng.annotations.Test;

import static com.facebook.presto.testing.assertions.Assert.assertEquals;

public class TestDateTimeUtils
{
    @Test(expectedExceptions = {Exception.class})
    public void testLongOverflowHigh()
    {
        DateTimeUtils.parseTimestampWithoutTimeZone("292278994-08-17 11:46:00.000");
    }

    @Test
    public void testWorkingTimestamps()
    {
        long actualMillis = DateTimeUtils.parseTimestampWithoutTimezoneJavaTime("292278993-08-17 11:46:00.000");
        long expectedMillis = DateTimeUtils.parseTimestampWithoutTimeZone("292278993-08-17 11:46:00.000");
        assertEquals(actualMillis, expectedMillis);

        actualMillis = DateTimeUtils.parseTimestampWithoutTimezoneJavaTime("2025-08-17 09:01:00.000");
        expectedMillis = DateTimeUtils.parseTimestampWithoutTimeZone("2025-08-17 09:01:00.000");
        assertEquals(actualMillis, expectedMillis);

        actualMillis = DateTimeUtils.parseTimestampWithoutTimezoneJavaTime("1960-08-17 11:46:00.000");
        expectedMillis = DateTimeUtils.parseTimestampWithoutTimeZone("1960-08-17 11:46:00.000");
        assertEquals(actualMillis, expectedMillis);

        actualMillis = DateTimeUtils.parseTimestampWithoutTimezoneJavaTime("0001-08-17 11:46:00.000999");
        expectedMillis = DateTimeUtils.parseTimestampWithoutTimeZone("0001-08-17 11:46:00.000999");
        assertEquals(actualMillis, expectedMillis);

        actualMillis = DateTimeUtils.parseTimestampWithoutTimezoneJavaTime("0001-08-17 11:46:00.000999 UTC");
        expectedMillis = DateTimeUtils.parseTimestampWithoutTimeZone("0001-08-17 11:46:00.000999 UTC");
        assertEquals(actualMillis, expectedMillis);

        actualMillis = DateTimeUtils.parseTimestampWithoutTimezoneJavaTime("0001-08-17 11:46:00.000999UTC");
        expectedMillis = DateTimeUtils.parseTimestampWithoutTimeZone("0001-08-17 11:46:00.000999UTC");
        assertEquals(actualMillis, expectedMillis);

        actualMillis = DateTimeUtils.parseTimestampWithoutTimezoneJavaTime("0001-08-17 11:46:00.000000999");
        expectedMillis = DateTimeUtils.parseTimestampWithoutTimeZone("0001-08-17 11:46:00.000000999");
        assertEquals(actualMillis, expectedMillis);

        actualMillis = DateTimeUtils.parseTimestampWithoutTimezoneJavaTime("2023-01-02");
        expectedMillis = DateTimeUtils.parseTimestampWithoutTimeZone("2023-01-02");
        assertEquals(actualMillis, expectedMillis);

        actualMillis = DateTimeUtils.parseTimestampWithoutTimezoneJavaTime("1996-01-02");
        expectedMillis = DateTimeUtils.parseTimestampWithoutTimeZone("1996-01-02");
        assertEquals(actualMillis, expectedMillis);

        actualMillis = DateTimeUtils.parseTimestampWithoutTimezoneJavaTime("2001-1-22 03:04:05.321 +07:09");
        expectedMillis = DateTimeUtils.parseTimestampWithoutTimeZone("2001-1-22 03:04:05.321 +07:09");
        assertEquals(actualMillis, expectedMillis);

        actualMillis = DateTimeUtils.parseTimestampWithoutTimezoneJavaTime("2001-1-22 03:04:05.321");
        expectedMillis = DateTimeUtils.parseTimestampWithoutTimeZone("2001-1-22 03:04:05.321");
        assertEquals(actualMillis, expectedMillis);

        actualMillis = DateTimeUtils.parseTimestampWithoutTimezoneJavaTime("0001-08-17 11:46:00.000999999");
        expectedMillis = DateTimeUtils.parseTimestampWithoutTimeZone("0001-08-17 11:46:00.000999999");
        assertEquals(actualMillis, expectedMillis);
    }
}
