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

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.util.DateTimeUtils.parseTimestampWithoutTimeZone;

public class TestDateTimeUtils
{
    @Test(expectedExceptions = {ArithmeticException.class})
    public void testLongOverflowHigh()
    {
        parseTimestampWithoutTimeZone("292278994-08-17 11:46:00.000");
    }

    @DataProvider
    private Object[][] testWorkingTimestampsProvider()
    {
        return new Object[][] {
                {"292278993-08-17 11:46:00.000", 9223372005335160000L},
                {"2025-08-17 09:01:00.000", 1755421260000L},
                {"1960-08-17 11:46:00.000", -295791240000L},
                {"0001-08-17 11:46:00.000999", -62115855240000L},
                {"0001-08-17 11:46:00.000999 UTC", -62115855240000L},
                {"0001-08-17 11:46:00.000999UTC", -62115855240000L},
                {"0001-08-17 11:46:00.000000999", -62115855240000L},
                {"2023-01-02", 1672617600000L},
                {"1996-01-02", 820540800000L},
                {"2001-1-22 03:04:05.321", 980132645321L},
                {"2001-1-22 03:04:05.321 +07:09", 980132645321L},
                {"0001-08-17 11:46:00.000999999", -62115855240000L}};
    }

    @Test(dataProvider = "testWorkingTimestampsProvider")
    public void testWorkingTimestamps(String value, long millis)
    {
        assertEquals(parseTimestampWithoutTimeZone(value), millis);
    }
}
