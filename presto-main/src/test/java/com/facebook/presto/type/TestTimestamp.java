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

import com.facebook.presto.common.type.SqlDate;
import org.testng.annotations.Test;

import java.time.LocalDateTime;

import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.testing.DateTimeTestingUtils.sqlTimestampOf;

public class TestTimestamp
        extends TestTimestampBase
{
    public TestTimestamp()
    {
        super(false);
    }

    @Test
    public void testCastFromVarcharContainingTimeZone()
    {
        assertFunction(
                "cast('2001-1-22 03:04:05.321 +07:09' as timestamp)",
                TIMESTAMP,
                sqlTimestampOf(LocalDateTime.of(2001, 1, 22, 3, 4, 5, 321_000_000)));
        assertFunction(
                "cast('2001-1-22 03:04:05.321123 +07:09' as timestamp)",
                TIMESTAMP,
                sqlTimestampOf(LocalDateTime.of(2001, 1, 22, 3, 4, 5, 321_000_000)));
        assertFunction(
                "cast('2001-1-22 03:04:05.321123456 +07:09' as timestamp)",
                TIMESTAMP,
                sqlTimestampOf(LocalDateTime.of(2001, 1, 22, 3, 4, 5, 321_000_000)));
        assertFunction(
                "cast('2001-1-22 03:04:05 +07:09' as timestamp)",
                TIMESTAMP,
                sqlTimestampOf(LocalDateTime.of(2001, 1, 22, 3, 4, 5)));
        assertFunction(
                "cast('2001-1-22 03:04 +07:09' as timestamp)",
                TIMESTAMP,
                sqlTimestampOf(LocalDateTime.of(2001, 1, 22, 3, 4, 0)));
        assertFunction(
                "cast('2001-1-22 +07:09' as timestamp)",
                TIMESTAMP,
                sqlTimestampOf(LocalDateTime.of(2001, 1, 22, 0, 0, 0)));

        assertFunction(
                "cast('2001-1-22 03:04:05.321 Asia/Oral' as timestamp)",
                TIMESTAMP,
                sqlTimestampOf(LocalDateTime.of(2001, 1, 22, 3, 4, 5, 321_000_000)));
        assertFunction(
                "cast('2001-1-22 03:04:05.321123 Asia/Oral' as timestamp)",
                TIMESTAMP,
                sqlTimestampOf(LocalDateTime.of(2001, 1, 22, 3, 4, 5, 321_000_000)));
        assertFunction(
                "cast('2001-1-22 03:04:05.321123456 Asia/Oral' as timestamp)",
                TIMESTAMP,
                sqlTimestampOf(LocalDateTime.of(2001, 1, 22, 3, 4, 5, 321_000_000)));
        assertFunction(
                "cast('2001-1-22 03:04:05 Asia/Oral' as timestamp)",
                TIMESTAMP,
                sqlTimestampOf(LocalDateTime.of(2001, 1, 22, 3, 4, 5)));
        assertFunction(
                "cast('2001-1-22 03:04 Asia/Oral' as timestamp)",
                TIMESTAMP,
                sqlTimestampOf(LocalDateTime.of(2001, 1, 22, 3, 4, 0)));
        assertFunction(
                "cast('2001-1-22 Asia/Oral' as timestamp)",
                TIMESTAMP,
                sqlTimestampOf(LocalDateTime.of(2001, 1, 22, 0, 0, 0)));
    }

    @Test
    public void testCastFromTimestampToDate()
    {
        assertFunction("cast(timestamp '1970-01-01 00:00:00.000' as date)",
                DATE,
                new SqlDate(0));

        assertFunction("cast(timestamp '1970-01-01 05:06:07.234' as date)",
                DATE,
                new SqlDate(0));

        assertFunction("cast(TIMESTAMP '1969-12-31 00:00:00.000' as date)",
                DATE,
                new SqlDate(-1));

        assertFunction("cast(timestamp '1969-12-31 05:06:07.234' as date)",
                DATE,
                new SqlDate(-1));
    }
}
