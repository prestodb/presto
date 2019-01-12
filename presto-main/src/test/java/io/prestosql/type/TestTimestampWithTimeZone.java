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
package io.prestosql.type;

import org.testng.annotations.Test;

import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.testing.DateTimeTestingUtils.sqlTimeOf;
import static io.prestosql.testing.DateTimeTestingUtils.sqlTimestampOf;

public class TestTimestampWithTimeZone
        extends TestTimestampWithTimeZoneBase
{
    public TestTimestampWithTimeZone()
    {
        super(false);
    }

    @Test
    @Override
    public void testCastToTime()
    {
        assertFunction("cast(TIMESTAMP '2001-1-22 03:04:05.321 +07:09' as time)",
                TIME,
                sqlTimeOf(3, 4, 5, 321, session));

        functionAssertions.assertFunctionString("cast(TIMESTAMP '2001-1-22 03:04:05.321 +07:09' as time)",
                TIME,
                "03:04:05.321");
    }

    @Test
    @Override
    public void testCastToTimeWithTimeZone()
    {
        super.testCastToTimeWithTimeZone();

        functionAssertions.assertFunctionString("cast(TIMESTAMP '2017-06-06 10:00:00.000 Europe/Warsaw' as time with time zone)",
                TIME_WITH_TIME_ZONE,
                "10:00:00.000 Europe/Warsaw");
        functionAssertions.assertFunctionString("cast(TIMESTAMP '2017-06-06 10:00:00.000 Asia/Kathmandu' as time with time zone)",
                TIME_WITH_TIME_ZONE,
                "10:00:00.000 Asia/Kathmandu");
    }

    @Test
    @Override
    public void testCastToTimestamp()
    {
        assertFunction("cast(TIMESTAMP '2001-1-22 03:04:05.321 +07:09' as timestamp)",
                TIMESTAMP,
                sqlTimestampOf(2001, 1, 22, 3, 4, 5, 321, session));

        // This TZ had switch in 2014, so if we test for 2014 and used unpacked value we would use wrong shift
        assertFunction("cast(TIMESTAMP '2001-1-22 03:04:05.321 Pacific/Bougainville' as timestamp)",
                TIMESTAMP,
                sqlTimestampOf(2001, 1, 22, 3, 4, 5, 321, session));
    }
}
