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

import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.testing.DateTimeTestingUtils.sqlTimestampOf;

public class TestTimestampLegacy
        extends TestTimestampBase
{
    public TestTimestampLegacy()
    {
        super(true);
    }

    @Override
    public void testCastFromSlice()
    {
        super.testCastFromSlice();
        assertFunction("cast('2001-1-22 03:04:05.321 +07:09' as timestamp)", TIMESTAMP, sqlTimestampOf(2001, 1, 22, 3, 4, 5, 321, WEIRD_ZONE, TIME_ZONE_KEY, session));
        assertFunction("cast('2001-1-22 03:04:05 +07:09' as timestamp)", TIMESTAMP, sqlTimestampOf(2001, 1, 22, 3, 4, 5, 0, WEIRD_ZONE, TIME_ZONE_KEY, session));
        assertFunction("cast('2001-1-22 03:04 +07:09' as timestamp)", TIMESTAMP, sqlTimestampOf(2001, 1, 22, 3, 4, 0, 0, WEIRD_ZONE, TIME_ZONE_KEY, session));
        assertFunction("cast('2001-1-22 +07:09' as timestamp)", TIMESTAMP, sqlTimestampOf(2001, 1, 22, 0, 0, 0, 0, WEIRD_ZONE, TIME_ZONE_KEY, session));

        assertFunction("cast('2001-1-22 03:04:05.321 Asia/Oral' as timestamp)", TIMESTAMP, sqlTimestampOf(2001, 1, 22, 3, 4, 5, 321, ORAL_ZONE, TIME_ZONE_KEY, session));
        assertFunction("cast('2001-1-22 03:04:05 Asia/Oral' as timestamp)", TIMESTAMP, sqlTimestampOf(2001, 1, 22, 3, 4, 5, 0, ORAL_ZONE, TIME_ZONE_KEY, session));
        assertFunction("cast('2001-1-22 03:04 Asia/Oral' as timestamp)", TIMESTAMP, sqlTimestampOf(2001, 1, 22, 3, 4, 0, 0, ORAL_ZONE, TIME_ZONE_KEY, session));
        assertFunction("cast('2001-1-22 Asia/Oral' as timestamp)", TIMESTAMP, sqlTimestampOf(2001, 1, 22, 0, 0, 0, 0, ORAL_ZONE, TIME_ZONE_KEY, session));
    }
}
