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

import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.testing.DateTimeTestingUtils.sqlTimeOf;

public class TestTimeWithTimeZoneLegacy
        extends TestTimeWithTimeZoneBase
{
    public TestTimeWithTimeZoneLegacy()
    {
        super(true);
    }

    @Test
    @Override
    public void testCastToTime()
    {
        assertFunction("cast(TIME '03:04:05.321 +07:09' as time)",
                TIME,
                sqlTimeOf(2, 4, 5, 321, session));
    }
}
