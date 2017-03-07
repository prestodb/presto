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
package com.facebook.presto.operator.scalar;

import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestDateTimeFunctionsLegacy
        extends TestDateTimeFunctionsBase
{
    @Test
    public TestDateTimeFunctionsLegacy()
    {
        super(
                testSessionBuilder()
                        .setTimeZoneKey(TIME_ZONE_KEY)
                        .setSystemProperty("legacy_timestamp", "true")
                        .build()
        );
    }

    @Test
    public void toIso8601ExistsForTimestamp()
    {
        assertFunction("to_iso8601(" + TIMESTAMP_LITERAL + ")", createVarcharType(35), TIMESTAMP_ISO8601_STRING);
    }

    @Test
    public void testFormatDateCanImplicitlyAddTimeZoneToTimestampLiteral()
    {
        assertFunction("format_datetime(" + TIMESTAMP_LITERAL + ", 'YYYY/MM/dd HH:mm ZZZZ')", VARCHAR, "2001/08/22 03:04 Asia/Kathmandu");
    }
}
