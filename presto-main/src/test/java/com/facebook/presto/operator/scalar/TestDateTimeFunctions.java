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

import com.facebook.presto.Session;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimestampType;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;

public class TestDateTimeFunctions
        extends TestDateTimeFunctionsBase
{
    public TestDateTimeFunctions()
    {
        super(false);
    }

    @Test
    public void testToIso8601ForTimestampWithoutTimeZone()
    {
        assertFunction("to_iso8601(" + TIMESTAMP_LITERAL + ")", createVarcharType(35), TIMESTAMP_ISO8601_STRING_NO_TIME_ZONE);
    }

    @Test
    public void testFormatDateCannotImplicitlyAddTimeZoneToTimestampLiteral()
    {
        assertInvalidFunction(
                "format_datetime(" + TIMESTAMP_LITERAL + ", 'YYYY/MM/dd HH:mm ZZZZ')",
                "format_datetime for TIMESTAMP type, cannot use 'Z' nor 'z' in format, as this type does not contain TZ information");
    }

    @Test
    public void testLocalTime()
    {
        Session localSession = Session.builder(session)
                .setStartTime(new DateTime(2017, 3, 1, 14, 30, 0, 0, DATE_TIME_ZONE).getMillis())
                .build();
        try (FunctionAssertions localAssertion = new FunctionAssertions(localSession)) {
            localAssertion.assertFunctionString("LOCALTIME", TimeType.TIME, "14:30:00.000");
        }
    }

    @Test
    public void testCurrentTime()
    {
        Session localSession = Session.builder(session)
                // we use Asia/Kathmandu here to test the difference in semantic change of current_time
                // between legacy and non-legacy timestamp
                .setTimeZoneKey(KATHMANDU_ZONE_KEY)
                .setStartTime(new DateTime(2017, 3, 1, 15, 45, 0, 0, KATHMANDU_ZONE).getMillis())
                .build();
        try (FunctionAssertions localAssertion = new FunctionAssertions(localSession)) {
            localAssertion.assertFunctionString("CURRENT_TIME", TIME_WITH_TIME_ZONE, "15:45:00.000 Asia/Kathmandu");
        }
    }

    @Test
    public void testLocalTimestamp()
    {
        Session localSession = Session.builder(session)
                .setStartTime(new DateTime(2017, 3, 1, 14, 30, 0, 0, DATE_TIME_ZONE).getMillis())
                .build();
        try (FunctionAssertions localAssertion = new FunctionAssertions(localSession)) {
            localAssertion.assertFunctionString("LOCALTIMESTAMP", TimestampType.TIMESTAMP, "2017-03-01 14:30:00.000");
        }
    }

    @Test
    public void testCurrentTimestamp()
    {
        Session localSession = Session.builder(session)
                .setStartTime(new DateTime(2017, 3, 1, 14, 30, 0, 0, DATE_TIME_ZONE).getMillis())
                .build();
        try (FunctionAssertions localAssertion = new FunctionAssertions(localSession)) {
            localAssertion.assertFunctionString("CURRENT_TIMESTAMP", TIMESTAMP_WITH_TIME_ZONE, "2017-03-01 14:30:00.000 Asia/Kabul");
            localAssertion.assertFunctionString("NOW()", TIMESTAMP_WITH_TIME_ZONE, "2017-03-01 14:30:00.000 Asia/Kabul");
        }
    }
}
