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
package io.prestosql.decoder.json;

import io.prestosql.spi.type.Type;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimeZoneKey.UTC_KEY;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static java.util.Arrays.asList;

public class TestSecondsSinceEpochJsonFieldDecoder
{
    private JsonFieldDecoderTester tester = new JsonFieldDecoderTester("seconds-since-epoch");

    @Test
    public void testDecode()
    {
        tester.assertDecodedAs("33701", TIME, 33701000);
        tester.assertDecodedAs("\"33701\"", TIME, 33701000);
        tester.assertDecodedAs("33701", TIME_WITH_TIME_ZONE, packDateTimeWithZone(33701000, UTC_KEY));
        tester.assertDecodedAs("\"33701\"", TIME_WITH_TIME_ZONE, packDateTimeWithZone(33701000, UTC_KEY));
        tester.assertDecodedAs("1519032101", TIMESTAMP, 1519032101000L);
        tester.assertDecodedAs("\"1519032101\"", TIMESTAMP, 1519032101000L);
        tester.assertDecodedAs("" + (Long.MAX_VALUE / 1000), TIMESTAMP, Long.MAX_VALUE / 1000 * 1000);
        tester.assertDecodedAs("" + (Long.MIN_VALUE / 1000), TIMESTAMP, Long.MIN_VALUE / 1000 * 1000);
        tester.assertDecodedAs("1519032101", TIMESTAMP_WITH_TIME_ZONE, packDateTimeWithZone(1519032101000L, UTC_KEY));
        tester.assertDecodedAs("\"1519032101\"", TIMESTAMP_WITH_TIME_ZONE, packDateTimeWithZone(1519032101000L, UTC_KEY));
    }

    @Test
    public void testDecodeNulls()
    {
        for (Type type : asList(TIME, TIME_WITH_TIME_ZONE, TIMESTAMP, TIMESTAMP_WITH_TIME_ZONE)) {
            tester.assertDecodedAsNull("null", type);
            tester.assertMissingDecodedAsNull(type);
        }
    }

    @Test
    public void testDecodeInvalid()
    {
        for (Type type : asList(TIME, TIME_WITH_TIME_ZONE, TIMESTAMP, TIMESTAMP_WITH_TIME_ZONE)) {
            tester.assertInvalidInput("{}", type, "could not parse non-value node as '.*' for column 'some_column'");
            tester.assertInvalidInput("[]", type, "could not parse non-value node as '.*' for column 'some_column'");
            tester.assertInvalidInput("[10]", type, "could not parse non-value node as '.*' for column 'some_column'");
            tester.assertInvalidInput("\"a\"", type, "could not parse value 'a' as '.*' for column 'some_column'");
            tester.assertInvalidInput("12345678901234567890", type, "could not parse value '12345678901234567890' as '.*' for column 'some_column'");
            tester.assertInvalidInput("" + (Long.MAX_VALUE / 1000 + 1), type, "could not parse value '9223372036854776' as '.*' for column 'some_column'");
            tester.assertInvalidInput("" + (Long.MIN_VALUE / 1000 - 1), type, "could not parse value '-9223372036854776' as '.*' for column 'some_column'");
            tester.assertInvalidInput("362016000.5", type, "could not parse value '3.620160005E8' as '.*' for column 'some_column'");
        }

        // TIME specific range checks
        tester.assertInvalidInput("-1", TIME, "could not parse value '-1' as 'time' for column 'some_column'");
        tester.assertInvalidInput("" + TimeUnit.DAYS.toSeconds(1) + 1, TIME, "could not parse value '864001' as 'time' for column 'some_column'");
        tester.assertInvalidInput("-1", TIME_WITH_TIME_ZONE, "could not parse value '-1' as 'time with time zone' for column 'some_column'");
        tester.assertInvalidInput("" + TimeUnit.DAYS.toSeconds(1) + 1, TIME_WITH_TIME_ZONE, "could not parse value '864001' as 'time with time zone' for column 'some_column'");
    }
}
