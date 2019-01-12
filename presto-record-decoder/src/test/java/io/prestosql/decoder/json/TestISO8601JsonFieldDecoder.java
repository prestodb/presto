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

import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimeZoneKey.UTC_KEY;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static java.util.Arrays.asList;

public class TestISO8601JsonFieldDecoder
{
    private JsonFieldDecoderTester tester = new JsonFieldDecoderTester("iso8601");

    @Test
    public void testDecode()
    {
        tester.assertDecodedAs("\"2018-02-19T09:20:11\"", TIMESTAMP, 1519032011000L);
        tester.assertDecodedAs("\"2018-02-19T09:20:11Z\"", TIMESTAMP, 1519032011000L);
        tester.assertDecodedAs("\"2018-02-19T09:20:11+10:00\"", TIMESTAMP, 1519032011000L);
        tester.assertDecodedAs("\"13:15:18\"", TIME, 47718000);
        tester.assertDecodedAs("\"13:15\"", TIME, 47700000);
        tester.assertDecodedAs("\"13:15:18Z\"", TIME, 47718000);
        tester.assertDecodedAs("\"13:15Z\"", TIME, 47700000);
        tester.assertDecodedAs("\"13:15:18+10:00\"", TIME, 47718000);
        tester.assertDecodedAs("\"13:15+10:00\"", TIME, 47700000);
        tester.assertDecodedAs("\"2018-02-11\"", DATE, 17573);
        tester.assertDecodedAs("\"2018-02-19T09:20:11Z\"", TIMESTAMP_WITH_TIME_ZONE, packDateTimeWithZone(1519032011000L, UTC_KEY));
        tester.assertDecodedAs("\"2018-02-19T12:20:11+03:00\"", TIMESTAMP_WITH_TIME_ZONE, packDateTimeWithZone(1519032011000L, "+03:00"));
        tester.assertDecodedAs("\"13:15:18Z\"", TIME_WITH_TIME_ZONE, packDateTimeWithZone(47718000, UTC_KEY));
        tester.assertDecodedAs("\"13:15:18+10:00\"", TIME_WITH_TIME_ZONE, packDateTimeWithZone(47718000, "+10:00"));
    }

    @Test
    public void testDecodeNulls()
    {
        for (Type type : asList(DATE, TIME, TIME_WITH_TIME_ZONE, TIMESTAMP, TIMESTAMP_WITH_TIME_ZONE)) {
            tester.assertDecodedAsNull("null", type);
            tester.assertMissingDecodedAsNull(type);
        }
    }

    @Test
    public void testDecodeInvalid()
    {
        tester.assertInvalidInput("1", TIMESTAMP, "could not parse value '1' as 'timestamp' for column 'some_column'");
        tester.assertInvalidInput("{}", TIMESTAMP, "could not parse non-value node as 'timestamp' for column 'some_column'");
        tester.assertInvalidInput("\"a\"", TIMESTAMP, "could not parse value 'a' as 'timestamp' for column 'some_column'");
        tester.assertInvalidInput("1", TIMESTAMP, "could not parse value '1' as 'timestamp' for column 'some_column'");

        tester.assertInvalidInput("\"2018-02-19T09:20:11\"", DATE, "could not parse value '2018-02-19T09:20:11' as 'date' for column 'some_column'");
        tester.assertInvalidInput("\"2018-02-19T09:20:11Z\"", DATE, "could not parse value '2018-02-19T09:20:11Z' as 'date' for column 'some_column'");
        tester.assertInvalidInput("\"09:20:11Z\"", DATE, "could not parse value '09:20:11Z' as 'date' for column 'some_column'");
        tester.assertInvalidInput("\"09:20:11\"", DATE, "could not parse value '09:20:11' as 'date' for column 'some_column'");

        tester.assertInvalidInput("\"2018-02-19T09:20:11\"", TIMESTAMP_WITH_TIME_ZONE, "could not parse value '2018-02-19T09:20:11' as 'timestamp with time zone' for column 'some_column'");
        tester.assertInvalidInput("\"09:20:11\"", TIMESTAMP_WITH_TIME_ZONE, "could not parse value '09:20:11' as 'timestamp with time zone' for column 'some_column'");
        tester.assertInvalidInput("\"09:20:11Z\"", TIMESTAMP_WITH_TIME_ZONE, "could not parse value '09:20:11Z' as 'timestamp with time zone' for column 'some_column'");
        tester.assertInvalidInput("\"2018-02-19\"", TIMESTAMP_WITH_TIME_ZONE, "could not parse value '2018-02-19' as 'timestamp with time zone' for column 'some_column'");

        tester.assertInvalidInput("\"2018-02-19T09:20:11\"", TIME, "could not parse value '2018-02-19T09:20:11' as 'time' for column 'some_column'");
        tester.assertInvalidInput("\"2018-02-19T09:20:11Z\"", TIME, "could not parse value '2018-02-19T09:20:11Z' as 'time' for column 'some_column'");
        tester.assertInvalidInput("\"2018-02-19\"", TIME, "could not parse value '2018-02-19' as 'time' for column 'some_column'");
        tester.assertInvalidInput("\"2018-02-19Z\"", TIME, "could not parse value '2018-02-19Z' as 'time' for column 'some_column'");

        tester.assertInvalidInput("\"2018-02-19T09:20:11\"", TIME_WITH_TIME_ZONE, "could not parse value '2018-02-19T09:20:11' as 'time with time zone' for column 'some_column'");
        tester.assertInvalidInput("\"2018-02-19T09:20:11Z\"", TIME_WITH_TIME_ZONE, "could not parse value '2018-02-19T09:20:11Z' as 'time with time zone' for column 'some_column'");
        tester.assertInvalidInput("\"09:20:11\"", TIME_WITH_TIME_ZONE, "could not parse value '09:20:11' as 'time with time zone' for column 'some_column'");
        tester.assertInvalidInput("\"2018-02-19\"", TIME_WITH_TIME_ZONE, "could not parse value '2018-02-19' as 'time with time zone' for column 'some_column'");
    }
}
