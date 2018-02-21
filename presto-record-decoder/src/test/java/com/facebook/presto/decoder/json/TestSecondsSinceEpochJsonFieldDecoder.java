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
package com.facebook.presto.decoder.json;

import com.facebook.presto.spi.type.Type;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static java.util.Arrays.asList;

public class TestSecondsSinceEpochJsonFieldDecoder
{
    private TemporalFieldDecoderTester tester = new TemporalFieldDecoderTester("seconds-since-epoch");

    @Test
    public void testDecode()
    {
        tester.assertDecodedAs("362016000", DATE, 4190);
        tester.assertDecodedAs("\"362016000\"", DATE, 4190);
        tester.assertDecodedAs("33701", TIME, 33701000);
        tester.assertDecodedAs("\"33701\"", TIME, 33701000);
        tester.assertDecodedAs("33701", TIME_WITH_TIME_ZONE, packDateTimeWithZone(33701000, UTC_KEY));
        tester.assertDecodedAs("\"33701\"", TIME_WITH_TIME_ZONE, packDateTimeWithZone(33701000, UTC_KEY));
        tester.assertDecodedAs("1519032101", TIMESTAMP, 1519032101000L);
        tester.assertDecodedAs("\"1519032101\"", TIMESTAMP, 1519032101000L);
        tester.assertDecodedAs("1519032101", TIMESTAMP_WITH_TIME_ZONE, packDateTimeWithZone(1519032101000L, UTC_KEY));
        tester.assertDecodedAs("\"1519032101\"", TIMESTAMP_WITH_TIME_ZONE, packDateTimeWithZone(1519032101000L, UTC_KEY));
    }

    @Test
    public void testDecodeNulls()
    {
        for (Type type : asList(DATE, TIME, TIME_WITH_TIME_ZONE, TIMESTAMP, TIMESTAMP_WITH_TIME_ZONE)) {
            tester.assertDecodedAsNull("null", type);
            tester.assertMissingDecodedAsNull(type);
        }
    }
}
