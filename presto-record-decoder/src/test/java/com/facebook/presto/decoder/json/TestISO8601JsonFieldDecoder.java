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

public class TestISO8601JsonFieldDecoder
{
    private TemporalFieldDecoderTester tester = new TemporalFieldDecoderTester("iso8601");

    @Test
    public void testDecode()
    {
        tester.assertDecodedAs("\"2018-02-11\"", DATE, 17573);
        tester.assertDecodedAs("\"1970-01-01T13:15:18\"", TIME, 47718000); // should it be supported really?
        tester.assertDecodedAs("\"1970-01-01T13:15:18\"", TIME_WITH_TIME_ZONE, packDateTimeWithZone(47718000, UTC_KEY)); // should it be supported really?
        tester.assertDecodedAs("\"2018-02-19T09:20:11\"", TIMESTAMP, 1519032011000L);
        tester.assertDecodedAs("\"2018-02-19T09:20:11\"", TIMESTAMP_WITH_TIME_ZONE, packDateTimeWithZone(1519032011000L, UTC_KEY));
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
