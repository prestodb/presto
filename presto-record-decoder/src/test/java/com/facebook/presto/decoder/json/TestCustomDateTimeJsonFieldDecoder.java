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

import com.facebook.presto.decoder.DecoderTestColumnHandle;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.ObjectMapperProvider;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestCustomDateTimeJsonFieldDecoder
{
    private final TemporalFieldDecoderTester timestampTester = new TemporalFieldDecoderTester("custom-date-time", "MM/yyyy/dd H:m:s");
    private final TemporalFieldDecoderTester timestampWithTimeZoneTester = new TemporalFieldDecoderTester("custom-date-time", "MM/yyyy/dd H:m:s Z");
    private final TemporalFieldDecoderTester timeTester = new TemporalFieldDecoderTester("custom-date-time", "mm:HH:ss");
    private final TemporalFieldDecoderTester dateTester = new TemporalFieldDecoderTester("custom-date-time", "MM/yyyy/dd");
    private final TemporalFieldDecoderTester timeJustHourTester = new TemporalFieldDecoderTester("custom-date-time", "HH");

    @Test
    public void testDecode()
    {
        timestampTester.assertDecodedAs("\"02/2018/19 9:20:11\"", TIMESTAMP, 1519032011000L);
        timestampWithTimeZoneTester.assertDecodedAs("\"02/2018/19 11:20:11 +02:00\"", TIMESTAMP, 1519032011000L);
        timestampTester.assertDecodedAs("\"02/2018/19 9:20:11\"", TIMESTAMP_WITH_TIME_ZONE, packDateTimeWithZone(1519032011000L, UTC_KEY));
        timestampWithTimeZoneTester.assertDecodedAs("\"02/2018/19 11:20:11 +02:00\"", TIMESTAMP_WITH_TIME_ZONE, packDateTimeWithZone(1519032011000L, UTC_KEY)); // TODO: extract TZ from pattern
        timeTester.assertDecodedAs("\"15:13:18\"", TIME, 47718000);
        timeJustHourTester.assertDecodedAs("\"15\"", TIME, 54000000);
        timeJustHourTester.assertDecodedAs("15", TIME, 54000000);
        timeTester.assertDecodedAs("\"15:13:18\"", TIME_WITH_TIME_ZONE, packDateTimeWithZone(47718000, UTC_KEY));
        dateTester.assertDecodedAs("\"02/2018/11\"", DATE, 17573);
    }

    @Test
    public void testDecodeNulls()
    {
        dateTester.assertDecodedAsNull("null", DATE);
        dateTester.assertMissingDecodedAsNull(DATE);

        timeTester.assertDecodedAsNull("null", TIME);
        timeTester.assertMissingDecodedAsNull(TIME);

        timeTester.assertDecodedAsNull("null", TIME_WITH_TIME_ZONE);
        timeTester.assertMissingDecodedAsNull(TIME_WITH_TIME_ZONE);

        timestampTester.assertDecodedAsNull("null", TIMESTAMP);
        timestampTester.assertMissingDecodedAsNull(TIMESTAMP);

        timestampTester.assertDecodedAsNull("null", TIMESTAMP_WITH_TIME_ZONE);
        timestampTester.assertMissingDecodedAsNull(TIMESTAMP_WITH_TIME_ZONE);
    }

    @Test
    public void testDecodeInvalid()
    {
        timestampTester.assertInvalidInput("1", TIMESTAMP, "could not parse value '1' as 'timestamp' for column 'some_column'");
        timestampTester.assertInvalidInput("{}", TIMESTAMP, "could not parse non-value node as 'timestamp' for column 'some_column'");
        timestampTester.assertInvalidInput("\"a\"", TIMESTAMP, "could not parse value 'a' as 'timestamp' for column 'some_column'");
        timestampTester.assertInvalidInput("\"15:13:18\"", TIMESTAMP, "could not parse value '15:13:18' as 'timestamp' for column 'some_column'");
        timestampTester.assertInvalidInput("\"02/2018/11\"", TIMESTAMP, "could not parse value '02/2018/11' as 'timestamp' for column 'some_column'");
    }

    @Test
    public void testInvalidFormatHint()
    {
        DecoderTestColumnHandle columnHandle = new DecoderTestColumnHandle(
                0,
                "some_column",
                TIMESTAMP,
                "mappedField",
                "custom-date-time",
                "XXMM/yyyy/dd H:m:sXX",
                false,
                false,
                false);
        assertThatThrownBy(() -> new JsonRowDecoderFactory(new ObjectMapperProvider().get()).create(emptyMap(), ImmutableSet.of(columnHandle)))
                .isInstanceOf(PrestoException.class)
                .hasMessageMatching("invalid joda pattern 'XXMM/yyyy/dd H:m:sXX' passed as format hint for column 'some_column'");
    }
}
