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
package com.facebook.presto.redis.decoder.hash;

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableSet;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Locale;
import java.util.Set;

import static com.facebook.presto.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class ISO8601HashRedisFieldDecoder
        extends HashRedisFieldDecoder
{
    private static final DateTimeFormatter FORMATTER = ISODateTimeFormat.dateTimeParser().withLocale(Locale.ENGLISH).withZoneUTC();

    @Override
    public Set<Class<?>> getJavaTypes()
    {
        return ImmutableSet.of(long.class);
    }

    @Override
    public String getFieldDecoderName()
    {
        return "iso8601";
    }

    @Override
    public FieldValueProvider decode(String value, DecoderColumnHandle columnHandle)
    {
        return new ISO8601HashRedisValueProvider(columnHandle, value);
    }

    private static class ISO8601HashRedisValueProvider
            extends HashRedisValueProvider
    {
        public ISO8601HashRedisValueProvider(DecoderColumnHandle columnHandle, String value)
        {
            super(columnHandle, value);
        }

        @Override
        public long getLong()
        {
            if (isNull()) {
                return 0L;
            }

            long millis = FORMATTER.parseMillis(getSlice().toStringAscii());

            Type type = columnHandle.getType();
            if (type.equals(DATE)) {
                return MILLISECONDS.toDays(millis);
            }
            if (type.equals(TIMESTAMP) || type.equals(TIME)) {
                return millis;
            }
            if (type.equals(TIMESTAMP_WITH_TIME_ZONE) || type.equals(TIME_WITH_TIME_ZONE)) {
                return packDateTimeWithZone(millis, 0);
            }

            return millis;
        }
    }
}
