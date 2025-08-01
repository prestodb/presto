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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldValueProvider;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;

import static com.facebook.presto.common.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class ISO8601HashRedisFieldDecoder
        extends HashRedisFieldDecoder
{
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd[ HH:mm:ss]");

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
            TemporalAccessor temporal = FORMATTER.parseBest(getSlice().toStringAscii(), LocalDateTime::from, LocalDate::from);
            LocalDateTime dateTime = temporal instanceof LocalDateTime ? LocalDateTime.from(temporal) : LocalDate.from(temporal).atStartOfDay();
            long millis = dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();

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
