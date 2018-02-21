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

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableSet;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Locale;
import java.util.Set;

import static com.facebook.presto.decoder.json.JsonRowDecoderFactory.throwUnsupportedColumnType;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static java.util.Objects.requireNonNull;

/**
 * Custom date format decoder.
 * <p>
 * <tt>formatHint</tt> uses {@link org.joda.time.format.DateTimeFormatter} format.
 * <p>
 * Uses hardcoded UTC timezone and english locale.
 */
public class CustomDateTimeJsonFieldDecoder
        implements JsonFieldDecoder
{
    private static final Set<Type> SUPPORTED_TYPES = ImmutableSet.of(DATE, TIME, TIME_WITH_TIME_ZONE, TIMESTAMP, TIMESTAMP_WITH_TIME_ZONE);

    private final DecoderColumnHandle columnHandle;

    public CustomDateTimeJsonFieldDecoder(DecoderColumnHandle columnHandle)
    {
        this.columnHandle = requireNonNull(columnHandle, "columnHandle is null");
        if (!SUPPORTED_TYPES.contains(columnHandle.getType())) {
            throwUnsupportedColumnType(columnHandle);
        }
    }

    @Override
    public FieldValueProvider decode(JsonNode value)
    {
        return new CustomDateTimeJsonValueProvider(value, columnHandle);
    }

    public static class CustomDateTimeJsonValueProvider
            extends AbstractDateTimeJsonValueProvider
    {
        public CustomDateTimeJsonValueProvider(JsonNode value, DecoderColumnHandle columnHandle)
        {
            super(value, columnHandle);
        }

        @Override
        protected long getMillis()
        {
            if (value.canConvertToLong()) {
                return value.asLong();
            }

            requireNonNull(columnHandle.getFormatHint(), "formatHint is null");
            String textValue = value.isValueNode() ? value.asText() : value.toString();

            DateTimeFormatter formatter = DateTimeFormat.forPattern(columnHandle.getFormatHint()).withLocale(Locale.ENGLISH).withZoneUTC();
            return formatter.parseMillis(textValue);
        }
    }
}
