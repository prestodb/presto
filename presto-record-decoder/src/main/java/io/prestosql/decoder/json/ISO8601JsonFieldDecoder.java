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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableSet;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.Type;

import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.prestosql.decoder.DecoderErrorCode.DECODER_CONVERSION_NOT_SUPPORTED;
import static io.prestosql.decoder.json.JsonRowDecoderFactory.throwUnsupportedColumnType;
import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_DATE;
import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_TIME;
import static java.time.format.DateTimeFormatter.ISO_TIME;
import static java.time.temporal.ChronoField.EPOCH_DAY;
import static java.time.temporal.ChronoField.INSTANT_SECONDS;
import static java.time.temporal.ChronoField.MILLI_OF_DAY;
import static java.time.temporal.ChronoField.MILLI_OF_SECOND;
import static java.util.Objects.requireNonNull;

/**
 * ISO 8601 date format decoder.
 * <p>
 * Uses hardcoded UTC timezone and english locale.
 */
public class ISO8601JsonFieldDecoder
        implements JsonFieldDecoder
{
    private static final Set<Type> SUPPORTED_TYPES = ImmutableSet.of(DATE, TIME, TIME_WITH_TIME_ZONE, TIMESTAMP, TIMESTAMP_WITH_TIME_ZONE);

    private final DecoderColumnHandle columnHandle;

    public ISO8601JsonFieldDecoder(DecoderColumnHandle columnHandle)
    {
        this.columnHandle = requireNonNull(columnHandle, "columnHandle is null");
        if (!SUPPORTED_TYPES.contains(columnHandle.getType())) {
            throwUnsupportedColumnType(columnHandle);
        }
    }

    @Override
    public FieldValueProvider decode(JsonNode value)
    {
        return new ISO8601JsonValueProvider(value, columnHandle);
    }

    private static class ISO8601JsonValueProvider
            extends FieldValueProvider
    {
        private final JsonNode value;
        private final DecoderColumnHandle columnHandle;

        public ISO8601JsonValueProvider(JsonNode value, DecoderColumnHandle columnHandle)
        {
            this.value = value;
            this.columnHandle = columnHandle;
        }

        @Override
        public boolean isNull()
        {
            return value.isMissingNode() || value.isNull();
        }

        @Override
        public long getLong()
        {
            Type columnType = columnHandle.getType();
            if (!value.isValueNode()) {
                throw new PrestoException(
                        DECODER_CONVERSION_NOT_SUPPORTED,
                        format("could not parse non-value node as '%s' for column '%s'", columnType, columnHandle.getName()));
            }

            try {
                String textValue = value.asText();
                if (columnType == TIMESTAMP) {
                    // Equivalent to: ISO_DATE_TIME.parse(textValue, LocalDateTime::from).toInstant(UTC).toEpochMilli();
                    TemporalAccessor parseResult = ISO_DATE_TIME.parse(textValue);
                    return TimeUnit.DAYS.toMillis(parseResult.getLong(EPOCH_DAY)) + parseResult.getLong(MILLI_OF_DAY);
                }
                if (columnType == TIMESTAMP_WITH_TIME_ZONE) {
                    // Equivalent to:
                    // ZonedDateTime dateTime = ISO_OFFSET_DATE_TIME.parse(textValue, ZonedDateTime::from);
                    // packDateTimeWithZone(dateTime.toInstant().toEpochMilli(), getTimeZoneKey(dateTime.getZone().getId()));
                    TemporalAccessor parseResult = ISO_OFFSET_DATE_TIME.parse(textValue);
                    return packDateTimeWithZone(parseResult.getLong(INSTANT_SECONDS) * 1000 + parseResult.getLong(MILLI_OF_SECOND), getTimeZoneKey(ZoneId.from(parseResult).getId()));
                }
                if (columnType == TIME) {
                    return ISO_TIME.parse(textValue).getLong(MILLI_OF_DAY);
                }
                if (columnType == TIME_WITH_TIME_ZONE) {
                    TemporalAccessor parseResult = ISO_OFFSET_TIME.parse(textValue);
                    return packDateTimeWithZone(parseResult.get(MILLI_OF_DAY), getTimeZoneKey(ZoneId.from(parseResult).getId()));
                }
                if (columnType == DATE) {
                    return ISO_DATE.parse(textValue).getLong(EPOCH_DAY);
                }
                throw new IllegalArgumentException("unsupported type " + columnType);
            }
            catch (DateTimeParseException e) {
                throw new PrestoException(
                        DECODER_CONVERSION_NOT_SUPPORTED,
                        format("could not parse value '%s' as '%s' for column '%s'", value.asText(), columnType, columnHandle.getName()));
            }
        }
    }
}
