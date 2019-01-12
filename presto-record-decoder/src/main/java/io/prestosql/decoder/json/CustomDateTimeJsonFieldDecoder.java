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
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Locale;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.decoder.DecoderErrorCode.DECODER_CONVERSION_NOT_SUPPORTED;
import static io.prestosql.decoder.json.JsonRowDecoderFactory.throwUnsupportedColumnType;
import static io.prestosql.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static java.lang.String.format;
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
    private final DateTimeFormatter formatter;

    public CustomDateTimeJsonFieldDecoder(DecoderColumnHandle columnHandle)
    {
        this.columnHandle = requireNonNull(columnHandle, "columnHandle is null");
        if (!SUPPORTED_TYPES.contains(columnHandle.getType())) {
            throwUnsupportedColumnType(columnHandle);
        }

        checkArgument(columnHandle.getFormatHint() != null, "format hint not defined for column '%s'", columnHandle.getName());
        try {
            formatter = DateTimeFormat.forPattern(columnHandle.getFormatHint()).withLocale(Locale.ENGLISH).withZoneUTC();
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(
                    GENERIC_USER_ERROR,
                    format("invalid joda pattern '%s' passed as format hint for column '%s'", columnHandle.getFormatHint(), columnHandle.getName()));
        }
    }

    @Override
    public FieldValueProvider decode(JsonNode value)
    {
        return new CustomDateTimeJsonValueProvider(value, columnHandle, formatter);
    }

    public static class CustomDateTimeJsonValueProvider
            extends AbstractDateTimeJsonValueProvider
    {
        private final DateTimeFormatter formatter;

        public CustomDateTimeJsonValueProvider(JsonNode value, DecoderColumnHandle columnHandle, DateTimeFormatter formatter)
        {
            super(value, columnHandle);
            this.formatter = formatter;
        }

        @Override
        protected long getMillis()
        {
            if (!value.isValueNode()) {
                throw new PrestoException(
                        DECODER_CONVERSION_NOT_SUPPORTED,
                        format("could not parse non-value node as '%s' for column '%s'", columnHandle.getType(), columnHandle.getName()));
            }
            try {
                return formatter.parseMillis(value.asText());
            }
            catch (IllegalArgumentException e) {
                throw new PrestoException(
                        DECODER_CONVERSION_NOT_SUPPORTED,
                        format("could not parse value '%s' as '%s' for column '%s'", value.asText(), columnHandle.getType(), columnHandle.getName()));
            }
        }
    }
}
