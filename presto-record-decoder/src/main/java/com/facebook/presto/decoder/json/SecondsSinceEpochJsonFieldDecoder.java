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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.decoder.DecoderErrorCode.DECODER_CONVERSION_NOT_SUPPORTED;
import static com.facebook.presto.decoder.json.JsonRowDecoderFactory.throwUnsupportedColumnType;
import static java.lang.Long.parseLong;
import static java.lang.Math.multiplyExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Seconds since the epoch date format decoder.
 * <p>
 * Uses hardcoded UTC timezone and english locale.
 */
public class SecondsSinceEpochJsonFieldDecoder
        implements JsonFieldDecoder
{
    private static final Set<Type> SUPPORTED_TYPES = ImmutableSet.of(TIME, TIME_WITH_TIME_ZONE, TIMESTAMP, TIMESTAMP_WITH_TIME_ZONE);

    private final DecoderColumnHandle columnHandle;

    public SecondsSinceEpochJsonFieldDecoder(DecoderColumnHandle columnHandle)
    {
        this.columnHandle = requireNonNull(columnHandle, "columnHandle is null");
        if (!SUPPORTED_TYPES.contains(columnHandle.getType())) {
            throwUnsupportedColumnType(columnHandle);
        }
    }

    @Override
    public FieldValueProvider decode(JsonNode value)
    {
        return new SecondsSinceEpochJsonValueProvider(value, columnHandle);
    }

    public static class SecondsSinceEpochJsonValueProvider
            extends AbstractDateTimeJsonValueProvider
    {
        public SecondsSinceEpochJsonValueProvider(JsonNode value, DecoderColumnHandle columnHandle)
        {
            super(value, columnHandle);
        }

        @Override
        protected long getMillis()
        {
            try {
                if (value.isIntegralNumber()
                        && !value.isBigInteger()) {
                    return multiplyExact(value.longValue(), 1000);
                }
                if (value.isValueNode()) {
                    return multiplyExact(parseLong(value.asText()), 1000);
                }
                throw new PrestoException(
                        DECODER_CONVERSION_NOT_SUPPORTED,
                        format("could not parse non-value node as '%s' for column '%s'", columnHandle.getType(), columnHandle.getName()));
            }
            catch (NumberFormatException | ArithmeticException e) {
                throw new PrestoException(
                        DECODER_CONVERSION_NOT_SUPPORTED,
                        format("could not parse value '%s' as '%s' for column '%s'", value.asText(), columnHandle.getType(), columnHandle.getName()));
            }
        }
    }
}
