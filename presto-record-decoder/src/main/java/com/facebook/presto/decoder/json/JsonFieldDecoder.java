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
import com.facebook.presto.decoder.FieldDecoder;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.decoder.DecoderErrorCode.DECODER_CONVERSION_NOT_SUPPORTED;
import static com.facebook.presto.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.facebook.presto.spi.type.Varchars.truncateToLength;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Default field decoder for the JSON format. Supports json format coercions to implicitly convert e.g. string to long values.
 */
public class JsonFieldDecoder
        implements FieldDecoder<JsonNode>
{
    @Override
    public Set<Class<?>> getJavaTypes()
    {
        return ImmutableSet.<Class<?>>of(boolean.class, long.class, double.class, Slice.class);
    }

    @Override
    public final String getRowDecoderName()
    {
        return JsonRowDecoder.NAME;
    }

    @Override
    public String getFieldDecoderName()
    {
        return FieldDecoder.DEFAULT_FIELD_DECODER_NAME;
    }

    @Override
    public FieldValueProvider decode(JsonNode value, DecoderColumnHandle columnHandle)
    {
        requireNonNull(columnHandle, "columnHandle is null");
        requireNonNull(value, "value is null");

        return new JsonValueProvider(value, columnHandle);
    }

    @Override
    public String toString()
    {
        return format("FieldDecoder[%s/%s]", getRowDecoderName(), getFieldDecoderName());
    }

    public static class JsonValueProvider
            extends FieldValueProvider
    {
        protected final JsonNode value;
        protected final DecoderColumnHandle columnHandle;

        public JsonValueProvider(JsonNode value, DecoderColumnHandle columnHandle)
        {
            this.value = value;
            this.columnHandle = columnHandle;
        }

        @Override
        public final boolean accept(DecoderColumnHandle columnHandle)
        {
            return this.columnHandle.equals(columnHandle);
        }

        @Override
        public final boolean isNull()
        {
            return value.isMissingNode() || value.isNull();
        }

        @Override
        public boolean getBoolean()
        {
            return value.asBoolean();
        }

        @Override
        public long getLong()
        {
            return value.asLong();
        }

        @Override
        public double getDouble()
        {
            return value.asDouble();
        }

        @Override
        public Slice getSlice()
        {
            String textValue = value.isValueNode() ? value.asText() : value.toString();
            if (isNull()) {
                return EMPTY_SLICE;
            }
            Slice slice = utf8Slice(textValue);
            if (isVarcharType(columnHandle.getType())) {
                slice = truncateToLength(slice, columnHandle.getType());
            }
            return slice;
        }
    }

    public abstract static class DateTimeJsonValueProvider
            extends JsonValueProvider
    {
        protected DateTimeJsonValueProvider(JsonNode value, DecoderColumnHandle columnHandle)
        {
            super(value, columnHandle);
        }

        @Override
        public boolean getBoolean()
        {
            throw new PrestoException(DECODER_CONVERSION_NOT_SUPPORTED, "conversion to boolean not supported");
        }

        @Override
        public double getDouble()
        {
            throw new PrestoException(DECODER_CONVERSION_NOT_SUPPORTED, "conversion to double not supported");
        }

        @Override
        public final long getLong()
        {
            long millis = getMillis();

            Type type = columnHandle.getType();
            if (type.equals(DATE)) {
                return TimeUnit.MILLISECONDS.toDays(millis);
            }
            if (type.equals(TIMESTAMP) || type.equals(TIME)) {
                return millis;
            }
            if (type.equals(TIMESTAMP_WITH_TIME_ZONE) || type.equals(TIME_WITH_TIME_ZONE)) {
                return packDateTimeWithZone(millis, 0);
            }

            return millis;
        }

        /**
         * @return epoch milliseconds in UTC
         */
        protected abstract long getMillis();
    }
}
