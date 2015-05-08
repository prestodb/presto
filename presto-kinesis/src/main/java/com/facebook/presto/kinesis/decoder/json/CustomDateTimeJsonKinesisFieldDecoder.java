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
package com.facebook.presto.kinesis.decoder.json;

import java.util.Locale;
import java.util.Set;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import io.airlift.slice.Slice;

import com.facebook.presto.kinesis.KinesisColumnHandle;
import com.facebook.presto.kinesis.KinesisFieldValueProvider;
import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableSet;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.facebook.presto.kinesis.KinesisErrorCode.KINESIS_CONVERSION_NOT_SUPPORTED;

public class CustomDateTimeJsonKinesisFieldDecoder
        extends JsonKinesisFieldDecoder
{
    @Override
    public Set<Class<?>> getJavaTypes()
    {
        return ImmutableSet.<Class<?>>of(long.class, Slice.class);
    }

    @Override
    public String getFieldDecoderName()
    {
        return "custom-date-time";
    }

    @Override
    public KinesisFieldValueProvider decode(JsonNode value, KinesisColumnHandle columnHandle)
    {
        checkNotNull(columnHandle, "columnHandle is null");
        checkNotNull(value, "value is null");

        return new CustomeDateTimeJsonKinesisValueProvider(value, columnHandle);
    }

    public static class CustomeDateTimeJsonKinesisValueProvider
            extends JsonKinesisValueProvider
    {
        public CustomeDateTimeJsonKinesisValueProvider(JsonNode value, KinesisColumnHandle columnHandle)
        {
            super(value, columnHandle);
        }

        @Override
        public boolean getBoolean()
        {
            throw new PrestoException(KINESIS_CONVERSION_NOT_SUPPORTED, "conversion to boolean not supported");
        }

        @Override
        public double getDouble()
        {
            throw new PrestoException(KINESIS_CONVERSION_NOT_SUPPORTED, "conversion to double not supported");
        }

        @Override
        public long getLong()
        {
            if (isNull()) {
                return 0L;
            }

            if (value.canConvertToLong()) {
                return value.asLong();
            }

            checkNotNull(columnHandle.getFormatHint(), "formatHint is null");
            String textValue = value.isValueNode() ? value.asText() : value.toString();

            DateTimeFormatter formatter = DateTimeFormat.forPattern(columnHandle.getFormatHint()).withLocale(Locale.ENGLISH).withZoneUTC();
            return formatter.parseMillis(textValue);
        }
    }
}
