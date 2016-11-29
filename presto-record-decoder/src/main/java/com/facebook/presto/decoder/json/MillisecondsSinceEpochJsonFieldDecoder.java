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
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Locale;
import java.util.Set;

import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;

/**
 * Milliseconds since the epoch date format decoder.
 * <p>
 * Uses hardcoded UTC timezone and english locale.
 */
public class MillisecondsSinceEpochJsonFieldDecoder
        extends JsonFieldDecoder
{
    @VisibleForTesting
    static final String NAME = "milliseconds-since-epoch";

    /**
     * Todo - configurable time zones and locales.
     */
    @VisibleForTesting
    static final DateTimeFormatter FORMATTER = ISODateTimeFormat.dateTime().withLocale(Locale.ENGLISH).withZoneUTC();

    @Override
    public Set<Class<?>> getJavaTypes()
    {
        return ImmutableSet.of(long.class, Slice.class);
    }

    @Override
    public String getFieldDecoderName()
    {
        return NAME;
    }

    @Override
    public FieldValueProvider decode(JsonNode value, DecoderColumnHandle columnHandle)
    {
        requireNonNull(columnHandle, "columnHandle is null");
        requireNonNull(value, "value is null");

        return new MillisecondsSinceEpochJsonValueProvider(value, columnHandle);
    }

    public static class MillisecondsSinceEpochJsonValueProvider
            extends DateTimeJsonValueProvider
    {
        public MillisecondsSinceEpochJsonValueProvider(JsonNode value, DecoderColumnHandle columnHandle)
        {
            super(value, columnHandle);
        }

        @Override
        protected long getMillis()
        {
            return isNull() ? 0L : value.asLong();
        }

        @Override
        public Slice getSlice()
        {
            return isNull() ? EMPTY_SLICE : utf8Slice(FORMATTER.print(value.asLong()));
        }
    }
}
