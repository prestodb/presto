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

import static java.util.Objects.requireNonNull;

/**
 * Milliseconds since the epoch date format decoder.
 * <p>
 * Uses hardcoded UTC timezone and english locale.
 */
public class MillisecondsSinceEpochJsonFieldDecoder
        extends JsonFieldDecoder
{
    @Override
    public FieldValueProvider decode(JsonNode value, DecoderColumnHandle columnHandle)
    {
        requireNonNull(columnHandle, "columnHandle is null");
        requireNonNull(value, "value is null");

        return new MillisecondsSinceEpochJsonValueProvider(value, columnHandle);
    }

    public static class MillisecondsSinceEpochJsonValueProvider
            extends AbstractDateTimeJsonValueProvider
    {
        public MillisecondsSinceEpochJsonValueProvider(JsonNode value, DecoderColumnHandle columnHandle)
        {
            super(value, columnHandle);
        }

        @Override
        protected long getMillis()
        {
            return value.asLong();
        }
    }
}
