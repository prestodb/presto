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

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;
import io.airlift.slice.Slice;

import java.util.Set;

import com.facebook.presto.kinesis.KinesisColumnHandle;
import com.facebook.presto.kinesis.KinesisFieldValueProvider;
import com.facebook.presto.kinesis.decoder.KinesisFieldDecoder;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableSet;

public class JsonKinesisFieldDecoder
        implements KinesisFieldDecoder<JsonNode>
{
    @Override
    public Set<Class<?>> getJavaTypes()
    {
        return ImmutableSet.<Class<?>>of(boolean.class, long.class, double.class, Slice.class);
    }

    @Override
    public final String getRowDecoderName()
    {
        return JsonKinesisRowDecoder.NAME;
    }

    @Override
    public String getFieldDecoderName()
    {
        return KinesisFieldDecoder.DEFAULT_FIELD_DECODER_NAME;
    }

    @Override
    public KinesisFieldValueProvider decode(JsonNode value, KinesisColumnHandle columnHandle)
    {
        checkNotNull(columnHandle, "columnHandle is null");
        checkNotNull(value, "value is null");

        return new JsonKinesisValueProvider(value, columnHandle);
    }

    @Override
    public String toString()
    {
        return format("FieldDecoder[%s/%s]", getRowDecoderName(), getFieldDecoderName());
    }

    public static class JsonKinesisValueProvider
            extends KinesisFieldValueProvider
    {
        protected final JsonNode value;
        protected final KinesisColumnHandle columnHandle;

        public JsonKinesisValueProvider(JsonNode value, KinesisColumnHandle columnHandle)
        {
            this.value = value;
            this.columnHandle = columnHandle;
        }

        @Override
        public final boolean accept(KinesisColumnHandle columnHandle)
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
            return isNull() ? EMPTY_SLICE : utf8Slice(textValue);
        }
    }
}
